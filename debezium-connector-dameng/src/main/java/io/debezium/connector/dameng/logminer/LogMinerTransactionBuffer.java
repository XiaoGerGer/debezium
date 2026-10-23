/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng.logminer;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Retains provisional transactions across mining windows. A boundary for an earlier
 * transaction requires reconstruction: DM can change old rows' rollback flags when
 * the complete range is mined. Files are disposable, never restart state.
 */
public final class LogMinerTransactionBuffer implements AutoCloseable {
    private static final Logger LOGGER = LoggerFactory.getLogger(LogMinerTransactionBuffer.class);
    private final Map<String, Transaction> transactions = new LinkedHashMap<>();
    private final long memoryLimit;
    private final long maxSqlBytes;
    private final long maxTransactions;
    private final LogMinerSpillStore store;
    private final long recoveryBaselineLsn;
    private long memoryBytes;
    private long bufferedRecords;
    private long spilledTransactions;
    private long windowBytes;
    private long windowRecords;
    private IllegalStateException capacityFailure;
    private boolean initialWindow = true;
    private boolean reconstructionRequired;
    private LogMinerRow fragment;
    private int fragmentSequence;
    private StringBuilder sql;
    private RuntimeException inputFailure;

    public LogMinerTransactionBuffer(long memoryLimit, long maxSqlBytes, long maxTransactions, Path spillPath, long spillLimit) {
        this(memoryLimit, maxSqlBytes, maxTransactions, spillPath, spillLimit, 0);
    }

    public LogMinerTransactionBuffer(long memoryLimit, long maxSqlBytes, long maxTransactions, Path spillPath, long spillLimit,
                                     long recoveryBaselineLsn) {
        if (memoryLimit <= 0 || maxSqlBytes <= 0 || maxTransactions <= 0 || spillLimit <= 0) {
            throw new IllegalArgumentException("Dameng transaction buffer limits must be positive");
        }
        this.memoryLimit = memoryLimit;
        this.maxSqlBytes = maxSqlBytes;
        this.maxTransactions = maxTransactions;
        this.recoveryBaselineLsn = recoveryBaselineLsn;
        store = new LogMinerSpillStore(spillPath, spillLimit);
    }

    public void beginWindow() {
        finishWindow();
        windowBytes = 0;
        windowRecords = 0;
    }

    public void add(LogMinerRow row) {
        checkCapacityFailure();
        try {
            addRow(row);
        }
        catch (RuntimeException e) {
            inputFailure = e;
            throw e;
        }
    }

    private void addRow(LogMinerRow row) {
        if (row.continuation() < 0 || row.continuation() > 1 || row.rollback() < 0 || row.rollback() > 1) {
            throw new IllegalStateException("Invalid Dameng CSF=" + row.continuation() + " or ROLL_BACK=" + row.rollback() + " at " + row.position());
        }
        // Tested DM builds number pieces 0, 1, ...; a gap, duplicate or orphan
        // suffix must never be interpreted as a complete SQL statement.
        final long expectedSequence = fragment == null ? 0 : (long) fragmentSequence + 1;
        if (row.sequence() != expectedSequence) {
            throw new IllegalStateException("Invalid Dameng SQL continuation SSN, expected " + expectedSequence + " at " + row.position());
        }
        if ((fragment != null || row.continuation() == 1) && (row.redo() == null || row.redo().isEmpty()
                || row.recordSetId() == null || row.recordSetId().isBlank())) {
            throw new IllegalStateException("Missing Dameng SQL continuation SQL_REDO or RS_ID at " + row.position());
        }
        final long bytes = estimatedBytes(row) + (row.continuation() != 0 || fragment != null
                ? 128L + (row.redo() == null ? 0 : 6L * row.redo().length())
                : 0);
        // Cumulative input is telemetry, not resident memory. Rows can spill or be
        // discarded while an arbitrarily long mining session keeps making progress.
        windowBytes += bytes;
        windowRecords++;
        if (fragment != null) {
            if (fragment.lsn() != row.lsn() || fragment.operation() != row.operation()
                    || !Objects.equals(fragment.transactionId(), row.transactionId()) || !Objects.equals(fragment.recordSetId(), row.recordSetId())
                    || fragment.rollback() != row.rollback()
                    || (row.objectId() > 0 && fragment.objectId() != row.objectId())
                    || (row.objectVersion() >= 0 && fragment.objectVersion() >= 0 && fragment.objectVersion() != row.objectVersion())) {
                throw new IllegalStateException("Interleaved or missing Dameng SQL continuation at " + row.position());
            }
            checkSqlSize(row, sql.length());
            sql.append(row.redo());
            fragmentSequence = row.sequence();
            if (row.continuation() == 0) {
                accept(fragment.withRedo(sql.toString()));
                fragment = null;
                sql = null;
            }
        }
        else if (row.continuation() != 0) {
            checkSqlSize(row, 0);
            fragment = row;
            fragmentSequence = row.sequence();
            sql = new StringBuilder(row.redo());
        }
        else {
            checkSqlSize(row, 0);
            accept(row);
        }
    }

    private void checkSqlSize(LogMinerRow row, int assembledCharacters) {
        final long characters = (long) assembledCharacters + (row.redo() == null ? 0 : row.redo().length());
        // Check before builder growth and the final String copy. The limit counts
        // UTF-16 payload bytes; JDBC input and temporary copies need additional heap.
        if (characters > maxSqlBytes / 2 || characters > Integer.MAX_VALUE - 8L) {
            throw capacityFailure("Dameng SQL at LSN " + row.lsn() + " exceeds log.mining.buffer.max.sql.bytes=" + maxSqlBytes);
        }
    }

    private IllegalStateException capacityFailure(String message) {
        capacityFailure = new IllegalStateException(message + ". No partial window can be published or its replay LSN advanced; retain archives for restart");
        return capacityFailure;
    }

    private void checkCapacityFailure() {
        if (inputFailure != null) {
            throw inputFailure;
        }
        if (capacityFailure != null) {
            throw capacityFailure;
        }
    }

    private void accept(LogMinerRow row) {
        if (row.operation() == 37) {
            return;
        }
        if (row.transactionId() == null) {
            throw new IllegalStateException("Missing Dameng transaction id at LSN " + row.lsn());
        }
        Transaction tx = transactions.get(row.transactionId());
        if (tx == null) {
            if (recoveryBaselineLsn > 0 && row.startLsn() == 0 && (row.operation() == 7 || row.operation() == 38 ||
                    (row.operation() == 36 && row.rollback() == 1 && "-1".equals(row.recordSetId())))) {
                // A terminal marker without any post-cut row has no captured
                // data to publish. Writers of captured tables were drained by
                // the recovery locks before the baseline was selected.
                return;
            }
            // A mid-transaction row without a remembered start cannot be safely committed.
            if (!initialWindow && recoveryBaselineLsn == 0 && row.startLsn() == 0 && row.operation() != 6) {
                throw new IllegalStateException("Missing cached Dameng transaction " + row.transactionId() + " at LSN " + row.lsn()
                        + "; retain archives and restart from a complete transaction boundary");
            }
            if (transactions.size() >= maxTransactions) {
                throw capacityFailure("Dameng resident transaction count exceeds log.mining.buffer.max.transactions=" + maxTransactions);
            }
            final long metadata = 256L + 2L * row.transactionId().length();
            makeRoom(metadata);
            if (metadata > memoryLimit - memoryBytes) {
                throw capacityFailure("Dameng pending transaction metadata exceeds log.mining.buffer.memory.bytes=" + memoryLimit);
            }
            tx = new Transaction(row, metadata);
            // Recovery schema locks drained every pre-cut writer of captured tables.
            // Thus an older transaction can contribute only post-cut captured rows,
            // even when LogMiner omits its pre-cut START. Archive continuity is still
            // validated on every read. V$TRX cannot be used as an allow-list here:
            // it races with commits between choosing the LSN and reading metadata.
            tx.recoverySuffix = recoveryBaselineLsn > 0 && row.operation() != 6 && row.startLsn() < recoveryBaselineLsn;
            transactions.put(tx.id, tx);
            memoryBytes += metadata;
        }
        if (tx.carried && (row.operation() == 7 || row.operation() == 38 || row.operation() == 36 || row.rollback() != 0)) {
            reconstructionRequired = true;
        }
        tx.startLsn = Math.min(tx.startLsn, row.startLsn() > 0 ? row.startLsn() : row.lsn());
        tx.startLsn = Math.max(tx.startLsn, recoveryBaselineLsn);
        switch (row.operation()) {
            case 7, 38 -> tx.commitLsn = row.lsn();
            case 36 -> {
                // Tested DM versions use RS_ID=-1 for the terminal full rollback.
                // START itself can be marked rolled back by ROLLBACK TO an early
                // savepoint, so neither START nor operation 36 alone ends a tx.
                // Unknown marker forms remain pending rather than losing history.
                tx.fullRollbackMarker = row.rollback() == 1 && "-1".equals(row.recordSetId());
                if (tx.recoverySuffix && tx.fullRollbackMarker) {
                    tx.startRolledBack = true;
                }
            }
            case 6 -> {
                // Use only together with the terminal full rollback marker and
                // an empty final image, never as a standalone completion signal.
                tx.startRolledBack = row.rollback() != 0;
            }
            case 15 -> {
                // PRE_COMMIT alone does not complete a transaction.
            }
            default -> {
                if (row.rollback() == 0) {
                    final long bytes = estimatedBytes(row);
                    makeRoom(bytes);
                    if (tx.file == null && bytes > memoryLimit - memoryBytes) {
                        spill(tx);
                    }
                    if (tx.file == null) {
                        tx.rows.add(row);
                        tx.memoryBytes += bytes;
                        memoryBytes += bytes;
                    }
                    else {
                        append(tx, row);
                    }
                    tx.recordCount++;
                    bufferedRecords++;
                }
            }
        }
    }

    public boolean requiresReconstruction() {
        return reconstructionRequired;
    }

    /** Discard provisional images before rebuilding the entire safe replay range. */
    public void resetForReconstruction() {
        finishWindow();
        for (var tx : List.copyOf(transactions.values())) {
            release(tx);
        }
        initialWindow = true;
        reconstructionRequired = false;
    }

    private void requireValidatedHistory() {
        finishWindow();
        if (reconstructionRequired) {
            throw new IllegalStateException("Dameng cross-window transaction history must be reconstructed before delivery or replay advancement");
        }
    }

    private void makeRoom(long bytes) {
        while (bytes > memoryLimit - memoryBytes) {
            final var largest = transactions.values().stream().filter(tx -> !tx.rows.isEmpty())
                    .max(Comparator.comparingLong(tx -> tx.memoryBytes)).orElse(null);
            if (largest == null) {
                break;
            }
            spill(largest);
        }
    }

    private void spill(Transaction tx) {
        tx.file = store.create();
        for (var row : tx.rows) {
            append(tx, row);
        }
        memoryBytes -= tx.memoryBytes;
        tx.memoryBytes = 0;
        tx.rows.clear();
        // Release the ArrayList backing storage as well as its row references.
        tx.rows = new ArrayList<>();
        spilledTransactions++;
        LOGGER.info("Dameng transaction {} at start LSN {} spilled to {} (memory threshold={} bytes, buffered records={})",
                tx.id, tx.startLsn, tx.file, memoryLimit, tx.recordCount);
    }

    private void append(Transaction tx, LogMinerRow row) {
        final long bytes = store.append(tx.file, row);
        tx.diskBytes += bytes;
        tx.maxFrameBytes = Math.max(tx.maxFrameBytes, bytes);
    }

    public void finishWindow() {
        checkCapacityFailure();
        if (fragment != null) {
            throw new IllegalStateException("Incomplete Dameng SQL continuation at LSN " + fragment.lsn());
        }
        store.flush();
    }

    public List<Transaction> committedTransactions() {
        requireValidatedHistory();
        return transactions.values().stream().filter(tx -> tx.commitLsn > 0)
                .sorted(Comparator.comparingLong(Transaction::commitLsn).thenComparing(Transaction::id)).toList();
    }

    public void release(Transaction tx) {
        if (tx.file != null) {
            store.delete(tx.file, tx.diskBytes);
        }
        memoryBytes -= tx.memoryBytes + tx.metadataBytes;
        bufferedRecords -= tx.recordCount;
        transactions.remove(tx.id);
    }

    public long completeWindow(long end) {
        requireValidatedHistory();
        for (var tx : transactions.values().stream().filter(Transaction::isFullyRolledBack).toList()) {
            release(tx);
        }
        transactions.values().forEach(tx -> tx.carried = true);
        initialWindow = false;
        return transactions.values().stream().mapToLong(tx -> tx.startLsn).min().orElse(end);
    }

    public long memoryBytes() {
        return memoryBytes;
    }

    public long diskBytes() {
        return store.bytes();
    }

    public long bufferedRecords() {
        return bufferedRecords;
    }

    public long spilledTransactions() {
        return spilledTransactions;
    }

    public long openTransactions() {
        return transactions.values().stream().filter(tx -> tx.commitLsn == 0 && !tx.isFullyRolledBack()).count();
    }

    public long windowBytes() {
        return windowBytes;
    }

    public long windowRecords() {
        return windowRecords;
    }

    public boolean isCapacityExceeded() {
        return capacityFailure != null;
    }

    public static boolean isStorageFailure(Throwable failure) {
        return failure instanceof LogMinerSpillStore.StorageException;
    }

    private static long estimatedBytes(LogMinerRow row) {
        return 384L + stringBytes(row.transactionId()) + stringBytes(row.owner()) + stringBytes(row.tableName()) + stringBytes(row.username())
                + stringBytes(row.redo()) + stringBytes(row.recordSetId());
    }

    private static long stringBytes(String value) {
        return value == null ? 0 : 40L + 2L * value.length();
    }

    @Override
    public void close() {
        try {
            store.close();
        }
        finally {
            transactions.clear();
            memoryBytes = 0;
            bufferedRecords = 0;
            fragment = null;
            sql = null;
        }
    }

    public final class Transaction {
        private final String id;
        private final long metadataBytes;
        private long startLsn;
        private long commitLsn;
        private boolean recoverySuffix;
        private boolean fullRollbackMarker;
        private boolean startRolledBack;
        private boolean carried;
        private List<LogMinerRow> rows = new ArrayList<>();
        private Path file;
        private long memoryBytes;
        private long diskBytes;
        private long recordCount;
        private long maxFrameBytes;

        private Transaction(LogMinerRow row, long metadataBytes) {
            id = row.transactionId();
            startLsn = row.startLsn() > 0 ? row.startLsn() : row.lsn();
            this.metadataBytes = metadataBytes;
        }

        public String id() {
            return id;
        }

        private boolean isFullyRolledBack() {
            return commitLsn == 0 && startRolledBack && fullRollbackMarker && recordCount == 0;
        }

        public long commitLsn() {
            return commitLsn;
        }

        public Cursor open() {
            return new Cursor(this);
        }

        private LogMinerSpillStore.Reader openFile() {
            return store.read(file, diskBytes, recordCount, maxFrameBytes);
        }
    }

    /** One row at a time even for a transaction much larger than the heap. */
    public static final class Cursor implements AutoCloseable {
        private final Transaction tx;
        private final LogMinerSpillStore.Reader reader;
        private int index;

        private Cursor(Transaction tx) {
            this.tx = tx;
            reader = tx.file == null ? null : tx.openFile();
        }

        public LogMinerRow next() {
            return reader == null ? (index < tx.rows.size() ? tx.rows.get(index++) : null) : reader.next();
        }

        @Override
        public void close() {
            if (reader != null) {
                reader.close();
            }
        }
    }
}
