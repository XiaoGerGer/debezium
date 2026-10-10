/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng.logminer;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * One complete, inclusive mining window. Transactions are reconstructed on each scan,
 * since DM can retroactively mark a previously returned row as rolled back to a savepoint.
 * Unfinished transactions pin the next scan to their first LSN; they are never auto-committed.
 */
public final class LogMinerBatch {
    public static final long DEFAULT_MAX_BUFFER_BYTES = 512L * 1024 * 1024;
    public static final long DEFAULT_MAX_BUFFER_RECORDS = 1_000_000L;
    // Allowance for the row, timestamp, list slot and a possible transaction/map entry.
    private static final long ROW_OVERHEAD_BYTES = 384;
    private static final long STRING_OVERHEAD_BYTES = 40;

    private final Map<String, Transaction> transactions = new LinkedHashMap<>();
    private final long maxBufferBytes;
    private final long maxBufferRecords;
    private long estimatedBufferBytes;
    private long bufferedRecordCount;
    private IllegalStateException capacityFailure;
    private LogMinerRow fragment;
    private StringBuilder sql;

    public LogMinerBatch() {
        this(DEFAULT_MAX_BUFFER_BYTES, DEFAULT_MAX_BUFFER_RECORDS);
    }

    public LogMinerBatch(long maxBufferBytes, long maxBufferRecords) {
        if (maxBufferBytes <= 0 || maxBufferRecords <= 0) {
            throw new IllegalArgumentException("Dameng LogMiner buffer byte and record limits must both be positive");
        }
        this.maxBufferBytes = maxBufferBytes;
        this.maxBufferRecords = maxBufferRecords;
    }

    public record LogMinerRow(long lsn, long startLsn, long commitLsn, String transactionId, int operation,
            int rollback, String owner, String tableName, String username, String redo,
            String recordSetId, int sequence, int continuation, Instant timestamp, long objectId) {
        public LogMinerRow(long lsn, long startLsn, long commitLsn, String transactionId, int operation,
                           int rollback, String owner, String tableName, String username, String redo,
                           String recordSetId, int sequence, int continuation, Instant timestamp) {
            this(lsn, startLsn, commitLsn, transactionId, operation, rollback, owner, tableName, username, redo,
                    recordSetId, sequence, continuation, timestamp, 0);
        }
    }

    public static final class Transaction {
        private final String id;
        private long startLsn;
        private long commitLsn;
        private boolean rolledBack;
        private final List<LogMinerRow> records = new ArrayList<>();
        private final List<LogMinerRow> recordsView = Collections.unmodifiableList(records);

        private Transaction(LogMinerRow row) {
            id = row.transactionId();
            startLsn = row.startLsn() > 0 ? row.startLsn() : row.lsn();
        }

        public String id() {
            return id;
        }

        public long startLsn() {
            return startLsn;
        }

        public long commitLsn() {
            return commitLsn;
        }

        public List<LogMinerRow> records() {
            return recordsView;
        }
    }

    public void add(LogMinerRow row) {
        checkCapacityFailure();
        if (fragment != null) {
            if (fragment.lsn() != row.lsn() || fragment.operation() != row.operation()
                    || !Objects.equals(fragment.transactionId(), row.transactionId())
                    || !Objects.equals(fragment.recordSetId(), row.recordSetId())) {
                throw new IllegalStateException("Interleaved or missing Dameng SQL continuation at LSN " + fragment.lsn());
            }
            reserve(row, true);
            sql.append(row.redo() == null ? "" : row.redo());
            if (row.continuation() == 0) {
                accept(new LogMinerRow(fragment.lsn(), fragment.startLsn(), fragment.commitLsn(), fragment.transactionId(),
                        fragment.operation(), fragment.rollback(), fragment.owner(), fragment.tableName(), fragment.username(),
                        sql.toString(), fragment.recordSetId(), fragment.sequence(), 0, fragment.timestamp(), fragment.objectId()));
                fragment = null;
                sql = null;
            }
        }
        else if (row.continuation() != 0) {
            reserve(row, true);
            fragment = row;
            sql = new StringBuilder(row.redo() == null ? "" : row.redo());
        }
        else {
            reserve(row, false);
            accept(row);
        }
    }

    /**
     * Charge the complete window, including markers, rolled-back rows and CSF
     * pieces. Not refunding discarded rows deliberately bounds scanning work as
     * well as retained data. The byte count is a conservative estimate, not a JVM
     * heap measurement; JDBC can allocate a row before it reaches this method.
     */
    private void reserve(LogMinerRow row, boolean continuation) {
        long bytes = ROW_OVERHEAD_BYTES + stringBytes(row.transactionId()) + stringBytes(row.owner())
                + stringBytes(row.tableName()) + stringBytes(row.username()) + stringBytes(row.redo())
                + stringBytes(row.recordSetId());
        if (continuation) {
            // Budget UTF-16 builder growth/copying and the final concatenated
            // string before allocating them, in addition to the incoming text.
            bytes += 128L + (row.redo() == null ? 0 : 6L * row.redo().length());
        }
        if (bufferedRecordCount >= maxBufferRecords || bytes > maxBufferBytes - estimatedBufferBytes) {
            capacityFailure = new IllegalStateException("Dameng LogMiner window buffer limit exceeded at LSN " + row.lsn()
                    + " (reserved records=" + bufferedRecordCount + ", estimated bytes=" + estimatedBufferBytes
                    + ", next estimated bytes=" + bytes + ", log.mining.buffer.max.records=" + maxBufferRecords
                    + ", log.mining.buffer.max.bytes=" + maxBufferBytes + "). No partial window can be published or its replay LSN advanced. "
                    + "Retain the required archive logs; reduce transaction size or the mining window, or increase these limits only after sizing the JVM heap.");
            throw capacityFailure;
        }
        bufferedRecordCount++;
        estimatedBufferBytes += bytes;
    }

    private static long stringBytes(String value) {
        return value == null ? 0 : STRING_OVERHEAD_BYTES + 2L * value.length();
    }

    private void checkCapacityFailure() {
        if (capacityFailure != null) {
            throw capacityFailure;
        }
    }

    /** Cumulative estimated reservation for the complete mining window. */
    public long getEstimatedBufferBytes() {
        return estimatedBufferBytes;
    }

    /** Input rows, including transaction markers and SQL continuation pieces. */
    public long getBufferedRecordCount() {
        return bufferedRecordCount;
    }

    /** Sample after scanning; computing this value walks all transactions in the window. */
    public long getOpenTransactionCount() {
        return transactions.values().stream().filter(tx -> tx.commitLsn == 0 && (!tx.rolledBack || !tx.records.isEmpty())).count();
    }

    public boolean isCapacityExceeded() {
        return capacityFailure != null;
    }

    private void accept(LogMinerRow row) {
        // Sequence allocation is independent of a user transaction, and must not
        // discard DML that happens to carry the same XID.
        if (row.operation() == 37) {
            return;
        }
        if (row.transactionId() == null) {
            throw new IllegalStateException("Missing Dameng transaction id at LSN " + row.lsn());
        }
        final Transaction tx = transactions.computeIfAbsent(row.transactionId(), ignored -> new Transaction(row));
        tx.startLsn = Math.min(tx.startLsn, row.startLsn() > 0 ? row.startLsn() : row.lsn());
        switch (row.operation()) {
            case 7, 38 -> tx.commitLsn = row.lsn();
            case 36 -> tx.rolledBack = true;
            case 6, 15 -> {
                // START and PRE_COMMIT do not make changes durable.
            }
            default -> {
                if (row.rollback() == 0) {
                    tx.records.add(row);
                }
            }
        }
    }

    public void finish() {
        checkCapacityFailure();
        if (fragment != null) {
            throw new IllegalStateException("Incomplete Dameng SQL continuation at LSN " + fragment.lsn());
        }
    }

    public List<Transaction> committedTransactions() {
        finish();
        return transactions.values().stream().filter(tx -> tx.commitLsn > 0)
                .sorted(Comparator.comparingLong(Transaction::commitLsn).thenComparing(Transaction::id)).toList();
    }

    public long restartLsn(long endLsn) {
        finish();
        return transactions.values().stream().filter(tx -> tx.commitLsn == 0 && (!tx.rolledBack || !tx.records.isEmpty()))
                .mapToLong(Transaction::startLsn).min().orElse(endLsn);
    }
}
