/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng;

import java.nio.file.Path;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.annotation.VisibleForTesting;
import io.debezium.connector.dameng.antlr.DamengDdlParser;
import io.debezium.connector.dameng.antlr.DamengDdlStatementAnalyzer.Analysis;
import io.debezium.connector.dameng.antlr.DamengDdlStatementAnalyzer.CascadeDropException;
import io.debezium.connector.dameng.antlr.DamengDdlStatementAnalyzer.Impact;
import io.debezium.connector.dameng.antlr.DamengDdlStatementAnalyzer.PartitionDataRemovalException;
import io.debezium.connector.dameng.antlr.DamengDdlStatementAnalyzer.PartitionExchangeException;
import io.debezium.connector.dameng.logminer.LogMinerArchiveWindow;
import io.debezium.connector.dameng.logminer.LogMinerRow;
import io.debezium.connector.dameng.logminer.LogMinerTransactionBuffer;
import io.debezium.connector.dameng.logminer.LogMinerTransactionBuffer.DispatchProgress;
import io.debezium.connector.dameng.logminer.LogMinerTransactionBuffer.Transaction;
import io.debezium.connector.dameng.logminer.events.EventType;
import io.debezium.connector.dameng.logminer.parser.LogMinerDmlParser;
import io.debezium.connector.dameng.logminer.parser.LogMinerDmlParser.DmlEntry;
import io.debezium.data.Envelope.Operation;
import io.debezium.jdbc.MainConnectionProvidingConnectionFactory;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.ChangeEventSource.ChangeEventSourceContext;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.relational.Attribute;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.schema.SchemaChangeEvent;
import io.debezium.schema.SchemaChangeEvent.SchemaChangeEventType;
import io.debezium.util.Clock;

/**
 * Reads DM archive logs in commit order. The persisted LSN is a replay position,
 * separate from the commit LSN and statement ordinal of the last delivered event.
 * Discovery advances through new ranges. A boundary for an earlier transaction triggers
 * reconstruction before delivery so that rollback flags are definitive and the replay
 * position cannot advance past an unfinished transaction.
 */
public final class DamengStreamingChangeEventSource implements StreamingChangeEventSource<DamengPartition, DamengOffsetContext> {
    private static final Logger LOGGER = LoggerFactory.getLogger(DamengStreamingChangeEventSource.class);

    private final DamengConnectorConfig config;
    private final DamengConnection connection;
    private final EventDispatcher<DamengPartition, TableId> dispatcher;
    private final ErrorHandler errorHandler;
    private final Clock clock;
    private final DamengDatabaseSchema schema;
    private final DamengStreamingChangeEventSourceMetrics metrics;
    private final LogMinerDmlParser parser = new LogMinerDmlParser();
    private final Map<TableId, CachedPhysicalTable> physicalTables = new HashMap<>();
    private final Map<Long, TableId> objectTables = new HashMap<>();
    private long objectTablesVersion = Long.MIN_VALUE;
    private DamengDdlParser ddlParser = new DamengDdlParser();
    private DamengOffsetContext offset;
    private DamengCaptureScopeChange captureScopeChange;

    private record CachedPhysicalTable(Table historical, long objectId, Table physical) {
    }

    public DamengStreamingChangeEventSource(DamengConnectorConfig config,
                                            MainConnectionProvidingConnectionFactory<DamengConnection> connectionFactory,
                                            EventDispatcher<DamengPartition, TableId> dispatcher, ErrorHandler errorHandler,
                                            Clock clock, DamengDatabaseSchema schema, DamengStreamingChangeEventSourceMetrics metrics) {
        this.config = config;
        this.connection = connectionFactory.mainConnection();
        this.dispatcher = dispatcher;
        this.errorHandler = errorHandler;
        this.clock = clock;
        this.schema = schema;
        this.metrics = Objects.requireNonNull(metrics);
    }

    @Override
    public void init(DamengOffsetContext offsetContext) {
        this.offset = offsetContext;
    }

    @Override
    public DamengOffsetContext getOffsetContext() {
        return offset;
    }

    void setCaptureScopeChange(DamengCaptureScopeChange captureScopeChange) {
        this.captureScopeChange = captureScopeChange;
    }

    @Override
    public void execute(ChangeEventSourceContext context, DamengPartition partition, DamengOffsetContext offsetContext)
            throws InterruptedException {
        this.offset = offsetContext;
        physicalTables.clear();
        objectTables.clear();
        objectTablesVersion = Long.MIN_VALUE;
        metrics.onRecoveryBaseline(offset);
        try (var buffer = new LogMinerTransactionBuffer(config.getLogMiningBufferMemoryBytes(), config.getLogMiningBufferMaxSqlBytes(),
                config.getLogMiningBufferMaxTransactions(), Path.of(config.getLogMiningBufferSpillPath()), config.getLogMiningBufferSpillMaxBytes(),
                offset.getRecoveryBaselineLsn())) {
            connection.validateLogMiningConfiguration();
            connection.validateCompatibilityEnvironment(offset);
            ddlParser = connection.createDdlParser();
            if (config.requiresDatabaseLineage()) {
                connection.validatePrimaryStandbyIdentity(offset);
            }
            else {
                offset.validateDatabaseMagic(connection.databaseMagic());
            }
            long scannedThrough = offset.getLsn();
            boolean firstWindow = true;
            final boolean archiveWindows = config.getMiningWindowStrategy() == DamengConnectorConfig.MiningWindowStrategy.ARCHIVE_BYTES;
            metrics.onMiningWindowConfigured(archiveWindows ? 0 : config.getLogMiningBatchSize(), archiveWindows ? config.getLogMiningWindowMaxBytes() : 0);
            LOGGER.info("Dameng forward mining window strategy={}, archiveByteBudget={}, configuredLsnSpan={}",
                    config.getMiningWindowStrategy().getValue(), config.getLogMiningWindowMaxBytes(), config.getLogMiningBatchSize());
            metrics.onWindowStarted(offset.getLsn(), scannedThrough, 0);
            while (context.isRunning()) {
                connection.validateUnchangedCompatibility();
                if (config.requiresDatabaseLineage()) {
                    // Cached transactions may include an unacknowledged old-primary
                    // tail. A changed source generation requires a fresh buffer.
                    connection.validateUnchangedPrimary();
                    connection.validatePrimaryStandbyIdentity(offset);
                }
                else {
                    connection.validateServerRole();
                }
                final List<DamengConnection.ArchiveLogFile> archives = archiveWindows ? connection.readArchiveLogFiles() : List.of();
                final long archived = archiveWindows ? archives.stream().mapToLong(DamengConnection.ArchiveLogFile::lastLsn).max().orElse(0L)
                        : connection.latestArchivedLsn();
                metrics.setLatestArchivedLsn(archived);
                final long available;
                if (config.isStandbyMiningEnabled()) {
                    final var progress = connection.readStandbyProgress();
                    available = Math.min(archived, progress.safeLsn());
                    metrics.onStandbyProgress(progress, available);
                }
                else {
                    available = archived;
                }
                if (available <= scannedThrough) {
                    dispatcher.dispatchHeartbeatEvent(partition, offset);
                    Thread.sleep(Math.max(100L, config.getPollInterval().toMillis()));
                    continue;
                }
                final long replayLsn = offset.getLsn();
                final long start = firstWindow ? replayLsn : scannedThrough + 1;
                final long end;
                if (archiveWindows) {
                    final var selected = LogMinerArchiveWindow.select(archives, start, scannedThrough, available, config.getLogMiningWindowMaxBytes());
                    end = selected.endLsn();
                    metrics.onArchiveWindowSelected(selected, end - scannedThrough, config.getLogMiningWindowMaxBytes());
                    LOGGER.debug("Dameng archive window start={}, end={}, files={}, fileBytes={}, budgetBytes={}",
                            start, end, selected.fileCount(), selected.fileBytes(), config.getLogMiningWindowMaxBytes());
                }
                else {
                    end = scannedThrough + Math.min(available - scannedThrough, config.getLogMiningBatchSize());
                }
                final long actualSpan = end - scannedThrough;
                metrics.onWindowStarted(replayLsn, scannedThrough, archived);
                final long windowStarted = System.nanoTime();
                if (replayLsn < start) {
                    // Temporary transaction files do not replace the archives needed on restart.
                    connection.archiveLogsForRange(replayLsn, end);
                }
                readBatch(context, start, end, buffer, false);
                final long forwardNanos = System.nanoTime() - windowStarted;
                if (!context.isRunning()) {
                    return;
                }
                final boolean reconstructed = buffer.requiresReconstruction();
                long reconstructionNanos = 0;
                if (reconstructed) {
                    // The old cache is provisional, even when it has spilled.
                    // Replace it with the final rollback view before any dispatch.
                    final long reconstructionStarted = System.nanoTime();
                    buffer.resetForReconstruction();
                    readBatch(context, replayLsn, end, buffer, true);
                    reconstructionNanos = System.nanoTime() - reconstructionStarted;
                    if (!context.isRunning()) {
                        return;
                    }
                }
                // A graceful switchover can demote a still-connected session.
                // Validate before publishing the window instead of mining a standby.
                connection.validateUnchangedPrimary();
                connection.validateUnchangedCompatibility();
                if (config.isStandbyMiningEnabled() && connection.readStandbyProgress().safeLsn() < end) {
                    throw new SQLException("Dameng standby replay boundary regressed while mining; reconnect before publishing this window", "08006");
                }
                metrics.onWindowCompleted(replayLsn, end, System.nanoTime() - windowStarted);
                final long dispatchStarted = System.nanoTime();
                for (Transaction transaction : buffer.committedTransactions()) {
                    if (!context.isRunning()) {
                        return;
                    }
                    dispatchTransaction(context, partition, transaction);
                    if (!context.isRunning()) {
                        return;
                    }
                    buffer.release(transaction);
                }
                if (!context.isRunning()) {
                    return;
                }
                final long dispatchNanos = System.nanoTime() - dispatchStarted;
                if (captureScopeChange != null) {
                    // The committed rows in this window have all been delivered.
                    // An idle new table can now activate even without a row at S.
                    captureScopeChange.scannedThrough(schema, end);
                }
                // Individual events retain the old replay position for a restart in the
                // middle of a transaction. Only a completed scan advances that position.
                offset.setLsn(buffer.completeWindow(end));
                updateTransactionMetrics(buffer);
                scannedThrough = end;
                firstWindow = false;
                metrics.setOldestReplayLsn(offset.getLsn());
                dispatcher.dispatchHeartbeatEvent(partition, offset);
                metrics.onIterationCompleted(actualSpan, forwardNanos, reconstructionNanos, dispatchNanos);
            }
        }
        catch (SQLException | RuntimeException e) {
            if (e instanceof SQLException sql) {
                LOGGER.error("Dameng LogMiner streaming failed (SQLState={}, errorCode={})", sql.getSQLState(), sql.getErrorCode(), e);
            }
            else {
                LOGGER.error("Dameng LogMiner streaming failed: {}", e.getMessage(), e);
            }
            errorHandler.setProducerThrowable(e);
        }
    }

    private void readBatch(ChangeEventSourceContext context, long start, long end, LogMinerTransactionBuffer buffer, boolean reconstruction) throws SQLException {
        buffer.beginWindow(new DispatchProgress(offset.getCommitLsn(), offset.getTransactionId()));
        final long started = System.nanoTime();
        Throwable failure = null;
        long readRows = 0;
        metrics.onMiningSessionStarted(reconstruction);
        try {
            connection.startLogMining(start, end, false);
            try (PreparedStatement statement = connection.readTablePreparedStatement(config, connection.logMinerContentsQuery(false), java.util.OptionalLong.empty())) {
                statement.setLong(1, start);
                statement.setLong(2, end);
                statement.setFetchSize(config.getQueryFetchSize());
                try (ResultSet rows = statement.executeQuery()) {
                    while (context.isRunning() && rows.next()) {
                        readRows++;
                        buffer.add(readRow(rows));
                        if ((buffer.windowRecords() & 1_023) == 0) {
                            metrics.onBufferUsageUpdated(buffer.windowBytes(), buffer.windowRecords());
                            updateTransactionMetrics(buffer);
                        }
                    }
                }
                if (context.isRunning()) {
                    buffer.finishWindow();
                }
            }
        }
        catch (SQLException | RuntimeException | Error e) {
            failure = e;
            throw e;
        }
        finally {
            try {
                connection.endLogMining();
            }
            catch (SQLException | RuntimeException cleanupFailure) {
                if (failure != null) {
                    failure.addSuppressed(cleanupFailure);
                }
                else {
                    throw cleanupFailure;
                }
            }
            finally {
                metrics.onBufferUpdated(buffer.windowBytes(), buffer.windowRecords(), buffer.openTransactions());
                updateTransactionMetrics(buffer);
                if (buffer.isCapacityExceeded()) {
                    metrics.onBufferLimitExceeded();
                }
                metrics.onMiningRowsRead(readRows, reconstruction);
                metrics.onMiningFinished(System.nanoTime() - started);
            }
        }
    }

    private void updateTransactionMetrics(LogMinerTransactionBuffer buffer) {
        metrics.onTransactionBufferUpdated(buffer.memoryBytes(), buffer.diskBytes(), buffer.bufferedRecords(), buffer.spilledTransactions(), buffer.openTransactions());
    }

    @VisibleForTesting
    LogMinerRow readRow(ResultSet rows) throws SQLException {
        final long lsn = rows.getLong("SCN");
        if (rows.wasNull() || lsn <= 0) {
            throw new IllegalStateException("Missing or invalid Dameng SCN in V$LOGMNR_CONTENTS");
        }
        final int operation = requiredInt(rows, "OPERATION_CODE", lsn);
        final int rollback = requiredInt(rows, "ROLL_BACK", lsn);
        final int sequence = requiredInt(rows, "SSN", lsn);
        final int continuation = requiredInt(rows, "CSF", lsn);
        final long objectId = rows.getLong("DATA_OBJ#");
        if (EventType.from(operation).isDataChange() && sequence == 0 && (rows.wasNull() || objectId <= 0)) {
            throw new IllegalStateException("Missing or invalid Dameng DML DATA_OBJ# at LSN " + lsn);
        }
        final byte[] xid = rows.getBytes("XID");
        final Timestamp timestamp = rows.getTimestamp("TIMESTAMP");
        // A range may start inside a transaction, or contain uncommitted/rolled-back
        // rows. Null START_SCN/COMMIT_SCN are valid and use the existing zero sentinel.
        return new LogMinerRow(lsn, rows.getLong("START_SCN"), rows.getLong("COMMIT_SCN"),
                xid == null ? null : HexFormat.of().withUpperCase().formatHex(xid), operation,
                rollback, rows.getString("SEG_OWNER"), rows.getString("TABLE_NAME"), rows.getString("USERNAME"),
                rows.getString("SQL_REDO"), rows.getString("RS_ID"), sequence, continuation,
                timestamp == null ? clock.currentTimeAsInstant() : timestamp.toInstant(), objectId,
                optionalLong(rows, "DATA_OBJV#"), optionalLong(rows, "RBASQN"), optionalLong(rows, "RBABLK"), (int) optionalLong(rows, "RBABYTE"));
    }

    private static int requiredInt(ResultSet rows, String field, long lsn) throws SQLException {
        final int value = rows.getInt(field);
        if (rows.wasNull()) {
            throw new IllegalStateException("Missing Dameng " + field + " at LSN " + lsn);
        }
        return value;
    }

    private static long optionalLong(ResultSet rows, String field) throws SQLException {
        final long value = rows.getLong(field);
        // Zero is a valid object version, archive number and block offset.
        return rows.wasNull() ? -1 : value;
    }

    private void dispatchTransaction(ChangeEventSourceContext context, DamengPartition partition, Transaction transaction) throws InterruptedException {
        final long previousCommit = offset.getCommitLsn();
        final long previousSerial = offset.getEventSerialNo();
        final String previousTransaction = offset.getTransactionId();
        if (new DispatchProgress(previousCommit, previousTransaction).canDiscard(transaction.commitLsn(), transaction.id())) {
            return;
        }
        long serial = 0;
        try (var records = transaction.open()) {
            for (LogMinerRow row = records.next(); row != null; row = records.next()) {
                if (!context.isRunning()) {
                    return;
                }
                final long ordinal = serial++;
                if (captureScopeChange != null) {
                    // Activation follows row position, independently of an older
                    // replay LSN pinned by an unrelated unfinished transaction.
                    captureScopeChange.beforeRow(schema, row.lsn());
                }
                if (transaction.commitLsn() == previousCommit && transaction.id().equals(previousTransaction) && ordinal <= previousSerial) {
                    continue;
                }
                final var eventType = row.eventType();
                final Analysis analysis = eventType == EventType.DDL ? analyzeDdl(row) : null;
                if (eventType == EventType.DDL && analysis == null) {
                    continue;
                }
                final TableId tableId = analysis == null ? tableId(row) : analysis.tableId();
                if (captureScopeChange != null && (analysis == null || analysis.impact() == Impact.TABLE)
                        && captureScopeChange.excludes(tableId, row.objectId(), row.lsn())) {
                    // The floor is permanent: a later commit can still contain
                    // pre-cut rows after its JDBC baseline has been installed.
                    // Analyze DDL scope first so a compound statement cannot
                    // silently discard changes to other, already captured tables.
                    continue;
                }
                if (captureScopeChange != null && analysis != null && analysis.impact() == Impact.TABLE
                        && analysis.renamedTableId() != null
                        && captureScopeChange.excludes(analysis.renamedTableId(), row.objectId(), row.lsn())) {
                    throw new IllegalStateException("Dameng historical rename from " + tableId + " to newly included table " + analysis.renamedTableId()
                            + " at LSN " + row.lsn() + " crosses its capture activation boundary; restore the previous capture filters, "
                            + "catch up through the rename, then retry the scope change; refusing to overwrite the new table baseline with pre-boundary history");
                }
                if (tableId == null) {
                    if (eventType != EventType.DDL && !eventType.isDataChange()) {
                        throw new IllegalStateException("Unsupported Dameng LogMiner operation " + row.operation() + " with unresolved table at "
                                + row.position() + "; cannot prove that the change is outside capture; refusing to advance the offset");
                    }
                    continue;
                }
                if (config.isInternalTableExcluded(tableId)) {
                    // Also discard definitions retained in history by older connector versions.
                    continue;
                }
                if (!config.getTableFilters().dataCollectionFilter().isIncluded(tableId)) {
                    if (eventType != EventType.DDL) {
                        continue;
                    }
                    if (schema.getTables().forTable(tableId) == null) {
                        final TableId renamed = analysis.renamedTableId();
                        if (renamed != null && config.getTableFilters().dataCollectionFilter().isIncluded(renamed)) {
                            throw new IllegalStateException("Cannot capture renamed Dameng table " + renamed + " without historical schema for " + tableId
                                    + "; a new snapshot is required");
                        }
                        continue;
                    }
                    // A previously captured table can leave the filter after a rename.
                    // Retain its subsequent DDL so restart and rename-back stay consistent.
                }
                if (!eventType.isDataChange() && eventType != EventType.DDL) {
                    throw new IllegalStateException("Unsupported Dameng LogMiner operation " + row.operation() + " for " + tableId
                            + " at " + row.position() + "; refusing to advance past an unhandled change");
                }
                if (eventType == EventType.DDL && row.objectId() <= 0) {
                    throw new IllegalStateException("Invalid Dameng DDL DATA_OBJ# " + row.objectId() + " for " + tableId
                            + " at LSN " + row.lsn() + "; refusing to replace the historical table identity or advance the offset");
                }
                final var historical = schema.getTables().forTable(tableId);
                if (DamengTableType.isNonRow(historical)) {
                    if (eventType.isDataChange() && DamengTableType.isExternal(historical)) {
                        throw new IllegalStateException("Unexpected row change for historical external table " + tableId + " at " + row.position());
                    }
                    if (historical.attributeWithName("dameng.object.id").asLong() != row.objectId()) {
                        throw new IllegalStateException("Dameng non-row table object identity mismatch for " + tableId + " at " + row.position());
                    }
                    if (eventType.isDataChange()) {
                        continue;
                    }
                }
                // Validate on a copy before advancing this row's offset. The live model is
                // changed only by DatabaseSchema when its schema event is applied.
                final List<DamengDdlParser.SchemaChange> ddlChanges = analysis == null ? List.of() : planDdl(analysis, row);
                if (ddlChanges == null) {
                    continue;
                }
                offset.setTransactionId(transaction.id());
                offset.setChangeLsn(row.lsn());
                offset.setCommitLsn(transaction.commitLsn());
                offset.setEventSerialNo(ordinal);
                offset.event(tableId, row.timestamp());
                if (eventType == EventType.DDL) {
                    dispatchDdl(partition, row, ddlChanges);
                }
                else if (eventType.isDataChange()) {
                    final Table table = schema.getTables().forTable(tableId);
                    if (table == null) {
                        throw new IllegalStateException("No historical schema for captured Dameng table " + tableId + " at LSN " + row.lsn());
                    }
                    if (row.redo() == null || row.redo().isBlank()) {
                        throw new IllegalStateException("Missing Dameng SQL_REDO for " + tableId + " at LSN " + row.lsn());
                    }
                    // A BEGIN record may be acknowledged before the first data record. Its
                    // offset must therefore replay that row, not claim that it was delivered.
                    offset.setEventSerialNo(ordinal - 1);
                    dispatcher.dispatchTransactionStartedEvent(partition, transaction.id(), offset, row.timestamp());
                    offset.setEventSerialNo(ordinal);
                    dispatcher.dispatchDataChangeEvent(partition, tableId,
                            new DamengChangeRecordEmitter(partition, offset, parseDml(row, physicalTable(row.objectId(), table)), clock, config, table));
                }
            }
        }
        offset.setCommitLsn(transaction.commitLsn());
        offset.setTransactionId(transaction.id());
        offset.setEventSerialNo(Long.MAX_VALUE);
        dispatcher.dispatchTransactionCommittedEvent(partition, offset, clock.currentTimeAsInstant());
    }

    private TableId tableId(LogMinerRow row) {
        final long version = schema.tableVersion();
        if (objectTablesVersion != version) {
            objectTables.clear();
            for (TableId id : schema.getTables().tableIds()) {
                if (config.isInternalTableExcluded(id)) {
                    // An old internal table's object id can already belong to a business table.
                    continue;
                }
                final var objectId = schema.getTables().forTable(id).attributeWithName("dameng.object.id");
                if (objectId != null) {
                    final var previous = objectTables.put(Long.parseLong(objectId.value()), id);
                    if (previous != null) {
                        throw new IllegalStateException("Duplicate Dameng historical DATA_OBJ# " + objectId.value() + " for " + previous + " and " + id);
                    }
                }
            }
            objectTablesVersion = version;
        }
        return objectTables.get(row.objectId());
    }

    private DmlEntry parseDml(LogMinerRow row, Table table) {
        try {
            return parser.parse(row.redo(), table);
        }
        catch (RuntimeException failure) {
            logSqlParsingFailure(row, failure);
            throw failure;
        }
    }

    private static void logSqlParsingFailure(LogMinerRow row, RuntimeException failure) {
        LOGGER.error("Failed to parse Dameng LogMiner SQL. Full LogMinerRow: {}", row, failure);
    }

    private Analysis analyzeDdl(LogMinerRow row) {
        if (row.redo() == null || row.redo().isBlank()) {
            throw new IllegalStateException("Missing Dameng DDL SQL_REDO at LSN " + row.lsn());
        }
        try {
            return ddlParser.analyze(row.redo(), row.owner() == null ? row.username() : row.owner());
        }
        catch (IllegalArgumentException failure) {
            if (failure instanceof PartitionExchangeException exchange) {
                handlePartitionExchange(row, exchange);
                return null;
            }
            if (failure instanceof CascadeDropException) {
                if (!config.isCascadeDropIgnored()) {
                    throw ddlFailure(row, failure);
                }
                LOGGER.warn("Ignoring Dameng cascading object drop at LSN {}, XID {}, DATA_OBJ# {} because cascade.drop.ignore=true. "
                        + "This DDL was ignored. SQL_REDO: '{}'; reason: {}",
                        row.lsn(), row.transactionId(), row.objectId(), row.redo(), failure.getMessage());
                return null;
            }
            if (failure instanceof PartitionDataRemovalException) {
                if (!config.isPartitionDropIgnored()) {
                    throw ddlFailure(row, failure);
                }
                LOGGER.warn("Ignoring Dameng partition data removal at LSN {}, DATA_OBJ# {} because partition.drop.ignore=true. "
                        + "Removed partition rows may remain downstream. SQL_REDO: '{}'; reason: {}",
                        row.lsn(), row.objectId(), row.redo(), failure.getMessage());
                return null;
            }
            if (skipUnparseableDdl(row, failure)) {
                return null;
            }
            throw ddlFailure(row, failure);
        }
    }

    private void handlePartitionExchange(LogMinerRow row, PartitionExchangeException exchange) {
        var table = exchange.tableId();
        if (table.schema() == null && row.owner() != null && !row.owner().isBlank()) {
            table = new TableId(table.catalog(), row.owner(), table.table());
        }
        var peer = exchange.exchangeTableId();
        if (peer.schema() == null) {
            // DM 8.1.3.162 reports the peer's DATA_OBJ#, while SEG_OWNER names the root schema.
            final var historicalPeer = tableId(row);
            if (historicalPeer != null && historicalPeer.table().equals(peer.table())) {
                peer = historicalPeer;
            }
        }
        if (table.schema() == null || table.schema().isBlank() || peer.schema() == null || peer.schema().isBlank()) {
            throw ddlFailure(row, new IllegalArgumentException("Cannot resolve both Dameng exchange table identities for " + table + " and " + peer
                    + "; qualify both table names, or restore the matching peer history for this DATA_OBJ#. "
                    + "SEG_OWNER is not a reliable default schema for WITH TABLE; refusing to ignore an exchange with unknown scope", exchange));
        }
        final boolean tableRelevant = isExchangeTableRelevant(table, row.lsn());
        final boolean peerRelevant = isExchangeTableRelevant(peer, row.lsn());
        if (!tableRelevant && !peerRelevant) {
            LOGGER.debug("Skipping Dameng {} {} between {} and {} outside processing scope at LSN {}, DATA_OBJ# {}. SQL_REDO: '{}'",
                    exchange.operation(), exchange.partitionName(), table, peer, row.lsn(), row.objectId(), row.redo());
            return;
        }
        if (!config.isPartitionExchangeIgnored()) {
            throw ddlFailure(row, exchange);
        }
        LOGGER.warn("Ignoring Dameng {} {} between {} and {} at LSN {}, XID {}, DATA_OBJ# {} because partition.exchange.ignore=true. "
                + "SQL_REDO: '{}'; reason: {}",
                exchange.operation(), exchange.partitionName(), table, peer, row.lsn(), row.transactionId(), row.objectId(), row.redo(), exchange.getMessage());
    }

    private boolean isExchangeTableRelevant(TableId table, long lsn) {
        if (config.isInternalTableExcluded(table) || (captureScopeChange != null && captureScopeChange.excludes(table, 0, lsn))) {
            return false;
        }
        return config.getTableFilters().dataCollectionFilter().isIncluded(table) || schema.getTables().forTable(table) != null;
    }

    private List<DamengDdlParser.SchemaChange> planDdl(Analysis analysis, LogMinerRow row) {
        if (analysis.isCtas() && !analysis.isTemporaryTable()) {
            return planCtas(analysis, row);
        }
        try {
            return ddlParser.plan(analysis, schema.getTables());
        }
        catch (IllegalArgumentException failure) {
            if (skipUnparseableDdl(row, failure)) {
                return null;
            }
            throw ddlFailure(row, failure);
        }
    }

    private List<DamengDdlParser.SchemaChange> planCtas(Analysis analysis, LogMinerRow row) {
        final var tableId = analysis.tableId();
        if (schema.getTables().forTable(tableId) != null) {
            throw new IllegalStateException("CTAS refers to an existing historical table " + tableId + " at " + row.position());
        }
        long onlineObjectId = -1;
        try {
            onlineObjectId = connection.getTableObjectId(tableId);
            if (onlineObjectId != row.objectId()) {
                throw new IllegalStateException("Online table object identity does not match the CTAS redo");
            }
            // Resolve CTAS columns and types through the online dictionary instead of inferring arbitrary AS SELECT queries.
            // This exposes the current schema, not the historical schema at the CTAS LSN.
            // The surrounding DATA_OBJ# checks detect object identity changes, but ALTER TABLE can preserve that identity.
            // If later ALTERs have already executed, replaying this CTAS can install a newer schema and misinterpret subsequent redo.
            // The target must retain its creation-time structure until this CTAS is first consumed.
            final var table = connection.readTableSchema(tableId);
            onlineObjectId = connection.getTableObjectId(tableId);
            if (onlineObjectId != row.objectId()) {
                throw new IllegalStateException("Online table object identity changed while reading the CTAS definition");
            }
            return List.of(new DamengDdlParser.SchemaChange(DamengDdlParser.Type.CREATE, table, tableId, null));
        }
        catch (SQLException | RuntimeException failure) {
            throw new IllegalStateException("Cannot resolve CTAS schema for " + tableId + " at " + row.position()
                    + "; CTAS currently requires a matching target table in the online dictionary; complete CTAS schema inference is not supported"
                    + "; redo object ID=" + row.objectId() + ", online object ID=" + (onlineObjectId < 0 ? "unavailable" : onlineObjectId)
                    + ". The original table may have been dropped, renamed, or recreated while its redo is still pending"
                    + "; historical schema and this statement's offset were not changed. Reason: " + failure.getMessage(), failure);
        }
    }

    private boolean skipUnparseableDdl(LogMinerRow row, IllegalArgumentException failure) {
        if (!config.skipUnparseableDdlStatements()) {
            return false;
        }
        LOGGER.warn("Skipping unparseable Dameng DDL at LSN {}, DATA_OBJ# {} because schema.history.internal.skip.unparseable.ddl=true. "
                + "Historical schema was not changed; subsequent events may fail or be decoded incorrectly if this DDL changed a captured table. "
                + "SQL_REDO: '{}'; reason: {}", row.lsn(), row.objectId(), row.redo(), failure.getMessage(), failure);
        return true;
    }

    private IllegalArgumentException ddlFailure(LogMinerRow row, IllegalArgumentException failure) {
        logSqlParsingFailure(row, failure);
        return new IllegalArgumentException("Cannot process Dameng DDL at LSN " + row.lsn() + ", DATA_OBJ# " + row.objectId()
                + ", XID " + row.transactionId() + ": " + failure.getMessage() + "; SQL_REDO: '" + row.redo()
                + "'; historical schema and this statement's offset were not changed", failure);
    }

    @VisibleForTesting
    Table physicalTable(long objectId, Table table) {
        final var cached = physicalTables.get(table.id());
        // Table definitions are immutable. Identity detects replacement even when Table.equals()
        // considers two definitions equal (for example when only historical attributes change).
        if (cached != null && cached.historical() == table && cached.objectId() == objectId) {
            return cached.physical();
        }
        // With no online dictionary DM emits OBJ# and COL n identifiers. Binding by the
        // historical ordinal avoids applying today's names and types to earlier redo.
        // SEG_OWNER and TABLE_NAME are null without an online dictionary. DATA_OBJ#
        // is the authoritative identity; SQL_REDO spells it as the quoted OBJ# name.
        final var physical = Table.editor().tableId(new TableId(null, null, "OBJ# " + objectId));
        for (var column : table.columns()) {
            physical.addColumn(column.edit().name("COL " + column.position()).create());
        }
        final Table result = physical.create();
        // Keep only the latest mapping for each table, not every historical schema version.
        physicalTables.put(table.id(), new CachedPhysicalTable(table, objectId, result));
        return result;
    }

    private void dispatchDdl(DamengPartition partition, LogMinerRow row, List<DamengDdlParser.SchemaChange> changes) throws InterruptedException {
        if (!changes.isEmpty()) {
            // DDL is infrequent; clearing also releases dropped/renamed table mappings.
            physicalTables.clear();
        }
        for (DamengDdlParser.SchemaChange change : changes) {
            final Table table = change.table().edit().addAttributes(change.table().attributes())
                    .addAttribute(Attribute.editor().name("dameng.object.id").value(Long.toString(row.objectId())).create()).create();
            final long serial = offset.getEventSerialNo();
            try {
                if (change.type() == DamengDdlParser.Type.TRUNCATE) {
                    // A schema event can be acknowledged before its truncate data event.
                    // Keep this statement replayable until the data event is delivered.
                    offset.setEventSerialNo(serial - 1);
                }
                final SchemaChangeEvent event;
                if (change.type() == DamengDdlParser.Type.RENAME) {
                    event = SchemaChangeEvent.ofRename(partition, offset, config.getDatabaseName(), change.tableId().schema(),
                            row.redo(), table, change.previousTableId());
                }
                else {
                    event = SchemaChangeEvent.of(SchemaChangeEventType.valueOf(change.type().name()), partition, offset,
                            config.getDatabaseName(), change.tableId().schema(), row.redo(), table, false);
                }
                final TableId dispatchId = config.getTableFilters().dataCollectionFilter().isIncluded(change.tableId())
                        ? change.tableId()
                        : change.previousTableId();
                if (DamengTableType.isNonRow(table)) {
                    // Persist lifecycle metadata without publishing a data or schema event.
                    schema.applySchemaChange(event);
                }
                else if (dispatchId != null && config.getTableFilters().dataCollectionFilter().isIncluded(dispatchId)) {
                    dispatcher.dispatchSchemaChangeEvent(partition, offset, dispatchId, receiver -> receiver.schemaChangeEvent(event));
                }
                else {
                    // This is historical metadata for a table that left capture. It
                    // still needs durable history, even with store-only-captured=true.
                    schema.applySchemaChange(event);
                }
            }
            finally {
                offset.setEventSerialNo(serial);
            }
            if (change.type() == DamengDdlParser.Type.TRUNCATE && !DamengTableType.isNonRow(table)) {
                dispatcher.dispatchDataChangeEvent(partition, change.tableId(), new DamengChangeRecordEmitter(partition, offset,
                        new DmlEntry(Operation.TRUNCATE, null, null, new BitSet(), new BitSet()), clock, config, table));
            }
        }
    }
}
