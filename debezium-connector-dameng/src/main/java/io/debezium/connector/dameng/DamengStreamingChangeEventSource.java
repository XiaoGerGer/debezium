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
import io.debezium.connector.dameng.ddl.DamengDdlStatementAnalyzer.Analysis;
import io.debezium.connector.dameng.ddl.DamengDdlStatementAnalyzer.Impact;
import io.debezium.connector.dameng.logminer.LogMinerArchiveWindow;
import io.debezium.connector.dameng.logminer.LogMinerRow;
import io.debezium.connector.dameng.logminer.LogMinerTransactionBuffer;
import io.debezium.connector.dameng.logminer.LogMinerTransactionBuffer.Transaction;
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
                LOGGER.error("Dameng LogMiner streaming failed", e);
            }
            errorHandler.setProducerThrowable(e);
        }
    }

    private void readBatch(ChangeEventSourceContext context, long start, long end, LogMinerTransactionBuffer buffer, boolean reconstruction) throws SQLException {
        buffer.beginWindow();
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
        if (operation >= 1 && operation <= 4 && sequence == 0 && (rows.wasNull() || objectId <= 0)) {
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
        if (transaction.commitLsn() < previousCommit
                || (transaction.commitLsn() == previousCommit && (previousTransaction == null
                        || transaction.id().compareTo(previousTransaction) < 0))) {
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
                final Analysis analysis = row.operation() == 5 ? analyzeDdl(row) : null;
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
                    if (row.operation() != 5 && (row.operation() < 1 || row.operation() > 4)) {
                        throw new IllegalStateException("Unsupported Dameng LogMiner operation " + row.operation() + " with unresolved table at "
                                + row.position() + "; cannot prove that the change is outside capture; refusing to advance the offset");
                    }
                    continue;
                }
                if (!config.getTableFilters().dataCollectionFilter().isIncluded(tableId)) {
                    if (row.operation() != 5) {
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
                if (row.operation() < 1 || row.operation() > 5) {
                    throw new IllegalStateException("Unsupported Dameng LogMiner operation " + row.operation() + " for " + tableId
                            + " at " + row.position() + "; refusing to advance past an unhandled change");
                }
                if (row.operation() == 5 && row.objectId() <= 0) {
                    throw new IllegalStateException("Invalid Dameng DDL DATA_OBJ# " + row.objectId() + " for " + tableId
                            + " at LSN " + row.lsn() + "; refusing to replace the historical table identity or advance the offset");
                }
                // Validate on a copy before advancing this row's offset. The live model is
                // changed only by DatabaseSchema when its schema event is applied.
                final List<DamengDdlParser.SchemaChange> ddlChanges = analysis == null ? List.of() : planDdl(analysis, row);
                offset.setTransactionId(transaction.id());
                offset.setChangeLsn(row.lsn());
                offset.setCommitLsn(transaction.commitLsn());
                offset.setEventSerialNo(ordinal);
                offset.event(tableId, row.timestamp());
                if (row.operation() == 5) {
                    dispatchDdl(partition, row, ddlChanges);
                }
                else if (row.operation() >= 1 && row.operation() <= 4) {
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
                            new DamengChangeRecordEmitter(partition, offset, parser.parse(row.redo(), physicalTable(row.objectId(), table)), clock, config, table));
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

    private Analysis analyzeDdl(LogMinerRow row) {
        if (row.redo() == null || row.redo().isBlank()) {
            throw new IllegalStateException("Missing Dameng DDL SQL_REDO at LSN " + row.lsn());
        }
        try {
            return ddlParser.analyze(row.redo(), row.owner() == null ? row.username() : row.owner());
        }
        catch (IllegalArgumentException failure) {
            throw ddlFailure(row, failure);
        }
    }

    private List<DamengDdlParser.SchemaChange> planDdl(Analysis analysis, LogMinerRow row) {
        try {
            return ddlParser.plan(analysis, schema.getTables());
        }
        catch (IllegalArgumentException failure) {
            throw ddlFailure(row, failure);
        }
    }

    private IllegalArgumentException ddlFailure(LogMinerRow row, IllegalArgumentException failure) {
        return new IllegalArgumentException("Cannot process Dameng DDL at LSN " + row.lsn() + ", DATA_OBJ# " + row.objectId()
                + ": " + failure.getMessage() + "; historical schema and this statement's offset were not changed", failure);
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
                if (dispatchId != null && config.getTableFilters().dataCollectionFilter().isIncluded(dispatchId)) {
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
            if (change.type() == DamengDdlParser.Type.TRUNCATE) {
                dispatcher.dispatchDataChangeEvent(partition, change.tableId(), new DamengChangeRecordEmitter(partition, offset,
                        new DmlEntry(Operation.TRUNCATE, null, null, new BitSet(), new BitSet()), clock, config, table));
            }
        }
    }
}
