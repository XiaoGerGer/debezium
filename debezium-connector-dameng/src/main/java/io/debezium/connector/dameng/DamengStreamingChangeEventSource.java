/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.BitSet;
import java.util.HexFormat;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.dameng.logminer.LogMinerBatch;
import io.debezium.connector.dameng.logminer.LogMinerBatch.LogMinerRow;
import io.debezium.connector.dameng.logminer.LogMinerBatch.Transaction;
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
 * Every window is reconstructed before delivery so that rollback flags are definitive
 * and all open transactions are known before advancing the replay position.
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
    private DamengDdlParser ddlParser = new DamengDdlParser();
    private DamengOffsetContext offset;

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

    @Override
    public void execute(ChangeEventSourceContext context, DamengPartition partition, DamengOffsetContext offsetContext)
            throws InterruptedException {
        this.offset = offsetContext;
        try {
            connection.validateLogMiningConfiguration();
            ddlParser = new DamengDdlParser(connection.getDatabaseCharset(),
                    !"0".equals(connection.getIniParameter("LENGTH_IN_CHAR")),
                    "CHAR".equalsIgnoreCase(connection.getIniParameter("NLS_LENGTH_SEMANTICS")),
                    "1".equals(connection.getIniParameter("NVARCHAR_LENGTH_IN_CHAR")));
            if (config.isFailoverEnabled()) {
                connection.validatePrimaryStandbyIdentity(offset);
            }
            else {
                offset.validateDatabaseMagic(connection.databaseMagic());
            }
            long scannedThrough = offset.getLsn();
            metrics.onWindowStarted(offset.getLsn(), scannedThrough, 0);
            while (context.isRunning()) {
                if (config.isFailoverEnabled()) {
                    connection.validatePrimaryStandbyIdentity(offset);
                }
                else {
                    connection.validateServerRole();
                }
                final long available = connection.latestArchivedLsn();
                metrics.setLatestArchivedLsn(available);
                if (available <= scannedThrough) {
                    dispatcher.dispatchHeartbeatEvent(partition, offset);
                    Thread.sleep(Math.max(100L, config.getPollInterval().toMillis()));
                    continue;
                }
                final long start = offset.getLsn();
                final long end = Math.min(available, scannedThrough + config.getLogMiningBatchSize());
                metrics.onWindowStarted(start, scannedThrough, available);
                final LogMinerBatch batch = readBatch(context, start, end);
                if (!context.isRunning()) {
                    return;
                }
                // A graceful switchover can demote a still-connected session.
                // Validate before publishing the window instead of mining a standby.
                connection.validateUnchangedPrimary();
                for (Transaction transaction : batch.committedTransactions()) {
                    if (!context.isRunning()) {
                        return;
                    }
                    dispatchTransaction(context, partition, transaction);
                }
                if (!context.isRunning()) {
                    return;
                }
                // Individual events retain the old replay position for a restart in the
                // middle of a transaction. Only a completed scan advances that position.
                offset.setLsn(batch.restartLsn(end));
                scannedThrough = end;
                metrics.setOldestReplayLsn(offset.getLsn());
                dispatcher.dispatchHeartbeatEvent(partition, offset);
            }
        }
        catch (SQLException | RuntimeException e) {
            LOGGER.error("Dameng LogMiner streaming failed", e);
            errorHandler.setProducerThrowable(e);
        }
    }

    private LogMinerBatch readBatch(ChangeEventSourceContext context, long start, long end) throws SQLException {
        final LogMinerBatch batch = new LogMinerBatch(config.getLogMiningBufferMaxBytes(), config.getLogMiningBufferMaxRecords());
        final long started = System.nanoTime();
        Throwable failure = null;
        boolean completed = false;
        try {
            connection.startLogMining(start, end, false);
            try (PreparedStatement statement = connection.readTablePreparedStatement(config, connection.logMinerContentsQuery(false), java.util.OptionalLong.empty())) {
                statement.setLong(1, start);
                statement.setLong(2, end);
                statement.setFetchSize(config.getQueryFetchSize());
                try (ResultSet rows = statement.executeQuery()) {
                    while (context.isRunning() && rows.next()) {
                        batch.add(readRow(rows));
                        if ((batch.getBufferedRecordCount() & 1_023) == 0) {
                            metrics.onBufferUsageUpdated(batch.getEstimatedBufferBytes(), batch.getBufferedRecordCount());
                        }
                    }
                }
                if (context.isRunning()) {
                    batch.finish();
                    completed = true;
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
                completed = false;
                if (failure != null) {
                    failure.addSuppressed(cleanupFailure);
                }
                else {
                    throw cleanupFailure;
                }
            }
            finally {
                metrics.onBufferUpdated(batch.getEstimatedBufferBytes(), batch.getBufferedRecordCount(), batch.getOpenTransactionCount());
                if (batch.isCapacityExceeded()) {
                    metrics.onBufferLimitExceeded();
                }
                final long duration = System.nanoTime() - started;
                if (completed && failure == null) {
                    // A completed scan is not a delivered/acknowledged offset.
                    // Preserve the old replay position until dispatch finishes.
                    metrics.onWindowCompleted(start, end, duration);
                }
                else {
                    metrics.onMiningFinished(duration);
                }
            }
        }
        return batch;
    }

    private LogMinerRow readRow(ResultSet rows) throws SQLException {
        final byte[] xid = rows.getBytes("XID");
        final Timestamp timestamp = rows.getTimestamp("TIMESTAMP");
        return new LogMinerRow(rows.getLong("SCN"), rows.getLong("START_SCN"), rows.getLong("COMMIT_SCN"),
                xid == null ? null : HexFormat.of().withUpperCase().formatHex(xid), rows.getInt("OPERATION_CODE"),
                rows.getInt("ROLL_BACK"), rows.getString("SEG_OWNER"), rows.getString("TABLE_NAME"), rows.getString("USERNAME"),
                rows.getString("SQL_REDO"), rows.getString("RS_ID"), rows.getInt("SSN"), rows.getInt("CSF"),
                timestamp == null ? clock.currentTimeAsInstant() : timestamp.toInstant(), rows.getLong("DATA_OBJ#"));
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
        for (LogMinerRow row : transaction.records()) {
            if (!context.isRunning()) {
                return;
            }
            final long ordinal = serial++;
            if (transaction.commitLsn() == previousCommit && transaction.id().equals(previousTransaction) && ordinal <= previousSerial) {
                continue;
            }
            final TableId tableId = tableId(row);
            if (tableId == null) {
                continue;
            }
            if (!config.getTableFilters().dataCollectionFilter().isIncluded(tableId)) {
                if (row.operation() != 5) {
                    continue;
                }
                if (schema.getTables().forTable(tableId) == null) {
                    final TableId renamed = ddlParser.resolveRenamedTableId(row.redo(), tableId.schema());
                    if (renamed != null && config.getTableFilters().dataCollectionFilter().isIncluded(renamed)) {
                        throw new IllegalStateException("Cannot capture renamed Dameng table " + renamed + " without historical schema for " + tableId
                                + "; a new snapshot is required");
                    }
                    continue;
                }
                // A previously captured table can leave the filter after a rename.
                // Retain its subsequent DDL so restart and rename-back stay consistent.
            }
            if (row.operation() == 5 && row.objectId() <= 0) {
                throw new IllegalStateException("Invalid Dameng DDL DATA_OBJ# " + row.objectId() + " for " + tableId
                        + " at LSN " + row.lsn() + "; refusing to replace the historical table identity or advance the offset");
            }
            offset.setTransactionId(transaction.id());
            offset.setChangeLsn(row.lsn());
            offset.setCommitLsn(transaction.commitLsn());
            offset.setEventSerialNo(ordinal);
            offset.event(tableId, row.timestamp());
            if (row.operation() == 5) {
                dispatchDdl(partition, tableId, row);
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
                        new DamengChangeRecordEmitter(partition, offset, parser.parse(row.redo(), physicalTable(row, table)), clock, config, table));
            }
            else {
                throw new IllegalStateException("Unsupported Dameng LogMiner operation " + row.operation() + " for " + tableId
                        + " at LSN " + row.lsn() + "; refusing to advance past an unhandled change");
            }
        }
        offset.setCommitLsn(transaction.commitLsn());
        offset.setTransactionId(transaction.id());
        offset.setEventSerialNo(Long.MAX_VALUE);
        dispatcher.dispatchTransactionCommittedEvent(partition, offset, clock.currentTimeAsInstant());
    }

    private TableId tableId(LogMinerRow row) {
        if (row.operation() == 5 && row.redo() != null) {
            return ddlParser.resolveTableId(row.redo(), row.owner() == null ? row.username() : row.owner());
        }
        for (TableId id : schema.getTables().tableIds()) {
            final var objectId = schema.getTables().forTable(id).attributeWithName("dameng.object.id");
            if (objectId != null && Long.parseLong(objectId.value()) == row.objectId()) {
                return id;
            }
        }
        return null;
    }

    private Table physicalTable(LogMinerRow row, Table table) {
        // With no online dictionary DM emits OBJ# and COL n identifiers. Binding by the
        // historical ordinal avoids applying today's names and types to earlier redo.
        // SEG_OWNER and TABLE_NAME are null without an online dictionary. DATA_OBJ#
        // is the authoritative identity; SQL_REDO spells it as the quoted OBJ# name.
        final var physical = Table.editor().tableId(new TableId(null, null, "OBJ# " + row.objectId()));
        for (var column : table.columns()) {
            physical.addColumn(column.edit().name("COL " + column.position()).create());
        }
        return physical.create();
    }

    private void dispatchDdl(DamengPartition partition, TableId tableId, LogMinerRow row) throws InterruptedException {
        for (DamengDdlParser.SchemaChange change : ddlParser.parse(row.redo(), schema.getTables(), tableId.schema())) {
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
