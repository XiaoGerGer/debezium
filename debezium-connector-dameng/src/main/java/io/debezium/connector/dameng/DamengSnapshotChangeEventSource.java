/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng;

import java.sql.SQLException;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import io.debezium.jdbc.MainConnectionProvidingConnectionFactory;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.source.SnapshottingTask;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.spi.SnapshotResult;
import io.debezium.relational.Attribute;
import io.debezium.relational.RelationalSnapshotChangeEventSource;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.schema.SchemaChangeEvent;
import io.debezium.snapshot.SnapshotterService;
import io.debezium.util.Clock;

public final class DamengSnapshotChangeEventSource extends RelationalSnapshotChangeEventSource<DamengPartition, DamengOffsetContext> {
    private final DamengConnectorConfig config;
    private final DamengConnection connection;
    private final SnapshotProgressListener<DamengPartition> listener;
    private final SnapshotterService snapshotterService;

    public DamengSnapshotChangeEventSource(DamengConnectorConfig config, MainConnectionProvidingConnectionFactory<DamengConnection> connectionFactory,
                                           DamengDatabaseSchema schema, EventDispatcher<DamengPartition, TableId> dispatcher, Clock clock,
                                           SnapshotProgressListener<DamengPartition> listener,
                                           NotificationService<DamengPartition, DamengOffsetContext> notifications,
                                           SnapshotterService snapshotterService) {
        super(config, connectionFactory, schema, dispatcher, clock, listener, notifications, snapshotterService);
        this.config = config;
        this.connection = connectionFactory.mainConnection();
        this.listener = listener;
        this.snapshotterService = snapshotterService;
    }

    @Override
    protected SnapshotContext<DamengPartition, DamengOffsetContext> prepare(DamengPartition partition, boolean onDemand) {
        return new DamengSnapshotContext(partition, config.getDatabaseName(), onDemand);
    }

    @Override
    public SnapshotResult<DamengOffsetContext> doExecute(ChangeEventSourceContext context, DamengOffsetContext previousOffset,
                                                         SnapshotContext<DamengPartition, DamengOffsetContext> snapshotContext,
                                                         SnapshottingTask snapshottingTask)
            throws Exception {
        final var deadline = new DamengSnapshotDeadline(connection.connection(), config.getSnapshotMaxDurationMs());
        Throwable failure = null;
        try {
            return super.doExecute(context, previousOffset, snapshotContext, snapshottingTask);
        }
        catch (Exception | Error e) {
            failure = e;
            throw e;
        }
        finally {
            // Synchronize with the timer before this thread can enter streaming;
            // an old timer must never abort a successfully completed snapshot.
            deadline.close();
            if (deadline.expired()) {
                Thread.interrupted();
                throw new DamengSnapshotDeadline.SnapshotTimeoutException(config.getSnapshotMaxDurationMs(), failure);
            }
        }
    }

    @Override
    protected Set<TableId> getAllTableIds(RelationalSnapshotContext<DamengPartition, DamengOffsetContext> context) throws SQLException {
        return connection.getAllTableIds(null);
    }

    @Override
    protected void lockTablesForSchemaSnapshot(ChangeEventSourceContext sourceContext,
                                               RelationalSnapshotContext<DamengPartition, DamengOffsetContext> context)
            throws SQLException, InterruptedException {
        lockTables(sourceContext, schemaTables(context));
    }

    private void lockTables(ChangeEventSourceContext sourceContext, Collection<TableId> tables) throws SQLException, InterruptedException {
        for (TableId table : tables) {
            if (!sourceContext.isRunning()) {
                throw new InterruptedException("Interrupted while locking " + table);
            }
            // Use the configured statement timeout without committing the transaction
            // that must hold these locks until the snapshot has finished.
            connection.executeWithoutCommitting("LOCK TABLE " + connection.quotedTableIdString(table) + " IN SHARE MODE");
        }
    }

    @Override
    protected void releaseSchemaSnapshotLocks(RelationalSnapshotContext<DamengPartition, DamengOffsetContext> context) throws SQLException {
        // Keep all shared table locks through the data snapshot. The base class rolls
        // back this transaction after data and schema have been read, including on failure.
    }

    @Override
    protected void releaseDataSnapshotLocks(RelationalSnapshotContext<DamengPartition, DamengOffsetContext> context) throws SQLException {
        // The base class calls this before publishing the completed snapshot offset.
        // A primary change during the scan must not be acknowledged as a complete snapshot.
        connection.validateUnchangedPrimary();
    }

    @Override
    protected void determineSnapshotOffset(RelationalSnapshotContext<DamengPartition, DamengOffsetContext> context,
                                           DamengOffsetContext previousOffset)
            throws Exception {
        if (previousOffset != null && !previousOffset.isInitialSnapshotRunning()) {
            context.offset = previousOffset;
        }
        else {
            context.offset = new DamengOffsetContext(config, connection.currentLsn());
        }
        validateDatabaseIdentity(context.offset);
    }

    @Override
    protected void readTableStructure(ChangeEventSourceContext sourceContext,
                                      RelationalSnapshotContext<DamengPartition, DamengOffsetContext> context,
                                      DamengOffsetContext offsetContext, SnapshottingTask task)
            throws SQLException, InterruptedException {
        if (!task.isOnDemand() && (offsetContext == null || offsetContext.isInitialSnapshotRunning())) {
            includeTablesCreatedDuringSnapshotPreparation(sourceContext, context, task);
        }
        // Tables omitted by the snapshot row filter still need their historical
        // column layout and object identity for subsequent dictionary-free redo.
        final Set<TableId> schemaTables = schemaTables(context);
        for (String schemaName : schemaTables.stream().map(TableId::schema).distinct().toList()) {
            if (!sourceContext.isRunning()) {
                throw new InterruptedException("Interrupted while reading schema " + schemaName);
            }
            // Keep excluded columns in the historical model: COL n identifies the
            // physical ordinal, even when a column is omitted from emitted events.
            connection.readSchema(context.tables, null, schemaName, schemaTables::contains, null, false);
        }
        for (TableId tableId : schemaTables) {
            final Table table = context.tables.forTable(tableId);
            if (table != null) {
                context.tables.overwriteTable(table.edit().addAttributes(table.attributes()).addAttribute(Attribute.editor().name("dameng.object.id")
                        .value(Long.toString(connection.getTableObjectId(tableId))).create()).create());
            }
        }
    }

    private void includeTablesCreatedDuringSnapshotPreparation(ChangeEventSourceContext sourceContext,
                                                               RelationalSnapshotContext<DamengPartition, DamengOffsetContext> context,
                                                               SnapshottingTask task)
            throws SQLException, InterruptedException {
        final var snapshotPatterns = getDataCollectionPattern(task.getDataCollections());
        while (sourceContext.isRunning()) {
            // The offset was read before this enumeration. Any table created after
            // enumeration will therefore have a CREATE record in the streaming range.
            // A table discovered here must be locked before choosing a newer offset.
            final Set<TableId> allTables = connection.getAllTableIds(null);
            final Set<TableId> schemaTables = schemaTables(context);
            final Set<TableId> addedSchemaTables = allTables.stream()
                    .filter(config.getTableFilters().dataCollectionFilter()::isIncluded)
                    .filter(tableId -> !schemaTables.contains(tableId))
                    .sorted().collect(Collectors.toCollection(LinkedHashSet::new));
            final Set<TableId> addedDataTables = determineDataCollectionsToBeSnapshotted(allTables, snapshotPatterns)
                    .filter(config.getTableFilters().dataCollectionFilter()::isIncluded)
                    .filter(tableId -> !context.capturedTables.contains(tableId))
                    .sorted().collect(Collectors.toCollection(LinkedHashSet::new));
            if (addedSchemaTables.isEmpty() && addedDataTables.isEmpty()) {
                return;
            }
            if (!addedSchemaTables.isEmpty()) {
                lockTables(sourceContext, addedSchemaTables);
            }
            final Set<TableId> captured = new LinkedHashSet<>(context.capturedTables);
            captured.addAll(addedDataTables);
            context.capturedTables = captured.stream().sorted().collect(Collectors.toCollection(LinkedHashSet::new));
            schemaTables.addAll(addedSchemaTables);
            context.capturedSchemaTables = schemaTables;
            if (!addedSchemaTables.isEmpty()) {
                context.offset = new DamengOffsetContext(config, connection.currentLsn());
                validateDatabaseIdentity(context.offset);
            }
            if (!addedDataTables.isEmpty()) {
                listener.monitoredDataCollectionsDetermined(context.partition, context.capturedTables);
            }
        }
        throw new InterruptedException("Interrupted while checking for new Dameng snapshot tables");
    }

    @Override
    protected Collection<TableId> getTablesForSchemaChange(RelationalSnapshotContext<DamengPartition, DamengOffsetContext> context) {
        return schemaTables(context);
    }

    private Set<TableId> schemaTables(RelationalSnapshotContext<DamengPartition, DamengOffsetContext> context) {
        final Set<TableId> tables = new LinkedHashSet<>(context.capturedTables);
        if (context.capturedSchemaTables != null) {
            context.capturedSchemaTables.stream().filter(config.getTableFilters().dataCollectionFilter()::isIncluded).forEach(tables::add);
        }
        return tables.stream().sorted().collect(Collectors.toCollection(LinkedHashSet::new));
    }

    private void validateDatabaseIdentity(DamengOffsetContext offset) throws SQLException {
        if (config.isFailoverEnabled()) {
            connection.validatePrimaryStandbyIdentity(offset);
        }
        else {
            offset.validateDatabaseMagic(connection.databaseMagic());
        }
    }

    @Override
    protected SchemaChangeEvent getCreateTableEvent(RelationalSnapshotContext<DamengPartition, DamengOffsetContext> context, Table table) {
        return SchemaChangeEvent.ofSnapshotCreate(context.partition, context.offset, context.catalogName, table);
    }

    @Override
    protected Optional<String> getSnapshotSelect(RelationalSnapshotContext<DamengPartition, DamengOffsetContext> context,
                                                 TableId tableId, List<String> columns) {
        return snapshotterService.getSnapshotQuery().snapshotQuery(connection.quotedTableIdString(tableId), columns);
    }

    @Override
    protected DamengOffsetContext copyOffset(RelationalSnapshotContext<DamengPartition, DamengOffsetContext> context) {
        return new DamengOffsetContext.Loader(config).load(context.offset.getOffset());
    }

    private static final class DamengSnapshotContext extends RelationalSnapshotContext<DamengPartition, DamengOffsetContext> {
        private DamengSnapshotContext(DamengPartition partition, String databaseName, boolean onDemand) {
            super(partition, databaseName, onDemand);
        }
    }
}
