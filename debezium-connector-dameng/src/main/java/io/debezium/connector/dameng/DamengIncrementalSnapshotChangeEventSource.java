/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng;

import java.sql.SQLException;
import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;

import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.jdbc.JdbcConnection;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.signal.SignalPayload;
import io.debezium.pipeline.signal.actions.snapshotting.SnapshotConfiguration;
import io.debezium.pipeline.source.snapshot.incremental.SignalBasedIncrementalSnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.DataChangeEventListener;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.relational.Attribute;
import io.debezium.relational.SnapshotChangeRecordEmitter;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.schema.DataCollectionSchema;
import io.debezium.util.Clock;

/** Uses independent snapshot transactions, routing standby watermarks to the primary. */
public final class DamengIncrementalSnapshotChangeEventSource extends SignalBasedIncrementalSnapshotChangeEventSource<DamengPartition, TableId> {
    private static final Logger LOGGER = LoggerFactory.getLogger(DamengIncrementalSnapshotChangeEventSource.class);
    private final DamengConnectorConfig config;
    private final DamengConnection connection;
    private final Clock clock;
    private final DamengIncrementalSnapshotMeter meter;
    private DamengStandbySupport standby;
    private DamengCaptureScopeChange captureScopeChange;

    void setStandbySupport(DamengStandbySupport standby) {
        this.standby = standby;
    }

    void setCaptureScopeChange(DamengCaptureScopeChange captureScopeChange) {
        this.captureScopeChange = captureScopeChange;
    }

    @Override
    public void init(DamengPartition partition, OffsetContext offset) {
        // An initialization always rereads the interrupted chunk. Never retain
        // rows buffered by a previous initialization, including removed tables.
        window.clear();
        if (offset instanceof DamengOffsetContext damengOffset) {
            pruneExcludedSnapshotTables(config, damengOffset);
            if (captureScopeChange != null) {
                final List<TableId> pending = damengOffset.getIncrementalSnapshotContext().getDataCollections().stream()
                        .map(collection -> collection.getId()).filter(captureScopeChange::isPending).distinct().toList();
                if (!pending.isEmpty()) {
                    throw new ConnectException("Cannot resume the Dameng incremental snapshot of tables " + pending
                            + " before their capture-scope schema boundary S is activated; stop this snapshot and resubmit it after CDC reaches S");
                }
            }
        }
        super.init(partition, offset);
    }

    @Override
    public void addDataCollectionNamesToSnapshot(SignalPayload<DamengPartition> signalPayload, SnapshotConfiguration snapshotConfiguration)
            throws InterruptedException {
        if (captureScopeChange != null) {
            // Match the same identifiers as the common regex expansion, but
            // include pending tables whose historical schemas are not active yet.
            final List<Pattern> requested = snapshotConfiguration.getDataCollections().stream().map(Pattern::compile).toList();
            final List<TableId> pending = captureScopeChange.pendingTables().stream()
                    .filter(table -> requested.stream().anyMatch(pattern -> pattern.matcher(table.identifier()).matches())
                            || snapshotConfiguration.getDataCollections().stream().anyMatch(name -> matchesExplicitTable(name, table)))
                    .toList();
            if (!pending.isEmpty()) {
                LOGGER.warn("Ignoring Dameng incremental snapshot request '{}' because tables {} have not reached their capture-scope schema boundary S. "
                        + "Resubmit the complete request after CDC activates these tables at S", signalPayload.id, pending);
                return;
            }
        }
        super.addDataCollectionNamesToSnapshot(signalPayload, snapshotConfiguration);
    }

    private static boolean matchesExplicitTable(String name, TableId table) {
        try {
            // The common expansion retains unmatched expressions and then
            // parses them as literal identifiers, including quoted names.
            return table.equals(TableId.parse(name, false));
        }
        catch (IllegalArgumentException e) {
            return false;
        }
    }

    /**
     * Removes excluded tables before either persisting a scope transition or
     * resuming its incremental snapshot. The caller persists the modified
     * offset state together with the scope transition, without advancing CDC.
     */
    static List<TableId> pruneExcludedSnapshotTables(DamengConnectorConfig config, DamengOffsetContext offset) {
        if (offset == null) {
            return List.of();
        }
        final var snapshot = offset.getIncrementalSnapshotContext();
        if (!snapshot.snapshotRunning()) {
            return List.of();
        }
        final List<TableId> excluded = snapshot.getDataCollections().stream()
                .map(collection -> collection.getId())
                .filter(table -> !config.getTableFilters().dataCollectionFilter().isIncluded(table))
                .distinct().toList();
        if (excluded.isEmpty()) {
            return excluded;
        }
        final boolean currentExcluded = excluded.contains(snapshot.currentDataCollectionId().getId());
        if (currentExcluded) {
            // nextDataCollection resets the old table's last key, maximum key
            // and schema. Merely removing its queue entry would reuse its keys
            // for the next table and silently omit rows from that table.
            snapshot.nextDataCollection();
            snapshot.revertChunk();
            // Old source-channel watermarks may be replayed after restart.
            // Invalidate them even when the remaining snapshot is paused.
            snapshot.startNewChunk();
        }
        for (TableId table : excluded) {
            snapshot.removeDataCollectionFromSnapshot(table.toDoubleQuotedString());
        }
        if (!snapshot.snapshotRunning()) {
            snapshot.unsetCorrelationId();
            if (snapshot.isSnapshotPaused()) {
                snapshot.resumeSnapshot();
            }
        }
        LOGGER.info("Dameng capture scope removed tables {} from the pending incremental snapshot (current table removed={}, remaining tables={})",
                excluded, currentExcluded, snapshot.dataCollectionsToBeSnapshottedCount());
        return excluded;
    }

    @Override
    protected void readChunk(DamengPartition partition, OffsetContext offset) throws InterruptedException {
        if (context != null && context.snapshotRunning()) {
            if (!context.isSnapshotPaused() || !meter.isRunning()) {
                meter.startTable(context.currentDataCollectionId().getId().identifier());
            }
            meter.remainingTables(context.dataCollectionsToBeSnapshottedCount());
            if (context.isSnapshotPaused()) {
                meter.paused();
            }
        }
        try {
            super.readChunk(partition, offset);
        }
        catch (InterruptedException | RuntimeException e) {
            meter.failed();
            throw e;
        }
    }

    @Override
    protected void tableScanStarted(TableId tableId) {
        meter.startTable(tableId.identifier());
        meter.remainingTables(context.dataCollectionsToBeSnapshottedCount());
        meter.windowOpened(context.currentChunkId());
    }

    @Override
    protected void chunkReadStarted(TableId tableId) {
        meter.phase("reading");
    }

    @Override
    protected void chunkReadCompleted(TableId tableId, long rows, long durationNanos) {
        meter.queryCompleted(rows, durationNanos);
        meter.phase("preparing");
    }

    @Override
    protected void sendWindowEvents(DamengPartition partition, OffsetContext offset) throws InterruptedException {
        // The superclass invokes this only after accepting the current CLOSE signal.
        final boolean measured = meter.windowCloseReceived(context.currentChunkId() + "-close");
        final long started = clock.currentTimeInNanos();
        try {
            super.sendWindowEvents(partition, offset);
            if (measured) {
                meter.windowCompleted(clock.currentTimeInNanos() - started);
            }
        }
        catch (InterruptedException | RuntimeException e) {
            meter.failed();
            throw e;
        }
    }

    @Override
    public void pauseSnapshot(DamengPartition partition, OffsetContext offset) {
        super.pauseSnapshot(partition, offset);
        if (context.isSnapshotPaused()) {
            meter.paused();
        }
    }

    @Override
    public void resumeSnapshot(DamengPartition partition, OffsetContext offset) throws InterruptedException {
        meter.resumed();
        super.resumeSnapshot(partition, offset);
    }

    @Override
    protected void postIncrementalSnapshotCompleted() {
        meter.finished();
    }

    @Override
    protected JdbcConnection getSignalConnection() {
        return standby == null ? connection : standby.primary();
    }

    @Override
    protected void emitWindowOpen(DamengPartition partition, OffsetContext offset) throws SQLException {
        meter.windowOpened(context.currentChunkId());
        if (standby == null) {
            super.emitWindowOpen(partition, offset);
            return;
        }
        standby.validate(connection);
        try {
            super.emitWindowOpen(partition, offset);
        }
        catch (SQLException e) {
            // A demotion between validation and INSERT can surface as -710
            // (standby write denied). Recheck roles before classifying the error.
            standby.validate(connection);
            throw e;
        }
        // Without this barrier a lagging replica could resurrect rows deleted
        // before the low watermark, outside the deduplication window.
        standby.awaitReplay(connection, standby.primary().currentLsn());
        connection.commit();
    }

    @Override
    protected void emitWindowClose(DamengPartition partition, OffsetContext offset) throws Exception {
        if (standby != null) {
            standby.validate(connection);
            connection.commit();
        }
        try {
            super.emitWindowClose(partition, offset);
            if (context.snapshotRunning()) {
                meter.windowCloseWritten();
            }
        }
        catch (SQLException e) {
            if (standby != null) {
                standby.validate(connection);
            }
            throw e;
        }
    }

    @Override
    protected void handleChunkError(Exception exception) {
        meter.failed();
        if (standby != null) {
            // Never acknowledge a skipped table after losing a cross-node barrier
            // or replica read. Recover the chunk from the durable offset instead.
            throw new ConnectException("Dameng standby incremental snapshot failed; the task must recover its incomplete chunk", exception);
        }
    }

    public DamengIncrementalSnapshotChangeEventSource(DamengConnectorConfig config, DamengConnection connection,
                                                      EventDispatcher<DamengPartition, TableId> dispatcher, DamengDatabaseSchema schema,
                                                      Clock clock, SnapshotProgressListener<DamengPartition> progressListener,
                                                      DataChangeEventListener<DamengPartition> dataChangeEventListener,
                                                      NotificationService<DamengPartition, DamengOffsetContext> notifications) {
        super(config, connection, dispatcher, schema, clock, progressListener, dataChangeEventListener, notifications);
        this.config = config;
        this.connection = connection;
        this.clock = clock;
        meter = progressListener instanceof DamengSnapshotChangeEventSourceMetrics metrics ? metrics.incrementalMeter()
                : new DamengIncrementalSnapshotMeter(clock);
    }

    @Override
    protected SnapshotChangeRecordEmitter<DamengPartition> getChangeRecordEmitter(DamengPartition partition, TableId tableId,
                                                                                  OffsetContext offset, Object[] row) {
        return new SnapshotChangeRecordEmitter<>(partition, offset, row, clock, connectorConfig) {
            @Override
            public void emitChangeRecords(DataCollectionSchema schema, Receiver<DamengPartition> receiver) throws InterruptedException {
                super.emitChangeRecords(schema, (eventPartition, eventSchema, operation, key, value, eventOffset, headers) -> {
                    receiver.changeRecord(eventPartition, eventSchema, operation, key, value, eventOffset, headers);
                    // Count only after the incremental receiver has successfully enqueued the event.
                    meter.eventEnqueued();
                });
            }
        };
    }

    @Override
    protected String getSignalTableName(String dataCollectionId) {
        return connection.quotedTableIdString(TableId.parse(dataCollectionId, false));
    }

    @Override
    protected Optional<String> getDatabaseName(Object dataCollectionId) {
        return Optional.empty();
    }

    @Override
    protected Table readSchemaForTable(TableId tableId) throws SQLException {
        final Table table = super.readSchemaForTable(tableId);
        return table.edit().addAttributes(table.attributes())
                .addAttribute(Attribute.editor().name("dameng.object.id").value(Long.toString(connection.getTableObjectId(tableId))).create())
                .create();
    }
}
