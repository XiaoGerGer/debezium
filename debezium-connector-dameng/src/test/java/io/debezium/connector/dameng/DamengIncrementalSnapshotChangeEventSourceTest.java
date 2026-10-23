/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

import org.apache.kafka.connect.errors.ConnectException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.connector.base.ChangeEventQueueMetrics;
import io.debezium.document.Document;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.signal.SignalPayload;
import io.debezium.pipeline.signal.actions.snapshotting.AdditionalCondition.AdditionalConditionBuilder;
import io.debezium.pipeline.signal.actions.snapshotting.SnapshotConfiguration;
import io.debezium.pipeline.source.snapshot.incremental.ChunkQueryBuilder;
import io.debezium.pipeline.source.snapshot.incremental.SignalBasedIncrementalSnapshotContext;
import io.debezium.pipeline.source.spi.DataChangeEventListener;
import io.debezium.pipeline.spi.ChangeRecordEmitter;
import io.debezium.relational.Column;
import io.debezium.relational.CustomConverterRegistry;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.history.MemorySchemaHistory;
import io.debezium.schema.SchemaChangeEvent;
import io.debezium.util.Clock;

class DamengIncrementalSnapshotChangeEventSourceTest {
    private final DamengConnection reader = mock(DamengConnection.class);
    private final DamengConnection primary = mock(DamengConnection.class);
    private final DamengStandbySupport standby = mock(DamengStandbySupport.class);
    private final DamengDatabaseSchema databaseSchema = mock(DamengDatabaseSchema.class);
    private final DamengPartition partition = new DamengPartition("dameng");
    private NotificationService<DamengPartition, DamengOffsetContext> notifications;
    private DamengIncrementalSnapshotChangeEventSource source;
    private DamengOffsetContext offset;
    private DamengConnectorConfig config;
    private DamengSnapshotChangeEventSourceMetrics metrics;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void before() throws Exception {
        config = new DamengConnectorConfig(DamengConnectorConfigTest.configuration()
                .with("signal.data.collection", "APP.SIGNAL")
                .with("schema.history.internal", MemorySchemaHistory.class.getName()).build());
        notifications = mock(NotificationService.class, RETURNS_DEEP_STUBS);
        metrics = metrics(Clock.SYSTEM);
        source = new DamengIncrementalSnapshotChangeEventSource(config, reader, mock(EventDispatcher.class), databaseSchema,
                Clock.SYSTEM, metrics, mock(DataChangeEventListener.class), notifications);
        offset = new DamengOffsetContext(config, 100);
        source.init(partition, offset);
        when(standby.primary()).thenReturn(primary);
        when(primary.currentLsn()).thenReturn(200L);
        when(reader.quotedTableIdString(any())).thenReturn("\"APP\".\"SIGNAL\"");
        when(reader.isValid()).thenReturn(true);
        source.setStandbySupport(standby);
    }

    @Test
    void shouldCommitThePrimaryWatermarkAndWaitForReplayBeforeReadingReplica() throws Exception {
        source.emitWindowOpen(partition, offset);
        final var order = inOrder(standby, primary, reader);
        order.verify(standby).validate(reader);
        order.verify(primary).prepareUpdate(any(String.class), any());
        order.verify(primary).commit();
        order.verify(primary).currentLsn();
        order.verify(standby).awaitReplay(reader, 200);
        order.verify(reader).commit();
        assertThat(source.getSignalConnection()).isSameAs(primary);
    }

    @Test
    void shouldLeaveTheReplicaReadTransactionUncommittedIfTheLowBarrierFails() throws Exception {
        doThrow(new SQLException("replication stalled", "08006")).when(standby).awaitReplay(reader, 200);
        assertThatThrownBy(() -> source.emitWindowOpen(partition, offset)).isInstanceOf(SQLException.class);
        verify(reader, never()).commit();
    }

    @Test
    void shouldRecognizePrimaryDemotionRacingWithWatermarkInsertWithoutRetryingStableWriteErrors() throws Exception {
        final var denied = new SQLException("Try to modify user database at standby mode", "42000", -710);
        doThrow(denied).when(primary).prepareUpdate(any(String.class), any());
        assertThatThrownBy(() -> source.emitWindowOpen(partition, offset)).isSameAs(denied);
        doNothing().doThrow(new SQLException("primary changed", "08006")).when(standby).validate(reader);
        assertThatThrownBy(() -> source.emitWindowOpen(partition, offset)).isInstanceOf(SQLException.class).hasMessageContaining("primary changed")
                .satisfies(error -> assertThat(((SQLException) error).getSQLState()).isEqualTo("08006"));
        verify(primary, never()).commit();
        verify(reader, never()).commit();
    }

    @Test
    void shouldAbortBeforeSkippingOrAcknowledgingTheCollectionAfterCoordinationFailure() throws Exception {
        final var context = offset.getIncrementalSnapshotContext();
        context.addDataCollectionNamesToSnapshot("request", List.of("APP.DATA"), List.of(), "");
        final var failure = new SQLException("primary changed", "08006");
        doThrow(failure).when(standby).validate(reader);
        assertThatThrownBy(() -> source.init(partition, offset)).hasMessageContaining("must recover its incomplete chunk").hasCause(failure);
        assertThat(context.snapshotRunning()).isTrue();
        assertThat(context.getDataCollections()).hasSize(1);
        assertThat(metrics.getIncrementalSnapshotRunning()).isFalse();
        assertThat(metrics.getIncrementalSnapshotPhase()).isEqualTo("failed");
        assertThat(metrics.getIncrementalSnapshotCurrentTable()).isEqualTo("APP.DATA");
        assertThat(metrics.getIncrementalSnapshotRowsScanned()).isZero();
        assertThat(metrics.getIncrementalSnapshotRowsEnqueued()).isZero();
        assertThat(metrics.getIncrementalSnapshotCurrentCloseWaitDurationMillis()).isZero();
        verifyNoInteractions(notifications);
        verify(primary, never()).commit();
    }

    @Test
    void shouldRetainSingleInstanceSignalRoutingAndErrorPolicy() {
        source.setStandbySupport(null);
        assertThat(source.getSignalConnection()).isSameAs(reader);
        source.handleChunkError(new SQLException("original skip behavior"));
        assertThat(metrics.getIncrementalSnapshotPhase()).isEqualTo("failed");
    }

    @Test
    void shouldExposeAnAlreadyPausedContextOnInitializationWithoutReadingOrWriting() {
        final var context = offset.getIncrementalSnapshotContext();
        context.addDataCollectionNamesToSnapshot("paused-request", List.of("APP.FIRST_TABLE", "APP.SECOND_TABLE"), List.of(), "");
        context.pauseSnapshot();
        clearInvocations(reader, primary, standby, notifications);
        source.init(partition, offset);
        assertThat(metrics.getIncrementalSnapshotRunning()).isTrue();
        assertThat(metrics.getIncrementalSnapshotPaused()).isTrue();
        assertThat(metrics.getIncrementalSnapshotPhase()).isEqualTo("paused");
        assertThat(metrics.getIncrementalSnapshotCurrentTable()).isEqualTo("APP.FIRST_TABLE");
        assertThat(metrics.getIncrementalSnapshotRemainingTableCount()).isEqualTo(2);
        assertThat(metrics.getIncrementalSnapshotRowsScanned()).isZero();
        assertThat(metrics.getIncrementalSnapshotRowsEnqueued()).isZero();
        assertThat(metrics.getIncrementalSnapshotWindowsCompleted()).isZero();
        assertThat(metrics.getIncrementalSnapshotCurrentCloseWaitDurationMillis()).isZero();
        verifyNoInteractions(reader, primary, standby, notifications);
    }

    @Test
    void shouldPruneQueuedExcludedTablesWithoutResettingTheCurrentSnapshotProgress() {
        final var context = offset.getIncrementalSnapshotContext();
        final var condition = AdditionalConditionBuilder.builder().dataCollection(Pattern.compile("APP\\..*")).filter("ID > 4").build();
        context.addDataCollectionNamesToSnapshot("request", List.of("APP.KEPT", "APP.REMOVED", "APP.KEPT_LATER"), List.of(condition), "ID");
        context.startNewChunk();
        final String chunk = context.currentChunkId();
        context.openWindow(chunk + "-open");
        context.sendEvent(new Object[]{ 7 });
        context.nextChunkPosition(new Object[]{ 10 });
        context.maximumKey(new Object[]{ 100 });
        final var currentSchema = table("KEPT");
        context.setSchema(currentSchema);
        context.setSchemaVerificationPassed(true);
        context.pauseSnapshot();
        final var narrowed = filteredConfig("APP\\.(KEPT|KEPT_LATER)");

        assertThat(DamengIncrementalSnapshotChangeEventSource.pruneExcludedSnapshotTables(narrowed, offset))
                .containsExactly(table("REMOVED").id());

        assertThat(context.getDataCollections()).extracting(collection -> collection.getId())
                .containsExactly(table("KEPT").id(), table("KEPT_LATER").id());
        assertThat(context.currentChunkId()).isEqualTo(chunk);
        assertThat(context.deduplicationNeeded()).isTrue();
        assertThat(context.chunkEndPosititon()).containsExactly(10);
        assertThat(context.maximumKey().orElseThrow()).containsExactly(100);
        assertThat(context.getSchema()).isSameAs(currentSchema);
        assertThat(context.isSchemaVerificationPassed()).isTrue();
        assertThat(context.isSnapshotPaused()).isTrue();
        assertThat(context.getCorrelationId()).isEqualTo("request");
        assertThat(context.getDataCollections().get(1).getAdditionalCondition()).contains("(ID > 4)");
        assertThat(context.getDataCollections().get(1).getSurrogateKey()).contains("ID");
        final var restored = SignalBasedIncrementalSnapshotContext.<TableId> load(offset.getOffset(), false);
        assertThat(restored.getDataCollections()).isEqualTo(context.getDataCollections());
        assertThat(restored.chunkEndPosititon()).containsExactly(7);
        assertThat(restored.maximumKey().orElseThrow()).containsExactly(100);
        assertThat(DamengIncrementalSnapshotChangeEventSource.pruneExcludedSnapshotTables(narrowed, offset)).isEmpty();
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldCancelTheExcludedCurrentTableBeforeResumingAnotherPausedTable() {
        final var context = offset.getIncrementalSnapshotContext();
        context.addDataCollectionNamesToSnapshot("request", List.of("APP.REMOVED", "APP.KEPT", "APP.REMOVED_LATER"), List.of(), "ID");
        context.startNewChunk();
        final String oldChunk = context.currentChunkId();
        context.openWindow(oldChunk + "-open");
        context.sendEvent(new Object[]{ 70 });
        context.nextChunkPosition(new Object[]{ 100 });
        context.maximumKey(new Object[]{ 1000 });
        context.setSchema(table("REMOVED"));
        context.setSchemaVerificationPassed(true);
        context.pauseSnapshot();
        final var narrowed = filteredConfig("APP\\.KEPT");
        source = new DamengIncrementalSnapshotChangeEventSource(narrowed, reader, mock(EventDispatcher.class), mock(DamengDatabaseSchema.class),
                Clock.SYSTEM, metrics, mock(DataChangeEventListener.class), notifications);
        clearInvocations(reader, primary, standby, notifications);

        source.init(partition, offset);

        assertThat(context.getDataCollections()).extracting(collection -> collection.getId()).containsExactly(table("KEPT").id());
        assertThat(context.currentChunkId()).isNotEqualTo(oldChunk);
        assertThat(context.openWindow(oldChunk + "-open")).isFalse();
        assertThat(context.closeWindow(oldChunk + "-close")).isFalse();
        assertThat(context.deduplicationNeeded()).isFalse();
        assertThat(context.chunkEndPosititon()).isNull();
        assertThat(context.maximumKey()).isEmpty();
        assertThat(context.getSchema()).isNull();
        assertThat(context.isSchemaVerificationPassed()).isFalse();
        assertThat(context.isSnapshotPaused()).isTrue();
        assertThat(context.getCorrelationId()).isEqualTo("request");
        assertThat(metrics.getIncrementalSnapshotCurrentTable()).isEqualTo("APP.KEPT");
        assertThat(metrics.getIncrementalSnapshotRemainingTableCount()).isEqualTo(1);
        assertThat(metrics.getIncrementalSnapshotPaused()).isTrue();
        final var restored = SignalBasedIncrementalSnapshotContext.<TableId> load(offset.getOffset(), false);
        assertThat(restored.currentDataCollectionId().getId()).isEqualTo(table("KEPT").id());
        assertThat(restored.chunkEndPosititon()).isNull();
        assertThat(restored.maximumKey()).isEmpty();
        assertThat(offset.getLsn()).isEqualTo(100);
        assertThat(offset.getCommitLsn()).isEqualTo(100);
        assertThat(offset.getEventSerialNo()).isZero();
        verifyNoInteractions(reader, primary, standby, notifications);
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldFinishWhenTheNarrowedScopeRemovesEveryPendingSnapshotTable() {
        final var context = offset.getIncrementalSnapshotContext();
        context.addDataCollectionNamesToSnapshot("request", List.of("APP.REMOVED", "APP.REMOVED_LATER"), List.of(), "");
        context.maximumKey(new Object[]{ 100 });
        context.sendEvent(new Object[]{ 7 });
        context.pauseSnapshot();
        final var narrowed = filteredConfig("APP\\.KEPT");
        source = new DamengIncrementalSnapshotChangeEventSource(narrowed, reader, mock(EventDispatcher.class), mock(DamengDatabaseSchema.class),
                Clock.SYSTEM, metrics, mock(DataChangeEventListener.class), notifications);
        clearInvocations(reader, primary, standby, notifications);

        source.init(partition, offset);

        assertThat(context.snapshotRunning()).isFalse();
        assertThat(context.currentDataCollectionId()).isNull();
        assertThat(context.maximumKey()).isEmpty();
        assertThat(context.chunkEndPosititon()).isNull();
        assertThat(context.getCorrelationId()).isNull();
        assertThat(context.isSnapshotPaused()).isFalse();
        assertThat(metrics.getIncrementalSnapshotRunning()).isFalse();
        assertThat(metrics.getIncrementalSnapshotCurrentTable()).isEmpty();
        assertThat(metrics.getIncrementalSnapshotPhase()).isEqualTo("idle");
        assertThat(SignalBasedIncrementalSnapshotContext.<TableId> load(offset.getOffset(), false).snapshotRunning()).isFalse();
        assertThat(offset.getLsn()).isEqualTo(100);
        assertThat(offset.getCommitLsn()).isEqualTo(100);
        verifyNoInteractions(reader, primary, standby, notifications);
    }

    @Test
    void shouldRemoveQuotedTableNamesWithoutConfusingDotsWithSchemaSeparators() {
        final var context = offset.getIncrementalSnapshotContext();
        final var kept = new TableId(null, "APP", "KEPT.DOT");
        final var removed = new TableId(null, "APP", "REMOVED.DOT");
        context.addDataCollectionNamesToSnapshot("request", List.of(kept.toDoubleQuotedString(), removed.toDoubleQuotedString()), List.of(), "");

        assertThat(DamengIncrementalSnapshotChangeEventSource.pruneExcludedSnapshotTables(filteredConfig("APP\\.KEPT\\.DOT"), offset))
                .containsExactly(removed);
        assertThat(context.getDataCollections()).extracting(collection -> collection.getId()).containsExactly(kept);
    }

    @Test
    void shouldLeaveEmptyAndUnchangedSnapshotContextsAlone() {
        assertThat(DamengIncrementalSnapshotChangeEventSource.pruneExcludedSnapshotTables(config, null)).isEmpty();
        assertThat(DamengIncrementalSnapshotChangeEventSource.pruneExcludedSnapshotTables(config, offset)).isEmpty();
        final var context = offset.getIncrementalSnapshotContext();
        context.addDataCollectionNamesToSnapshot("request", List.of("APP.KEPT"), List.of(), "");
        context.maximumKey(new Object[]{ 100 });
        context.nextChunkPosition(new Object[]{ 10 });
        assertThat(DamengIncrementalSnapshotChangeEventSource.pruneExcludedSnapshotTables(config, offset)).isEmpty();
        assertThat(context.currentDataCollectionId().getId()).isEqualTo(table("KEPT").id());
        assertThat(context.maximumKey().orElseThrow()).containsExactly(100);
        assertThat(context.chunkEndPosititon()).containsExactly(10);
    }

    @Test
    void shouldIgnoreAnEntireSnapshotRequestContainingAnExplicitPendingTable() throws Exception {
        final var change = mock(DamengCaptureScopeChange.class);
        when(change.pendingTables()).thenReturn(List.of(table("PENDING").id()));
        source.setCaptureScopeChange(change);
        final var before = offset.getOffset();
        clearInvocations(reader, primary, standby, notifications, databaseSchema);

        request("APP.KEPT", "APP.PENDING");

        assertThat(offset.getIncrementalSnapshotContext().snapshotRunning()).isFalse();
        assertThat(offset.getIncrementalSnapshotContext().getDataCollections()).isEmpty();
        assertThat(offset.getOffset()).isEqualTo(before);
        verifyNoInteractions(reader, primary, standby, notifications, databaseSchema);
    }

    @Test
    void shouldRejectARegexMatchingPendingTablesBeforeTheyAreKnownToTheSchema() throws Exception {
        final var change = mock(DamengCaptureScopeChange.class);
        when(change.pendingTables()).thenReturn(List.of(table("PENDING").id()));
        source.setCaptureScopeChange(change);
        final var context = offset.getIncrementalSnapshotContext();
        context.addDataCollectionNamesToSnapshot("existing-request", List.of("APP.KEPT"), List.of(), "ID");
        context.maximumKey(new Object[]{ 100 });
        context.nextChunkPosition(new Object[]{ 10 });
        context.pauseSnapshot();
        clearInvocations(reader, primary, standby, notifications, databaseSchema);

        request("APP\\..*");

        assertThat(context.getDataCollections()).extracting(collection -> collection.getId()).containsExactly(table("KEPT").id());
        assertThat(context.getCorrelationId()).isEqualTo("existing-request");
        assertThat(context.maximumKey().orElseThrow()).containsExactly(100);
        assertThat(context.chunkEndPosititon()).containsExactly(10);
        assertThat(context.isSnapshotPaused()).isTrue();
        verifyNoInteractions(reader, primary, standby, notifications, databaseSchema);
    }

    @Test
    void shouldRejectAQuotedPendingTableWhoseNameContainsADot() throws Exception {
        final var pending = table("PENDING.DOT").id();
        final var change = mock(DamengCaptureScopeChange.class);
        when(change.pendingTables()).thenReturn(List.of(pending));
        source.setCaptureScopeChange(change);
        clearInvocations(reader, primary, standby, notifications, databaseSchema);

        request(pending.toDoubleQuotedString());

        assertThat(offset.getIncrementalSnapshotContext().snapshotRunning()).isFalse();
        assertThat(offset.getIncrementalSnapshotContext().getDataCollections()).isEmpty();
        verifyNoInteractions(reader, primary, standby, notifications, databaseSchema);
    }

    @Test
    void shouldFailBeforeAnySchemaReadWhenTheRestoredSnapshotContainsAPendingTable() {
        final var change = mock(DamengCaptureScopeChange.class);
        when(change.isPending(table("PENDING").id())).thenReturn(true);
        source.setCaptureScopeChange(change);
        final var context = offset.getIncrementalSnapshotContext();
        context.addDataCollectionNamesToSnapshot("request", List.of("APP.KEPT", "APP.PENDING"), List.of(), "");
        context.pauseSnapshot();
        clearInvocations(reader, primary, standby, notifications, databaseSchema);

        assertThatThrownBy(() -> source.init(partition, offset))
                .isInstanceOf(ConnectException.class).hasMessageContaining("APP.PENDING").hasMessageContaining("boundary S");

        assertThat(context.getDataCollections()).hasSize(2);
        assertThat(context.isSnapshotPaused()).isTrue();
        assertThat(context.getCorrelationId()).isEqualTo("request");
        verifyNoInteractions(reader, primary, standby, notifications, databaseSchema);
    }

    @Test
    void shouldAcceptSnapshotRequestsOnceAllRequestedTablesHaveBeenActivated() throws Exception {
        final var change = mock(DamengCaptureScopeChange.class);
        when(change.pendingTables()).thenReturn(List.of(table("UNRELATED_PENDING").id()));
        source.setCaptureScopeChange(change);
        final var context = offset.getIncrementalSnapshotContext();
        context.addDataCollectionNamesToSnapshot("existing-request", List.of("APP.KEPT"), List.of(), "");
        context.pauseSnapshot();
        clearInvocations(reader, primary, standby, notifications, databaseSchema);

        request("APP.ACTIVATED");

        assertThat(context.getDataCollections()).extracting(collection -> collection.getId())
                .containsExactly(table("KEPT").id(), table("ACTIVATED").id());
        assertThat(context.isSnapshotPaused()).isTrue();
        verify(databaseSchema).tableIds();
        verifyNoInteractions(reader, primary, standby, notifications);
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldMeasureRealChunkReadsAndEnqueuesAcrossTwoTablesAndClearCompletedState() throws Exception {
        final var nanos = new AtomicLong();
        final Clock clock = () -> TimeUnit.NANOSECONDS.toMillis(nanos.get());
        metrics = metrics(clock);
        final var first = table("FIRST_TABLE");
        final var second = table("SECOND_TABLE");
        final ChunkQueryBuilder<TableId> queries = mock(ChunkQueryBuilder.class);
        doReturn(queries).when(reader).chunkQueryBuilder(config);
        when(queries.prepareTable(any(), any())).thenAnswer(invocation -> invocation.getArgument(1));
        when(queries.getQueryColumns(any(), any())).thenAnswer(invocation -> ((Table) invocation.getArgument(1)).primaryKeyColumns());
        when(queries.buildChunkQuery(any(), any(), any())).thenReturn("SELECT CHUNK");
        when(queries.buildMaxPrimaryKeyQuery(any(), any(), any())).thenReturn("SELECT MAXIMUM KEY");
        doAnswer(invocation -> {
            final var current = offset.getIncrementalSnapshotContext().currentDataCollectionId().getId();
            assertThat(metrics.getIncrementalSnapshotCurrentTable()).isEqualTo(current.identifier());
            assertThat(metrics.getIncrementalSnapshotPhase()).isEqualTo("preparing");
            return new Object[]{ 2 };
        }).when(reader).queryAndMap(anyString(), any());
        final var statement = mock(PreparedStatement.class);
        when(queries.readTableChunkStatement(any(), any(), anyString())).thenReturn(statement);
        final var firstRows = rows(true);
        final var firstEnd = rows(false);
        final var secondRows = rows(true);
        final var secondEnd = rows(false);
        when(statement.executeQuery()).thenReturn(firstRows, firstEnd, secondRows, secondEnd);
        when(reader.rowToArray(any(), any(), any())).thenAnswer(invocation -> {
            nanos.addAndGet(TimeUnit.MILLISECONDS.toNanos(7));
            return new Object[]{ ((Table) invocation.getArgument(0)).id().equals(first.id()) ? 1 : 2, "snapshot" };
        });
        try (var schema = schema(first, second)) {
            final EventDispatcher<DamengPartition, TableId> dispatcher = mock(EventDispatcher.class);
            final EventDispatcher.SnapshotReceiver<DamengPartition> receiver = mock(EventDispatcher.SnapshotReceiver.class);
            when(dispatcher.getIncrementalSnapshotChangeEventReceiver(any())).thenReturn(receiver);
            doAnswer(invocation -> {
                nanos.addAndGet(TimeUnit.MILLISECONDS.toNanos(3));
                return null;
            }).when(receiver).changeRecord(any(), any(), any(), any(), any(), any(), any());
            doAnswer(invocation -> {
                final TableId tableId = invocation.getArgument(1);
                assertThat(metrics.getIncrementalSnapshotCurrentTable()).isEqualTo(tableId.identifier());
                final ChangeRecordEmitter<DamengPartition> emitter = invocation.getArgument(2);
                emitter.emitChangeRecords(schema.schemaFor(tableId), receiver);
                return null;
            }).when(dispatcher).dispatchSnapshotEvent(any(), any(), any(), any());
            source = new DamengIncrementalSnapshotChangeEventSource(config, reader, dispatcher, schema, clock, metrics,
                    mock(DataChangeEventListener.class), notifications);
            source.init(partition, offset);
            final var context = offset.getIncrementalSnapshotContext();
            context.addDataCollectionNamesToSnapshot("request", List.of(first.id().identifier(), second.id().identifier()), List.of(), "");
            source.readChunk(partition, offset);
            assertThat(metrics.getIncrementalSnapshotRunning()).isTrue();
            assertThat(metrics.getIncrementalSnapshotCurrentTable()).isEqualTo(first.id().identifier());
            assertThat(metrics.getIncrementalSnapshotRowsScanned()).isEqualTo(1);
            assertThat(metrics.getIncrementalSnapshotRowsEnqueued()).isZero();
            assertThat(metrics.getIncrementalSnapshotLastQueryDurationMillis()).isEqualTo(7);
            assertThat(metrics.getIncrementalSnapshotPhase()).isEqualTo("waiting_close");
            nanos.addAndGet(TimeUnit.MILLISECONDS.toNanos(25));
            source.pauseSnapshot(partition, offset);
            assertThat(metrics.getIncrementalSnapshotPaused()).isTrue();
            assertThat(metrics.getIncrementalSnapshotPhase()).isEqualTo("paused");
            source.closeWindow(partition, context.currentChunkId() + "-close", offset);
            assertThat(metrics.getIncrementalSnapshotRowsScanned()).isEqualTo(1);
            assertThat(metrics.getIncrementalSnapshotRowsEnqueued()).isEqualTo(1);
            assertThat(metrics.getIncrementalSnapshotPaused()).isTrue();
            source.resumeSnapshot(partition, offset);
            assertThat(metrics.getIncrementalSnapshotPaused()).isFalse();
            assertThat(metrics.getIncrementalSnapshotRowsScanned()).isEqualTo(2);
            assertThat(metrics.getIncrementalSnapshotRowsEnqueued()).isEqualTo(1);
            assertThat(metrics.getIncrementalSnapshotWindowsCompleted()).isEqualTo(1);
            assertThat(metrics.getIncrementalSnapshotCurrentTable()).isEqualTo(second.id().identifier());
            assertThat(metrics.getIncrementalSnapshotLastCloseWaitDurationMillis()).isEqualTo(25);
            source.closeWindow(partition, context.currentChunkId() + "-close", offset);
            assertThat(metrics.getIncrementalSnapshotRowsScanned()).isEqualTo(2);
            assertThat(metrics.getIncrementalSnapshotRowsEnqueued()).isEqualTo(2);
            assertThat(metrics.getIncrementalSnapshotWindowsCompleted()).isEqualTo(2);
            assertThat(metrics.getIncrementalSnapshotRunning()).isFalse();
            assertThat(metrics.getIncrementalSnapshotCurrentTable()).isEmpty();
            assertThat(metrics.getIncrementalSnapshotPhase()).isEqualTo("idle");
            final long lastEmit = metrics.getIncrementalSnapshotLastEmitDurationMillis();
            assertThat(lastEmit).isEqualTo(3);
            final var terminalClose = context.currentChunkId() + "-close";
            source.closeWindow(partition, terminalClose, offset);
            source.closeWindow(partition, terminalClose, offset);
            assertThat(metrics.getIncrementalSnapshotWindowsCompleted()).isEqualTo(2);
            assertThat(metrics.getIncrementalSnapshotRowsEnqueued()).isEqualTo(2);
            assertThat(metrics.getIncrementalSnapshotLastEmitDurationMillis()).isEqualTo(lastEmit);
            assertThat(metrics.getIncrementalSnapshotRunning()).isFalse();
            assertThat(metrics.getIncrementalSnapshotCurrentTable()).isEmpty();
            assertThat(metrics.getIncrementalSnapshotPhase()).isEqualTo("idle");
        }
    }

    @Test
    void shouldCountAnIncrementalRowOnlyAfterTheReceiverAcceptsIt() throws Exception {
        final var table = table("ENQUEUE");
        try (var schema = schema(table)) {
            offset.event(table.id(), Instant.EPOCH);
            source.getChangeRecordEmitter(partition, table.id(), offset, new Object[]{ 1, "accepted" })
                    .emitChangeRecords(schema.schemaFor(table.id()), (eventPartition, collection, operation, key, value, eventOffset, headers) -> {
                        assertThat(value.getString("op")).isEqualTo("r");
                        assertThat(value.getStruct("after").getInt32("ID")).isEqualTo(1);
                        assertThat(metrics.getIncrementalSnapshotRowsEnqueued()).isZero();
                    });
            assertThat(metrics.getIncrementalSnapshotRowsEnqueued()).isEqualTo(1);
            assertThatThrownBy(() -> source.getChangeRecordEmitter(partition, table.id(), offset, new Object[]{ 2, "interrupted" })
                    .emitChangeRecords(schema.schemaFor(table.id()), (eventPartition, collection, operation, key, value, eventOffset, headers) -> {
                        throw new InterruptedException("queue stopped");
                    })).isInstanceOf(InterruptedException.class).hasMessage("queue stopped");
            assertThat(metrics.getIncrementalSnapshotRowsEnqueued()).isEqualTo(1);
        }
    }

    @Test
    void shouldNotCountAnIncrementalRowWhenColumnConversionFailsBeforeEnqueue() throws Exception {
        final var table = table("CONVERSION");
        try (var schema = schema(table)) {
            offset.event(table.id(), Instant.EPOCH);
            final var received = new AtomicLong();
            assertThatThrownBy(() -> source.getChangeRecordEmitter(partition, table.id(), offset, new Object[]{ "invalid integer", "failed" })
                    .emitChangeRecords(schema.schemaFor(table.id()),
                            (eventPartition, collection, operation, key, value, eventOffset, headers) -> received.incrementAndGet()))
                    .isInstanceOf(RuntimeException.class);
            assertThat(received).hasValue(0);
            assertThat(metrics.getIncrementalSnapshotRowsEnqueued()).isZero();
        }
    }

    private DamengConnectorConfig filteredConfig(String includeList) {
        return new DamengConnectorConfig(config.getConfig().edit().with("table.include.list", includeList).build());
    }

    private void request(String... tables) throws InterruptedException {
        source.addDataCollectionNamesToSnapshot(
                new SignalPayload<>(partition, "new-request", "execute-snapshot", Document.create(), offset, Map.of()),
                SnapshotConfiguration.Builder.builder().dataCollections(List.of(tables)).surrogateKey("").build());
    }

    private DamengSnapshotChangeEventSourceMetrics metrics(Clock clock) {
        final var taskContext = spy(new DamengTaskContext(config.getConfig(), config));
        when(taskContext.getClock()).thenReturn(clock);
        return new DamengSnapshotChangeEventSourceMetrics(taskContext, mock(ChangeEventQueueMetrics.class), new DamengEventMetadataProvider());
    }

    private DamengDatabaseSchema schema(Table... tables) {
        final var schema = new DamengDatabaseSchema(config, new DamengValueConverters(config), config.schemaNameAdjuster(),
                config.getTopicNamingStrategy(CommonConnectorConfig.TOPIC_NAMING_STRATEGY), new CustomConverterRegistry(List.of()),
                new DamengTaskContext(config.getConfig(), config));
        for (var table : tables) {
            schema.applySchemaChange(SchemaChangeEvent.ofSnapshotCreate(partition, offset, "DAMENG", table));
        }
        return schema;
    }

    private static Table table(String name) {
        return Table.editor().tableId(new TableId(null, "APP", name))
                .addColumn(Column.editor().name("ID").position(1).jdbcType(Types.INTEGER).type("INTEGER").optional(false).create())
                .addColumn(Column.editor().name("NAME").position(2).jdbcType(Types.VARCHAR).type("VARCHAR").length(100).create())
                .setPrimaryKeyNames("ID").create();
    }

    private static ResultSet rows(boolean populated) throws SQLException {
        final var rows = mock(ResultSet.class);
        final var metadata = mock(ResultSetMetaData.class);
        when(metadata.getColumnCount()).thenReturn(2);
        when(metadata.getColumnName(1)).thenReturn("ID");
        when(metadata.getColumnName(2)).thenReturn("NAME");
        when(rows.getMetaData()).thenReturn(metadata);
        when(rows.next()).thenReturn(populated, false);
        return rows;
    }
}
