/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Types;
import java.time.Instant;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.connector.base.ChangeEventQueueMetrics;
import io.debezium.jdbc.MainConnectionProvidingConnectionFactory;
import io.debezium.junit.logging.LogInterceptor;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.source.SnapshottingTask;
import io.debezium.pipeline.source.spi.ChangeEventSource.ChangeEventSourceContext;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.relational.Column;
import io.debezium.relational.RelationalSnapshotChangeEventSource.RelationalSnapshotContext;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables.TableFilter;
import io.debezium.snapshot.SnapshotterService;
import io.debezium.snapshot.spi.SnapshotQuery;
import io.debezium.util.Clock;

import ch.qos.logback.classic.Logger;

class DamengSnapshotChangeEventSourceTest {
    private static final TableId EXISTING = new TableId(null, "APP", "EXISTING");
    private static final TableId ADDED = new TableId(null, "APP", "ADDED");
    private DamengConnection connection;
    private ChangeEventSourceContext running;
    private SnapshotProgressListener<DamengPartition> listener;
    private SnapshotterService snapshotterService;
    private Clock clock;
    private final LogInterceptor logs = LogInterceptor.forClass(DamengSnapshotChangeEventSource.class);

    @AfterEach
    void stopCapturingLogs() {
        logs.stop();
        ((Logger) LoggerFactory.getLogger(DamengSnapshotChangeEventSource.class)).detachAppender(logs);
    }

    @BeforeEach
    @SuppressWarnings("unchecked")
    void before() throws Exception {
        connection = mock(DamengConnection.class);
        running = mock(ChangeEventSourceContext.class);
        listener = mock(SnapshotProgressListener.class);
        snapshotterService = mock(SnapshotterService.class);
        clock = Clock.SYSTEM;
        when(connection.quotedTableIdString(any())).thenAnswer(invocation -> ((TableId) invocation.getArgument(0)).toDoubleQuotedString());
        when(connection.currentLsn()).thenReturn(100L, 200L, 300L);
        when(connection.databaseMagic()).thenReturn(42L);
        when(running.isRunning()).thenReturn(true);
    }

    @Test
    void shouldHoldPrimaryLocksBeforeChoosingAReplayedStandbySnapshotBoundary() throws Exception {
        final var source = source(DamengConnectorConfigTest.configuration().with("database.standby.hosts", "replica:5236").build());
        final var primary = mock(DamengConnection.class);
        final var standby = mock(DamengStandbySupport.class);
        when(standby.primary()).thenReturn(primary);
        when(primary.quotedTableIdString(EXISTING)).thenReturn(EXISTING.toDoubleQuotedString());
        when(standby.snapshotBoundary(connection)).thenReturn(432L);
        source.setStandbySupport(standby);
        final var context = context();
        source.lockTablesForSchemaSnapshot(running, context);
        source.determineSnapshotOffset(context, null);
        final var order = inOrder(primary, standby, connection);
        order.verify(primary).executeWithoutCommitting("LOCK TABLE \"APP\".\"EXISTING\" IN SHARE MODE");
        order.verify(standby).snapshotBoundary(connection);
        order.verify(connection).validatePrimaryStandbyIdentity(context.offset);
        assertThat(context.offset.getLsn()).isEqualTo(432);
        verify(connection, never()).executeWithoutCommitting(any());
        verify(primary, never()).commit();
        doThrow(new SQLException("primary changed", "08006")).when(standby).validate(connection);
        assertThatThrownBy(() -> source.releaseDataSnapshotLocks(context)).isInstanceOf(SQLException.class).hasMessageContaining("primary changed");
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldPersistOneRecoveryBaselineWithoutPublishingIndividualSchemaEvents() throws Exception {
        final var config = new DamengConnectorConfig(DamengConnectorConfigTest.configuration().with("recovery.restart.nonce", "first").build());
        final var recovery = mock(DamengRecovery.class);
        when(recovery.isRequested()).thenReturn(true);
        final var schema = mock(DamengDatabaseSchema.class);
        final var dispatcher = mock(EventDispatcher.class);
        final MainConnectionProvidingConnectionFactory<DamengConnection> factory = mock(MainConnectionProvidingConnectionFactory.class);
        when(factory.mainConnection()).thenReturn(connection);
        final var source = new DamengSnapshotChangeEventSource(config, factory, schema, dispatcher, clock, listener,
                mock(NotificationService.class), snapshotterService, recovery);
        final var context = context();
        context.offset = new DamengOffsetContext(config, 100);
        final var table = Table.editor().tableId(EXISTING).create();
        context.tables.overwriteTable(table);
        final var task = source.getSnapshottingTask(context.partition, null);
        assertThat(task.snapshotSchema()).isTrue();
        assertThat(task.snapshotData()).isFalse();
        source.createSchemaChangeEventsForTables(running, context, task);
        final var order = inOrder(connection, schema, recovery);
        order.verify(connection).validateUnchangedPrimary();
        order.verify(connection).validateUnchangedCompatibility();
        order.verify(schema).registerRecoveryTables(List.of(table));
        order.verify(recovery).persist(context.offset, List.of(table));
        org.mockito.Mockito.verifyNoInteractions(dispatcher);
    }

    @ParameterizedTest
    @ValueSource(strings = { "stopped", "missing", "history" })
    @SuppressWarnings("unchecked")
    void shouldAbortRecoveryBeforeAnyEventIfBaselineCannotBeEstablished(String fault) throws Exception {
        final var config = new DamengConnectorConfig(DamengConnectorConfigTest.configuration().build());
        final var recovery = mock(DamengRecovery.class);
        when(recovery.isRequested()).thenReturn(true);
        final var dispatcher = mock(EventDispatcher.class);
        final MainConnectionProvidingConnectionFactory<DamengConnection> factory = mock(MainConnectionProvidingConnectionFactory.class);
        when(factory.mainConnection()).thenReturn(connection);
        final var source = new DamengSnapshotChangeEventSource(config, factory, mock(DamengDatabaseSchema.class), dispatcher, clock, listener,
                mock(NotificationService.class), snapshotterService, recovery);
        final var context = context();
        context.offset = new DamengOffsetContext(config, 100);
        if (!fault.equals("missing")) {
            context.tables.overwriteTable(Table.editor().tableId(EXISTING).create());
        }
        when(running.isRunning()).thenReturn(!fault.equals("stopped"));
        doAnswer(invocation -> {
            final DamengOffsetContext candidate = invocation.getArgument(0);
            candidate.setRecoveryBaseline("first", candidate.getLsn());
            throw new org.apache.kafka.connect.errors.ConnectException("history unavailable");
        }).when(recovery).persist(any(), any());
        assertThatThrownBy(() -> source.createSchemaChangeEventsForTables(running, context, source.getSnapshottingTask(context.partition, null)))
                .hasMessageContaining(switch (fault) {
                    case "stopped" -> "Interrupted";
                    case "missing" -> "Missing table structure";
                    default -> "history unavailable";
                });
        org.mockito.Mockito.verifyNoInteractions(dispatcher);
        assertThat(context.offset.getRecoveryNonce()).as("an aborted notification must not acknowledge an unpersisted generation").isNull();
        if (!fault.equals("history")) {
            verify(recovery, never()).persist(any(), any());
        }
    }

    @Test
    void shouldLogEachAcquisitionAndReleaseOnlyAfterTheTransactionRollsBack() throws Exception {
        final var nanos = new AtomicLong();
        clock = mock(Clock.class);
        when(clock.currentTimeInNanos()).thenAnswer(invocation -> nanos.get());
        final var source = source(DamengConnectorConfigTest.configuration().build());
        final var context = context();
        context.capturedTables.add(ADDED);
        doAnswer(invocation -> {
            assertThat(logs.containsMessage("Dameng snapshot acquiring shared lock on table")).isTrue();
            nanos.addAndGet(TimeUnit.MILLISECONDS.toNanos(25));
            return null;
        }).when(connection).executeWithoutCommitting(any());
        source.lockTablesForSchemaSnapshot(running, context);
        assertThat(logs.containsMessage("acquired shared lock on table APP.EXISTING in 25 ms (currently locked tables=2)")).isTrue();
        nanos.addAndGet(TimeUnit.MILLISECONDS.toNanos(100));
        source.releaseSchemaSnapshotLocks(context);
        source.releaseDataSnapshotLocks(context);
        assertThat(logs.containsMessage("shared table locks released")).isFalse();
        final var jdbc = mock(Connection.class);
        doAnswer(invocation -> {
            assertThat(logs.containsMessage("shared table locks released")).isFalse();
            nanos.addAndGet(TimeUnit.MILLISECONDS.toNanos(10));
            return null;
        }).when(jdbc).rollback();
        source.rollbackTransaction(jdbc);
        assertThat(logs.containsMessage("shared table locks released (tables=2, total acquisition time=50 ms, "
                + "maximum single-table acquisition time=25 ms, holding time=135 ms)")).isTrue();
        verify(jdbc).rollback();
    }

    @Test
    void shouldLogFailedAcquisitionAndNotCountTheFailedTableAsLocked() throws Exception {
        final var source = source(DamengConnectorConfigTest.configuration().build());
        doThrow(new SQLException("lock timed out")).when(connection).executeWithoutCommitting(any());
        assertThatThrownBy(() -> source.lockTablesForSchemaSnapshot(running, context())).isInstanceOf(SQLException.class);
        assertThat(logs.containsWarnMessage("could not acquire shared lock on table APP.EXISTING")).isTrue();
        assertThat(logs.containsWarnMessage("currently locked tables=0")).isTrue();
        source.rollbackTransaction(mock(Connection.class));
        assertThat(logs.containsMessage("shared table locks released (tables=0")).isTrue();
    }

    @Test
    void shouldKeepObservedLocksAndWarnWhenRollbackFails() throws Exception {
        final var raw = DamengConnectorConfigTest.configuration().build();
        final var config = new DamengConnectorConfig(raw);
        final var metrics = new DamengSnapshotChangeEventSourceMetrics(new DamengTaskContext(raw, config),
                mock(ChangeEventQueueMetrics.class), new DamengEventMetadataProvider());
        listener = metrics;
        final var source = source(raw);
        source.lockTablesForSchemaSnapshot(running, context());
        final var jdbc = mock(Connection.class);
        doThrow(new SQLException("connection lost")).when(jdbc).rollback();
        assertThatThrownBy(() -> source.rollbackTransaction(jdbc)).isInstanceOf(RuntimeException.class).hasCauseInstanceOf(SQLException.class);
        assertThat(metrics.getSnapshotLocksHeld()).isTrue();
        assertThat(metrics.getLockedTableCount()).isEqualTo(1);
        assertThat(logs.containsWarnMessage("table lock release is unconfirmed")).isTrue();
        assertThat(logs.containsMessage("shared table locks released")).isFalse();
        source.rollbackTransaction(null);
        assertThat(metrics.getSnapshotLocksHeld()).isTrue();
    }

    @Test
    void shouldLockNewlyDiscoveredTablesBeforeAdvancingSnapshotPosition() throws Exception {
        final var source = source(DamengConnectorConfigTest.configuration().with("table.include.list", "APP\\.EXISTING,APP\\.ADDED").build());
        final var context = context();
        when(connection.getAllTableIds(null)).thenReturn(Set.of(EXISTING, ADDED));

        source.lockTablesForSchemaSnapshot(running, context);
        source.determineSnapshotOffset(context, null);
        source.readTableStructure(running, context, null, new SnapshottingTask(true, true, List.of(), Map.of(), false));

        assertThat(context.capturedTables).containsExactly(ADDED, EXISTING);
        assertThat(context.offset.getLsn()).isEqualTo(200);
        final var order = inOrder(connection);
        order.verify(connection).executeWithoutCommitting("LOCK TABLE \"APP\".\"EXISTING\" IN SHARE MODE");
        order.verify(connection).currentLsn();
        order.verify(connection).getAllTableIds(null);
        order.verify(connection).executeWithoutCommitting("LOCK TABLE \"APP\".\"ADDED\" IN SHARE MODE");
        order.verify(connection).currentLsn();
        order.verify(connection).getAllTableIds(null);
        verify(listener).monitoredDataCollectionsDetermined(context.partition, context.capturedTables);
        assertThat(logs.containsWarnMessage("table.include.list rule")).isFalse();
    }

    @Test
    void shouldWarnAboutAnUndiscoveredConfiguredTableAndLogOnlySelectedTables() throws Exception {
        final var source = source(DamengConnectorConfigTest.configuration()
                .with("table.include.list", "APP\\.EXISTING,APP\\.MISSING").with("database.user", "CDC_USER").build());
        final var context = context();
        context.capturedSchemaTables.add(new TableId(null, "SYSDBA", "##PLAN_TABLE"));
        when(connection.getAllTableIds(null)).thenReturn(Set.of(EXISTING));

        source.determineSnapshotOffset(context, null);
        source.readTableStructure(running, context, null, new SnapshottingTask(true, true, List.of(), Map.of(), false));

        assertThat(logs.containsWarnMessage("table.include.list rule 'APP\\.MISSING' did not select any table visible to database user 'CDC_USER'")).isTrue();
        assertThat(logs.containsWarnMessage("table.include.list rule 'APP\\.EXISTING'")).isFalse();
        assertThat(logs.containsWarnMessage("metadata/SELECT privileges")).isTrue();
        assertThat(logs.containsMessage("Dameng tables selected for schema capture: [APP.EXISTING]")).isTrue();
        assertThat(logs.containsMessage("Dameng tables selected for data snapshot: [APP.EXISTING]")).isTrue();
        assertThat(context.offset.getLsn()).isEqualTo(100);
    }

    @Test
    void shouldWarnAboutUnmatchedFutureTablePatternsWithoutStoppingTheSnapshot() throws Exception {
        final var source = source(DamengConnectorConfigTest.configuration()
                .with("table.include.list", "APP\\.EXISTING,APP\\.FUTURE_.*").build());
        final var context = context();
        when(connection.getAllTableIds(null)).thenReturn(Set.of(EXISTING));

        source.determineSnapshotOffset(context, null);
        source.readTableStructure(running, context, null, new SnapshottingTask(true, true, List.of(), Map.of(), false));

        assertThat(logs.containsWarnMessage("table.include.list rule 'APP\\.FUTURE_.*'")).isTrue();
        assertThat(logs.containsWarnMessage("future tables matching the capture filters can still be captured")).isTrue();
        assertThat(context.capturedTables).containsExactly(EXISTING);
    }

    @Test
    void shouldUseFullNameMatchingWhenWarningAboutIncludePatterns() throws Exception {
        final var source = source(DamengConnectorConfigTest.configuration().with("table.include.list", "APP\\.EXIST").build());
        final var context = context();
        context.capturedTables.clear();
        context.capturedSchemaTables.clear();
        when(connection.getAllTableIds(null)).thenReturn(Set.of(EXISTING));

        source.determineSnapshotOffset(context, null);
        source.readTableStructure(running, context, null, new SnapshottingTask(true, true, List.of(), Map.of(), false));

        assertThat(logs.containsWarnMessage("table.include.list rule 'APP\\.EXIST'")).isTrue();
        assertThat(logs.containsMessage("Dameng tables selected for schema capture: []")).isTrue();
        assertThat(logs.containsMessage("Dameng tables selected for data snapshot: []")).isTrue();
    }

    @Test
    void shouldPreserveCaseInsensitiveMatchingQuantifiersAndEscapedCommasInDiagnostics() throws Exception {
        final var table = new TableId(null, "APP", "ORDER,LOG12");
        final var source = source(DamengConnectorConfigTest.configuration().with("table.include.list", "app\\.order\\,log\\d{1\\,3}").build());
        final var context = context();
        context.capturedTables = new LinkedHashSet<>(Set.of(table));
        context.capturedSchemaTables = new LinkedHashSet<>(Set.of(table));
        when(connection.getAllTableIds(null)).thenReturn(Set.of(table));

        source.determineSnapshotOffset(context, null);
        source.readTableStructure(running, context, null, new SnapshottingTask(true, true, List.of(), Map.of(), false));

        assertThat(logs.containsWarnMessage("table.include.list rule")).isFalse();
        assertThat(logs.containsMessage("Dameng tables selected for schema capture: [APP.ORDER,LOG12]")).isTrue();
    }

    @Test
    void shouldNotWarnForSchemaTablesExcludedOnlyFromTheRowSnapshot() throws Exception {
        final var source = source(DamengConnectorConfigTest.configuration().with("table.include.list", "APP\\.EXISTING,APP\\.ADDED").build());
        final var context = context();
        context.capturedSchemaTables.add(ADDED);
        when(connection.getAllTableIds(null)).thenReturn(Set.of(EXISTING, ADDED));

        source.determineSnapshotOffset(context, null);
        source.readTableStructure(running, context, null, new SnapshottingTask(true, true, List.of("APP\\.EXISTING"), Map.of(), false));

        assertThat(logs.containsWarnMessage("table.include.list rule")).isFalse();
        assertThat(logs.containsMessage("Dameng tables selected for schema capture: [APP.ADDED, APP.EXISTING]")).isTrue();
        assertThat(logs.containsMessage("Dameng tables selected for data snapshot: [APP.EXISTING]")).isTrue();
    }

    @Test
    void shouldNotWarnForOtherConfiguredTablesDuringAnOnDemandSnapshot() throws Exception {
        final var source = source(DamengConnectorConfigTest.configuration().with("table.include.list", "APP\\.EXISTING,APP\\.ADDED").build());
        final var context = context();

        source.readTableStructure(running, context, null, new SnapshottingTask(true, true, List.of("APP\\.EXISTING"), Map.of(), true));

        assertThat(logs.containsWarnMessage("table.include.list rule")).isFalse();
        verify(connection, never()).getAllTableIds(null);
    }

    @Test
    void shouldLogNoDataTablesForASchemaOnlySnapshot() throws Exception {
        final var source = source(DamengConnectorConfigTest.configuration().build());
        final var context = context();
        when(connection.getAllTableIds(null)).thenReturn(Set.of(EXISTING));

        source.determineSnapshotOffset(context, null);
        source.readTableStructure(running, context, null, new SnapshottingTask(true, false, List.of(), Map.of(), false));

        assertThat(logs.containsMessage("Dameng tables selected for schema capture: [APP.EXISTING]")).isTrue();
        assertThat(logs.containsMessage("Dameng tables selected for data snapshot: []")).isTrue();
        assertThat(logs.containsWarnMessage("table.include.list rule")).isFalse();
    }

    @Test
    void shouldRepeatDiscoveryUntilNoAdditionalTableAppears() throws Exception {
        final var third = new TableId(null, "APP", "THIRD");
        final var source = source(DamengConnectorConfigTest.configuration().build());
        final var context = context();
        when(connection.getAllTableIds(null)).thenReturn(Set.of(EXISTING, ADDED), Set.of(EXISTING, ADDED, third));

        source.determineSnapshotOffset(context, null);
        source.readTableStructure(running, context, null, new SnapshottingTask(true, true, List.of(), Map.of(), false));

        assertThat(context.capturedTables).containsExactly(ADDED, EXISTING, third);
        assertThat(context.offset.getLsn()).isEqualTo(300);
        verify(connection).executeWithoutCommitting("LOCK TABLE \"APP\".\"ADDED\" IN SHARE MODE");
        verify(connection).executeWithoutCommitting("LOCK TABLE \"APP\".\"THIRD\" IN SHARE MODE");
        verify(connection, times(3)).getAllTableIds(null);
    }

    @Test
    void shouldRespectSnapshotAndTableFiltersDuringRediscovery() throws Exception {
        final var excluded = new TableId(null, "APP", "EXCLUDED");
        final var otherSnapshot = new TableId(null, "APP", "OTHER_SNAPSHOT");
        final var source = source(DamengConnectorConfigTest.configuration().with("table.exclude.list", "APP\\.EXCLUDED").build());
        final var context = context();
        when(connection.getAllTableIds(null)).thenReturn(Set.of(EXISTING, ADDED, excluded, otherSnapshot));

        source.determineSnapshotOffset(context, null);
        source.readTableStructure(running, context, null,
                new SnapshottingTask(true, true, List.of("APP\\.(EXISTING|ADDED|EXCLUDED)"), Map.of(), false));

        assertThat(context.capturedTables).containsExactly(ADDED, EXISTING);
        assertThat(context.capturedSchemaTables).containsExactlyInAnyOrder(ADDED, EXISTING, otherSnapshot);
        verify(connection, never()).executeWithoutCommitting("LOCK TABLE \"APP\".\"EXCLUDED\" IN SHARE MODE");
        verify(connection).executeWithoutCommitting("LOCK TABLE \"APP\".\"OTHER_SNAPSHOT\" IN SHARE MODE");
    }

    @Test
    void shouldRediscoverSignalTableOutsideTheUserTableIncludeList() throws Exception {
        final var signal = new TableId(null, "APP", "SIGNAL");
        final var source = source(DamengConnectorConfigTest.configuration().with("table.include.list", "APP\\.EXISTING")
                .with("signal.data.collection", "APP.SIGNAL").build());
        final var context = context();
        when(connection.getAllTableIds(null)).thenReturn(Set.of(EXISTING, ADDED, signal));

        source.determineSnapshotOffset(context, null);
        source.readTableStructure(running, context, null, new SnapshottingTask(true, true, List.of(), Map.of(), false));

        assertThat(context.capturedTables).containsExactly(EXISTING, signal);
        verify(connection).executeWithoutCommitting("LOCK TABLE \"APP\".\"SIGNAL\" IN SHARE MODE");
        verify(connection, never()).executeWithoutCommitting("LOCK TABLE \"APP\".\"ADDED\" IN SHARE MODE");
    }

    @Test
    void shouldKeepOriginalPositionWhenNoCapturedTableWasAdded() throws Exception {
        final var source = source(DamengConnectorConfigTest.configuration().build());
        final var context = context();
        when(connection.getAllTableIds(null)).thenReturn(Set.of(EXISTING));

        source.determineSnapshotOffset(context, null);
        source.readTableStructure(running, context, null, new SnapshottingTask(true, true, List.of(), Map.of(), false));

        assertThat(context.offset.getLsn()).isEqualTo(100);
        verify(connection).currentLsn();
        verify(connection, never()).executeWithoutCommitting(any());
    }

    @Test
    void shouldNotAdvanceARecoveredStreamingOffset() throws Exception {
        final var config = new DamengConnectorConfig(DamengConnectorConfigTest.configuration().build());
        final var previous = new DamengOffsetContext(config, 50);
        final var source = source(DamengConnectorConfigTest.configuration().build());
        final var context = context();

        source.determineSnapshotOffset(context, previous);
        source.readTableStructure(running, context, previous, new SnapshottingTask(true, false, List.of(), Map.of(), false));

        assertThat(context.offset).isSameAs(previous);
        verify(connection, never()).currentLsn();
        verify(connection, never()).getAllTableIds(null);
    }

    @Test
    void shouldAbortRediscoveryWhenTheConnectorStops() throws Exception {
        final var source = source(DamengConnectorConfigTest.configuration().build());
        final var context = context();
        source.determineSnapshotOffset(context, null);
        when(running.isRunning()).thenReturn(false);

        assertThatThrownBy(() -> source.readTableStructure(running, context, null, new SnapshottingTask(true, true, List.of(), Map.of(), false)))
                .isInstanceOf(InterruptedException.class);
        assertThat(context.offset.getLsn()).isEqualTo(100);
    }

    @Test
    void shouldDelegateSelectToTheConfiguredSnapshotQuery() {
        final var source = source(DamengConnectorConfigTest.configuration().build());
        final var query = mock(SnapshotQuery.class);
        final List<String> columns = List.of("\"ID\"", "\"NAME\"");
        when(snapshotterService.getSnapshotQuery()).thenReturn(query);
        when(query.snapshotQuery("\"APP\".\"EXISTING\"", columns)).thenReturn(Optional.of("SELECT custom_projection FROM custom_table"));

        assertThat(source.getSnapshotSelect(context(), EXISTING, columns)).contains("SELECT custom_projection FROM custom_table");
        verify(query).snapshotQuery("\"APP\".\"EXISTING\"", columns);
    }

    @Test
    void shouldUseSchemaTableSetWhenPersistingDefinitionsForNonSnapshottedTables() {
        final var source = source(DamengConnectorConfigTest.configuration().build());
        final var context = context();
        context.capturedSchemaTables = new LinkedHashSet<>(Set.of(EXISTING, ADDED));

        assertThat(source.getTablesForSchemaChange(context)).containsExactlyInAnyOrder(EXISTING, ADDED);
    }

    @Test
    void shouldExcludeUncapturedSchemaTablesAndKeepTheSignalTable() throws Exception {
        final var signal = new TableId(null, "APP", "SIGNAL");
        final var source = source(DamengConnectorConfigTest.configuration().with("table.include.list", "APP\\.EXISTING")
                .with("signal.data.collection", "APP.SIGNAL").build());
        final var context = context();
        context.capturedTables.add(signal);
        context.capturedSchemaTables.add(ADDED);

        assertThat(source.getTablesForSchemaChange(context)).containsExactly(EXISTING, signal);
        source.lockTablesForSchemaSnapshot(running, context);

        verify(connection).executeWithoutCommitting("LOCK TABLE \"APP\".\"EXISTING\" IN SHARE MODE");
        verify(connection).executeWithoutCommitting("LOCK TABLE \"APP\".\"SIGNAL\" IN SHARE MODE");
        verify(connection, never()).executeWithoutCommitting("LOCK TABLE \"APP\".\"ADDED\" IN SHARE MODE");
    }

    @Test
    void shouldLockTablesExcludedFromTheRowSnapshotBeforeReadingTheOffset() throws Exception {
        final var source = source(DamengConnectorConfigTest.configuration().build());
        final var context = context();
        context.capturedSchemaTables.add(ADDED);

        source.lockTablesForSchemaSnapshot(running, context);
        source.determineSnapshotOffset(context, null);

        final var order = inOrder(connection);
        order.verify(connection).executeWithoutCommitting("LOCK TABLE \"APP\".\"ADDED\" IN SHARE MODE");
        order.verify(connection).executeWithoutCommitting("LOCK TABLE \"APP\".\"EXISTING\" IN SHARE MODE");
        order.verify(connection).currentLsn();
        assertThat(context.capturedTables).containsExactly(EXISTING);
    }

    @Test
    void shouldRejectSnapshotCompletionAfterPrimaryChanges() throws Exception {
        final var source = source(DamengConnectorConfigTest.configuration().build());
        final var context = context();
        source.determineSnapshotOffset(context, null);
        context.offset.preSnapshotStart(false);
        doThrow(new SQLException("Primary changed during the snapshot", "08006")).when(connection).validateUnchangedPrimary();

        assertThatThrownBy(() -> source.releaseDataSnapshotLocks(context))
                .isInstanceOf(SQLException.class).hasMessageContaining("Primary changed");
        assertThat(context.offset.getOffset().get("snapshot_completed")).isEqualTo(false);
    }

    @Test
    void shouldValidatePrimaryAtTheSnapshotCompletionBoundary() throws Exception {
        final var source = source(DamengConnectorConfigTest.configuration().build());
        final var context = context();
        source.determineSnapshotOffset(context, null);
        context.offset.preSnapshotStart(false);

        source.releaseDataSnapshotLocks(context);
        context.offset.preSnapshotCompletion();

        verify(connection).validateUnchangedPrimary();
        assertThat(context.offset.getOffset().get("snapshot_completed")).isEqualTo(true);
    }

    @Test
    void shouldDiscoverSchemaOnlyTablesWithoutAddingTheirRowsToTheSnapshot() throws Exception {
        final var source = source(DamengConnectorConfigTest.configuration().build());
        final var context = context();
        when(connection.getAllTableIds(null)).thenReturn(Set.of(EXISTING, ADDED));

        source.determineSnapshotOffset(context, null);
        source.readTableStructure(running, context, null,
                new SnapshottingTask(true, true, List.of("APP\\.EXISTING"), Map.of(), false));

        assertThat(context.capturedTables).containsExactly(EXISTING);
        assertThat(context.capturedSchemaTables).containsExactlyInAnyOrder(EXISTING, ADDED);
        assertThat(context.offset.getLsn()).isEqualTo(200);
        final var order = inOrder(connection);
        order.verify(connection).currentLsn();
        order.verify(connection).getAllTableIds(null);
        order.verify(connection).executeWithoutCommitting("LOCK TABLE \"APP\".\"ADDED\" IN SHARE MODE");
        order.verify(connection).currentLsn();
        order.verify(connection).getAllTableIds(null);
        verify(listener, never()).monitoredDataCollectionsDetermined(any(), any());
    }

    @Test
    void shouldReadSchemaForTablesExcludedFromTheDataSnapshot() throws Exception {
        final var source = source(DamengConnectorConfigTest.configuration().with("column.exclude.list", "APP\\.ADDED\\.SECRET").build());
        final var context = context();
        context.capturedSchemaTables = new LinkedHashSet<>(Set.of(EXISTING, ADDED));
        context.tables.overwriteTable(Table.editor().tableId(ADDED)
                .addColumn(Column.editor().name("ID").jdbcType(Types.INTEGER).position(1).create())
                .addColumn(Column.editor().name("SECRET").jdbcType(Types.VARCHAR).position(2).create())
                .addColumn(Column.editor().name("VALUE").jdbcType(Types.VARCHAR).position(3).create()).create());
        when(connection.getTableObjectId(ADDED)).thenReturn(123L);

        source.readTableStructure(running, context, null, new SnapshottingTask(true, true, List.of(), Map.of(), true));

        final ArgumentCaptor<TableFilter> filter = ArgumentCaptor.forClass(TableFilter.class);
        verify(connection).readSchema(any(), isNull(), eq("APP"), filter.capture(), isNull(), eq(false));
        assertThat(filter.getValue().isIncluded(EXISTING)).isTrue();
        assertThat(filter.getValue().isIncluded(ADDED)).isTrue();
        assertThat(context.tables.forTable(ADDED).attributeWithName("dameng.object.id").asLong()).isEqualTo(123);
        assertThat(context.tables.forTable(ADDED).columns()).extracting(Column::name).containsExactly("ID", "SECRET", "VALUE");
        assertThat(context.tables.forTable(ADDED).columnWithName("VALUE").position()).isEqualTo(3);
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldUseConfiguredDatabaseNameForInitialSnapshotSchemaChanges() {
        final var configuration = DamengConnectorConfigTest.configuration()
                .with("database.dbname", "DM_TEST")
                .with("include.schema.changes", true)
                .build();
        final var source = source(configuration);
        final var context = (RelationalSnapshotContext<DamengPartition, DamengOffsetContext>) source.prepare(new DamengPartition("dameng"), false);
        context.offset = new DamengOffsetContext(new DamengConnectorConfig(configuration), 100L);
        context.offset.event(EXISTING, Instant.now());
        final var table = Table.editor().tableId(EXISTING)
                .addColumn(Column.editor().name("ID").jdbcType(Types.INTEGER).position(1).create())
                .create();

        final var event = source.getCreateTableEvent(context, table);

        assertThat(event.getDatabase()).isEqualTo("DM_TEST");
        assertThat(event.getSource().getString("db")).isEqualTo("DM_TEST");
        assertThat(event.getSchema()).isEqualTo("APP");
        assertThat(event.isFromSnapshot()).isTrue();
        assertThat(table.id().catalog()).isNull();
    }

    @Test
    void shouldHonorACustomSnapshotQueryThatSkipsATable() {
        final var source = source(DamengConnectorConfigTest.configuration().build());
        final var query = mock(SnapshotQuery.class);
        when(snapshotterService.getSnapshotQuery()).thenReturn(query);
        when(query.snapshotQuery("\"APP\".\"EXISTING\"", List.of("\"ID\""))).thenReturn(Optional.empty());

        assertThat(source.getSnapshotSelect(context(), EXISTING, List.of("\"ID\""))).isEmpty();
    }

    @SuppressWarnings("unchecked")
    private DamengSnapshotChangeEventSource source(Configuration configuration) {
        final MainConnectionProvidingConnectionFactory<DamengConnection> factory = mock(MainConnectionProvidingConnectionFactory.class);
        when(factory.mainConnection()).thenReturn(connection);
        return new DamengSnapshotChangeEventSource(new DamengConnectorConfig(configuration), factory, mock(DamengDatabaseSchema.class),
                mock(EventDispatcher.class), clock, listener, mock(NotificationService.class), snapshotterService);
    }

    private RelationalSnapshotContext<DamengPartition, DamengOffsetContext> context() {
        final var context = new RelationalSnapshotContext<DamengPartition, DamengOffsetContext>(new DamengPartition("dameng"), null, false);
        context.capturedTables = new LinkedHashSet<>(Set.of(EXISTING));
        context.capturedSchemaTables = new LinkedHashSet<>(Set.of(EXISTING));
        return context;
    }
}
