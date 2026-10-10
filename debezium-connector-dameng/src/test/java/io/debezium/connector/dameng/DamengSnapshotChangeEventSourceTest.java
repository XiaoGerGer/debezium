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
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.SQLException;
import java.sql.Types;
import java.time.Instant;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import io.debezium.config.Configuration;
import io.debezium.jdbc.MainConnectionProvidingConnectionFactory;
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

class DamengSnapshotChangeEventSourceTest {
    private static final TableId EXISTING = new TableId(null, "APP", "EXISTING");
    private static final TableId ADDED = new TableId(null, "APP", "ADDED");
    private DamengConnection connection;
    private ChangeEventSourceContext running;
    private SnapshotProgressListener<DamengPartition> listener;
    private SnapshotterService snapshotterService;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void before() throws Exception {
        connection = mock(DamengConnection.class);
        running = mock(ChangeEventSourceContext.class);
        listener = mock(SnapshotProgressListener.class);
        snapshotterService = mock(SnapshotterService.class);
        when(connection.quotedTableIdString(any())).thenAnswer(invocation -> ((TableId) invocation.getArgument(0)).toDoubleQuotedString());
        when(connection.currentLsn()).thenReturn(100L, 200L, 300L);
        when(connection.databaseMagic()).thenReturn(42L);
        when(running.isRunning()).thenReturn(true);
    }

    @Test
    void shouldLockNewlyDiscoveredTablesBeforeAdvancingSnapshotPosition() throws Exception {
        final var source = source(DamengConnectorConfigTest.configuration().build());
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
                mock(EventDispatcher.class), Clock.SYSTEM, listener, mock(NotificationService.class), snapshotterService);
    }

    private RelationalSnapshotContext<DamengPartition, DamengOffsetContext> context() {
        final var context = new RelationalSnapshotContext<DamengPartition, DamengOffsetContext>(new DamengPartition("dameng"), null, false);
        context.capturedTables = new LinkedHashSet<>(Set.of(EXISTING));
        context.capturedSchemaTables = new LinkedHashSet<>(Set.of(EXISTING));
        return context;
    }
}
