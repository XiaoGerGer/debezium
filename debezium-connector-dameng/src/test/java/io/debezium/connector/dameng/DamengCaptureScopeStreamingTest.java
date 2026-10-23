/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.base.ChangeEventQueueMetrics;
import io.debezium.jdbc.MainConnectionProvidingConnectionFactory;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.ChangeEventSource.ChangeEventSourceContext;
import io.debezium.pipeline.spi.ChangeRecordEmitter;
import io.debezium.relational.Attribute;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.util.Clock;

/** Verifies the permanent per-table cut in the existing commit-ordered streaming pipeline. */
class DamengCaptureScopeStreamingTest {
    private static final TableId RETAINED = new TableId(null, "APP", "A");
    private static final TableId ADDED = new TableId(null, "APP", "B");
    private static final long BOUNDARY = 110;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final AtomicReference<Throwable> failure = new AtomicReference<>();
    private final List<Map<String, Object>> delivered = new ArrayList<>();
    private final List<TableId> deliveredTables = new ArrayList<>();
    private final Tables tables = new Tables();
    private final AtomicLong tableVersion = new AtomicLong();
    private DamengConnectorConfig config;
    private DamengDatabaseSchema schema;
    private DamengCaptureScopeChange scope;
    private DamengStreamingChangeEventSource source;
    private ChangeEventSourceContext context;
    private EventDispatcher<DamengPartition, TableId> dispatcher;
    private ResultSet rows;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void before() throws Exception {
        config = new DamengConnectorConfig(Configuration.create().with("topic.prefix", "dm")
                .with("database.hostname", "localhost").with("database.user", "APP").with("database.password", "test")
                .with("table.include.list", "APP\\.A,APP\\.B").with("log.mining.window.strategy", "fixed").build());
        context = mock(ChangeEventSourceContext.class);
        when(context.isRunning()).thenAnswer(invocation -> running.get());
        final var connection = mock(DamengConnection.class);
        when(connection.createDdlParser()).thenReturn(new DamengDdlParser());
        when(connection.logMinerContentsQuery(false)).thenReturn("mine");
        final var statement = mock(PreparedStatement.class);
        rows = mock(ResultSet.class);
        when(connection.readTablePreparedStatement(any(), anyString(), any())).thenReturn(statement);
        when(statement.executeQuery()).thenReturn(rows);
        final var polls = new AtomicInteger();
        when(connection.latestArchivedLsn()).thenAnswer(invocation -> {
            if (polls.getAndIncrement() > 0) {
                running.set(false);
            }
            return 150L;
        });
        final MainConnectionProvidingConnectionFactory<DamengConnection> factory = mock(MainConnectionProvidingConnectionFactory.class);
        when(factory.mainConnection()).thenReturn(connection);
        dispatcher = mock(EventDispatcher.class);
        doAnswer(invocation -> {
            final ChangeRecordEmitter<DamengPartition> emitter = invocation.getArgument(2);
            delivered.add(new HashMap<>(emitter.getOffset().getOffset()));
            deliveredTables.add(invocation.getArgument(1));
            return true;
        }).when(dispatcher).dispatchDataChangeEvent(any(), any(), any());
        final var errorHandler = mock(ErrorHandler.class);
        doAnswer(invocation -> {
            failure.set(invocation.getArgument(0));
            return null;
        }).when(errorHandler).setProducerThrowable(any());
        schema = mock(DamengDatabaseSchema.class);
        tables.overwriteTable(table(RETAINED, 7));
        when(schema.getTables()).thenReturn(tables);
        when(schema.tableVersion()).thenAnswer(invocation -> tableVersion.get());
        final var metrics = new DamengStreamingChangeEventSourceMetrics(new DamengTaskContext(config.getConfig(), config),
                mock(ChangeEventQueueMetrics.class), new DamengEventMetadataProvider(), tables::tableIds);
        source = new DamengStreamingChangeEventSource(config, factory, dispatcher, errorHandler, Clock.SYSTEM, schema, metrics);
        scope = mock(DamengCaptureScopeChange.class);
        when(scope.excludes(any(), anyLong(), anyLong())).thenAnswer(invocation -> {
            final TableId id = invocation.getArgument(0);
            final long objectId = invocation.getArgument(1);
            final long lsn = invocation.getArgument(2);
            return (ADDED.equals(id) || objectId == 8) && lsn <= BOUNDARY;
        });
        source.setCaptureScopeChange(scope);
    }

    @Test
    void shouldFilterAnUnknownAddedObjectWithoutRenumberingTheRetainedRow() throws Exception {
        records(List.of(dml(101, 8, 90, 120, 1), dml(102, 7, 90, 120, 1), commit(120, 90, 1)));
        execute();
        assertThat(failure.get()).isNull();
        assertThat(deliveredTables).containsExactly(RETAINED);
        assertThat(delivered).singleElement().satisfies(offset -> assertThat(offset)
                .containsEntry("change_lsn", 102L).containsEntry("event_serial_no", 1L));
        verify(scope).beforeRow(schema, 101);
        verify(scope).beforeRow(schema, 102);
        verify(scope).scannedThrough(schema, 150);
    }

    @Test
    void shouldInstallTheAddedBaselineBeforeResolvingItsFirstPostCutObject() throws Exception {
        doAnswer(invocation -> {
            if ((long) invocation.getArgument(1) > BOUNDARY) {
                tables.overwriteTable(table(ADDED, 8));
                tableVersion.incrementAndGet();
            }
            return null;
        }).when(scope).beforeRow(eq(schema), anyLong());
        records(List.of(dml(110, 8, 90, 120, 1), dml(111, 8, 90, 120, 1), commit(120, 90, 1)));
        execute();
        assertThat(failure.get()).isNull();
        assertThat(deliveredTables).containsExactly(ADDED);
        assertThat(delivered).singleElement().satisfies(offset -> assertThat(offset)
                .containsEntry("change_lsn", 111L).containsEntry("event_serial_no", 1L));
    }

    @Test
    void shouldKeepFilteringOlderRowsAfterAYoungerTransactionHasActivatedTheTable() throws Exception {
        tables.overwriteTable(table(ADDED, 8));
        records(List.of(dml(111, 8, 100, 120, 1), commit(120, 100, 1),
                dml(105, 8, 90, 130, 2), dml(106, 7, 90, 130, 2), commit(130, 90, 2)));
        execute();
        assertThat(failure.get()).isNull();
        assertThat(deliveredTables).containsExactly(ADDED, RETAINED);
        assertThat(delivered.get(1)).containsEntry("change_lsn", 106L).containsEntry("event_serial_no", 1L);
    }

    @Test
    void shouldSkipUnsupportedSingleTableDdlBeforeTheCutWithoutPlanningIt() throws Exception {
        tables.overwriteTable(table(ADDED, 8));
        records(List.of(ddl(101, 8, "ALTER TABLE APP.B MOVE"), dml(102, 7, 90, 120, 1), commit(120, 90, 1)));
        execute();
        assertThat(failure.get()).isNull();
        assertThat(deliveredTables).containsExactly(RETAINED);
        assertThat(delivered).singleElement().satisfies(offset -> assertThat(offset).containsEntry("event_serial_no", 1L));
        verify(dispatcher, never()).dispatchSchemaChangeEvent(any(), any(), any(TableId.class), any());
    }

    @Test
    void shouldNotUseOneObjectNumberToDiscardAnUnsupportedCompoundDdl() throws Exception {
        records(List.of(ddl(101, 8, "CREATE SCHEMA APP CREATE TABLE B (ID INT) CREATE TABLE A (ID INT)"), commit(120, 90, 1)));
        execute();
        assertThat(failure.get()).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("compound scope");
        assertThat(delivered).isEmpty();
        verify(scope, never()).excludes(any(), anyLong(), anyLong());
        verify(scope, never()).scannedThrough(any(), anyLong());
    }

    @Test
    void shouldFailClosedWhenAnOldTableRenameCrossesTheAddedTableBoundary() throws Exception {
        records(List.of(ddl(101, 7, "ALTER TABLE APP.A RENAME TO APP.B"), commit(120, 90, 1)));
        execute();
        assertThat(failure.get()).isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("historical rename").hasMessageContaining("activation boundary")
                .hasMessageContaining("restore the previous capture filters");
        assertThat(delivered).isEmpty();
        verify(scope).excludes(ADDED, 7, 101);
        verify(dispatcher, never()).dispatchSchemaChangeEvent(any(), any(), any(TableId.class), any());
    }

    @Test
    void shouldActivateAnIdleAddedTableAfterACompletelyDispatchedEmptyWindow() throws Exception {
        records(List.of());
        execute();
        assertThat(failure.get()).isNull();
        verify(scope).scannedThrough(schema, 150);
        verify(scope, never()).beforeRow(any(), anyLong());
    }

    @Test
    void shouldNotMarkTheWindowScannedWhenDispatchIsInterrupted() throws Exception {
        doAnswer(invocation -> {
            running.set(false);
            return true;
        }).when(dispatcher).dispatchDataChangeEvent(any(), any(), any());
        records(List.of(dml(101, 7, 90, 120, 1), commit(120, 90, 1)));
        execute();
        assertThat(failure.get()).isNull();
        verify(scope, never()).scannedThrough(any(), anyLong());
    }

    @Test
    void shouldRunActivationBeforeSkippingAnAlreadyAcknowledgedOrdinal() throws Exception {
        records(List.of(dml(111, 7, 90, 120, 1), dml(112, 7, 90, 120, 1), commit(120, 90, 1)));
        final var offset = new DamengOffsetContext(config, 90);
        offset.setCommitLsn(120);
        offset.setTransactionId("01");
        offset.setEventSerialNo(0);
        source.execute(context, new DamengPartition("dm"), offset);
        assertThat(failure.get()).isNull();
        verify(scope).beforeRow(schema, 111);
        verify(scope).beforeRow(schema, 112);
        assertThat(delivered).singleElement().satisfies(value -> assertThat(value)
                .containsEntry("change_lsn", 112L).containsEntry("event_serial_no", 1L));
    }

    @Test
    void shouldReplayTheFirstDataRowAfterAnAcknowledgedBeginOnlyCheckpoint() throws Exception {
        records(List.of(dml(111, 7, 90, 120, 1), dml(112, 7, 90, 120, 1), commit(120, 90, 1)));
        final var offset = new DamengOffsetContext(config, 90);
        offset.setCommitLsn(120);
        offset.setTransactionId("01");
        offset.setEventSerialNo(-1);

        source.execute(context, new DamengPartition("dm"), offset);

        assertThat(failure.get()).isNull();
        assertThat(deliveredTables).containsExactly(RETAINED, RETAINED);
        assertThat(delivered).extracting(value -> value.get("change_lsn")).containsExactly(111L, 112L);
        assertThat(delivered).extracting(value -> value.get("event_serial_no")).containsExactly(0L, 1L);
    }

    private void execute() throws InterruptedException {
        source.execute(context, new DamengPartition("dm"), new DamengOffsetContext(config, 90));
    }

    private static Table table(TableId id, long objectId) {
        return Table.editor().tableId(id)
                .addColumn(Column.editor().name("ID").position(1).jdbcType(Types.INTEGER).type("INT").create())
                .setPrimaryKeyNames("ID").addAttribute(Attribute.editor().name("dameng.object.id").value(Long.toString(objectId)).create()).create();
    }

    private void records(List<Map<String, Object>> records) throws Exception {
        final var index = new AtomicInteger(-1);
        when(rows.next()).thenAnswer(invocation -> index.incrementAndGet() < records.size());
        final var wasNull = new AtomicBoolean();
        when(rows.getLong(anyString())).thenAnswer(invocation -> {
            final var value = (Number) records.get(index.get()).get(invocation.getArgument(0));
            wasNull.set(value == null);
            return value == null ? 0L : value.longValue();
        });
        when(rows.getInt(anyString())).thenAnswer(invocation -> {
            final var value = (Number) records.get(index.get()).getOrDefault(invocation.getArgument(0), 0);
            wasNull.set(value == null);
            return value == null ? 0 : value.intValue();
        });
        when(rows.wasNull()).thenAnswer(invocation -> wasNull.get());
        when(rows.getString(anyString())).thenAnswer(invocation -> records.get(index.get()).get(invocation.getArgument(0)));
        when(rows.getBytes("XID")).thenAnswer(invocation -> records.get(index.get()).get("XID"));
        when(rows.getTimestamp("TIMESTAMP")).thenReturn(Timestamp.valueOf("2026-10-22 12:00:00"));
    }

    private static Map<String, Object> dml(long lsn, long objectId, long start, long commit, int xid) {
        return Map.of("SCN", lsn, "START_SCN", start, "COMMIT_SCN", commit, "XID", new byte[]{ (byte) xid },
                "OPERATION_CODE", 1, "DATA_OBJ#", objectId,
                "SQL_REDO", "INSERT INTO \"150\".\"OBJ# " + objectId + "\"(\"COL 1\") VALUES(1)");
    }

    private static Map<String, Object> ddl(long lsn, long objectId, String sql) {
        return Map.of("SCN", lsn, "START_SCN", 90L, "COMMIT_SCN", 120L, "XID", new byte[]{ 1 },
                "OPERATION_CODE", 5, "DATA_OBJ#", objectId, "SQL_REDO", sql);
    }

    private static Map<String, Object> commit(long lsn, long start, int xid) {
        return Map.of("SCN", lsn, "START_SCN", start, "COMMIT_SCN", lsn, "XID", new byte[]{ (byte) xid }, "OPERATION_CODE", 7);
    }
}
