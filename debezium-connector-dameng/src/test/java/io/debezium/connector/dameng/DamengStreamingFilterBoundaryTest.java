/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.jdbc.MainConnectionProvidingConnectionFactory;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.ChangeEventSource.ChangeEventSourceContext;
import io.debezium.pipeline.spi.Offsets;
import io.debezium.pipeline.spi.SchemaChangeEventEmitter;
import io.debezium.relational.Attribute;
import io.debezium.relational.Column;
import io.debezium.relational.CustomConverterRegistry;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.relational.history.MemorySchemaHistory;
import io.debezium.schema.SchemaChangeEvent;
import io.debezium.util.Clock;

class DamengStreamingFilterBoundaryTest {
    private static final TableId INCLUDED = new TableId(null, "APP", "A");
    private static final TableId EXCLUDED = new TableId(null, "APP", "B");
    private final DamengConnectorConfig config = new DamengConnectorConfig(DamengConnectorConfigTest.configuration()
            .with("log.mining.window.strategy", "fixed")
            .with("table.include.list", "APP.A")
            .with("schema.history.internal", MemorySchemaHistory.class.getName())
            .with("schema.history.internal.store.only.captured.tables.ddl", true).build());
    private final DamengPartition partition = new DamengPartition(config.getLogicalName());
    private final DamengOffsetContext offset = new DamengOffsetContext(config, 90);
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final AtomicReference<Throwable> failure = new AtomicReference<>();
    private final List<TableId> delivered = new ArrayList<>();
    private DamengDatabaseSchema schema;
    private DamengStreamingChangeEventSource source;
    private ChangeEventSourceContext context;
    private ResultSet rows;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void before() throws Exception {
        schema = new DamengDatabaseSchema(config, new DamengValueConverters(config), config.schemaNameAdjuster(),
                config.getTopicNamingStrategy(CommonConnectorConfig.TOPIC_NAMING_STRATEGY), new CustomConverterRegistry(List.of()),
                new DamengTaskContext(config.getConfig(), config));
        final Table table = Table.editor().tableId(INCLUDED)
                .addColumn(Column.editor().name("ID").position(1).jdbcType(Types.INTEGER).type("INT").create())
                .setPrimaryKeyNames("ID").addAttribute(Attribute.editor().name("dameng.object.id").value("7").create()).create();
        schema.applySchemaChange(SchemaChangeEvent.ofSnapshotCreate(partition, offset, "DAMENG", table));
        context = mock(ChangeEventSourceContext.class);
        when(context.isRunning()).thenAnswer(invocation -> running.get());
        final var connection = mock(DamengConnection.class);
        when(connection.createDdlParser()).thenReturn(new DamengDdlParser(StandardCharsets.UTF_8, false, false, false));
        final var jdbc = mock(Connection.class);
        final var statement = mock(PreparedStatement.class);
        rows = mock(ResultSet.class);
        when(connection.connection()).thenReturn(jdbc);
        when(connection.logMinerContentsQuery(false)).thenReturn("mine");
        when(connection.readTablePreparedStatement(config, "mine", OptionalLong.empty())).thenReturn(statement);
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
        final EventDispatcher<DamengPartition, TableId> dispatcher = mock(EventDispatcher.class);
        doAnswer(invocation -> {
            delivered.add(invocation.getArgument(1));
            return true;
        }).when(dispatcher).dispatchDataChangeEvent(any(), any(), any());
        doAnswer(invocation -> {
            final TableId dispatchId = invocation.getArgument(2);
            // Match EventDispatcher's store-only-captured filtering while applying
            // real schema/history changes through the supplied callback.
            if (config.getTableFilters().dataCollectionFilter().isIncluded(dispatchId)) {
                final SchemaChangeEventEmitter emitter = invocation.getArgument(3);
                emitter.emitSchemaChangeEvent(schema::applySchemaChange);
            }
            return null;
        }).when(dispatcher).dispatchSchemaChangeEvent(any(), any(), any(TableId.class), any());
        final var handler = mock(ErrorHandler.class);
        doAnswer(invocation -> {
            failure.set(invocation.getArgument(0));
            return null;
        }).when(handler).setProducerThrowable(any());
        source = new DamengStreamingChangeEventSource(config, factory, dispatcher, handler, Clock.SYSTEM, schema,
                mock(DamengStreamingChangeEventSourceMetrics.class));
    }

    @AfterEach
    void closeSchema() {
        schema.close();
    }

    @Test
    void shouldSkipCompleteNonTableDdlAndContinueDataCapture() throws Exception {
        run(List.of(ddl(101, "// 权限\nGRANT SELECT ON APP.A TO U; REVOKE SELECT ON APP.A FROM U;"),
                ddl(102, "COMMENT ON COLUMN APP.A.ID IS q'[it's;--/*]'"),
                ddl(103, "CREATE PROCEDURE P AS BEGIN NULL; END;"), dml(104), commit()));
        assertThat(failure.get()).isNull();
        assertThat(delivered).containsExactly(INCLUDED);
        assertThat(recover().forTable(INCLUDED).retrieveColumnNames()).containsExactly("ID");
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "CREATE INDEX I ON APP.A(ID); ALTER TABLE APP.A ADD LOST INT",
            "CREATE PROCEDURE P AS BEGIN NULL; END; DROP TABLE APP.A",
            "ALTER TABLE APP.A ADD LOST INT; ALTER TABLE APP.A ADD LOST2 INT",
            "ALTER TABLE APP.A MOVE",
            "ALTER TABLE APP.A ADD LOST INT UNKNOWN",
            "CREATE SCHEMA S AUTHORIZATION SYSDBA CREATE TABLE A(ID INT)",
            "DROP SCHEMA APP CASCADE",
            "DROP USER APP CASCADE",
            "/* unclosed"
    })
    void shouldRejectUnsafeDdlBeforeChangingSchemaHistoryOrThisRowsOffset(String sql) throws Exception {
        final var original = schema.getTables().forTable(INCLUDED);
        offset.validateDatabaseMagic(0);
        final var before = offset.getOffset();
        run(List.of(ddl(101, sql), commit()));
        assertThat(failure.get()).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("LSN 101", "character");
        assertThat(schema.getTables().forTable(INCLUDED)).isEqualTo(original);
        assertThat(recover().forTable(INCLUDED)).isEqualTo(original);
        assertThat(offset.getOffset()).isEqualTo(before);
        assertThat(delivered).isEmpty();
    }

    @ParameterizedTest
    @ValueSource(strings = { "", " " })
    void shouldRejectMissingDdlEvenWhenItsObjectIsOutsideCapture(String redo) throws Exception {
        final var row = new HashMap<>(ddl(101, redo));
        row.put("DATA_OBJ#", 99L);
        run(List.of(row, commit()));
        assertThat(failure.get()).isInstanceOf(IllegalStateException.class).hasMessageContaining("Missing Dameng DDL SQL_REDO", "101");
        assertThat(offset.getLsn()).isEqualTo(90);
    }

    @Test
    void shouldPersistRenameOutOfCaptureAndSkipItsDml() throws Exception {
        run(List.of(dml(100), ddl(101, "ALTER TABLE APP.A RENAME TO B"), dml(102), commit()));
        assertThat(failure.get()).isNull();
        assertThat(delivered).containsExactly(INCLUDED);
        final Tables recovered = recover();
        assertThat(recovered.forTable(INCLUDED)).isNull();
        assertThat(recovered.forTable(EXCLUDED)).isNotNull();
        assertThat(recovered.forTable(EXCLUDED).attributeWithName("dameng.object.id").asLong()).isEqualTo(7L);
    }

    @Test
    void shouldTrackExcludedTableDdlAndResumeCaptureAfterRenameBack() throws Exception {
        run(List.of(ddl(101, "ALTER TABLE APP.A RENAME TO B"),
                ddl(102, "ALTER TABLE APP.B ADD NAME VARCHAR(50)"),
                ddl(103, "ALTER TABLE APP.B RENAME TO A"), dml(104), commit()));
        assertThat(failure.get()).isNull();
        assertThat(delivered).containsExactly(INCLUDED);
        final Tables recovered = recover();
        assertThat(recovered.forTable(EXCLUDED)).isNull();
        assertThat(recovered.forTable(INCLUDED).retrieveColumnNames()).containsExactly("ID", "NAME");
    }

    @Test
    void shouldPersistDropOfPreviouslyCapturedExcludedTable() throws Exception {
        run(List.of(ddl(101, "ALTER TABLE APP.A RENAME TO B"), ddl(102, "DROP TABLE APP.B"), commit()));
        assertThat(failure.get()).isNull();
        assertThat(recover().tableIds()).isEmpty();
    }

    @Test
    void shouldFailWhenUnknownExcludedTableEntersCaptureByRename() throws Exception {
        run(List.of(ddl(101, "ALTER TABLE APP.UNKNOWN RENAME TO A"), commit()));
        assertThat(failure.get()).isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("historical schema").hasMessageContaining("snapshot");
        assertThat(offset.getLsn()).isEqualTo(90);
        assertThat(delivered).isEmpty();
    }

    @ParameterizedTest
    @ValueSource(longs = { 0, -1 })
    void shouldRejectInvalidDdlIdentityBeforeMutatingTheHistoricalSchemaOrOffset(long objectId) throws Exception {
        final Map<String, Object> alter = new HashMap<>(ddl(101, "ALTER TABLE APP.A ADD LOST INT"));
        alter.put("DATA_OBJ#", objectId);
        final Map<String, ?> before = offset.getOffset();

        run(List.of(alter, commit()));

        assertThat(failure.get()).isInstanceOf(IllegalStateException.class).hasMessageContaining("DATA_OBJ#", "APP.A", "101");
        assertThat(schema.getTables().forTable(INCLUDED).retrieveColumnNames()).containsExactly("ID");
        assertThat(schema.getTables().forTable(INCLUDED).attributeWithName("dameng.object.id").asLong()).isEqualTo(7L);
        assertThat(offset.getLsn()).isEqualTo(((Number) before.get("lsn")).longValue());
        assertThat(offset.getCommitLsn()).isEqualTo(((Number) before.get("commit_lsn")).longValue());
        assertThat(offset.getEventSerialNo()).isEqualTo(((Number) before.get("event_serial_no")).longValue());
        assertThat(offset.getTransactionId()).isNull();
        assertThat(delivered).isEmpty();
    }

    @Test
    void shouldReplayCreateForATableCreatedWhileStopped() throws Exception {
        schema.applySchemaChange(SchemaChangeEvent.ofDrop(partition, offset, "DAMENG", INCLUDED.schema(),
                "DROP TABLE APP.A", schema.getTables().forTable(INCLUDED)));
        final Map<String, Object> create = new HashMap<>(ddl(101, "CREATE TABLE APP.A (ID INT PRIMARY KEY)"));
        create.put("DATA_OBJ#", 9L);
        final Map<String, Object> insert = new HashMap<>(dml(102));
        insert.put("DATA_OBJ#", 9L);
        insert.put("SQL_REDO", "INSERT INTO \"150\".\"OBJ# 9\"(\"COL 1\") VALUES(1)");

        run(List.of(create, insert, commit()));

        assertThat(failure.get()).isNull();
        assertThat(delivered).containsExactly(INCLUDED);
        assertThat(recover().forTable(INCLUDED).attributeWithName("dameng.object.id").asLong()).isEqualTo(9L);
    }

    @Test
    void shouldReplayDropAndRecreateWithoutConsultingCurrentCatalog() throws Exception {
        final Map<String, Object> create = new HashMap<>(ddl(102, "CREATE TABLE APP.A (ID INT PRIMARY KEY, LABEL VARCHAR(30))"));
        create.put("DATA_OBJ#", 9L);
        final Map<String, Object> insert = new HashMap<>(dml(103));
        insert.put("DATA_OBJ#", 9L);
        insert.put("SQL_REDO", "INSERT INTO \"150\".\"OBJ# 9\"(\"COL 1\",\"COL 2\") VALUES(1,'new shape')");

        run(List.of(dml(100), ddl(101, "DROP TABLE APP.A"), create, insert, dml(104), commit()));

        assertThat(failure.get()).isNull();
        assertThat(delivered).containsExactly(INCLUDED, INCLUDED);
        assertThat(recover().forTable(INCLUDED).retrieveColumnNames()).containsExactly("ID", "LABEL");
        assertThat(recover().forTable(INCLUDED).attributeWithName("dameng.object.id").asLong()).isEqualTo(9L);
    }

    private Tables recover() throws Exception {
        final Tables recovered = new Tables();
        schema.getSchemaHistory().recover(Offsets.of(partition, offset), recovered, null);
        return recovered;
    }

    private void run(List<Map<String, Object>> records) throws Exception {
        final var index = new AtomicInteger(-1);
        when(rows.next()).thenAnswer(invocation -> index.incrementAndGet() < records.size());
        when(rows.getLong(anyString())).thenAnswer(invocation -> ((Number) records.get(index.get()).getOrDefault(invocation.getArgument(0), 0L)).longValue());
        when(rows.getInt(anyString())).thenAnswer(invocation -> ((Number) records.get(index.get()).getOrDefault(invocation.getArgument(0), 0)).intValue());
        when(rows.getString(anyString())).thenAnswer(invocation -> records.get(index.get()).get(invocation.getArgument(0)));
        when(rows.getBytes("XID")).thenAnswer(invocation -> records.get(index.get()).get("XID"));
        when(rows.getTimestamp("TIMESTAMP")).thenReturn(Timestamp.valueOf("2026-09-24 12:00:00"));
        source.execute(context, partition, offset);
    }

    private static Map<String, Object> ddl(long lsn, String sql) {
        return Map.of("SCN", lsn, "START_SCN", 90L, "COMMIT_SCN", 120L, "XID", new byte[]{ 1 },
                "OPERATION_CODE", 5, "DATA_OBJ#", 7L, "SQL_REDO", sql);
    }

    private static Map<String, Object> dml(long lsn) {
        return Map.of("SCN", lsn, "START_SCN", 90L, "COMMIT_SCN", 120L, "XID", new byte[]{ 1 },
                "OPERATION_CODE", 1, "DATA_OBJ#", 7L, "SQL_REDO", "INSERT INTO \"150\".\"OBJ# 7\"(\"COL 1\") VALUES(1)");
    }

    private static Map<String, Object> commit() {
        return Map.of("SCN", 120L, "START_SCN", 90L, "COMMIT_SCN", 120L, "XID", new byte[]{ 1 }, "OPERATION_CODE", 7);
    }
}
