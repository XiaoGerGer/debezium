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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.connector.dameng.antlr.DamengDdlParser;
import io.debezium.data.Envelope.Operation;
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

class DamengNonTableDdlStreamingTest {
    private static final TableId CAPTURED = new TableId(null, "APP", "A");
    private final DamengConnectorConfig config = new DamengConnectorConfig(DamengConnectorConfigTest.configuration()
            .with("log.mining.window.strategy", "fixed")
            .with("poll.interval.ms", 1)
            .with("table.include.list", "APP.A")
            .with("schema.history.internal", MemorySchemaHistory.class.getName())
            .with("schema.history.internal.store.only.captured.tables.ddl", true).build());
    private final DamengPartition partition = new DamengPartition(config.getLogicalName());
    private final DamengOffsetContext offset = new DamengOffsetContext(config, 90);
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final AtomicReference<Throwable> failure = new AtomicReference<>();
    private final AtomicInteger schemaEvents = new AtomicInteger();
    private final List<TableId> delivered = new ArrayList<>();
    private final List<Operation> operations = new ArrayList<>();
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
        final var table = Table.editor().tableId(CAPTURED)
                .addColumn(Column.editor().name("ID").position(1).jdbcType(Types.INTEGER).type("INT").create())
                .setPrimaryKeyNames("ID").addAttribute(Attribute.editor().name("dameng.object.id").value("7").create()).create();
        schema.applySchemaChange(SchemaChangeEvent.ofSnapshotCreate(partition, offset, "DAMENG", table));
        context = mock(ChangeEventSourceContext.class);
        when(context.isRunning()).thenAnswer(invocation -> running.get());
        final var connection = mock(DamengConnection.class);
        when(connection.createDdlParser()).thenReturn(new DamengDdlParser());
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
            final DamengChangeRecordEmitter emitter = invocation.getArgument(2);
            operations.add(emitter.getOperation());
            return true;
        }).when(dispatcher).dispatchDataChangeEvent(any(), any(), any());
        doAnswer(invocation -> {
            schemaEvents.incrementAndGet();
            final TableId tableId = invocation.getArgument(2);
            if (config.getTableFilters().dataCollectionFilter().isIncluded(tableId)) {
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

    static Stream<Arguments> safeStatementsWithObjectMetadata() {
        // Every grammar seed exercises streaming once. Object metadata is orthogonal to
        // classification, so repeat only ordinary, quoted, stored-object and table-referencing forms.
        final var baseline = DamengNonTableDdlTest.safeStatements().map(sql -> Arguments.of(sql, 0L, "", ""));
        final var metadataVariants = Stream.of(
                "CREATE USER CDC_USER IDENTIFIED BY Test_123456",
                "CREATE DIRECTORY \"DROP TABLE APP.A\" AS '/tmp/; ALTER TABLE APP.A ADD BAD INT'",
                "ALTER PACKAGE APP.CDC_PACKAGE COMPILE CASCADE DEBUG",
                "CREATE BITMAP INDEX BI ON APP.A(U.V) FROM APP.A A, APP.U U WHERE A.ID=U.ID",
                "DROP INDEX APP.BI",
                "CREATE MATERIALIZED VIEW APP.MV AS SELECT ID FROM APP.A",
                "REFRESH MATERIALIZED VIEW APP.MV COMPLETE USING TRUNCATE",
                "STAT ON APP.A")
                .flatMap(sql -> Stream.of(
                        Arguments.of(sql, 7L, "APP", "A"),
                        Arguments.of(sql, 99L, "OTHER", "B")));
        return Stream.concat(baseline, metadataVariants);
    }

    @ParameterizedTest
    @MethodSource("safeStatementsWithObjectMetadata")
    void shouldContinueThroughMaintenanceRegardlessOfRedoTableMetadata(String sql, long objectId, String owner, String tableName) throws Exception {
        final var original = schema.getTables().forTable(CAPTURED);

        run(List.of(ddl(sql, objectId, owner, tableName), marker(), commit()));

        assertThat(failure.get()).isNull();
        assertThat(delivered).containsExactly(CAPTURED);
        assertThat(operations).containsExactly(Operation.CREATE);
        assertThat(schemaEvents.get()).isZero();
        assertThat(schema.getTables().forTable(CAPTURED)).isSameAs(original);
        final var recovered = recover();
        assertThat(recovered.tableIds()).containsExactly(CAPTURED);
        assertThat(recovered.forTable(CAPTURED).retrieveColumnNames()).containsExactly("ID");
        assertThat(recovered.forTable(CAPTURED).attributeWithName("dameng.object.id").asLong()).isEqualTo(7L);
        assertThat(offset.getCommitLsn()).isEqualTo(120L);
    }

    static Stream<Arguments> unsafeStatementsWithObjectMetadata() {
        // Reject every unsafe seed. Representative cascade, compound, lexical and trailing-DDL
        // failures also prove that an unrelated object cannot bypass validation through filtering.
        final var baseline = DamengNonTableDdlTest.unsafeStatements().map(sql -> Arguments.of(sql, 0L, "", ""));
        final var metadataVariants = Stream.of(
                "DROP SCHEMA CDC_SCHEMA CASCADE",
                "CREATE SCHEMA CDC_SCHEMA AUTHORIZATION SYSDBA CREATE TABLE A(ID INT)",
                "CREATE DIRECTORY CDC_DIRECTORY AS '/tmp/cdc",
                "CREATE USER CDC_USER IDENTIFIED BY Test_123456; ALTER TABLE APP.A ADD BAD INT")
                .map(sql -> Arguments.of(sql, 99L, "OTHER", "B"));
        return Stream.concat(baseline, metadataVariants);
    }

    @ParameterizedTest
    @MethodSource("unsafeStatementsWithObjectMetadata")
    void shouldRejectUnsafeDdlBeforeFilteringOrAcknowledgingItsMarker(String sql, long objectId, String owner, String tableName) throws Exception {
        final var original = schema.getTables().forTable(CAPTURED);
        offset.validateDatabaseMagic(0);
        final var before = offset.getOffset();

        run(List.of(ddl(sql, objectId, owner, tableName), marker(), commit()));

        assertThat(failure.get()).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("LSN 101");
        assertThat(delivered).isEmpty();
        assertThat(schemaEvents.get()).isZero();
        assertThat(offset.getOffset()).isEqualTo(before);
        assertThat(schema.getTables().forTable(CAPTURED)).isSameAs(original);
        assertThat(recover().forTable(CAPTURED)).isEqualTo(original);
    }

    @ParameterizedTest
    @ValueSource(strings = { "MTAB$_MV", "MLOG$_A", "BMJ$_BI", "bmj$_bi" })
    void shouldDiscardLegacyInternalTableDataAndDdlWithoutDiscardingTheBusinessTransaction(String name) throws Exception {
        final var internal = addHistoricalInternalTable(name, 99);
        final var original = schema.getTables().forTable(CAPTURED);
        final var internalDml = Map.<String, Object> of("SCN", 102L, "START_SCN", 90L, "COMMIT_SCN", 120L, "XID", new byte[]{ 1 },
                "OPERATION_CODE", 1, "DATA_OBJ#", 99L, "SQL_REDO", "unparsed internal row");

        run(List.of(ddl("TRUNCATE TABLE APP.\"" + name + "\"", 99, "", ""), internalDml, marker(), commit()));

        assertThat(failure.get()).isNull();
        assertThat(delivered).containsExactly(CAPTURED);
        assertThat(operations).containsExactly(Operation.CREATE);
        assertThat(schemaEvents.get()).isZero();
        assertThat(schema.getTables().forTable(CAPTURED)).isSameAs(original);
        assertThat(schema.getTables().forTable(internal.id())).isSameAs(internal);
        assertThat(offset.getCommitLsn()).isEqualTo(120L);
    }

    @ParameterizedTest
    @ValueSource(strings = { "MTAB$_MV", "MLOG$_A", "BMJ$_BI", "bmj$_bi" })
    void shouldIgnoreLegacyInternalObjectIdsReusedByBusinessTables(String name) throws Exception {
        addHistoricalInternalTable(name, 7);

        run(List.of(marker(), commit()));

        assertThat(failure.get()).isNull();
        assertThat(delivered).containsExactly(CAPTURED);
        assertThat(operations).containsExactly(Operation.CREATE);
        assertThat(offset.getCommitLsn()).isEqualTo(120L);
    }

    private Table addHistoricalInternalTable(String name, long objectId) {
        final var table = Table.editor().tableId(new TableId(null, "APP", name))
                .addColumn(Column.editor().name("ID").position(1).jdbcType(Types.INTEGER).type("INT").create())
                .addAttribute(Attribute.editor().name("dameng.object.id").value(Long.toString(objectId)).create()).create();
        schema.applySchemaChange(SchemaChangeEvent.ofSnapshotCreate(partition, offset, "DAMENG", table));
        return schema.getTables().forTable(table.id());
    }

    private Tables recover() throws Exception {
        final var recovered = new Tables();
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

    private static Map<String, Object> ddl(String sql, long objectId, String owner, String tableName) {
        return Map.of("SCN", 101L, "START_SCN", 90L, "COMMIT_SCN", 120L, "XID", new byte[]{ 1 },
                "OPERATION_CODE", 5, "DATA_OBJ#", objectId, "SQL_REDO", sql, "SEG_OWNER", owner, "TABLE_NAME", tableName);
    }

    private static Map<String, Object> marker() {
        return Map.of("SCN", 102L, "START_SCN", 90L, "COMMIT_SCN", 120L, "XID", new byte[]{ 1 },
                "OPERATION_CODE", 1, "DATA_OBJ#", 7L, "SQL_REDO", "INSERT INTO \"150\".\"OBJ# 7\"(\"COL 1\") VALUES(1)");
    }

    private static Map<String, Object> commit() {
        return Map.of("SCN", 120L, "START_SCN", 90L, "COMMIT_SCN", 120L, "XID", new byte[]{ 1 }, "OPERATION_CODE", 7);
    }
}
