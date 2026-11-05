/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
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
import java.util.stream.Stream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.connector.dameng.antlr.DamengDdlParser;
import io.debezium.connector.dameng.antlr.DamengDdlStatementAnalyzer.PartitionExchangeException;
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
    private final DamengConnectorConfig config = new DamengConnectorConfig(DamengTestConfiguration.configuration()
            .with("log.mining.window.strategy", "fixed")
            .with("table.include.list", "APP.A")
            .with("schema.history.internal", MemorySchemaHistory.class.getName())
            .with("schema.history.internal.store.only.captured.tables.ddl", true).build());
    private final DamengPartition partition = new DamengPartition(config.getLogicalName());
    private final DamengOffsetContext offset = new DamengOffsetContext(config, 90);
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final AtomicReference<Throwable> failure = new AtomicReference<>();
    private final List<TableId> delivered = new ArrayList<>();
    private final List<TableId> publishedSchemas = new ArrayList<>();
    private DamengDatabaseSchema schema;
    private DamengStreamingChangeEventSource source;
    private ChangeEventSourceContext context;
    private ResultSet rows;
    private DamengConnection connection;
    private MainConnectionProvidingConnectionFactory<DamengConnection> factory;
    private EventDispatcher<DamengPartition, TableId> dispatcher;

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
        connection = mock(DamengConnection.class);
        when(connection.createDdlParser()).thenReturn(new DamengDdlParser(StandardCharsets.UTF_8, false, false, false));
        final var jdbc = mock(Connection.class);
        final var statement = mock(PreparedStatement.class);
        rows = mock(ResultSet.class);
        when(connection.connection()).thenReturn(jdbc);
        when(connection.logMinerContentsQuery(false)).thenReturn("mine");
        when(connection.readTablePreparedStatement(any(DamengConnectorConfig.class), eq("mine"), eq(OptionalLong.empty()))).thenReturn(statement);
        when(statement.executeQuery()).thenReturn(rows);
        final var polls = new AtomicInteger();
        when(connection.latestArchivedLsn()).thenAnswer(invocation -> {
            if (polls.getAndIncrement() > 0) {
                running.set(false);
            }
            return 150L;
        });
        factory = mock(MainConnectionProvidingConnectionFactory.class);
        when(factory.mainConnection()).thenReturn(connection);
        dispatcher = mock(EventDispatcher.class);
        doAnswer(invocation -> {
            delivered.add(invocation.getArgument(1));
            return true;
        }).when(dispatcher).dispatchDataChangeEvent(any(), any(), any());
        doAnswer(invocation -> {
            final TableId dispatchId = invocation.getArgument(2);
            // Match EventDispatcher's store-only-captured filtering while applying
            // real schema/history changes through the supplied callback.
            if (config.getTableFilters().dataCollectionFilter().isIncluded(dispatchId)) {
                publishedSchemas.add(dispatchId);
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

    @ParameterizedTest
    @ValueSource(longs = { -1, 0, 7, 99 })
    void shouldSkipAnExchangeOutsideBothTableScopesRegardlessOfItsObjectNumber(long objectId) throws Exception {
        final var original = schema.getTables().forTable(INCLUDED);
        final Map<String, Object> exchange = new HashMap<>(ddl(101, "ALTER TABLE APP.B EXCHANGE PARTITION P1 WITH TABLE OTHER.C"));
        exchange.put("DATA_OBJ#", objectId);
        run(List.of(exchange, dml(102), commit()));
        assertThat(failure.get()).isNull();
        assertThat(delivered).containsExactly(INCLUDED);
        assertThat(publishedSchemas).isEmpty();
        assertThat(schema.getTables().forTable(INCLUDED)).isSameAs(original);
        assertThat(recover().tableIds()).containsExactly(INCLUDED);
        assertThat(offset.getCommitLsn()).isEqualTo(120);
        verify(connection, never()).readTableSchema(any());
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "ALTER TABLE APP.A EXCHANGE PARTITION P1 WITH TABLE OTHER.B",
            "ALTER TABLE OTHER.B EXCHANGE SUBPARTITION S1 WITH TABLE APP.A",
            "ALTER TABLE APP.A EXCHANGE PARTITION P1 WITH TABLE APP.MTAB$_IGNORED"
    })
    void shouldRejectAnExchangeWhenEitherParticipantIsCaptured(String sql) throws Exception {
        offset.validateDatabaseMagic(0);
        final Map<String, ?> before = offset.getOffset();
        final Map<String, Object> exchange = new HashMap<>(ddl(101, sql));
        exchange.put("DATA_OBJ#", 99L);
        run(List.of(exchange, dml(102), commit()));
        assertThat(failure.get()).isInstanceOf(IllegalArgumentException.class).hasCauseInstanceOf(PartitionExchangeException.class);
        assertThat(offset.getOffset()).isEqualTo(before);
        assertThat(delivered).isEmpty();
        assertThat(publishedSchemas).isEmpty();
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "ALTER TABLE APP.B EXCHANGE PARTITION P1 WITH TABLE OTHER.C",
            "ALTER TABLE OTHER.C EXCHANGE PARTITION P1 WITH TABLE APP.B"
    })
    void shouldProtectRetainedHistoricalTablesOutsideTheCurrentCaptureFilter(String sql) throws Exception {
        final var historical = schema.getTables().forTable(INCLUDED).edit().tableId(EXCLUDED)
                .addAttribute(Attribute.editor().name("dameng.object.id").value("9").create()).create();
        schema.applySchemaChange(SchemaChangeEvent.ofSnapshotCreate(partition, offset, "DAMENG", historical));
        final var retained = schema.getTables().forTable(EXCLUDED);
        offset.validateDatabaseMagic(0);
        final var before = offset.getOffset();
        run(List.of(ddl(101, sql), commit()));
        assertThat(failure.get()).isInstanceOf(IllegalArgumentException.class).hasCauseInstanceOf(PartitionExchangeException.class);
        assertThat(offset.getOffset()).isEqualTo(before);
        assertThat(schema.getTables().forTable(EXCLUDED)).isSameAs(retained);
        assertThat(publishedSchemas).isEmpty();
    }

    @Test
    void shouldKeepExcludedInternalTableHistoryOutsideExchangeProcessingScope() throws Exception {
        final var internal = schema.getTables().forTable(INCLUDED).edit().tableId(new TableId(null, "APP", "MTAB$_IGNORED"))
                .addAttribute(Attribute.editor().name("dameng.object.id").value("9").create()).create();
        schema.applySchemaChange(SchemaChangeEvent.ofSnapshotCreate(partition, offset, "DAMENG", internal));
        run(List.of(ddl(101, "ALTER TABLE APP.MTAB$_IGNORED EXCHANGE PARTITION P1 WITH TABLE OTHER.C"), dml(102), commit()));
        assertThat(failure.get()).isNull();
        assertThat(delivered).containsExactly(INCLUDED);
        assertThat(publishedSchemas).isEmpty();
    }

    static Stream<Arguments> exchangeActivationBoundaries() {
        return Stream.of(Arguments.of(false, false), Arguments.of(true, false), Arguments.of(false, true), Arguments.of(true, true));
    }

    @ParameterizedTest
    @MethodSource("exchangeActivationBoundaries")
    void shouldApplyActivationBoundariesSeparatelyToBothExchangeParticipants(boolean leftExcluded, boolean rightExcluded) throws Exception {
        configureSource(config.getConfig().edit().with("table.include.list", "APP\\..*").build());
        final var scope = mock(DamengCaptureScopeChange.class);
        when(scope.excludes(INCLUDED, 0, 101)).thenReturn(leftExcluded);
        when(scope.excludes(EXCLUDED, 0, 101)).thenReturn(rightExcluded);
        source.setCaptureScopeChange(scope);
        run(List.of(ddl(101, "ALTER TABLE APP.A EXCHANGE PARTITION P1 WITH TABLE APP.B"), commit()));
        if (leftExcluded && rightExcluded) {
            assertThat(failure.get()).isNull();
            assertThat(offset.getCommitLsn()).isEqualTo(120);
        }
        else {
            assertThat(failure.get()).isInstanceOf(IllegalArgumentException.class).hasCauseInstanceOf(PartitionExchangeException.class);
            assertThat(offset.getCommitLsn()).isLessThan(120);
        }
        verify(scope).excludes(INCLUDED, 0, 101);
        verify(scope).excludes(EXCLUDED, 0, 101);
        assertThat(publishedSchemas).isEmpty();
    }

    @Test
    void shouldResolveAnUnqualifiedExchangePeerFromHistoryRatherThanTheRootSchema() throws Exception {
        configureSource(config.getConfig().edit().with(DamengConnectorConfig.PARTITION_EXCHANGE_IGNORE, true).build());
        final Map<String, Object> exchange = new HashMap<>(ddl(101, "ALTER TABLE OTHER.B EXCHANGE PARTITION P1 WITH TABLE A"));
        exchange.put("SEG_OWNER", "OTHER");
        run(List.of(exchange, dml(102), commit()));
        assertThat(failure.get()).isNull();
        assertThat(delivered).containsExactly(INCLUDED);
        assertThat(publishedSchemas).isEmpty();
        assertThat(recover().tableIds()).containsExactly(INCLUDED);
    }

    @ParameterizedTest
    @ValueSource(strings = { "unknownObject", "wrongName", "missingRootOwner" })
    void shouldRejectUnknownExchangeIdentitiesEvenWithBothSkippingOptionsEnabled(String reason) throws Exception {
        configureSource(config.getConfig().edit().with(DamengConnectorConfig.PARTITION_EXCHANGE_IGNORE, true)
                .with("schema.history.internal.skip.unparseable.ddl", true).build());
        final var sql = reason.equals("missingRootOwner") ? "ALTER TABLE B EXCHANGE PARTITION P1 WITH TABLE APP.A"
                : "ALTER TABLE OTHER.B EXCHANGE PARTITION P1 WITH TABLE " + (reason.equals("wrongName") ? "UNKNOWN" : "A");
        final Map<String, Object> exchange = new HashMap<>(ddl(101, sql));
        exchange.put("SEG_OWNER", reason.equals("missingRootOwner") ? null : "OTHER");
        exchange.put("USERNAME", "50331649");
        exchange.put("DATA_OBJ#", reason.equals("unknownObject") ? 99L : 7L);
        offset.validateDatabaseMagic(0);
        final var before = offset.getOffset();
        run(List.of(exchange, dml(102), commit()));
        assertThat(failure.get()).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("Cannot resolve both", "unknown scope");
        assertThat(offset.getOffset()).isEqualTo(before);
        assertThat(delivered).isEmpty();
        assertThat(publishedSchemas).isEmpty();
    }

    private void configureSource(Configuration configuration) {
        final var sourceConfig = new DamengConnectorConfig(configuration);
        final var handler = mock(ErrorHandler.class);
        doAnswer(invocation -> {
            failure.set(invocation.getArgument(0));
            return null;
        }).when(handler).setProducerThrowable(any());
        source = new DamengStreamingChangeEventSource(sourceConfig, factory, dispatcher, handler, Clock.SYSTEM, schema,
                mock(DamengStreamingChangeEventSourceMetrics.class));
    }

    @ParameterizedTest
    @EnumSource(value = DamengTableType.class, names = { "EXTERNAL", "TEMPORARY" })
    void shouldReplayNonRowRenameDropAndOrdinaryRecreationFromRecoveredHistory(DamengTableType kind) throws Exception {
        schema.applySchemaChange(SchemaChangeEvent.ofSnapshotCreate(partition, offset, "DAMENG", new DamengTableType.Identity(kind, 7).table(INCLUDED)));
        schema.recover(Offsets.of(partition, offset));
        final Map<String, Object> create = new HashMap<>(ddl(104, "CREATE TABLE APP.A (ID INT PRIMARY KEY)"));
        create.put("DATA_OBJ#", 9L);
        final Map<String, Object> insert = new HashMap<>(dml(105));
        insert.put("DATA_OBJ#", 9L);
        insert.put("SQL_REDO", "INSERT INTO \"150\".\"OBJ# 9\"(\"COL 1\") VALUES(1)");

        run(List.of(ddl(101, "ALTER TABLE APP.A RENAME TO B"), ddl(102, "ALTER TABLE APP.B RENAME TO A"),
                ddl(103, "DROP TABLE APP.A CASCADE"), create, insert, commit()));

        assertThat(failure.get()).isNull();
        assertThat(delivered).containsExactly(INCLUDED);
        assertThat(publishedSchemas).containsExactly(INCLUDED);
        final var restored = recover().forTable(INCLUDED);
        assertThat(DamengTableType.isNonRow(restored)).isFalse();
        assertThat(restored.attributeWithName("dameng.object.id").asLong()).isEqualTo(9);
        assertThat(schema.schemaFor(INCLUDED)).isNotNull();
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void shouldRejectExternalIdentityMismatchOrUnexpectedDmlBeforeAdvancingOffset(boolean rowChange) throws Exception {
        final var external = DamengTableType.externalTable(INCLUDED, 7);
        schema.applySchemaChange(SchemaChangeEvent.ofSnapshotCreate(partition, offset, "DAMENG", external));
        offset.validateDatabaseMagic(0);
        final var before = offset.getOffset();
        final Map<String, Object> change = new HashMap<>(rowChange ? dml(101) : ddl(101, "DROP TABLE APP.A"));
        if (!rowChange) {
            change.put("DATA_OBJ#", 9L);
        }
        run(List.of(change, commit()));
        assertThat(failure.get()).isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(rowChange ? "Unexpected row change" : "object identity mismatch");
        assertThat(schema.getTables().forTable(INCLUDED)).isEqualTo(external);
        assertThat(offset.getOffset()).isEqualTo(before);
        assertThat(delivered).isEmpty();
        assertThat(publishedSchemas).isEmpty();
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

    @ParameterizedTest
    @ValueSource(strings = {
            "CREATE EXTERNAL TABLE APP.B(ID INT) FROM DEFAULT DIRECTORY D LOCATION ('ext.ctl')",
            "CREATE EXTERNAL TABLE IF NOT EXISTS APP.B(ID INT COMMENT 'key') FROM DEFAULT DIRECTORY D LOCATION ('ext.ctl')",
            "CREATE EXTERNAL TABLE APP.B(ID INT) FROM DATAFILE DEFAULT DIRECTORY D LOCATION ('ext.csv')",
            "CREATE EXTERNAL TABLE IF NOT EXISTS APP.B(ID INT) FROM DATAFILE DEFAULT DIRECTORY D LOCATION ('ext.csv')"
                    + " PARMS(FIELDS DELIMITED BY '|', RECORDS DELIMITED BY 0x0a, CHARACTER_CODE 'UTF-8')",
            "DROP TABLE APP.B",
            "DROP TABLE IF EXISTS APP.B",
            "DROP TABLE APP.B RESTRICT",
            "DROP TABLE APP.B CASCADE"
    })
    void shouldSkipExcludedExternalTableDdlAndContinueDataCapture(String sql) throws Exception {
        final var original = schema.getTables().forTable(INCLUDED);
        final Map<String, Object> external = new HashMap<>(ddl(101, sql));
        external.put("DATA_OBJ#", 99L);

        run(List.of(external, dml(102), commit()));

        assertThat(failure.get()).isNull();
        assertThat(delivered).containsExactly(INCLUDED);
        assertThat(schema.getTables().tableIds()).containsExactly(INCLUDED);
        assertThat(schema.getTables().forTable(INCLUDED)).isSameAs(original);
        final var recovered = recover();
        assertThat(recovered.tableIds()).containsExactly(INCLUDED);
        assertThat(recovered.forTable(INCLUDED).retrieveColumnNames()).containsExactly("ID");
        assertThat(offset.getCommitLsn()).isEqualTo(120L);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "CREATE GLOBAL TEMPORARY TABLE APP.A(ID INT) ON COMMIT DELETE ROWS",
            "CREATE GLOBAL TEMPORARY TABLE APP.A(ID INT) ON COMMIT PRESERVE ROWS",
            "CREATE EXTERNAL TABLE APP.A(ID INT) FROM DEFAULT DIRECTORY D LOCATION ('ext.ctl')",
            "CREATE EXTERNAL TABLE APP.A(ID INT) FROM DATAFILE DEFAULT DIRECTORY D LOCATION ('ext.csv') PARMS(CHARACTER_CODE 'UTF-8')"
    })
    void shouldTrackNonRowTableDefinitionsWithoutPublishingDataSchema(String sql) throws Exception {
        final Map<String, Object> create = new HashMap<>(ddl(102, sql));
        create.put("DATA_OBJ#", 9L);
        run(List.of(ddl(101, "DROP TABLE APP.A"), create, commit()));

        assertThat(failure.get()).isNull();
        assertThat(delivered).isEmpty();
        assertThat(schema.schemaFor(INCLUDED)).isNull();
        final var external = recover().forTable(INCLUDED);
        assertThat(DamengTableType.isNonRow(external)).isTrue();
        assertThat(DamengTableType.isTemporary(external)).isEqualTo(sql.contains("TEMPORARY"));
        assertThat(publishedSchemas).containsExactly(INCLUDED);
        assertThat(external.attributeWithName("dameng.object.id").asLong()).isEqualTo(9);
        assertThat(external.columns()).isEmpty();
    }

    @Test
    void shouldSkipTemporaryAlterTruncateAndRowsThenCaptureOrdinaryRecreation() throws Exception {
        final var temporary = DamengTableType.temporaryTable(INCLUDED, 7);
        schema.applySchemaChange(SchemaChangeEvent.ofSnapshotCreate(partition, offset, "DAMENG", temporary));
        final Map<String, Object> create = new HashMap<>(ddl(107, "CREATE TABLE APP.A(ID INT PRIMARY KEY)"));
        create.put("DATA_OBJ#", 9L);
        final Map<String, Object> insert = new HashMap<>(dml(108));
        insert.put("DATA_OBJ#", 9L);
        insert.put("SQL_REDO", "INSERT INTO \"150\".\"OBJ# 9\"(\"COL 1\") VALUES(1)");

        run(List.of(ddl(101, "ALTER TABLE APP.A ADD V VARCHAR(30)"), ddl(102, "ALTER TABLE APP.A RENAME COLUMN V TO VALUE"),
                ddl(103, "TRUNCATE TABLE APP.A"), dml(104), ddl(105, "DROP TABLE APP.A"), create, insert, commit()));

        assertThat(failure.get()).isNull();
        assertThat(delivered).containsExactly(INCLUDED);
        assertThat(publishedSchemas).containsExactly(INCLUDED);
        assertThat(DamengTableType.isNonRow(recover().forTable(INCLUDED))).isFalse();
        assertThat(schema.schemaFor(INCLUDED)).isNotNull();
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "CREATE GLOBAL TEMPORARY TABLE APP.B(ID INT) ON COMMIT DELETE ROWS; DROP TABLE APP.A",
            "CREATE GLOBAL TEMPORARY TABLE APP.B(ID INT) ON COMMIT PRESERVE ROWS; ALTER TABLE APP.A ADD LOST INT",
            "CREATE EXTERNAL TABLE APP.B(ID INT) FROM DEFAULT DIRECTORY D LOCATION ('ext.ctl'); DROP TABLE APP.A",
            "CREATE EXTERNAL TABLE APP.B(ID INT) FROM DATAFILE DEFAULT DIRECTORY D LOCATION ('ext.csv') ALTER TABLE APP.A ADD LOST INT",
            "DROP TABLE APP.B; DROP TABLE APP.A"
    })
    void shouldRejectCompoundExternalTableDdlBeforeCaptureFiltering(String sql) throws Exception {
        final var original = schema.getTables().forTable(INCLUDED);
        offset.validateDatabaseMagic(0);
        final var before = offset.getOffset();

        run(List.of(ddl(101, sql), dml(102), commit()));

        assertThat(failure.get()).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("LSN 101");
        assertThat(delivered).isEmpty();
        assertThat(schema.getTables().forTable(INCLUDED)).isSameAs(original);
        assertThat(offset.getOffset()).isEqualTo(before);
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

    @Test
    void shouldPersistOnlineCtasStructureAndDecodeItsPhysicalColumns() throws Exception {
        schema.applySchemaChange(SchemaChangeEvent.ofDrop(partition, offset, "DAMENG", INCLUDED.schema(),
                "DROP TABLE APP.A", schema.getTables().forTable(INCLUDED)));
        final var online = Table.editor().tableId(INCLUDED)
                .addColumn(Column.editor().name("ID").position(1).jdbcType(Types.INTEGER).type("INT").optional(true).create())
                .addColumn(Column.editor().name("LABEL").position(2).jdbcType(Types.VARCHAR).type("VARCHAR").length(30).create()).create();
        when(connection.getTableObjectId(INCLUDED)).thenReturn(9L);
        when(connection.readTableSchema(INCLUDED)).thenReturn(online);
        final Map<String, Object> create = new HashMap<>(ddl(101,
                "CREATE TABLE APP.A AS SELECT X.ID,ROW_NUMBER() OVER(ORDER BY X.ID) AS LABEL FROM APP.UNKNOWN_SOURCE X"));
        create.put("DATA_OBJ#", 9L);
        final Map<String, Object> insert = new HashMap<>(dml(102));
        insert.put("DATA_OBJ#", 9L);
        insert.put("SQL_REDO", "INSERT INTO \"150\".\"OBJ# 9\"(\"COL 1\",\"COL 2\") VALUES(1,'value')");

        run(List.of(create, insert, commit()));

        assertThat(failure.get()).isNull();
        assertThat(delivered).containsExactly(INCLUDED);
        assertThat(publishedSchemas).containsExactly(INCLUDED);
        final var restored = recover().forTable(INCLUDED);
        assertThat(restored.columns()).containsExactlyElementsOf(online.columns());
        assertThat(restored.primaryKeyColumnNames()).isEmpty();
        assertThat(restored.attributeWithName("dameng.object.id").asLong()).isEqualTo(9L);
        verify(connection).readTableSchema(INCLUDED);
    }

    @Test
    void shouldSkipExcludedCtasAndItsDmlWithoutReadingTheOnlineDictionary() throws Exception {
        final Map<String, Object> create = new HashMap<>(ddl(101,
                "CREATE TABLE APP.B AS SELECT ROW_NUMBER() OVER(ORDER BY ID) AS RN FROM APP.MISSING_SOURCE"));
        create.put("DATA_OBJ#", 99L);
        final Map<String, Object> excludedInsert = new HashMap<>(dml(102));
        excludedInsert.put("DATA_OBJ#", 99L);
        run(List.of(create, excludedInsert, dml(103), commit()));

        assertThat(failure.get()).isNull();
        assertThat(delivered).containsExactly(INCLUDED);
        assertThat(publishedSchemas).isEmpty();
        assertThat(recover().forTable(EXCLUDED)).isNull();
        assertThat(offset.getCommitLsn()).isEqualTo(120L);
        verify(connection, never()).getTableObjectId(any());
        verify(connection, never()).readTableSchema(any());
    }

    @ParameterizedTest
    @ValueSource(strings = { "missing", "mismatch", "changed", "queryFailure" })
    void shouldFailUnresolvableCtasWithoutAdvancingItsOffsetOrChangingHistory(String reason) throws Exception {
        schema.applySchemaChange(SchemaChangeEvent.ofDrop(partition, offset, "DAMENG", INCLUDED.schema(),
                "DROP TABLE APP.A", schema.getTables().forTable(INCLUDED)));
        offset.validateDatabaseMagic(0);
        final var before = offset.getOffset();
        when(connection.getTableObjectId(INCLUDED)).thenReturn(7L);
        when(connection.readTableSchema(INCLUDED)).thenReturn(Table.editor().tableId(INCLUDED)
                .addColumn(Column.editor().name("ID").position(1).jdbcType(Types.INTEGER).type("INT").create()).create());
        switch (reason) {
            case "missing" -> when(connection.getTableObjectId(INCLUDED)).thenThrow(new SQLException("No Dameng table object exists for APP.A"));
            case "mismatch" -> when(connection.getTableObjectId(INCLUDED)).thenReturn(8L);
            case "changed" -> when(connection.getTableObjectId(INCLUDED)).thenReturn(7L, 8L);
            case "queryFailure" -> when(connection.readTableSchema(INCLUDED)).thenThrow(new SQLException("Metadata permission denied"));
            default -> throw new AssertionError(reason);
        }
        // Metadata resolution failures must still fail with the syntax-error escape hatch enabled.
        final var skipConfig = new DamengConnectorConfig(config.getConfig().edit()
                .with("schema.history.internal.skip.unparseable.ddl", true).build());
        final var handler = mock(ErrorHandler.class);
        doAnswer(invocation -> {
            failure.set(invocation.getArgument(0));
            return null;
        }).when(handler).setProducerThrowable(any());
        source = new DamengStreamingChangeEventSource(skipConfig, factory, dispatcher, handler, Clock.SYSTEM, schema,
                mock(DamengStreamingChangeEventSourceMetrics.class));

        run(List.of(ddl(101, "CREATE TABLE APP.A AS SELECT 1 AS ID FROM DUAL"), dml(102), commit()));

        assertThat(failure.get()).isInstanceOf(IllegalStateException.class)
                .hasMessageContainingAll("CTAS", "APP.A", "LSN 101", "online dictionary", "redo object ID=7", "online object ID=",
                        "dropped, renamed, or recreated", "redo is still pending");
        if (reason.equals("mismatch") || reason.equals("changed")) {
            assertThat(failure.get()).hasMessageContaining("online object ID=8");
        }
        assertThat(offset.getOffset()).isEqualTo(before);
        assertThat(schema.getTables().forTable(INCLUDED)).isNull();
        assertThat(recover().forTable(INCLUDED)).isNull();
        assertThat(delivered).isEmpty();
        assertThat(publishedSchemas).isEmpty();
    }

    @ParameterizedTest
    @ValueSource(strings = { "; ALTER TABLE APP.A ADD LOST INT", " ALTER TABLE APP.A ADD LOST INT" })
    void shouldRejectCompoundCtasBeforeExcludedTableFiltering(String suffix) throws Exception {
        offset.validateDatabaseMagic(0);
        final var before = offset.getOffset();
        run(List.of(ddl(101, "CREATE TABLE APP.B AS SELECT 1 AS ID FROM DUAL" + suffix), dml(102), commit()));
        assertThat(failure.get()).isInstanceOf(IllegalArgumentException.class);
        assertThat(offset.getOffset()).isEqualTo(before);
        assertThat(recover().forTable(INCLUDED).retrieveColumnNames()).containsExactly("ID");
        assertThat(delivered).isEmpty();
        verify(connection, never()).readTableSchema(any());
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
