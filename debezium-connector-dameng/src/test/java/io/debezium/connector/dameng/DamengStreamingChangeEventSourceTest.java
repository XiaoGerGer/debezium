/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

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

class DamengStreamingChangeEventSourceTest {
    @org.junit.jupiter.api.io.TempDir
    java.nio.file.Path spillDirectory;

    private static final TableId TABLE_ID = new TableId(null, "APP", "T");
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final List<Map<String, Object>> delivered = new ArrayList<>();
    private final List<Map<String, ?>> schemaOffsets = new ArrayList<>();
    private final AtomicReference<Throwable> failure = new AtomicReference<>();
    private int stopAfter = Integer.MAX_VALUE;
    private DamengConnectorConfig config;
    private DamengStreamingChangeEventSource source;
    private ResultSet rows;
    private ChangeEventSourceContext context;
    private DamengConnection connection;
    private MainConnectionProvidingConnectionFactory<DamengConnection> factory;
    private EventDispatcher<DamengPartition, TableId> dispatcher;
    private ErrorHandler errorHandler;
    private DamengDatabaseSchema schema;
    private DamengStreamingChangeEventSourceMetrics metrics;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void before() throws Exception {
        context = mock(ChangeEventSourceContext.class);
        when(context.isRunning()).thenAnswer(invocation -> running.get());
        config = new DamengConnectorConfig(Configuration.create().with("topic.prefix", "dm")
                .with("log.mining.window.strategy", "fixed")
                .with("database.hostname", "localhost").with("database.user", "APP").with("database.password", "test")
                .with("table.include.list", "APP.T").build());
        connection = mock(DamengConnection.class);
        when(connection.createDdlParser()).thenReturn(new DamengDdlParser());
        final var jdbc = mock(Connection.class);
        final var statement = mock(PreparedStatement.class);
        rows = mock(ResultSet.class);
        when(connection.connection()).thenReturn(jdbc);
        when(connection.logMinerContentsQuery(false)).thenReturn("mine");
        when(connection.readTablePreparedStatement(any(), anyString(), any())).thenReturn(statement);
        when(statement.executeQuery()).thenReturn(rows);
        final var polls = new AtomicInteger();
        when(connection.latestArchivedLsn()).thenAnswer(invocation -> {
            if (polls.getAndIncrement() > 0) {
                running.set(false);
            }
            return 150L;
        });
        doAnswer(invocation -> List.of(
                new DamengConnection.ArchiveLogFile("archive", 1, connection.latestArchivedLsn(), 42, 0, 0, 0, 0, 64)))
                .when(connection).readArchiveLogFiles();
        factory = mock(MainConnectionProvidingConnectionFactory.class);
        when(factory.mainConnection()).thenReturn(connection);
        dispatcher = mock(EventDispatcher.class);
        doAnswer(invocation -> {
            final ChangeRecordEmitter<DamengPartition> emitter = invocation.getArgument(2);
            delivered.add(new java.util.HashMap<>(emitter.getOffset().getOffset()));
            if (delivered.size() == stopAfter) {
                running.set(false);
            }
            return true;
        }).when(dispatcher).dispatchDataChangeEvent(any(), any(), any());
        doAnswer(invocation -> {
            final DamengOffsetContext offset = invocation.getArgument(1);
            schemaOffsets.add(offset.getOffset());
            return null;
        }).when(dispatcher).dispatchSchemaChangeEvent(any(), any(), any(TableId.class), any());
        schema = mock(DamengDatabaseSchema.class);
        final var tables = new Tables();
        tables.overwriteTable(Table.editor().tableId(TABLE_ID)
                .addColumn(Column.editor().name("ID").position(1).jdbcType(Types.INTEGER).type("INT").create())
                .setPrimaryKeyNames("ID").addAttribute(Attribute.editor().name("dameng.object.id").value("7").create()).create());
        when(schema.getTables()).thenReturn(tables);
        errorHandler = mock(ErrorHandler.class);
        doAnswer(invocation -> {
            failure.set(invocation.getArgument(0));
            return null;
        }).when(errorHandler).setProducerThrowable(any());
        createSource();
    }

    private void createSource() {
        metrics = new DamengStreamingChangeEventSourceMetrics(new DamengTaskContext(config.getConfig(), config),
                mock(ChangeEventQueueMetrics.class), new DamengEventMetadataProvider(), () -> schema.getTables().tableIds());
        final var sourceFactory = new DamengChangeEventSourceFactory(config, factory, null, schema, dispatcher, errorHandler,
                Clock.SYSTEM, null, null, metrics);
        source = (DamengStreamingChangeEventSource) sourceFactory.getStreamingChangeEventSource();
    }

    @ParameterizedTest
    @ValueSource(strings = { "fixed", "archive_bytes" })
    void shouldBoundStandbyMiningByCompletedReplayInsteadOfReceivedArchives(String strategy) throws Exception {
        config = new DamengConnectorConfig(config.getConfig().edit().with("database.standby.hosts", "replica:5236")
                .with("log.mining.window.strategy", strategy).build());
        createSource();
        when(connection.readStandbyProgress()).thenReturn(new DamengConnection.StandbyProgress(140, 125));
        records(List.of(dml(101, 90, 120, 1), commit(120, 90, 1)));
        final var offset = new DamengOffsetContext(config, 90);
        source.execute(context, new DamengPartition("dm"), offset);
        assertThat(failure.get()).isNull();
        verify(connection).startLogMining(90, 125, false);
        assertThat(offset.getLsn()).isEqualTo(125);
        assertThat(delivered).hasSize(1);
        assertThat(metrics.getLatestArchivedLsn()).isEqualTo(150);
        assertThat(metrics.getStandbyReplayableLsn()).isEqualTo(140);
        assertThat(metrics.getStandbyReplayedLsn()).isEqualTo(125);
        assertThat(metrics.getStandbyMiningUpperLsn()).isEqualTo(125);
        metrics.reset();
        assertThat(metrics.getStandbyMiningUpperLsn()).isZero();
    }

    @ParameterizedTest
    @ValueSource(strings = { "fixed", "archive_bytes" })
    void shouldRejectAStandbyReplayRegressionBeforePublishingAnyRecords(String strategy) throws Exception {
        config = new DamengConnectorConfig(config.getConfig().edit().with("database.standby.hosts", "replica:5236")
                .with("log.mining.window.strategy", strategy).build());
        createSource();
        when(connection.readStandbyProgress()).thenReturn(new DamengConnection.StandbyProgress(150, 150), new DamengConnection.StandbyProgress(100, 100));
        records(List.of(dml(101, 90, 120, 1), commit(120, 90, 1)));
        final var offset = new DamengOffsetContext(config, 90);
        source.execute(context, new DamengPartition("dm"), offset);
        assertThat(failure.get()).hasMessageContaining("replay boundary regressed");
        assertThat(delivered).isEmpty();
        assertThat(offset.getLsn()).isEqualTo(90);
    }

    @Test
    void shouldStopBeforeRebindingACachedWindowToANewPrimaryGeneration() throws Exception {
        config = new DamengConnectorConfig(config.getConfig().edit().with("database.standby.hosts", "replica:5236").build());
        createSource();
        doThrow(new SQLException("primary changed", "08006")).when(connection).validateUnchangedPrimary();
        final var offset = new DamengOffsetContext(config, 90);
        source.execute(context, new DamengPartition("dm"), offset);
        assertThat(failure.get()).hasMessageContaining("primary changed");
        verify(connection, times(1)).validatePrimaryStandbyIdentity(offset);
        verify(connection, never()).startLogMining(anyLong(), anyLong(), anyBoolean());
        assertThat(offset.getLsn()).isEqualTo(90);
    }

    @Test
    void shouldReusePhysicalMetadataAcrossInterleavedTables() {
        final var first = schema.getTables().forTable(TABLE_ID);
        final var second = Table.editor().tableId(new TableId(null, "APP", "OTHER"))
                .addColumn(Column.editor().name("VALUE").jdbcType(Types.VARCHAR).type("VARCHAR").length(30).create()).create();
        final var firstPhysical = source.physicalTable(7, first);
        final var secondPhysical = source.physicalTable(8, second);

        assertThat(firstPhysical.id().table()).isEqualTo("OBJ# 7");
        assertThat(secondPhysical.id().table()).isEqualTo("OBJ# 8");
        assertThat(firstPhysical.columnWithName("COL 1").jdbcType()).isEqualTo(Types.INTEGER);
        assertThat(secondPhysical.columnWithName("COL 1").jdbcType()).isEqualTo(Types.VARCHAR);
        assertThat(source.physicalTable(7, first)).isSameAs(firstPhysical);
        assertThat(source.physicalTable(8, second)).isSameAs(secondPhysical);
    }

    @Test
    void shouldRebuildPhysicalMetadataAfterHistoricalSchemaReplacement() {
        final var original = schema.getTables().forTable(TABLE_ID);
        final var originalPhysical = source.physicalTable(7, original);
        final var altered = original.edit()
                .addColumn(Column.editor().name("LABEL").jdbcType(Types.CHAR).type("CHAR", "CHAR(10 CHAR)")
                        .length(10).charsetName("UTF-8").create())
                .reorderColumn("LABEL", null).create();
        final var alteredPhysical = source.physicalTable(7, altered);

        assertThat(alteredPhysical).isNotSameAs(originalPhysical);
        assertThat(alteredPhysical.retrieveColumnNames()).containsExactly("COL 1", "COL 2");
        final var label = alteredPhysical.columnWithName("COL 1");
        assertThat(label.jdbcType()).isEqualTo(Types.CHAR);
        assertThat(label.typeExpression()).isEqualTo("CHAR(10 CHAR)");
        assertThat(label.length()).isEqualTo(10);
        assertThat(label.charsetName()).isEqualTo("UTF-8");
        assertThat(alteredPhysical.columnWithName("COL 2").jdbcType()).isEqualTo(Types.INTEGER);
        assertThat(source.physicalTable(7, altered)).isSameAs(alteredPhysical);
        // Replaying an earlier schema must never return the most recent mapping.
        final var replayed = source.physicalTable(7, original);
        assertThat(replayed.retrieveColumnNames()).containsExactly("COL 1");
        assertThat(replayed.columnWithName("COL 1").jdbcType()).isEqualTo(Types.INTEGER);
        assertThat(original.retrieveColumnNames()).containsExactly("ID");
    }

    @Test
    void shouldRebuildPhysicalMetadataForARecreatedObjectOrEqualSchemaInstance() {
        final var original = schema.getTables().forTable(TABLE_ID);
        final var cached = source.physicalTable(7, original);
        final var replacement = original.edit().create();
        assertThat(replacement).isEqualTo(original).isNotSameAs(original);
        final var replaced = source.physicalTable(7, replacement);
        assertThat(replaced).isNotSameAs(cached);
        final var recreated = source.physicalTable(99, replacement);
        assertThat(recreated).isNotSameAs(replaced);
        assertThat(recreated.id().table()).isEqualTo("OBJ# 99");
        assertThat(source.physicalTable(99, replacement)).isSameAs(recreated);
    }

    @ParameterizedTest
    @ValueSource(strings = { "ALTER TABLE APP.T ADD NAME VARCHAR(30)", "DROP TABLE APP.T", "ALTER TABLE APP.T RENAME TO T2", "TRUNCATE TABLE APP.T" })
    void shouldReleaseCachedPhysicalMetadataWhenApplyingDdl(String ddl) throws Exception {
        final var table = schema.getTables().forTable(TABLE_ID);
        final var beforeDdl = new AtomicReference<Table>();
        final var afterDdl = new AtomicReference<Table>();
        doAnswer(invocation -> {
            beforeDdl.compareAndSet(null, source.physicalTable(7, table));
            return true;
        }).when(dispatcher).dispatchDataChangeEvent(any(), any(), any());
        doAnswer(invocation -> {
            afterDdl.set(source.physicalTable(7, table));
            return null;
        }).when(dispatcher).dispatchSchemaChangeEvent(any(), any(), any(TableId.class), any());
        records(List.of(dml(101, 90, 110, 1), commit(110, 90, 1),
                Map.of("SCN", 121L, "START_SCN", 120L, "COMMIT_SCN", 130L, "XID", new byte[]{ 2 },
                        "OPERATION_CODE", 5, "DATA_OBJ#", 7L, "SQL_REDO", ddl),
                commit(130, 120, 2)));

        source.execute(context, new DamengPartition("dm"), new DamengOffsetContext(config, 90));

        assertThat(failure.get()).isNull();
        assertThat(beforeDdl.get()).isNotNull();
        assertThat(afterDdl.get()).isNotNull().isNotSameAs(beforeDdl.get());
    }

    @Test
    void shouldResumeAfterTheLastDeliveredStatementInsideATransaction() throws Exception {
        records(List.of(dml(101, 90, 120, 1), dml(102, 90, 120, 1), commit(120, 90, 1)));
        final var offset = new DamengOffsetContext(config, 90);
        source.init(offset);
        assertThat(source.getOffsetContext()).isSameAs(offset);
        offset.setCommitLsn(120);
        offset.setTransactionId("01");
        offset.setEventSerialNo(0);
        source.execute(context, new DamengPartition("dm"), offset);
        assertThat(failure.get()).isNull();
        assertThat(delivered).hasSize(1);
        assertThat(delivered.get(0)).containsEntry("change_lsn", 102L).containsEntry("event_serial_no", 1L)
                .containsEntry("lsn", 90L);
    }

    @ParameterizedTest
    @org.junit.jupiter.params.provider.CsvSource({ "false,fixed", "true,fixed", "false,archive_bytes", "true,archive_bytes" })
    void shouldCacheNewRangesAndReconstructOnlyAtCommit(boolean spill, String strategy) throws Exception {
        config = new DamengConnectorConfig(config.getConfig().edit().with("log.mining.batch.size", 10)
                .with("log.mining.window.strategy", strategy).with("log.mining.window.max.bytes", 64)
                .with("log.mining.buffer.memory.bytes", spill ? 512 : 1048576).with("log.mining.buffer.spill.path", spillDirectory.toString()).build());
        createSource();
        final var polls = new AtomicInteger();
        when(connection.latestArchivedLsn()).thenAnswer(invocation -> {
            if (polls.incrementAndGet() > 3) {
                running.set(false);
            }
            return 120L;
        });
        doAnswer(invocation -> {
            connection.latestArchivedLsn();
            return List.of(new DamengConnection.ArchiveLogFile("first", 90, 100, 42, 89, 0, 0, 0, 64),
                    new DamengConnection.ArchiveLogFile("second", 101, 110, 42, 100, 0, 0, 0, 64),
                    new DamengConnection.ArchiveLogFile("third", 111, 120, 42, 110, 0, 0, 0, 64));
        }).when(connection).readArchiveLogFiles();
        final List<Long> starts = new ArrayList<>();
        doAnswer(invocation -> {
            final long start = invocation.getArgument(0);
            final long end = invocation.getArgument(1);
            starts.add(start);
            if (end == 100) {
                records(List.of(dml(95, 92, 0, 1)));
            }
            else if (end == 110) {
                records(List.of(dml(105, 0, 0, 1)));
            }
            else if (start == 111) {
                records(List.of(commit(115, 0, 1)));
            }
            else if (start == 92) {
                records(List.of(dml(95, 92, 115, 1), dml(105, 92, 115, 1), commit(115, 92, 1)));
            }
            else {
                throw new AssertionError("Unexpected historical re-read " + start);
            }
            return null;
        }).when(connection).startLogMining(anyLong(), anyLong(), eq(false));
        final var offset = new DamengOffsetContext(config, 90);

        source.execute(context, new DamengPartition("dm"), offset);

        assertThat(failure.get()).isNull();
        assertThat(starts).containsExactly(90L, 101L, 111L, 92L);
        assertThat(delivered).extracting(value -> value.get("change_lsn"))
                .containsExactly(95L, 105L);
        assertThat(delivered).allSatisfy(value -> assertThat(value).containsEntry("lsn", 92L));
        assertThat(offset.getLsn()).isEqualTo(120);
        assertThat(metrics.getMiningSessions()).isEqualTo(4);
        assertThat(metrics.getReconstructionSessions()).isEqualTo(1);
        assertThat(metrics.getMinedRows()).isEqualTo(6);
        assertThat(metrics.getReconstructedRows()).isEqualTo(3);
        assertThat(metrics.getSpilledTransactions()).isEqualTo(spill ? 2 : 0);
        try (var files = java.nio.file.Files.list(spillDirectory)) {
            assertThat(files.toList()).isEmpty();
        }
        verify(connection).archiveLogsForRange(92, 110);
        verify(connection).archiveLogsForRange(92, 120);
        verify(connection, times(4)).endLogMining();
    }

    @Test
    void shouldFailBeforeMiningIfSelectedArchiveSizeIsUnavailable() throws Exception {
        config = new DamengConnectorConfig(config.getConfig().edit().with("log.mining.window.strategy", "archive_bytes").build());
        createSource();
        doReturn(List.of(new DamengConnection.ArchiveLogFile("unknown", 1, 150, 42, 0))).when(connection).readArchiveLogFiles();
        final var offset = new DamengOffsetContext(config, 90);
        source.execute(context, new DamengPartition("dm"), offset);
        assertThat(failure.get()).hasMessageContaining("LEN");
        assertThat(offset.getLsn()).isEqualTo(90);
        assertThat(delivered).isEmpty();
        verify(connection, never()).startLogMining(anyLong(), anyLong(), anyBoolean());
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void shouldKeepTheReplayPositionWhenArchivesOrForwardMiningAreUnavailable(boolean archiveMissing) throws Exception {
        config = new DamengConnectorConfig(config.getConfig().edit().with("log.mining.batch.size", 10)
                .with("log.mining.window.strategy", "fixed").build());
        createSource();
        when(connection.latestArchivedLsn()).thenReturn(110L);
        final var unavailable = new SQLException("historical range unavailable");
        if (archiveMissing) {
            doThrow(unavailable).when(connection).archiveLogsForRange(92, 110);
        }
        doAnswer(invocation -> {
            final long start = invocation.getArgument(0);
            if (start == 90) {
                records(List.of(dml(95, 92, 0, 1)));
            }
            else {
                throw unavailable;
            }
            return null;
        }).when(connection).startLogMining(anyLong(), anyLong(), eq(false));
        final var offset = new DamengOffsetContext(config, 90);

        source.execute(context, new DamengPartition("dm"), offset);

        assertThat(failure.get()).isSameAs(unavailable);
        assertThat(delivered).isEmpty();
        assertThat(offset.getLsn()).isEqualTo(92);
        assertThat(metrics.getScannedUpperLsn()).isEqualTo(100);
        assertThat(metrics.getOldestReplayLsn()).isEqualTo(92);
        if (archiveMissing) {
            verify(connection, times(1)).startLogMining(anyLong(), anyLong(), eq(false));
        }
        else {
            verify(connection).startLogMining(101, 110, false);
            verify(connection, times(2)).endLogMining();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void shouldKeepTheReplayPositionWhenHistoricalReconstructionIsUnavailable(boolean archiveMissing) throws Exception {
        config = new DamengConnectorConfig(config.getConfig().edit().with("log.mining.batch.size", 10)
                .with("log.mining.window.strategy", "fixed").build());
        createSource();
        when(connection.latestArchivedLsn()).thenReturn(110L);
        final var unavailable = new SQLException("historical range unavailable");
        if (archiveMissing) {
            doThrow(unavailable).when(connection).archiveLogsForRange(92, 110);
        }
        doAnswer(invocation -> {
            final long start = invocation.getArgument(0);
            if (start == 90) {
                records(List.of(dml(95, 92, 0, 1)));
            }
            else if (start == 101) {
                records(List.of(commit(105, 0, 1)));
            }
            else {
                throw unavailable;
            }
            return null;
        }).when(connection).startLogMining(anyLong(), anyLong(), eq(false));
        final var offset = new DamengOffsetContext(config, 90);

        source.execute(context, new DamengPartition("dm"), offset);

        assertThat(failure.get()).isSameAs(unavailable);
        assertThat(delivered).isEmpty();
        assertThat(offset.getLsn()).isEqualTo(92);
        assertThat(metrics.getScannedUpperLsn()).isEqualTo(100);
        assertThat(metrics.getOldestReplayLsn()).isEqualTo(92);
        if (archiveMissing) {
            verify(connection, times(1)).startLogMining(anyLong(), anyLong(), eq(false));
        }
        else {
            verify(connection).startLogMining(92, 110, false);
            verify(connection, times(3)).endLogMining();
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "path", "quota", "corruption" })
    void shouldNotAdvanceTheOffsetWhenTemporaryStorageFails(String mode) throws Exception {
        final var path = mode.equals("path") ? java.nio.file.Files.writeString(spillDirectory.resolve("not-directory"), "keep") : spillDirectory;
        config = new DamengConnectorConfig(config.getConfig().edit().with("log.mining.buffer.memory.bytes", 512)
                .with("log.mining.buffer.spill.path", path.toString())
                .with("log.mining.buffer.spill.max.bytes", mode.equals("quota") ? 1 : 1048576).build());
        createSource();
        records(List.of(dml(101, 90, 120, 1), commit(120, 90, 1)));
        if (mode.equals("corruption")) {
            doAnswer(invocation -> {
                try (var files = java.nio.file.Files.walk(spillDirectory)) {
                    final var file = files.filter(java.nio.file.Files::isRegularFile).findFirst().orElseThrow();
                    final var bytes = java.nio.file.Files.readAllBytes(file);
                    bytes[bytes.length - 1] ^= 1;
                    java.nio.file.Files.write(file, bytes);
                }
                return null;
            }).when(connection).validateUnchangedPrimary();
        }
        final var offset = new DamengOffsetContext(config, 90);
        source.execute(context, new DamengPartition("dm"), offset);
        assertThat(failure.get()).isNotNull();
        assertThat(io.debezium.connector.dameng.logminer.LogMinerTransactionBuffer.isStorageFailure(failure.get())).isTrue();
        assertThat(delivered).isEmpty();
        assertThat(offset.getLsn()).isEqualTo(90);
        assertThat(offset.getCommitLsn()).isEqualTo(90);
        if (!mode.equals("path")) {
            try (var files = java.nio.file.Files.list(spillDirectory)) {
                assertThat(files.toList()).isEmpty();
            }
        }
    }

    @Test
    void shouldPinAnEarlierOpenTransactionWhileDeliveringAnotherCommit() throws Exception {
        records(List.of(dml(101, 90, 0, 1), dml(110, 105, 120, 2), commit(120, 105, 2)));
        final var offset = new DamengOffsetContext(config, 90);
        source.execute(context, new DamengPartition("dm"), offset);
        assertThat(failure.get()).isNull();
        assertThat(delivered).hasSize(1);
        assertThat(offset.getLsn()).isEqualTo(90);
        assertThat(delivered.get(0)).containsEntry("lsn", 90L).containsEntry("mining_transaction_id", "02");
        assertThat(metrics.getOpenTransactions()).isEqualTo(1);
        assertThat(metrics.getOldestReplayLsn()).isEqualTo(90);
        assertThat(metrics.getScannedUpperLsn()).isEqualTo(150);
        assertThat(metrics.getLatestArchivedLsn()).isEqualTo(150);
        assertThat(metrics.getWindowBufferRecords()).isEqualTo(3);
        assertThat(metrics.getWindowBufferEstimatedBytes()).isPositive();
    }

    @Test
    void shouldNotReplayAnAlreadyCompletedTransaction() throws Exception {
        records(List.of(dml(101, 90, 120, 1), commit(120, 90, 1)));
        final var offset = new DamengOffsetContext(config, 90);
        offset.setCommitLsn(120);
        source.execute(context, new DamengPartition("dm"), offset);
        assertThat(failure.get()).isNull();
        assertThat(delivered).isEmpty();
    }

    @Test
    void shouldWaitForAnArchivedWindowWithoutMiningOrAdvancingTheReplayPosition() throws Exception {
        when(connection.latestArchivedLsn()).thenAnswer(invocation -> {
            running.set(false);
            return 90L;
        });
        final var offset = new DamengOffsetContext(config, 90);

        source.execute(context, new DamengPartition("dm"), offset);

        assertThat(failure.get()).isNull();
        assertThat(offset.getLsn()).isEqualTo(90);
        assertThat(metrics.getLatestArchivedLsn()).isEqualTo(90);
        assertThat(delivered).isEmpty();
        verify(connection, never()).startLogMining(anyLong(), anyLong(), anyBoolean());
    }

    @Test
    void shouldNotPublishAWindowMinedFromADemotedPrimary() throws Exception {
        records(List.of(dml(101, 90, 120, 1), commit(120, 90, 1)));
        final var demotion = new SQLException("primary was demoted", "08S01");
        doThrow(demotion).when(connection).validateUnchangedPrimary();
        final var offset = new DamengOffsetContext(config, 90);

        source.execute(context, new DamengPartition("dm"), offset);

        assertThat(failure.get()).isSameAs(demotion);
        assertThat(delivered).isEmpty();
        assertThat(offset.getLsn()).isEqualTo(90);
        assertThat(metrics.getWindowBufferRecords()).isEqualTo(2);
        verify(connection).endLogMining();
    }

    @Test
    void shouldNotPublishAWindowMinedAcrossChangedLexicalSettings() throws Exception {
        records(List.of(dml(101, 90, 120, 1), commit(120, 90, 1)));
        final var drift = new IllegalStateException("lexical settings changed during mining");
        doNothing().doThrow(drift).when(connection).validateUnchangedCompatibility();
        final var offset = new DamengOffsetContext(config, 90);

        source.execute(context, new DamengPartition("dm"), offset);

        assertThat(failure.get()).isSameAs(drift);
        assertThat(delivered).isEmpty();
        assertThat(schemaOffsets).isEmpty();
        assertThat(offset.getLsn()).isEqualTo(90);
        assertThat(offset.getCommitLsn()).isEqualTo(90);
        verify(connection).endLogMining();
    }

    @Test
    void shouldCloseMiningAndRetainTheReplayPositionWhenTheQueryFails() throws Exception {
        final var queryFailure = new SQLException("archive read failed", "08S01");
        when(rows.next()).thenThrow(queryFailure);
        final var offset = new DamengOffsetContext(config, 90);

        source.execute(context, new DamengPartition("dm"), offset);

        assertThat(failure.get()).isSameAs(queryFailure);
        assertThat(delivered).isEmpty();
        assertThat(offset.getLsn()).isEqualTo(90);
        assertThat(metrics.getScannedUpperLsn()).isEqualTo(90);
        verify(connection).endLogMining();
    }

    @Test
    void shouldKeepTheLastDeliveredOrdinalWhenStoppedDuringDispatch() throws Exception {
        records(List.of(dml(101, 90, 120, 1), dml(102, 90, 120, 1), commit(120, 90, 1)));
        stopAfter = 1;
        final var offset = new DamengOffsetContext(config, 90);
        source.execute(context, new DamengPartition("dm"), offset);
        assertThat(failure.get()).isNull();
        assertThat(delivered).hasSize(1);
        assertThat(offset.getLsn()).isEqualTo(90);
        assertThat(offset.getEventSerialNo()).isZero();
        assertThat(offset.getTransactionId()).isEqualTo("01");
        assertThat(metrics.getScannedUpperLsn()).isEqualTo(150);
        assertThat(metrics.getOldestReplayLsn()).isEqualTo(90);
    }

    @Test
    void shouldStopBeforePublishingTheNextCommittedTransaction() throws Exception {
        records(List.of(dml(101, 90, 120, 1), commit(120, 90, 1), dml(131, 125, 140, 2), commit(140, 125, 2)));
        doAnswer(invocation -> {
            running.set(false);
            return null;
        }).when(dispatcher).dispatchTransactionCommittedEvent(any(), any(), any());
        final var offset = new DamengOffsetContext(config, 90);

        source.execute(context, new DamengPartition("dm"), offset);

        assertThat(failure.get()).isNull();
        assertThat(delivered).hasSize(1);
        assertThat(delivered.get(0)).containsEntry("mining_transaction_id", "01");
        assertThat(offset.getLsn()).isEqualTo(90);
    }

    @Test
    void shouldSkipAnUnknownObjectWithoutSkippingTheNextCapturedRow() throws Exception {
        final var unknown = new java.util.HashMap<>(dml(101, 90, 120, 1));
        unknown.put("DATA_OBJ#", 99L);
        records(List.of(unknown, dml(102, 90, 120, 1), commit(120, 90, 1)));

        source.execute(context, new DamengPartition("dm"), new DamengOffsetContext(config, 90));

        assertThat(failure.get()).isNull();
        assertThat(delivered).singleElement().satisfies(value -> assertThat(value)
                .containsEntry("change_lsn", 102L).containsEntry("event_serial_no", 1L));
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "GRANT SELECT ON APP.T TO CDC_USER",
            "REVOKE SELECT ON APP.T FROM CDC_USER",
            "// 授予要采集表的 select 权限\nGRANT SELECT ON APP.T TO CDC_USER",
            "// 回收权限\r\nREVOKE SELECT ON APP.T FROM CDC_USER"
    })
    void shouldSkipPrivilegeDdlAndContinueDeliveringCapturedChanges(String ddl) throws Exception {
        final var privilege = Map.<String, Object> of("SCN", 101L, "START_SCN", 90L, "COMMIT_SCN", 110L, "XID", new byte[]{ 1 },
                "OPERATION_CODE", 5, "DATA_OBJ#", 7L, "SQL_REDO", ddl);
        records(List.of(privilege, commit(110, 90, 1), dml(121, 115, 130, 2), commit(130, 115, 2)));
        final var historicalTable = schema.getTables().forTable(TABLE_ID);
        final var offset = new DamengOffsetContext(config, 90);

        source.execute(context, new DamengPartition("dm"), offset);

        assertThat(failure.get()).isNull();
        assertThat(delivered).singleElement().satisfies(value -> assertThat(value).containsEntry("change_lsn", 121L));
        assertThat(schemaOffsets).isEmpty();
        assertThat(schema.getTables().forTable(TABLE_ID)).isSameAs(historicalTable);
        assertThat(offset.getLsn()).isEqualTo(150);
        assertThat(offset.getCommitLsn()).isEqualTo(130);
    }

    @Test
    void shouldFailOnUnknownTableDdlAfterPrivilegesWithoutAdvancingPastIt() throws Exception {
        final var privilege = Map.<String, Object> of("SCN", 101L, "START_SCN", 90L, "COMMIT_SCN", 110L, "XID", new byte[]{ 1 },
                "OPERATION_CODE", 5, "DATA_OBJ#", 7L, "SQL_REDO", "GRANT SELECT ON APP.T TO CDC_USER");
        final var unsupported = Map.<String, Object> of("SCN", 121L, "START_SCN", 115L, "COMMIT_SCN", 130L, "XID", new byte[]{ 2 },
                "OPERATION_CODE", 5, "DATA_OBJ#", 7L, "SQL_REDO", "// unsupported table change\nALTER TABLE APP.T MOVE");
        records(List.of(privilege, commit(110, 90, 1), unsupported, dml(122, 115, 130, 2), commit(130, 115, 2)));
        final var historicalTable = schema.getTables().forTable(TABLE_ID);
        final var offset = new DamengOffsetContext(config, 90);

        source.execute(context, new DamengPartition("dm"), offset);

        assertThat(failure.get()).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("Unsupported ALTER TABLE");
        assertThat(delivered).isEmpty();
        assertThat(schemaOffsets).isEmpty();
        assertThat(schema.getTables().forTable(TABLE_ID)).isSameAs(historicalTable);
        assertThat(offset.getLsn()).isEqualTo(90);
    }

    @Test
    void shouldFailOnMissingRedoInsteadOfAcknowledgingTheCapturedRow() throws Exception {
        final var missingRedo = new java.util.HashMap<>(dml(101, 90, 120, 1));
        missingRedo.remove("SQL_REDO");
        records(List.of(missingRedo, commit(120, 90, 1)));
        final var offset = new DamengOffsetContext(config, 90);

        source.execute(context, new DamengPartition("dm"), offset);

        assertThat(failure.get()).isInstanceOf(IllegalStateException.class).hasMessageContaining("Missing Dameng SQL_REDO");
        assertThat(delivered).isEmpty();
        assertThat(offset.getLsn()).isEqualTo(90);
    }

    @Test
    void shouldSampleBufferUsageDuringLargeWindows() throws Exception {
        final Map<String, Object> sequenceAllocation = Map.of("SCN", 101L, "OPERATION_CODE", 37);
        records(java.util.Collections.nCopies(1_025, sequenceAllocation));
        metrics = spy(metrics);
        source = new DamengStreamingChangeEventSource(config, factory, dispatcher, errorHandler, Clock.SYSTEM, schema, metrics);

        source.execute(context, new DamengPartition("dm"), new DamengOffsetContext(config, 90));

        assertThat(failure.get()).isNull();
        assertThat(delivered).isEmpty();
        assertThat(metrics.getWindowBufferRecords()).isEqualTo(1_025);
        verify(metrics).onBufferUsageUpdated(anyLong(), eq(1_024L));
    }

    @Test
    void shouldResumeTheNextTransactionSharingACommitPosition() throws Exception {
        records(List.of(dml(101, 90, 120, 1), commit(120, 90, 1), dml(110, 105, 120, 2), commit(120, 105, 2)));
        final var offset = new DamengOffsetContext(config, 90);
        offset.setCommitLsn(120);
        offset.setTransactionId("01");
        offset.setEventSerialNo(Long.MAX_VALUE);
        source.execute(context, new DamengPartition("dm"), offset);
        assertThat(failure.get()).isNull();
        assertThat(delivered).hasSize(1);
        assertThat(delivered.get(0)).containsEntry("mining_transaction_id", "02").containsEntry("event_serial_no", 0L);
    }

    @ParameterizedTest
    @ValueSource(strings = { "SCN", "OPERATION_CODE", "ROLL_BACK", "SSN", "CSF", "DATA_OBJ#" })
    void shouldRejectNullRequiredNumericFieldsBeforePublishingAnyTransaction(String field) throws Exception {
        final var invalid = new java.util.HashMap<>(dml(130, 125, 140, 2));
        invalid.put(field, null);
        records(List.of(dml(101, 90, 120, 1), commit(120, 90, 1), invalid, commit(140, 125, 2)));
        final var offset = new DamengOffsetContext(config, 90);
        source.execute(context, new DamengPartition("dm"), offset);
        assertThat(failure.get()).isInstanceOf(IllegalStateException.class).hasMessageContaining(field);
        assertThat(delivered).isEmpty();
        assertThat(offset.getLsn()).isEqualTo(90);
        assertThat(offset.getCommitLsn()).isEqualTo(90);
    }

    @Test
    void shouldPreserveNullableMarkerMetadataAndZeroObjectVersion() throws Exception {
        final var marker = new java.util.HashMap<>(commit(120, 0, 1));
        marker.put("START_SCN", null);
        marker.put("COMMIT_SCN", null);
        marker.put("DATA_OBJV#", 0L);
        marker.put("RBASQN", 0L);
        records(List.of(marker));
        assertThat(rows.next()).isTrue();
        final var row = source.readRow(rows);
        assertThat(row.startLsn()).isZero();
        assertThat(row.commitLsn()).isZero();
        assertThat(row.objectId()).isZero();
        assertThat(row.objectVersion()).isZero();
        assertThat(row.archiveSequence()).isZero();
        assertThat(row.blockNumber()).isEqualTo(-1);
        assertThat(row.byteOffset()).isEqualTo(-1);
    }

    @ParameterizedTest
    @ValueSource(longs = { 0, -1, 999 })
    void shouldRejectUnhandledOperationsWithUnresolvedObjectsAndKeepDiagnosticsAfterSpilling(long objectId) throws Exception {
        config = new DamengConnectorConfig(config.getConfig().edit().with("log.mining.buffer.memory.bytes", 512)
                .with("log.mining.buffer.spill.path", spillDirectory.toString()).build());
        createSource();
        final var unsupported = new java.util.HashMap<>(dml(101, 90, 120, 1));
        unsupported.put("OPERATION_CODE", 255);
        unsupported.put("DATA_OBJ#", objectId);
        unsupported.put("DATA_OBJV#", 2L);
        unsupported.put("RBASQN", 0L);
        unsupported.put("RBABLK", 123L);
        unsupported.put("RBABYTE", 45L);
        records(List.of(unsupported, commit(120, 90, 1)));
        final var offset = new DamengOffsetContext(config, 90);
        source.execute(context, new DamengPartition("dm"), offset);
        assertThat(failure.get()).isInstanceOf(IllegalStateException.class)
                .hasMessageContainingAll("operation 255", "unresolved table", "LSN 101", "RBABLK=123", "RBABYTE=45", "DATA_OBJV#=2")
                .hasMessageNotContaining("INSERT INTO");
        assertThat(delivered).isEmpty();
        assertThat(offset.getLsn()).isEqualTo(90);
        assertThat(offset.getCommitLsn()).isEqualTo(90);
    }

    @Test
    void shouldSkipUnhandledOperationsOnHistoricallyKnownExcludedTables() throws Exception {
        final var excluded = new TableId(null, "APP", "EXCLUDED");
        schema.getTables().overwriteTable(schema.getTables().forTable(TABLE_ID).edit().tableId(excluded)
                .addAttribute(Attribute.editor().name("dameng.object.id").value("8").create()).create());
        final var unsupported = new java.util.HashMap<>(dml(101, 90, 120, 1));
        unsupported.put("OPERATION_CODE", 255);
        unsupported.put("DATA_OBJ#", 8L);
        records(List.of(unsupported, commit(120, 90, 1)));
        final var offset = new DamengOffsetContext(config, 90);
        source.execute(context, new DamengPartition("dm"), offset);
        assertThat(failure.get()).isNull();
        assertThat(delivered).isEmpty();
        assertThat(offset.getLsn()).isEqualTo(150);
    }

    @Test
    void shouldIndexHistoricalObjectsOnceForManyRowsIncludingUnknownObjects() throws Exception {
        final var tables = spy(schema.getTables());
        when(schema.getTables()).thenReturn(tables);
        final List<Map<String, Object>> input = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            final var change = new java.util.HashMap<>(dml(101, 90, 120, 1));
            change.put("DATA_OBJ#", i % 2 == 0 ? 7L : 99L);
            input.add(change);
        }
        input.add(commit(120, 90, 1));
        records(input);
        source.execute(context, new DamengPartition("dm"), new DamengOffsetContext(config, 90));
        assertThat(failure.get()).isNull();
        assertThat(delivered).hasSize(50);
        verify(tables, times(1)).tableIds();
    }

    @Test
    void shouldRejectDuplicateHistoricalObjectIdentities() throws Exception {
        schema.getTables().overwriteTable(schema.getTables().forTable(TABLE_ID).edit().tableId(new TableId(null, "APP", "DUPLICATE"))
                .addAttribute(Attribute.editor().name("dameng.object.id").value("7").create()).create());
        records(List.of(dml(101, 90, 120, 1), commit(120, 90, 1)));
        source.execute(context, new DamengPartition("dm"), new DamengOffsetContext(config, 90));
        assertThat(failure.get()).hasMessageContaining("Duplicate Dameng historical DATA_OBJ# 7");
        assertThat(delivered).isEmpty();
    }

    @ParameterizedTest
    @ValueSource(ints = { 13, 14, 255, 999 })
    void shouldFailInsteadOfSkippingAnUnhandledCapturedOperation(int operation) throws Exception {
        final var unsupported = new java.util.HashMap<>(dml(101, 90, 120, 1));
        unsupported.put("OPERATION_CODE", operation);
        records(List.of(unsupported, commit(120, 90, 1)));
        final var offset = new DamengOffsetContext(config, 90);
        source.execute(context, new DamengPartition("dm"), offset);
        assertThat(failure.get()).isInstanceOf(IllegalStateException.class).hasMessageContaining("Unsupported Dameng LogMiner operation " + operation);
        assertThat(delivered).isEmpty();
        assertThat(offset.getLsn()).isEqualTo(90);
    }

    @Test
    void shouldKeepTruncateReplayableUntilItsDataEventIsAcknowledged() throws Exception {
        records(List.of(truncate(), commit(120, 90, 1)));
        source.execute(context, new DamengPartition("dm"), new DamengOffsetContext(config, 90));

        assertThat(failure.get()).isNull();
        assertThat(schemaOffsets).hasSize(1);
        assertThat(schemaOffsets.get(0).get("event_serial_no")).isEqualTo(-1L);
        assertThat(delivered).singleElement().satisfies(offset -> assertThat(offset)
                .containsEntry("event_serial_no", 0L).containsEntry("change_lsn", 101L));
    }

    @Test
    void shouldReplayTruncateAfterAcknowledgingOnlyItsSchemaEvent() throws Exception {
        records(List.of(truncate(), commit(120, 90, 1)));
        final var offset = new DamengOffsetContext(config, 90);
        offset.setCommitLsn(120);
        offset.setTransactionId("01");
        offset.setEventSerialNo(-1);
        source.execute(context, new DamengPartition("dm"), offset);

        assertThat(failure.get()).isNull();
        assertThat(delivered).hasSize(1);
    }

    @Test
    void shouldPublishBeyondIgnoredLegacyBudgets() throws Exception {
        config = new DamengConnectorConfig(config.getConfig().edit().with("log.mining.buffer.max.bytes", 1)
                .with("log.mining.buffer.max.records", 1).build());
        createSource();
        records(List.of(dml(101, 90, 120, 1), commit(120, 90, 1)));
        final var offset = new DamengOffsetContext(config, 90);
        source.execute(context, new DamengPartition("dm"), offset);
        assertThat(failure.get()).isNull();
        assertThat(delivered).hasSize(1);
        assertThat(metrics.getBufferLimitFailures()).isZero();
        assertThat(metrics.getWindowBufferRecords()).isEqualTo(2);
        assertThat(metrics.getWindowBufferEstimatedBytes()).isGreaterThan(1);
        assertThat(offset.getLsn()).isEqualTo(150);
    }

    @Test
    void shouldFailWithoutPublishingOrAdvancingOffsetsForOversizedSql() throws Exception {
        config = new DamengConnectorConfig(config.getConfig().edit().with("log.mining.buffer.max.sql.bytes", 8).build());
        createSource();
        records(List.of(dml(101, 90, 120, 1), commit(120, 90, 1)));
        final var offset = new DamengOffsetContext(config, 90);
        source.execute(context, new DamengPartition("dm"), offset);
        assertThat(failure.get()).hasMessageContaining("max.sql.bytes");
        assertThat(delivered).isEmpty();
        assertThat(metrics.getBufferLimitFailures()).isEqualTo(1);
        assertThat(offset.getLsn()).isEqualTo(90);
        assertThat(offset.getCommitLsn()).isEqualTo(90);
    }

    @Test
    void shouldRecordCapacityFailureWithoutPublishingOrHidingItBehindCleanupFailure() throws Exception {
        config = new DamengConnectorConfig(config.getConfig().edit().with("log.mining.buffer.max.transactions", 1).build());
        createSource();
        records(List.of(dml(101, 90, 110, 1), commit(110, 90, 1), dml(120, 115, 0, 2)));
        final var cleanupFailure = new SQLException("cleanup connection lost", "08S01");
        doThrow(cleanupFailure).when(connection).endLogMining();
        final var offset = new DamengOffsetContext(config, 90);
        source.execute(context, new DamengPartition("dm"), offset);

        assertThat(failure.get()).isInstanceOf(IllegalStateException.class).hasMessageContaining("log.mining.buffer.max.transactions");
        assertThat(failure.get().getSuppressed()).containsExactly(cleanupFailure);
        assertThat(delivered).isEmpty();
        assertThat(offset.getLsn()).isEqualTo(90);
        assertThat(metrics.getBufferLimitFailures()).isEqualTo(1);
        assertThat(metrics.getWindowBufferRecords()).isEqualTo(3);
        assertThat(metrics.getPeakWindowBufferRecords()).isEqualTo(3);
        assertThat(metrics.getScannedUpperLsn()).isEqualTo(90);
        assertThat(metrics.getOldestReplayLsn()).isEqualTo(90);
        assertThat(metrics.getLatestArchivedLsn()).isEqualTo(150);
    }

    @Test
    void shouldNotReportAFailedMiningSessionAsACompletedScan() throws Exception {
        records(List.of(dml(101, 90, 120, 1), commit(120, 90, 1)));
        final var cleanupFailure = new SQLException("end mining failed");
        doThrow(cleanupFailure).when(connection).endLogMining();
        final var offset = new DamengOffsetContext(config, 90);
        source.execute(context, new DamengPartition("dm"), offset);

        assertThat(failure.get()).isSameAs(cleanupFailure);
        assertThat(delivered).isEmpty();
        assertThat(offset.getLsn()).isEqualTo(90);
        assertThat(metrics.getBufferLimitFailures()).isZero();
        assertThat(metrics.getScannedUpperLsn()).isEqualTo(90);
        assertThat(metrics.getWindowBufferRecords()).isEqualTo(2);
    }

    @Test
    void shouldKeepLastCompletedScanPositionWhenReadingIsInterrupted() throws Exception {
        records(List.of(dml(101, 90, 120, 1), commit(120, 90, 1)));
        doAnswer(invocation -> {
            running.set(false);
            return new byte[]{ 1 };
        }).when(rows).getBytes("XID");
        source.execute(context, new DamengPartition("dm"), new DamengOffsetContext(config, 90));

        assertThat(failure.get()).isNull();
        assertThat(delivered).isEmpty();
        assertThat(metrics.getScannedUpperLsn()).isEqualTo(90);
        assertThat(metrics.getWindowBufferRecords()).isEqualTo(1);
        assertThat(metrics.getOpenTransactions()).isEqualTo(1);
    }

    private Map<String, Object> truncate() {
        return Map.of("SCN", 101L, "START_SCN", 90L, "COMMIT_SCN", 120L, "XID", new byte[]{ 1 },
                "OPERATION_CODE", 5, "DATA_OBJ#", 7L, "SQL_REDO", "TRUNCATE TABLE APP.T");
    }

    private void records(List<Map<String, Object>> records) throws Exception {
        org.mockito.Mockito.reset(rows);
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
        when(rows.getTimestamp("TIMESTAMP")).thenReturn(Timestamp.valueOf("2026-09-24 12:00:00"));
    }

    private Map<String, Object> dml(long lsn, long start, long commit, int xid) {
        return Map.of("SCN", lsn, "START_SCN", start, "COMMIT_SCN", commit, "XID", new byte[]{ (byte) xid },
                "OPERATION_CODE", 1, "DATA_OBJ#", 7L, "SQL_REDO", "INSERT INTO \"150\".\"OBJ# 7\"(\"COL 1\") VALUES(1)");
    }

    private Map<String, Object> commit(long lsn, long start, int xid) {
        return Map.of("SCN", lsn, "START_SCN", start, "COMMIT_SCN", lsn, "XID", new byte[]{ (byte) xid }, "OPERATION_CODE", 7);
    }
}
