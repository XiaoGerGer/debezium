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
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
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
                .with("database.hostname", "localhost").with("database.user", "APP").with("database.password", "test")
                .with("table.include.list", "APP.T").build());
        connection = mock(DamengConnection.class);
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

    @Test
    void shouldFailInsteadOfSkippingAnUnhandledCapturedOperation() throws Exception {
        final var unsupported = new java.util.HashMap<>(dml(101, 90, 120, 1));
        unsupported.put("OPERATION_CODE", 255);
        records(List.of(unsupported, commit(120, 90, 1)));
        final var offset = new DamengOffsetContext(config, 90);
        source.execute(context, new DamengPartition("dm"), offset);
        assertThat(failure.get()).isInstanceOf(IllegalStateException.class).hasMessageContaining("Unsupported Dameng LogMiner operation 255");
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
    void shouldRecordCapacityFailureWithoutPublishingOrHidingItBehindCleanupFailure() throws Exception {
        config = new DamengConnectorConfig(config.getConfig().edit().with("log.mining.buffer.max.records", 2).build());
        createSource();
        records(List.of(dml(101, 90, 110, 1), commit(110, 90, 1), dml(120, 115, 0, 2)));
        final var cleanupFailure = new SQLException("cleanup connection lost", "08S01");
        doThrow(cleanupFailure).when(connection).endLogMining();
        final var offset = new DamengOffsetContext(config, 90);
        source.execute(context, new DamengPartition("dm"), offset);

        assertThat(failure.get()).isInstanceOf(IllegalStateException.class).hasMessageContaining("buffer limit");
        assertThat(failure.get().getSuppressed()).containsExactly(cleanupFailure);
        assertThat(delivered).isEmpty();
        assertThat(offset.getLsn()).isEqualTo(90);
        assertThat(metrics.getBufferLimitFailures()).isEqualTo(1);
        assertThat(metrics.getWindowBufferRecords()).isEqualTo(2);
        assertThat(metrics.getPeakWindowBufferRecords()).isEqualTo(2);
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
        final var index = new AtomicInteger(-1);
        when(rows.next()).thenAnswer(invocation -> index.incrementAndGet() < records.size());
        when(rows.getLong(anyString())).thenAnswer(invocation -> ((Number) records.get(index.get()).getOrDefault(invocation.getArgument(0), 0L)).longValue());
        when(rows.getInt(anyString())).thenAnswer(invocation -> ((Number) records.get(index.get()).getOrDefault(invocation.getArgument(0), 0)).intValue());
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
