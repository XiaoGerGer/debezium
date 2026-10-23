/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.debezium.config.Configuration;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;
import io.debezium.storage.file.history.FileSchemaHistory;

/**
 * Repeatable large-transaction correctness tests against the real DM LogMiner. The
 * row count and payload size can be increased without changing the expected events.
 * No throughput threshold is imposed on shared CI hosts.
 */
class DamengLargeTransactionIT extends AbstractAsyncEngineConnectorTest {
    private static final int ROWS = Integer.getInteger("dameng.stress.rows", 10_000);
    private static final int PAYLOAD_BYTES = Integer.getInteger("dameng.stress.payload.bytes", 512);
    private static final Duration TIMEOUT = Duration.ofSeconds(Long.getLong("dameng.stress.timeout.seconds", 300));
    private static final int QUEUE_CAPACITY = 64;
    private static final int JDBC_BATCH_SIZE = 250;
    private static final MemoryMXBean MEMORY = ManagementFactory.getMemoryMXBean();

    @TempDir
    Path temporaryDirectory;

    private Connection connection;
    private String table;
    private Configuration config;
    private String payload;
    private final AtomicReference<Throwable> connectorFailure = new AtomicReference<>();
    private final AtomicLong sampledHeapPeakBytes = new AtomicLong();
    private ScheduledExecutorService heapSampler;
    private long deliveredDataEvents;

    @BeforeAll
    static void startDatabase() throws Exception {
        assertThat(ROWS).as("large-transaction row count").isGreaterThanOrEqualTo(1_000);
        assertThat(PAYLOAD_BYTES).as("ASCII payload bytes").isBetween(1, 2_000);
        assertThat(TIMEOUT).isPositive();
        DamengTestDatabase.start();
    }

    @AfterAll
    static void stopDatabase() {
        DamengTestDatabase.stop();
    }

    @Override
    protected int getMaximumEnqueuedRecordCount() {
        return QUEUE_CAPACITY;
    }

    @BeforeEach
    void before() throws Exception {
        heapSampler = Executors.newSingleThreadScheduledExecutor(runnable -> {
            final var thread = new Thread(runnable, "dameng-stress-heap-sampler");
            thread.setDaemon(true);
            return thread;
        });
        sampleHeap();
        heapSampler.scheduleAtFixedRate(this::sampleHeap, 100, 100, TimeUnit.MILLISECONDS);
        connection = newConnection();
        table = "DBZ_STRESS_" + UUID.randomUUID().toString().replace("-", "").substring(0, 12).toUpperCase();
        payload = "x".repeat(PAYLOAD_BYTES);
        execute(connection, "CREATE TABLE " + tableName() + " (ID INT PRIMARY KEY, REVISION INT, PAYLOAD VARCHAR(2000))");
        connection.commit();
        config = Configuration.create()
                .with("log.mining.window.strategy", System.getProperty("dameng.test.window.strategy", "archive_bytes"))
                .with("topic.prefix", "dameng")
                .with("log.mining.buffer.memory.bytes", 64 * 1024)
                .with("log.mining.buffer.spill.path", temporaryDirectory.resolve("spill").toString())
                .with("database.hostname", DamengTestDatabase.hostname())
                .with("database.port", DamengTestDatabase.port())
                .with("database.user", DamengTestDatabase.user())
                .with("database.password", DamengTestDatabase.password())
                .with("database.dbname", System.getProperty("dameng.database", "DAMENG"))
                .with("table.include.list", DamengTestDatabase.schema() + "\\." + table)
                .with("schema.history.internal", FileSchemaHistory.class.getName())
                .with("schema.history.internal.file.filename", temporaryDirectory.resolve("schema-history.dat").toString())
                .with("snapshot.mode", "no_data")
                .with("poll.interval.ms", 100)
                .with("include.schema.changes", false)
                .with("tombstones.on.delete", false)
                .with("max.queue.size", QUEUE_CAPACITY)
                .with("max.batch.size", QUEUE_CAPACITY / 2)
                .with("query.fetch.size", 257)
                .with("log.mining.batch.size", Integer.getInteger("dameng.stress.mining.batch.size", 1_000))
                .build();
    }

    @AfterEach
    void after() throws Exception {
        try {
            try {
                if (connection != null) {
                    connection.rollback();
                }
            }
            finally {
                try {
                    stopConnector();
                }
                finally {
                    if (connection != null) {
                        try {
                            execute(connection, "DROP TABLE " + tableName());
                            connection.commit();
                        }
                        finally {
                            connection.close();
                        }
                    }
                }
            }
        }
        finally {
            if (heapSampler != null) {
                heapSampler.shutdownNow();
                heapSampler.awaitTermination(5, TimeUnit.SECONDS);
            }
        }
    }

    @Test
    void shouldDeliverLargeMixedTransactionUnderBackpressure() throws Exception {
        config = config.edit().with("provide.transaction.metadata", true).build();
        startEngine();
        final long started = System.nanoTime();
        insertRows(connection, 1, ROWS);
        final int updated = ROWS / 2;
        final int deleted = ROWS / 4;
        execute(connection, "UPDATE " + tableName() + " SET REVISION=1 WHERE ID<=" + updated);
        execute(connection, "DELETE FROM " + tableName() + " WHERE ID<=" + deleted);
        connection.commit();

        // Withhold consumption until the downstream queue fills. Both queues are
        // intentionally far smaller than one transaction, so delivery must resume.
        Awaitility.await("consumer queue is saturated by the large transaction").atMost(TIMEOUT)
                .until(() -> {
                    assertConnectorHealthy();
                    return consumedLines.size() == QUEUE_CAPACITY;
                });
        final long deadline = deadline();
        final String transactionId = assertBeginTransaction(nextRecord(deadline));
        long order = 0;
        Object commitLsn = null;
        for (int id = 1; id <= ROWS; id++) {
            final var record = assertInsert(nextRecord(deadline), id);
            assertRecordTransactionMetadata(record, transactionId, ++order, order);
            if (commitLsn == null) {
                commitLsn = record.sourceOffset().get("commit_lsn");
            }
            assertThat(record.sourceOffset().get("commit_lsn")).isEqualTo(commitLsn);
        }
        // Set-based DML need not visit the primary key in numeric order. Check
        // every key exactly once rather than imposing a database execution plan.
        final var updatedIds = new java.util.BitSet(updated + 1);
        for (int index = 0; index < updated; index++) {
            final var record = nextRecord(deadline);
            final var value = envelope(record);
            assertThat(value.getString("op")).isEqualTo("u");
            final int id = value.getStruct("after").getInt32("ID");
            assertThat(id).isBetween(1, updated);
            assertThat(updatedIds.get(id)).as("duplicate update for id %s", id).isFalse();
            updatedIds.set(id);
            assertRow(value.getStruct("before"), id, 0);
            assertRow(value.getStruct("after"), id, 1);
            assertRecordTransactionMetadata(record, transactionId, ++order, order);
            assertThat(record.sourceOffset().get("commit_lsn")).isEqualTo(commitLsn);
        }
        final var deletedIds = new java.util.BitSet(deleted + 1);
        for (int index = 0; index < deleted; index++) {
            final var record = nextRecord(deadline);
            final var value = envelope(record);
            assertThat(value.getString("op")).isEqualTo("d");
            final int id = value.getStruct("before").getInt32("ID");
            assertThat(id).isBetween(1, deleted);
            assertThat(deletedIds.get(id)).as("duplicate delete for id %s", id).isFalse();
            deletedIds.set(id);
            assertRow(value.getStruct("before"), id, 1);
            assertThat(value.get("after")).isNull();
            assertRecordTransactionMetadata(record, transactionId, ++order, order);
            assertThat(record.sourceOffset().get("commit_lsn")).isEqualTo(commitLsn);
        }
        assertEndTransaction(nextRecord(deadline), transactionId, (int) order,
                Map.of(DamengTestDatabase.schema() + "." + table, (int) order));
        assertNoRecords();
        report("mixed transaction with bounded queues", ROWS + (long) updated + deleted, started);
    }

    @Test
    void shouldKeepLargeOpenTransactionAcrossOtherCommits() throws Exception {
        startEngine();
        final long started = System.nanoTime();
        final int firstHalf = ROWS / 2;
        insertRows(connection, 1, firstHalf);
        final var firstMarker = commitMarker(ROWS + 1);
        // Seeing another transaction proves the miner scanned past the open
        // transaction without publishing its uncommitted rows.
        insertRows(connection, firstHalf + 1, ROWS);
        final var secondMarker = commitMarker(ROWS + 2);
        connection.commit();

        final long deadline = deadline();
        final var first = assertInsert(nextRecord(deadline), 1);
        assertThat(((Number) firstMarker.sourceOffset().get("lsn")).longValue())
                .isLessThanOrEqualTo(((Number) first.sourceOffset().get("change_lsn")).longValue());
        assertThat(((Number) secondMarker.sourceOffset().get("lsn")).longValue())
                .isLessThanOrEqualTo(((Number) first.sourceOffset().get("change_lsn")).longValue());
        for (int id = 2; id <= ROWS; id++) {
            assertInsert(nextRecord(deadline), id);
        }
        assertNoRecords();
        report("open transaction across other commits", ROWS + 2L, started);
    }

    @Test
    void shouldDiscardLargeTransactionAfterFullRollback() throws Exception {
        startEngine();
        final long started = System.nanoTime();
        insertRows(connection, 1, ROWS);
        commitMarker(ROWS + 1);
        connection.rollback();
        commitMarker(ROWS + 2);
        assertNoRecords();
        report("full rollback", ROWS + 2L, started);
    }

    @Test
    void shouldDiscardLargeSavepointSuffixAndRetainOriginalBeforeImages() throws Exception {
        startEngine();
        final long started = System.nanoTime();
        final int retained = ROWS / 2;
        insertRows(connection, 1, retained);
        final var savepoint = connection.setSavepoint("DBZ_STRESS_SAVEPOINT");
        insertRows(connection, retained + 1, ROWS);
        execute(connection, "UPDATE " + tableName() + " SET REVISION=99 WHERE ID<=" + retained);
        commitMarker(ROWS + 1);
        try (var files = java.nio.file.Files.walk(temporaryDirectory.resolve("spill"))) {
            assertThat(files.filter(java.nio.file.Files::isRegularFile).count()).isPositive();
        }
        connection.rollback(savepoint);
        execute(connection, "UPDATE " + tableName() + " SET REVISION=1 WHERE ID=1");
        connection.commit();

        final long deadline = deadline();
        for (int id = 1; id <= retained; id++) {
            assertInsert(nextRecord(deadline), id);
        }
        final var update = envelope(nextRecord(deadline));
        assertThat(update.getString("op")).isEqualTo("u");
        assertRow(update.getStruct("before"), 1, 0);
        assertRow(update.getStruct("after"), 1, 1);
        assertNoRecords();
        report("savepoint rollback", ROWS + (long) retained + 2, started);
    }

    @Test
    void shouldResumeLargeTransactionFromAcknowledgedMiddleOffset() throws Exception {
        startEngine();
        final long started = System.nanoTime();
        insertRows(connection, 1, ROWS);
        connection.commit();
        final long deadline = deadline();
        final int acknowledged = ROWS / 2;
        SourceRecord checkpoint = null;
        for (int id = 1; id <= ROWS; id++) {
            final var record = assertInsert(nextRecord(deadline), id);
            if (id == acknowledged) {
                checkpoint = record;
            }
        }
        assertThat(checkpoint).isNotNull();
        assertNoRecords();
        stopConnector();
        // Simulate a durable offset acknowledging only the first half of the
        // transaction. Later deliveries were not durably acknowledged downstream.
        storeOffsets(config, Map.of(checkpoint.sourcePartition(), checkpoint.sourceOffset()));
        consumedLines.clear();
        insertRows(connection, ROWS + 1, ROWS + 1);
        connection.commit();
        startEngine();

        final long resumedDeadline = deadline();
        final Object commitLsn = checkpoint.sourceOffset().get("commit_lsn");
        for (int id = acknowledged + 1; id <= ROWS; id++) {
            final var record = assertInsert(nextRecord(resumedDeadline), id);
            assertThat(record.sourceOffset().get("commit_lsn")).isEqualTo(commitLsn);
            assertThat(((Number) record.sourceOffset().get("event_serial_no")).longValue())
                    .isGreaterThan(((Number) checkpoint.sourceOffset().get("event_serial_no")).longValue());
        }
        assertInsert(nextRecord(resumedDeadline), ROWS + 1);
        assertNoRecords();
        report("resume at a durable mid-transaction offset", ROWS + 1L, started);
    }

    private SourceRecord commitMarker(int id) throws Exception {
        try (Connection markerConnection = newConnection()) {
            insertRows(markerConnection, id, id);
            markerConnection.commit();
        }
        return assertInsert(nextRecord(deadline()), id);
    }

    private void insertRows(Connection writer, int first, int last) throws SQLException {
        try (var statement = writer.prepareStatement("INSERT INTO " + tableName() + " VALUES(?,0,?)")) {
            statement.setString(2, payload);
            int pending = 0;
            for (int id = first; id <= last; id++) {
                statement.setInt(1, id);
                statement.addBatch();
                pending++;
                if (pending == JDBC_BATCH_SIZE || id == last) {
                    assertThat(statement.executeBatch()).hasSize(pending);
                    statement.clearBatch();
                    pending = 0;
                }
            }
        }
    }

    private SourceRecord assertInsert(SourceRecord record, int id) {
        final var value = envelope(record);
        assertThat(value.getString("op")).isEqualTo("c");
        assertThat(value.get("before")).isNull();
        assertRow(value.getStruct("after"), id, 0);
        assertThat(((Struct) record.key()).getInt32("ID")).isEqualTo(id);
        return record;
    }

    private void assertRow(Struct row, int id, int revision) {
        assertThat(row.getInt32("ID")).isEqualTo(id);
        assertThat(row.getInt32("REVISION")).isEqualTo(revision);
        assertThat(row.getString("PAYLOAD")).isEqualTo(payload);
    }

    private SourceRecord nextRecord(long deadline) throws Exception {
        while (System.nanoTime() < deadline) {
            assertConnectorHealthy();
            final var record = consumedLines.poll(100, TimeUnit.MILLISECONDS);
            if (record != null) {
                if (envelope(record).schema().field("op") != null) {
                    deliveredDataEvents++;
                    assertThat(record.topic()).isEqualTo("dameng." + DamengTestDatabase.schema() + "." + table);
                    assertThat(record.key()).isInstanceOf(Struct.class);
                    final var row = envelope(record).getStruct("after") != null
                            ? envelope(record).getStruct("after")
                            : envelope(record).getStruct("before");
                    assertThat(((Struct) record.key()).getInt32("ID")).isEqualTo(row.getInt32("ID"));
                }
                envelope(record).validate();
                if (record.key() instanceof Struct key) {
                    key.validate();
                }
                return record;
            }
        }
        throw new AssertionError("Dameng stress test did not receive all expected records within " + TIMEOUT);
    }

    private void assertNoRecords() throws Exception {
        final long deadline = System.nanoTime() + Duration.ofSeconds(2).toNanos();
        while (System.nanoTime() < deadline) {
            assertConnectorHealthy();
            assertThat(consumedLines.poll(100, TimeUnit.MILLISECONDS)).as("no duplicates or rolled-back rows").isNull();
        }
    }

    private void startEngine() throws Exception {
        connectorFailure.set(null);
        start(DamengConnector.class, config, (success, message, error) -> {
            if (!success) {
                connectorFailure.set(error != null ? error : new IllegalStateException(message));
            }
        }, null);
        assertConnectorHealthy();
        waitForStreamingRunning("dameng", "dameng");
    }

    private void assertConnectorHealthy() {
        final Throwable failure = connectorFailure.get();
        if (failure != null) {
            throw new AssertionError("Dameng connector failed during large-transaction test", failure);
        }
    }

    private long deadline() {
        return System.nanoTime() + TIMEOUT.toNanos();
    }

    private void report(String scenario, long workloadRowChanges, long started) {
        sampleHeap();
        // This is sampled usage of the entire test JVM, including the driver,
        // assertions and framework. It is not the connector's retained heap.
        logger.info("Dameng stress scenario '{}': rows={}, payloadBytes={}, workloadRowChanges={}, deliveredDataEvents={}, elapsedMs={}, "
                + "testJvmSampledHeapPeakBytes={}, testJvmHeapMaxBytes={}, heapSamplingIntervalMs=100",
                scenario, ROWS, PAYLOAD_BYTES, workloadRowChanges, deliveredDataEvents, TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started),
                sampledHeapPeakBytes.get(), MEMORY.getHeapMemoryUsage().getMax());
    }

    private void sampleHeap() {
        sampledHeapPeakBytes.accumulateAndGet(MEMORY.getHeapMemoryUsage().getUsed(), Math::max);
    }

    private static Struct envelope(SourceRecord record) {
        return (Struct) record.value();
    }

    private static Connection newConnection() throws SQLException {
        final var result = DriverManager.getConnection(DamengTestDatabase.jdbcUrl(), DamengTestDatabase.user(), DamengTestDatabase.password());
        result.setAutoCommit(false);
        return result;
    }

    private String tableName() {
        return '"' + DamengTestDatabase.schema() + "\".\"" + table + '"';
    }

    private static void execute(Connection writer, String sql) throws SQLException {
        try (var statement = writer.createStatement()) {
            statement.execute(sql);
        }
    }
}
