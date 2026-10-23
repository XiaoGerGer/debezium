/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.lang.management.ManagementFactory;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import javax.management.ObjectName;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import io.debezium.config.Configuration;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;
import io.debezium.storage.file.history.FileSchemaHistory;

/** Exercises safety limits with actual DM locks and archived transactions. */
class DamengRuntimeSafetyIT extends AbstractAsyncEngineConnectorTest {
    @TempDir
    Path directory;
    private Connection writer;
    private String table;
    private Configuration config;
    private final AtomicReference<Throwable> failure = new AtomicReference<>();

    @BeforeAll
    static void startDatabase() throws Exception {
        DamengTestDatabase.start();
    }

    @AfterAll
    static void stopDatabase() {
        DamengTestDatabase.stop();
    }

    @Override
    protected int getMaximumEnqueuedRecordCount() {
        return 64;
    }

    @BeforeEach
    void before() throws Exception {
        writer = DriverManager.getConnection(DamengTestDatabase.jdbcUrl(), DamengTestDatabase.user(), DamengTestDatabase.password());
        writer.setAutoCommit(false);
        table = "DBZ_SAFETY_" + UUID.randomUUID().toString().replace("-", "").substring(0, 12).toUpperCase();
        execute("CREATE TABLE " + tableName() + " (ID INT PRIMARY KEY, REVISION INT)");
        writer.commit();
        config = Configuration.create()
                .with("log.mining.window.strategy", System.getProperty("dameng.test.window.strategy", "archive_bytes"))
                .with("topic.prefix", "dameng")
                .with("database.hostname", DamengTestDatabase.hostname())
                .with("database.port", DamengTestDatabase.port())
                .with("database.user", DamengTestDatabase.user())
                .with("database.password", DamengTestDatabase.password())
                .with("database.dbname", System.getProperty("dameng.database", "DAMENG"))
                .with("table.include.list", DamengTestDatabase.schema() + "\\." + table)
                .with("schema.history.internal", FileSchemaHistory.class.getName())
                .with("schema.history.internal.file.filename", directory.resolve("history.dat").toString())
                .with("snapshot.mode", "no_data")
                .with("include.schema.changes", false)
                .with("max.queue.size", 64)
                .with("max.batch.size", 32)
                .with("poll.interval.ms", 100)
                .build();
    }

    @AfterEach
    void after() throws Exception {
        try {
            if (writer != null) {
                writer.rollback();
            }
        }
        finally {
            try {
                stopConnector();
                DamengTestDatabase.restoreManagedArchives();
            }
            finally {
                if (writer != null) {
                    try {
                        execute("DROP TABLE " + tableName());
                        writer.commit();
                    }
                    finally {
                        writer.close();
                    }
                }
            }
        }
    }

    @Test
    void shouldExposeSnapshotLockWaitingAndConfirmedReleaseThroughJmx() throws Exception {
        execute("INSERT INTO " + tableName() + " VALUES(1,0)");
        config = config.edit().with("snapshot.mode", "initial").with("snapshot.max.duration.ms", 30_000).build();
        final var mbeans = ManagementFactory.getPlatformMBeanServer();
        final var name = new ObjectName("debezium.dameng:type=connector-metrics,context=snapshot,server=dameng");
        startEngine();
        Awaitility.await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
            assertThat(mbeans.isRegistered(name)).isTrue();
            assertThat(mbeans.getAttribute(name, "SnapshotLockWaiting")).isEqualTo(true);
            assertThat(mbeans.getAttribute(name, "SnapshotLocksHeld")).isEqualTo(false);
            assertThat(mbeans.getAttribute(name, "LockedTableCount")).isEqualTo(0);
            assertThat((Long) mbeans.getAttribute(name, "SnapshotLockWaitDurationMillis")).isGreaterThanOrEqualTo(200L);
        });
        // End the business transaction so the snapshot can acquire its shared lock.
        writer.commit();
        Awaitility.await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
            assertThat(mbeans.getAttribute(name, "SnapshotCompleted")).isEqualTo(true);
            assertThat(mbeans.getAttribute(name, "SnapshotLockWaiting")).isEqualTo(false);
            assertThat(mbeans.getAttribute(name, "SnapshotLocksHeld")).isEqualTo(false);
            assertThat(mbeans.getAttribute(name, "LockedTableCount")).isEqualTo(0);
            assertThat(mbeans.getAttribute(name, "SnapshotLockHoldDurationMillis")).isEqualTo(0L);
            assertThat(mbeans.getAttribute(name, "LastSnapshotLockStatisticsAvailable")).isEqualTo(true);
            assertThat(mbeans.getAttribute(name, "LastSnapshotLockedTableCount")).isEqualTo(1);
            assertThat((Long) mbeans.getAttribute(name, "LastSnapshotLockWaitDurationMillis")).isGreaterThanOrEqualTo(200L);
            assertThat((Long) mbeans.getAttribute(name, "LastSnapshotLockHoldDurationMillis")).isGreaterThanOrEqualTo(0L);
        });
        assertThat(failure.get()).isNull();
    }

    @Test
    void shouldApplyQueryTimeoutToSnapshotLockWaits() throws Exception {
        execute("INSERT INTO " + tableName() + " VALUES(1,0)");
        final var lockConfig = new DamengConnectorConfig(config.edit().with("database.query.timeout.ms", 1_000).build());
        try (var reader = new DamengConnection(lockConfig.getJdbcConfig(), false)) {
            final long started = System.nanoTime();
            assertThatThrownBy(() -> reader.executeWithoutCommitting("LOCK TABLE " + tableName() + " IN SHARE MODE"))
                    .isInstanceOf(SQLException.class);
            assertThat(Duration.ofNanos(System.nanoTime() - started)).isLessThan(Duration.ofSeconds(15));
            reader.connection().rollback();
        }
    }

    @Test
    void shouldReleaseSnapshotLocksWhenDownstreamStallsPastTheDeadline() throws Exception {
        insertRows(500);
        writer.commit();
        config = config.edit().with("snapshot.mode", "initial").with("snapshot.max.duration.ms", 2_000).build();
        startEngine();
        // The consumer deliberately does not drain its 64-record queue. Snapshot
        // work must terminate even while the event producer is blocked enqueueing.
        Awaitility.await().atMost(Duration.ofSeconds(15)).until(() -> consumedLines.size() == 64);
        // This write waits for the shared lock to be released by the deadline.
        execute("UPDATE " + tableName() + " SET REVISION=1 WHERE ID=1");
        writer.commit();
        // The test consumer itself is blocked. Let it drain only after proving
        // lock release so the engine can surface its terminal failure callback.
        Awaitility.await().atMost(Duration.ofSeconds(30)).until(() -> {
            consumedLines.clear();
            return failure.get() != null;
        });
        assertThat(failure.get()).hasStackTraceContaining("snapshot.max.duration.ms");
        try (var statement = writer.createStatement(); var result = statement.executeQuery("SELECT REVISION FROM " + tableName() + " WHERE ID=1")) {
            assertThat(result.next()).isTrue();
            assertThat(result.getInt(1)).isEqualTo(1);
        }
    }

    @Test
    void shouldReleaseSnapshotLocksWhenStoppedWithABlockedConsumer() throws Exception {
        insertRows(500);
        writer.commit();
        // Explicit shutdown must release the lock well before the snapshot timer.
        config = config.edit().with("snapshot.mode", "initial").with("snapshot.max.duration.ms", 60_000).build();
        startEngine();
        Awaitility.await().atMost(Duration.ofSeconds(15)).until(() -> consumedLines.size() == 64);
        assertThat(failure.get()).isNull();
        try (var statement = writer.createStatement()) {
            statement.setQueryTimeout(1);
            assertThatThrownBy(() -> statement.executeUpdate("UPDATE " + tableName() + " SET REVISION=1 WHERE ID=1"))
                    .as("the stalled snapshot must still hold its shared table lock")
                    .isInstanceOf(SQLException.class);
        }
        writer.rollback();

        final var shutdown = Executors.newScheduledThreadPool(2);
        try {
            final long started = System.nanoTime();
            final var stopped = shutdown.submit(() -> {
                stopConnector();
            });
            // The framework's consumer spins while its queue is full. Drain it
            // concurrently so engine shutdown can reach the connector task.
            shutdown.scheduleWithFixedDelay(consumedLines::clear, 0, 10, TimeUnit.MILLISECONDS);
            try (var statement = writer.createStatement()) {
                statement.setQueryTimeout(10);
                assertThat(statement.executeUpdate("UPDATE " + tableName() + " SET REVISION=1 WHERE ID=1")).isEqualTo(1);
            }
            writer.commit();
            assertThat(Duration.ofNanos(System.nanoTime() - started)).isLessThan(Duration.ofSeconds(10));
            stopped.get(10, TimeUnit.SECONDS);
            assertThat(failure.get()).isNull();
            try (var statement = writer.createStatement(); var result = statement.executeQuery("SELECT REVISION FROM " + tableName() + " WHERE ID=1")) {
                assertThat(result.next()).isTrue();
                assertThat(result.getInt(1)).isEqualTo(1);
            }
        }
        finally {
            shutdown.shutdownNow();
            assertThat(shutdown.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void shouldDeliverAnEntireTransactionBeyondLegacyBudgetsWithForcedSpilling() throws Exception {
        config = config.edit().with("log.mining.buffer.max.records", 1).with("log.mining.buffer.max.bytes", 1)
                .with("log.mining.buffer.memory.bytes", 1024).with("log.mining.buffer.spill.path", directory.resolve("spill").toString()).build();
        startEngine();
        waitForStreamingRunning("dameng", "dameng");
        insertRows(200);
        writer.commit();
        final var ids = new java.util.HashSet<Integer>();
        for (int i = 0; i < 200; i++) {
            final var event = (Struct) nextDataRecord().value();
            assertThat(event.getString("op")).isEqualTo("c");
            assertThat(ids.add(event.getStruct("after").getInt32("ID"))).isTrue();
        }
        assertThat(ids).hasSize(200).contains(1, 200);
        assertThat(failure.get()).isNull();
        final var mbeans = ManagementFactory.getPlatformMBeanServer();
        final var name = new ObjectName("debezium.dameng:type=connector-metrics,context=streaming,server=dameng");
        assertThat((Long) mbeans.getAttribute(name, "SpilledTransactions")).isPositive();
        assertThat(mbeans.getAttribute(name, "BufferLimitFailures")).isEqualTo(0L);
    }

    @Test
    void shouldFailBeforePublishingWhenOneSqlPayloadExceedsItsLimit() throws Exception {
        config = config.edit().with("log.mining.buffer.max.sql.bytes", 8).build();
        startEngine();
        waitForStreamingRunning("dameng", "dameng");
        insertRows(1);
        writer.commit();
        awaitFailure("log.mining.buffer.max.sql.bytes");
        assertThat(consumedLines).as("an oversized SQL must not publish a partial transaction").isEmpty();
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void shouldFailWithoutPublishingOrAdvancingOffsetsWhenRequiredArchivesDisappear(boolean openTransaction) throws Exception {
        Assumptions.assumeTrue(System.getProperty("dameng.hostname") == null,
                "archive removal is restricted to the fixture-owned disposable database");
        startEngine();
        waitForStreamingRunning("dameng", "dameng");
        execute("INSERT INTO " + tableName() + " VALUES(1,0)");
        writer.commit();
        SourceRecord checkpoint = nextDataRecord();
        assertThat(((Struct) checkpoint.value()).getStruct("after").getInt32("ID")).isEqualTo(1);
        if (openTransaction) {
            execute("INSERT INTO " + tableName() + " VALUES(2,0)");
            try (var marker = DriverManager.getConnection(DamengTestDatabase.jdbcUrl(), DamengTestDatabase.user(), DamengTestDatabase.password());
                    var statement = marker.createStatement()) {
                statement.execute("INSERT INTO " + tableName() + " VALUES(3,0)");
            }
            checkpoint = nextDataRecord();
            assertThat(((Struct) checkpoint.value()).getStruct("after").getInt32("ID")).isEqualTo(3);
            assertThat(((Number) checkpoint.sourceOffset().get("lsn")).longValue())
                    .as("the committed marker must retain an earlier replay position for the open transaction")
                    .isLessThan(((Number) checkpoint.sourceOffset().get("change_lsn")).longValue());
        }
        stopConnector();
        storeOffsets(config, Map.of(checkpoint.sourcePartition(), checkpoint.sourceOffset()));
        consumedLines.clear();
        if (!openTransaction) {
            execute("INSERT INTO " + tableName() + " VALUES(2,0)");
        }
        writer.commit();
        try (var database = new DamengConnection(new DamengConnectorConfig(config).getJdbcConfig(), false)) {
            final long end = database.currentLsn();
            Awaitility.await("the offline change is archived before retention loss").atMost(Duration.ofSeconds(30))
                    .until(() -> database.latestArchivedLsn() >= end);
            final long restart = ((Number) checkpoint.sourceOffset().get("lsn")).longValue();
            assertThat(database.archiveLogContaining(restart)).as("the restart position exists before fault injection").isPresent();
            DamengTestDatabase.hideManagedArchives(database.readArchiveLogFiles().stream().map(DamengConnection.ArchiveLogFile::path).toList());
        }
        failure.set(null);
        startEngine();
        Awaitility.await("archive loss must fail instead of skipping historical changes").atMost(Duration.ofSeconds(45))
                .until(() -> failure.get() != null);
        assertThat(failure.get()).hasMessageContaining("no longer available on the server");
        stopConnector();
        assertThat(consumedLines).as("neither a new snapshot nor partial incremental data may hide missing archives").isEmpty();
        assertThat(readLastCommittedOffset(config, checkpoint.sourcePartition()))
                .as("the durable checkpoint must remain unchanged after archive loss")
                .containsAllEntriesOf(checkpoint.sourceOffset());
        try (var statement = writer.createStatement(); var result = statement.executeQuery("SELECT COUNT(*) FROM " + tableName())) {
            assertThat(result.next()).isTrue();
            assertThat(result.getInt(1)).isEqualTo(openTransaction ? 3 : 2);
        }
    }

    private SourceRecord nextDataRecord() throws Exception {
        final long deadline = System.nanoTime() + Duration.ofSeconds(45).toNanos();
        while (System.nanoTime() < deadline) {
            assertThat(failure.get()).isNull();
            if (waitForAvailableRecords(100, TimeUnit.MILLISECONDS)) {
                final var record = consumeRecord();
                if (record != null && record.value() instanceof Struct value && value.schema().field("op") != null) {
                    value.validate();
                    return record;
                }
            }
        }
        throw new AssertionError("No Dameng data record received within 45 seconds");
    }

    private void insertRows(int count) throws SQLException {
        try (var statement = writer.prepareStatement("INSERT INTO " + tableName() + " VALUES(?,0)")) {
            for (int id = 1; id <= count; id++) {
                statement.setInt(1, id);
                statement.addBatch();
            }
            assertThat(statement.executeBatch()).hasSize(count);
        }
    }

    private void startEngine() {
        start(DamengConnector.class, config, (success, message, error) -> {
            if (!success) {
                failure.set(error == null ? new IllegalStateException(message) : error);
            }
        }, null);
    }

    private void awaitFailure(String expectedMessage) {
        Awaitility.await().atMost(Duration.ofSeconds(30)).until(() -> failure.get() != null);
        assertThat(failure.get()).hasStackTraceContaining(expectedMessage);
    }

    private String tableName() {
        return '"' + DamengTestDatabase.schema() + "\".\"" + table + '"';
    }

    private void execute(String sql) throws SQLException {
        try (var statement = writer.createStatement()) {
            statement.setQueryTimeout(5);
            statement.execute(sql);
        }
    }
}
