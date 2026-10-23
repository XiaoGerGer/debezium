/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.management.ManagementFactory;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import javax.management.ObjectName;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import io.debezium.config.Configuration;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;
import io.debezium.storage.file.history.FileSchemaHistory;

/** Recovery uses test-owned tables and offsets; it never changes a running external task. */
class DamengRestartNonceIT extends AbstractAsyncEngineConnectorTest {
    @TempDir
    Path directory;
    private Connection writer;
    private String table;
    private String signal;
    private String other;
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

    @BeforeEach
    void before() throws Exception {
        writer = DriverManager.getConnection(DamengTestDatabase.jdbcUrl(), DamengTestDatabase.user(), DamengTestDatabase.password());
        writer.setAutoCommit(false);
        table = "DBZ_NONCE_" + UUID.randomUUID().toString().replace("-", "").substring(0, 12).toUpperCase();
        signal = table + "_S";
        other = table + "_OTHER";
        execute("CREATE TABLE " + qualified(table) + " (ID INT PRIMARY KEY, NAME VARCHAR(100))");
        execute("CREATE TABLE " + qualified(other) + " (ID INT PRIMARY KEY)");
        execute("INSERT INTO " + qualified(table) + " VALUES(1,'old')");
        writer.commit();
        config = Configuration.create().with("topic.prefix", "dameng")
                .with("database.hostname", DamengTestDatabase.hostname()).with("database.port", DamengTestDatabase.port())
                .with("database.user", DamengTestDatabase.user()).with("database.password", DamengTestDatabase.password())
                .with("table.include.list", DamengTestDatabase.schema() + "\\." + table)
                .with("signal.data.collection", qualified(signal)).with("incremental.snapshot.chunk.size", 1)
                .with("schema.history.internal", FileSchemaHistory.class.getName())
                .with("schema.history.internal.file.filename", directory.resolve("history.dat").toString())
                .with("snapshot.mode", "initial").with("poll.interval.ms", 100).with("heartbeat.interval.ms", 100)
                .with("offset.flush.interval.ms", 100).with("include.schema.changes", false).with("tombstones.on.delete", false).build();
    }

    @AfterEach
    void after() throws Exception {
        writer.rollback();
        stopConnector();
        try {
            for (String name : List.of(signal, table, other)) {
                try {
                    execute("DROP TABLE " + qualified(name));
                }
                catch (java.sql.SQLException ignored) {
                    // Some failure cases stop before the signal table is created.
                }
            }
        }
        finally {
            writer.close();
        }
    }

    @Test
    void shouldSkipDataByDefaultAndResumeSameNonceIncludingAfterRemovingIt() throws Exception {
        config = config.edit().with("recovery.restart.nonce", "first").build();
        startAndWait();
        assertNoData();
        insert(2);
        final var first = nextData();
        assertThat(value(first).getString("op")).isEqualTo("c");
        final Object baseline = first.sourceOffset().get("recovery_baseline_lsn");
        stopConnector();
        consumedLines.clear();
        insert(3);
        startAndWait();
        final var second = nextData();
        assertThat(value(second).getStruct("after").getInt32("ID")).isEqualTo(3);
        assertThat(second.sourceOffset().get("recovery_baseline_lsn")).isEqualTo(baseline);
        stopConnector();
        consumedLines.clear();
        config = config.edit().without("recovery.restart.nonce").build();
        insert(4);
        startAndWait();
        final var third = nextData();
        assertThat(value(third).getStruct("after").getInt32("ID")).isEqualTo(4);
        assertThat(third.sourceOffset().get("recovery_nonce")).isEqualTo("first");
        assertThat(third.sourceOffset().get("recovery_baseline_lsn")).isEqualTo(baseline);
    }

    @Test
    void shouldPersistBaselineBeforeTheFirstBusinessRecordAndSurviveLostOffsetCheckpoint() throws Exception {
        startAndWait();
        final var old = nextData();
        stopConnector();
        consumedLines.clear();
        config = config.edit().with("recovery.restart.nonce", "first").build();
        startAndWait();
        assertNoData();
        final var baseline = historyBaseline();
        stopConnector();
        consumedLines.clear();
        // Model a crash before Connect acknowledges any recovery record.
        storeOffsets(config, Map.of(old.sourcePartition(), old.sourceOffset()));
        insert(2);
        startAndWait();
        final var record = nextData();
        assertThat(value(record).getStruct("after").getInt32("ID")).isEqualTo(2);
        assertThat(record.sourceOffset().get("recovery_baseline_lsn").toString()).isEqualTo(baseline);
        assertThat(java.nio.file.Files.readAllLines(directory.resolve("history.dat")).stream()
                .filter(line -> line.contains("dameng_recovery_baseline"))).hasSize(1);
    }

    @Test
    void shouldReplaceInvalidLegacyOffsetAndUseCurrentSchemaThenAcceptAnotherNonce() throws Exception {
        startAndWait();
        final var old = nextData();
        stopConnector();
        consumedLines.clear();
        final Map<String, Object> legacy = new HashMap<>(old.sourceOffset());
        legacy.remove("compatibility_signature");
        legacy.put("lsn", 1L);
        storeOffsets(config, Map.of(old.sourcePartition(), legacy));
        execute("ALTER TABLE " + qualified(table) + " RENAME COLUMN NAME TO LABEL");
        insert(2);
        config = config.edit().with("recovery.restart.nonce", "first").build();
        startAndWait();
        assertNoData();
        insert(3);
        final var first = nextData();
        assertThat(value(first).getStruct("after").getString("LABEL")).isEqualTo("row-3");
        assertThat(value(first).getStruct("after").schema().field("NAME")).isNull();
        stopConnector();
        consumedLines.clear();
        insert(4);
        config = config.edit().with("recovery.restart.nonce", "second").build();
        startAndWait();
        assertNoData();
        insert(5);
        final var second = nextData();
        assertThat(second.sourceOffset().get("recovery_nonce")).isEqualTo("second");
        assertThat(((Number) second.sourceOffset().get("recovery_baseline_lsn")).longValue())
                .isGreaterThan(((Number) first.sourceOffset().get("recovery_baseline_lsn")).longValue());
        stopConnector();
        consumedLines.clear();
        insert(6);
        startAndWait();
        assertThat(value(nextData()).getStruct("after").getString("LABEL")).isEqualTo("row-6");
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void shouldBackfillOnlyAutomaticallyWhenRequestedOrAfterAManualSignal(boolean automatic) throws Exception {
        config = config.edit().with("recovery.restart.nonce", "first")
                .with("recovery.incremental.snapshot.auto.start", automatic).build();
        startAndWait();
        if (!automatic) {
            assertNoData();
            execute("INSERT INTO " + qualified(signal) + " VALUES('manual-1','execute-snapshot','{\"type\":\"incremental\",\"data-collections\":[\""
                    + qualified(table) + "\"]}')");
            writer.commit();
        }
        final var row = nextData();
        assertThat(value(row).getString("op")).isEqualTo("r");
        assertThat(value(row).getStruct("source").getString("snapshot")).isEqualTo("incremental");
        assertThat(value(row).getStruct("after").getInt32("ID")).isEqualTo(1);
        assertNoData();
        stopConnector();
        consumedLines.clear();
        config = config.edit().with("recovery.incremental.snapshot.auto.start", true).build();
        startAndWait();
        assertNoData();
    }

    @ParameterizedTest
    @ValueSource(strings = { "commit", "savepoint", "rollback", "restart" })
    void shouldCapturePostCutChangesOfTransactionsAlreadyActiveOnOtherTables(String outcome) throws Exception {
        try (var active = DriverManager.getConnection(DamengTestDatabase.jdbcUrl(), DamengTestDatabase.user(), DamengTestDatabase.password())) {
            active.setAutoCommit(false);
            try {
                active.createStatement().execute("INSERT INTO " + qualified(other) + " VALUES(1)");
                config = config.edit().with("recovery.restart.nonce", "first").with("log.mining.batch.size", 10)
                        .with("log.mining.window.strategy", "fixed").build();
                startAndWait();
                assertNoData();
                active.createStatement().execute("INSERT INTO " + qualified(table) + " VALUES(2,'keep')");
                final var savepoint = active.setSavepoint("RECOVERY_POINT");
                active.createStatement().execute("INSERT INTO " + qualified(table) + " VALUES(3,'suffix')");
                // Force several forward windows while the old transaction stays open.
                insert(9);
                assertThat(value(nextData()).getStruct("after").getInt32("ID")).isEqualTo(9);
                if (outcome.equals("restart")) {
                    stopConnector();
                    consumedLines.clear();
                    startAndWait();
                }
                if (outcome.equals("rollback")) {
                    active.rollback();
                }
                else {
                    if (outcome.equals("savepoint")) {
                        active.rollback(savepoint);
                    }
                    active.commit();
                    final var kept = nextData();
                    assertThat(value(kept).getStruct("after").getInt32("ID")).isEqualTo(2);
                    assertThat(((Number) kept.sourceOffset().get("lsn")).longValue())
                            .isGreaterThanOrEqualTo(((Number) kept.sourceOffset().get("recovery_baseline_lsn")).longValue());
                    if (outcome.equals("commit") || outcome.equals("restart")) {
                        assertThat(value(nextData()).getStruct("after").getInt32("ID")).isEqualTo(3);
                    }
                }
                insert(10);
                assertThat(value(nextData()).getStruct("after").getInt32("ID")).isEqualTo(10);
                assertNoData();
            }
            finally {
                active.rollback();
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void shouldPersistNoBaselineUntilCapturedWritersFinishAndRespectTheLockDeadline(boolean timeout) throws Exception {
        execute("INSERT INTO " + qualified(table) + " VALUES(2,'before cut')");
        config = config.edit().with("recovery.restart.nonce", "lock-test")
                .with("snapshot.max.duration.ms", timeout ? 3_000 : 30_000).build();
        startEngine();
        final var mbeans = ManagementFactory.getPlatformMBeanServer();
        final var name = new ObjectName("debezium.dameng:type=connector-metrics,context=snapshot,server=dameng");
        Awaitility.await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
            healthy();
            assertThat(mbeans.getAttribute(name, "SnapshotLockWaiting")).isEqualTo(true);
        });
        assertNoBaseline();
        if (timeout) {
            Awaitility.await().atMost(Duration.ofSeconds(15)).until(() -> failure.get() != null);
            assertNoBaseline();
            writer.rollback();
        }
        else {
            writer.commit();
            waitForStreamingRunning("dameng", "dameng");
            assertNoData();
            assertThat(historyBaseline()).isNotBlank();
            insert(3);
            assertThat(value(nextData()).getStruct("after").getInt32("ID")).isEqualTo(3);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void shouldAllowCdcWithoutPrimaryKeysButRejectAutomaticBackfillBeforePersistingBaseline(boolean automatic) throws Exception {
        execute("DROP TABLE " + qualified(table));
        execute("CREATE TABLE " + qualified(table) + " (ID INT, NAME VARCHAR(100))");
        insert(1);
        config = config.edit().with("recovery.restart.nonce", "no-pk")
                .with("recovery.incremental.snapshot.auto.start", automatic).build();
        if (automatic) {
            config = config.edit().with("notification.enabled.channels", "sink")
                    .with("notification.sink.topic.name", "recovery-notifications").build();
            startEngine();
            Awaitility.await().atMost(Duration.ofSeconds(30)).until(() -> failure.get() != null);
            assertThat(failure.get()).hasStackTraceContaining("requires primary keys");
            assertNoBaseline();
            stopConnector();
            consumedLines.clear();
            execute("ALTER TABLE " + qualified(table) + " ADD PRIMARY KEY(ID)");
            writer.commit();
            // Correcting a pre-persistence failure can reuse the same incident,
            // even if an ABORTED sink notification has checkpointed its offset.
            startAndWait();
            final var backfill = nextData();
            assertThat(value(backfill).getString("op")).isEqualTo("r");
            assertThat(value(backfill).getStruct("after").getInt32("ID")).isEqualTo(1);
        }
        else {
            startAndWait();
            assertNoData();
            insert(2);
            final var change = nextData();
            assertThat(change.key()).isNull();
            assertThat(value(change).getStruct("after").getInt32("ID")).isEqualTo(2);
        }
    }

    private void assertNoBaseline() throws Exception {
        final var history = directory.resolve("history.dat");
        if (java.nio.file.Files.exists(history)) {
            assertThat(java.nio.file.Files.readString(history)).doesNotContain("dameng_recovery_baseline");
        }
    }

    private void startAndWait() throws Exception {
        startEngine();
        waitForStreamingRunning("dameng", "dameng");
        healthy();
    }

    private void startEngine() throws Exception {
        failure.set(null);
        start(DamengConnector.class, config, (success, message, error) -> {
            if (!success) {
                failure.set(error == null ? new IllegalStateException(message) : error);
            }
        }, null);
    }

    private SourceRecord nextData() throws Exception {
        final long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
        while (System.nanoTime() < deadline) {
            healthy();
            if (waitForAvailableRecords(100, TimeUnit.MILLISECONDS)) {
                final var record = consumeRecord();
                if (record.topic().equals("dameng." + qualified(table)) && record.value() instanceof Struct value && value.schema().field("op") != null) {
                    return record;
                }
            }
        }
        throw new AssertionError("No recovery change received");
    }

    private void assertNoData() throws Exception {
        final long deadline = System.nanoTime() + Duration.ofSeconds(1).toNanos();
        final List<SourceRecord> unexpected = new ArrayList<>();
        while (System.nanoTime() < deadline) {
            healthy();
            if (waitForAvailableRecords(100, TimeUnit.MILLISECONDS)) {
                final var record = consumeRecord();
                if (record.topic().equals("dameng." + qualified(table)) && record.value() instanceof Struct value && value.schema().field("op") != null) {
                    unexpected.add(record);
                }
            }
        }
        assertThat(unexpected).isEmpty();
    }

    private String historyBaseline() throws Exception {
        final String line = java.nio.file.Files.readAllLines(directory.resolve("history.dat")).stream()
                .filter(value -> value.contains("dameng_recovery_baseline")).findFirst().orElseThrow();
        return io.debezium.document.DocumentReader.defaultReader().read(line).getDocument("position").get("recovery_baseline_lsn").asObject().toString();
    }

    private void healthy() {
        if (failure.get() != null) {
            throw new AssertionError("Recovery task failed", failure.get());
        }
    }

    private void insert(int id) throws Exception {
        execute("INSERT INTO " + qualified(table) + " VALUES(" + id + ",'row-" + id + "')");
        writer.commit();
    }

    private void execute(String sql) throws Exception {
        try (var statement = writer.createStatement()) {
            statement.execute(sql);
        }
    }

    private static String qualified(String name) {
        return DamengTestDatabase.schema() + "." + name;
    }

    private static Struct value(SourceRecord record) {
        return (Struct) record.value();
    }
}
