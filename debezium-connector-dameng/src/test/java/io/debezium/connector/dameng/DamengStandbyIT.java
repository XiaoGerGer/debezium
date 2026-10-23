/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.management.ManagementFactory;
import java.nio.file.Path;
import java.sql.DriverManager;
import java.time.Duration;
import java.util.HashSet;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import javax.management.ObjectName;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import io.debezium.config.Configuration;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;
import io.debezium.storage.file.history.FileSchemaHistory;

/** Exercises the physical standby, including its completed replay boundary. */
class DamengStandbyIT extends AbstractAsyncEngineConnectorTest {
    @TempDir
    Path directory;
    private final AtomicReference<Throwable> failure = new AtomicReference<>();

    @Test
    void shouldRecoverSpilledStandbyTransactionsAndExcludeRolledBackRows() throws Exception {
        try (var environment = new DamengPrimaryStandbyEnvironment()) {
            environment.start();
            final String table = "DBZ_ST_TX_" + UUID.randomUUID().toString().replace("-", "").substring(0, 10).toUpperCase();
            execute(environment, "CREATE TABLE SYSDBA." + table + " (ID INT PRIMARY KEY, LABEL VARCHAR(100))");
            final var spill = directory.resolve("spill");
            final var config = configuration(environment, table).with("snapshot.mode", "no_data")
                    .with("log.mining.window.strategy", "fixed").with("log.mining.batch.size", 256)
                    .with("log.mining.buffer.memory.bytes", 1024).with("log.mining.buffer.spill.path", spill.toString()).build();
            try {
                startEngine(config);
                Awaitility.await().atMost(Duration.ofSeconds(45)).ignoreExceptions().untilAsserted(() -> {
                    final var offset = readLastCommittedOffset(config, new DamengPartition("dameng").getSourcePartition());
                    assertThat(offset).containsEntry("snapshot_completed", true);
                });
                execute(environment, "INSERT INTO SYSDBA." + table + " VALUES(0,'started')");
                assertThat(afterId(nextData(table))).isZero();
                try (var writer = environment.primaryConnection(); var statement = writer.createStatement()) {
                    writer.setAutoCommit(false);
                    for (int id = 1; id <= 30; id++) {
                        statement.execute("INSERT INTO SYSDBA." + table + " VALUES(" + id + ",'pending')");
                    }
                    execute(environment, "INSERT INTO SYSDBA." + table + " VALUES(100,'cross window')");
                    assertThat(afterId(nextData(table))).isEqualTo(100);
                    try (var files = java.nio.file.Files.walk(spill)) {
                        assertThat(files.anyMatch(java.nio.file.Files::isRegularFile)).as("pending transaction must actually spill").isTrue();
                    }
                    stopConnector();
                    consumedLines.clear();
                    startEngine(config);
                    execute(environment, "INSERT INTO SYSDBA." + table + " VALUES(101,'recovered')");
                    int recovered;
                    do {
                        recovered = afterId(nextData(table));
                        assertThat(recovered).isIn(100, 101);
                    } while (recovered != 101);
                    writer.commit();
                    final var ids = new HashSet<Integer>();
                    for (int i = 0; i < 30; i++) {
                        final int id = afterId(nextData(table));
                        assertThat(id).isBetween(1, 30);
                        assertThat(ids.add(id)).isTrue();
                    }
                    statement.execute("INSERT INTO SYSDBA." + table + " VALUES(31,'kept')");
                    final var savepoint = writer.setSavepoint("DBZ_ST_KEEP");
                    statement.execute("INSERT INTO SYSDBA." + table + " VALUES(32,'undone')");
                    execute(environment, "INSERT INTO SYSDBA." + table + " VALUES(102,'before partial rollback')");
                    assertThat(afterId(nextData(table))).isEqualTo(102);
                    writer.rollback(savepoint);
                    statement.execute("INSERT INTO SYSDBA." + table + " VALUES(33,'after rollback')");
                    writer.commit();
                    assertThat(afterId(nextData(table))).isEqualTo(31);
                    assertThat(afterId(nextData(table))).isEqualTo(33);
                    statement.execute("INSERT INTO SYSDBA." + table + " VALUES(34,'full rollback')");
                    writer.rollback();
                    execute(environment, "INSERT INTO SYSDBA." + table + " VALUES(103,'finished')");
                    assertThat(afterId(nextData(table))).isEqualTo(103);
                    assertThat(failure.get()).isNull();
                }
                stopConnector();
                consumedLines.clear();
                execute(environment, "INSERT INTO SYSDBA." + table + " VALUES(104,'explicitly skipped')");
                final var recovery = config.edit().with("recovery.restart.nonce", "standby-latest").build();
                startEngine(recovery);
                Awaitility.await().atMost(Duration.ofSeconds(45)).ignoreExceptions().untilAsserted(() -> {
                    final var offset = readLastCommittedOffset(recovery, new DamengPartition("dameng").getSourcePartition());
                    assertThat(offset).containsEntry(DamengOffsetContext.RECOVERY_NONCE_KEY, "standby-latest");
                });
                execute(environment, "INSERT INTO SYSDBA." + table + " VALUES(105,'after recovery baseline')");
                assertThat(afterId(nextData(table))).isEqualTo(105);
            }
            finally {
                stopConnector();
            }
        }
    }

    private static int afterId(SourceRecord record) {
        return ((Struct) record.value()).getStruct("after").getInt32("ID");
    }

    @Test
    void shouldExpandAndShrinkStandbyCaptureScopeWithoutLosingRetainedOrRestartChanges() throws Exception {
        try (var environment = new DamengPrimaryStandbyEnvironment()) {
            environment.start();
            final var suffix = UUID.randomUUID().toString().replace("-", "").substring(0, 10).toUpperCase();
            final String retained = "DBZ_ST_SCOPE_A_" + suffix;
            final String added = "DBZ_ST_SCOPE_B_" + suffix;
            final String signal = "DBZ_ST_SCOPE_S_" + suffix;
            execute(environment, "CREATE TABLE SYSDBA." + retained + " (ID INT PRIMARY KEY, NAME VARCHAR(100))",
                    "CREATE TABLE SYSDBA." + added + " (ID INT PRIMARY KEY, NAME VARCHAR(100))",
                    "CREATE TABLE SYSDBA." + signal + " (ID VARCHAR(64) PRIMARY KEY, TYPE VARCHAR(64), DATA VARCHAR(2000))");
            awaitStandbyReplay(environment);
            final var original = configuration(environment, retained).with("snapshot.mode", "no_data")
                    .with("signal.data.collection", "SYSDBA." + signal)
                    .with("table.include.list", "SYSDBA\\." + retained + ",SYSDBA\\." + signal).build();
            final var expanded = original.edit()
                    .with("table.include.list", "SYSDBA\\." + retained + ",SYSDBA\\." + signal + ",SYSDBA\\." + added).build();
            final java.util.concurrent.Callable<SourceRecord> nextBusiness = () -> {
                final long deadline = System.nanoTime() + Duration.ofSeconds(90).toNanos();
                while (System.nanoTime() < deadline) {
                    assertThat(failure.get()).isNull();
                    final var record = consumedLines.poll(1, TimeUnit.SECONDS);
                    if (record != null && record.value() instanceof Struct value
                            && (record.topic().equals("dameng.SYSDBA." + retained) || record.topic().equals("dameng.SYSDBA." + added))) {
                        assertThat(value.getString("op")).isEqualTo("c");
                        return record;
                    }
                }
                throw new AssertionError("Missing standby capture-scope event");
            };
            try {
                startEngine(original);
                waitForStreamingRunning("dameng", "dameng");
                execute(environment, "INSERT INTO SYSDBA." + retained + " VALUES(1,'started')");
                final var started = nextBusiness.call();
                assertThat(started.topic()).isEqualTo("dameng.SYSDBA." + retained);
                assertThat(afterId(started)).isEqualTo(1);
                awaitDurableOffset(original, started);
                stopConnector();
                consumedLines.clear();

                // B's old redo uses NAME. Its new standby baseline uses LABEL,
                // while A must still replay changes preceding that baseline.
                execute(environment, "INSERT INTO SYSDBA." + added + " VALUES(1,'before boundary')",
                        "ALTER TABLE SYSDBA." + added + " RENAME COLUMN NAME TO LABEL",
                        "INSERT INTO SYSDBA." + retained + " VALUES(2,'retained backlog')");
                awaitStandbyReplay(environment);
                startEngine(expanded);
                waitForStreamingRunning("dameng", "dameng");
                final var backlog = nextBusiness.call();
                assertThat(backlog.topic()).isEqualTo("dameng.SYSDBA." + retained);
                assertThat(afterId(backlog)).isEqualTo(2);
                execute(environment, "INSERT INTO SYSDBA." + added + " VALUES(2,'after boundary')");
                final var activated = nextBusiness.call();
                assertThat(activated.topic()).isEqualTo("dameng.SYSDBA." + added);
                assertThat(afterId(activated)).isEqualTo(2);
                final var after = ((Struct) activated.value()).getStruct("after");
                assertThat(after.getString("LABEL")).isEqualTo("after boundary");
                assertThat(after.schema().field("NAME")).isNull();
                awaitDurableOffset(expanded, activated);
                stopConnector();
                consumedLines.clear();

                // Restarting an accepted scope must reuse B's original boundary.
                execute(environment, "INSERT INTO SYSDBA." + added + " VALUES(3,'restart backlog')",
                        "INSERT INTO SYSDBA." + retained + " VALUES(3,'restart backlog')");
                awaitStandbyReplay(environment);
                startEngine(expanded);
                waitForStreamingRunning("dameng", "dameng");
                final var restartedAdded = nextBusiness.call();
                assertThat(restartedAdded.topic()).isEqualTo("dameng.SYSDBA." + added);
                assertThat(afterId(restartedAdded)).isEqualTo(3);
                assertThat(((Struct) restartedAdded.value()).getStruct("after").getString("LABEL")).isEqualTo("restart backlog");
                final var restartedRetained = nextBusiness.call();
                assertThat(restartedRetained.topic()).isEqualTo("dameng.SYSDBA." + retained);
                assertThat(afterId(restartedRetained)).isEqualTo(3);
                awaitDurableOffset(expanded, restartedRetained);
                stopConnector();
                consumedLines.clear();

                // Removing B must preserve A even when both changes belong to
                // the same committed transaction waiting in the standby redo.
                try (var writer = environment.primaryConnection(); var statement = writer.createStatement()) {
                    writer.setAutoCommit(false);
                    statement.execute("INSERT INTO SYSDBA." + added + " VALUES(4,'removed backlog')");
                    statement.execute("INSERT INTO SYSDBA." + retained + " VALUES(4,'retained backlog')");
                    writer.commit();
                    awaitStandbyReplay(environment);
                    startEngine(original);
                    waitForStreamingRunning("dameng", "dameng");
                    final var shrunkBacklog = nextBusiness.call();
                    assertThat(shrunkBacklog.topic()).isEqualTo("dameng.SYSDBA." + retained);
                    assertThat(afterId(shrunkBacklog)).isEqualTo(4);
                    statement.execute("INSERT INTO SYSDBA." + added + " VALUES(5,'removed live')");
                    statement.execute("INSERT INTO SYSDBA." + retained + " VALUES(5,'retained live')");
                    writer.commit();
                    final var shrunkLive = nextBusiness.call();
                    assertThat(shrunkLive.topic()).isEqualTo("dameng.SYSDBA." + retained);
                    assertThat(afterId(shrunkLive)).isEqualTo(5);
                    awaitDurableOffset(original, shrunkLive);
                }
                final long deadline = System.nanoTime() + Duration.ofSeconds(1).toNanos();
                while (System.nanoTime() < deadline) {
                    assertThat(failure.get()).isNull();
                    final var record = consumedLines.poll(100, TimeUnit.MILLISECONDS);
                    if (record != null && record.value() instanceof Struct) {
                        assertThat(record.topic()).isNotIn("dameng.SYSDBA." + retained, "dameng.SYSDBA." + added);
                    }
                }
            }
            finally {
                stopConnector();
            }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "insert_insert", "insert_delete" })
    void shouldSnapshotAndStreamOnStandbyWithPrimarySignals(String strategy) throws Exception {
        try (var environment = new DamengPrimaryStandbyEnvironment()) {
            environment.start();
            final String table = "DBZ_ST_" + UUID.randomUUID().toString().replace("-", "").substring(0, 10).toUpperCase();
            final String signal = table + "_SIGNAL";
            execute(environment, "CREATE TABLE SYSDBA." + table + " (ID INT PRIMARY KEY, LABEL VARCHAR(100))",
                    "INSERT INTO SYSDBA." + table + " VALUES(1,'initial')", "INSERT INTO SYSDBA." + table + " VALUES(2,'initial')");
            final var config = configuration(environment, table).with("snapshot.mode", "initial")
                    .with("signal.data.collection", "SYSDBA." + signal)
                    .with("incremental.snapshot.chunk.size", 1).with("incremental.snapshot.watermarking.strategy", strategy).build();
            try {
                startEngine(config);
                final var ids = new HashSet<Integer>();
                for (int i = 0; i < 2; i++) {
                    final var row = nextData(table);
                    final var value = (Struct) row.value();
                    assertThat(value.getString("op")).isEqualTo("r");
                    ids.add(value.getStruct("after").getInt32("ID"));
                }
                assertThat(ids).containsExactlyInAnyOrder(1, 2);
                execute(environment, "UPDATE SYSDBA." + table + " SET LABEL='updated' WHERE ID=1",
                        "DELETE FROM SYSDBA." + table + " WHERE ID=2", "INSERT INTO SYSDBA." + table + " VALUES(3,'inserted')");
                assertThat(((Struct) nextData(table).value()).getString("op")).isEqualTo("u");
                assertThat(((Struct) nextData(table).value()).getString("op")).isEqualTo("d");
                assertThat(((Struct) nextData(table).value()).getString("op")).isEqualTo("c");
                final var mbeans = ManagementFactory.getPlatformMBeanServer();
                final var metrics = new ObjectName("debezium.dameng:type=connector-metrics,context=streaming,server=dameng");
                assertThat((Long) mbeans.getAttribute(metrics, "StandbyReplayableLsn")).isPositive();
                assertThat((Long) mbeans.getAttribute(metrics, "StandbyReplayedLsn")).isPositive();
                assertThat((Long) mbeans.getAttribute(metrics, "StandbyMiningUpperLsn")).isPositive();
                execute(environment, "INSERT INTO SYSDBA." + signal + " VALUES('backfill','execute-snapshot','{\"data-collections\":[\"SYSDBA."
                        + table + "\"],\"type\":\"incremental\"}')");
                ids.clear();
                long lastSnapshotCommitLsn = 0;
                for (int i = 0; i < 2; i++) {
                    final var record = nextData(table);
                    final var value = (Struct) record.value();
                    lastSnapshotCommitLsn = ((Number) record.sourceOffset().get("commit_lsn")).longValue();
                    assertThat(value.getString("op")).isEqualTo("r");
                    assertThat(value.getStruct("source").getString("snapshot")).isEqualTo("incremental");
                    ids.add(value.getStruct("after").getInt32("ID"));
                }
                assertThat(ids).containsExactlyInAnyOrder(1, 3);
                // Reading the last row does not imply the final empty chunk and its
                // watermark have completed. Observe the persisted snapshot context.
                final long snapshotCommitLsn = lastSnapshotCommitLsn;
                Awaitility.await().atMost(Duration.ofSeconds(45)).ignoreExceptions().untilAsserted(() -> {
                    final var offset = readLastCommittedOffset(config, new DamengPartition("dameng").getSourcePartition());
                    assertThat(offset).isNotNull();
                    assertThat(((Number) offset.get("commit_lsn")).longValue()).isGreaterThanOrEqualTo(snapshotCommitLsn);
                    assertThat(offset).doesNotContainKey("incremental_snapshot_collections");
                });
                try (var primary = environment.primaryConnection();
                        var statement = primary.createStatement();
                        var rows = statement.executeQuery("SELECT COUNT(*) FROM SYSDBA." + signal + " WHERE TYPE LIKE 'snapshot-window-%'")) {
                    assertThat(rows.next()).isTrue();
                    if (strategy.equals("insert_insert")) {
                        assertThat(rows.getInt(1)).isGreaterThanOrEqualTo(4);
                    }
                    else {
                        assertThat(rows.getInt(1)).isZero();
                    }
                }
                // Driver routing must never redirect a capture connection to the primary.
                try (var reader = new DamengConnection(new DamengConnectorConfig(config).getJdbcConfig())) {
                    assertThat(reader.isStandbyMining()).isTrue();
                    reader.validateServerRole();
                    reader.query("SELECT MODE$ FROM V$INSTANCE", rows -> {
                        assertThat(rows.next()).isTrue();
                        assertThat(rows.getString(1)).isEqualTo("STANDBY");
                    });
                }
                stopConnector();
                consumedLines.clear();
                startEngine(config);
                execute(environment, "INSERT INTO SYSDBA." + table + " VALUES(4,'restart')");
                final var restarted = nextData(table);
                assertThat(afterId(restarted)).isEqualTo(4);
                awaitDurableOffset(config, restarted);
                environment.switchover();
                execute(environment, "INSERT INTO SYSDBA." + table + " VALUES(5,'switchover')");
                final var switched = nextData(table);
                assertThat(afterId(switched)).isEqualTo(5);
                awaitDurableOffset(config, switched);
                environment.restartStandby();
                execute(environment, "INSERT INTO SYSDBA." + table + " VALUES(6,'standby restart')");
                assertThat(afterId(nextData(table))).isEqualTo(6);
                assertThat(failure.get()).isNull();
            }
            finally {
                stopConnector();
            }
        }
    }

    private Configuration.Builder configuration(DamengPrimaryStandbyEnvironment environment, String table) {
        return Configuration.create().with("topic.prefix", "dameng").with("database.failover.hosts", environment.endpoints())
                .with("database.standby.hosts", environment.endpoints())
                .with("database.user", DamengPrimaryStandbyEnvironment.USER).with("database.password", DamengPrimaryStandbyEnvironment.PASSWORD)
                .with("table.include.list", "SYSDBA\\." + table).with("schema.history.internal", FileSchemaHistory.class.getName())
                .with("schema.history.internal.file.filename", directory.resolve("history.dat").toString())
                .with("poll.interval.ms", 100).with("offset.flush.interval.ms", 100).with("heartbeat.interval.ms", 100)
                .with("retriable.restart.connector.wait.ms", 200).with("database.query.timeout.ms", 10_000)
                .with("include.schema.changes", false).with("tombstones.on.delete", false);
    }

    private void startEngine(Configuration config) {
        start(DamengConnector.class, config, (success, message, error) -> {
            if (!success) {
                failure.set(error == null ? new IllegalStateException(message) : error);
            }
        }, null);
    }

    private void awaitDurableOffset(Configuration config, SourceRecord record) {
        Awaitility.await().atMost(Duration.ofSeconds(15)).ignoreExceptions().untilAsserted(() -> {
            final var offset = readLastCommittedOffset(config, new DamengPartition("dameng").getSourcePartition());
            assertThat(offset).isNotNull();
            assertThat(((Number) offset.get("commit_lsn")).longValue()).isGreaterThanOrEqualTo(((Number) record.sourceOffset().get("commit_lsn")).longValue());
        });
    }

    private SourceRecord nextData(String table) throws Exception {
        final long deadline = System.nanoTime() + Duration.ofSeconds(90).toNanos();
        while (System.nanoTime() < deadline) {
            assertThat(failure.get()).isNull();
            final var record = consumedLines.poll(1, TimeUnit.SECONDS);
            if (record != null && record.topic().equals("dameng.SYSDBA." + table) && record.value() instanceof Struct) {
                return record;
            }
        }
        throw new AssertionError("Missing standby event for " + table);
    }

    private static void execute(DamengPrimaryStandbyEnvironment environment, String... statements) throws Exception {
        try (var primary = environment.primaryConnection(); var statement = primary.createStatement()) {
            for (String sql : statements) {
                statement.execute(sql);
            }
        }
    }

    private static void awaitStandbyReplay(DamengPrimaryStandbyEnvironment environment) throws Exception {
        final long boundary;
        try (var primary = environment.primaryConnection();
                var statement = primary.createStatement();
                var rows = statement.executeQuery("SELECT CUR_LSN FROM V$RLOG")) {
            assertThat(rows.next()).isTrue();
            boundary = rows.getLong(1);
        }
        Awaitility.await().atMost(Duration.ofSeconds(45)).ignoreExceptions().untilAsserted(() -> {
            try (var standby = DriverManager.getConnection(environment.standbyJdbcUrl(), DamengPrimaryStandbyEnvironment.USER,
                    DamengPrimaryStandbyEnvironment.PASSWORD);
                    var statement = standby.createStatement();
                    var rows = statement.executeQuery("SELECT DSC_SEQNO, RPKG_LSN FROM V$RAPPLY_PARALLEL_INFO")) {
                assertThat(rows.next()).isTrue();
                assertThat(rows.getInt(1)).isZero();
                assertThat(rows.getLong(2)).isGreaterThanOrEqualTo(boundary);
                assertThat(rows.next()).isFalse();
            }
        });
    }

    @Test
    void shouldReadReplayProgress() throws Exception {
        try (var environment = new DamengPrimaryStandbyEnvironment()) {
            environment.start();
            try (var primary = environment.primaryConnection();
                    var standby = DriverManager.getConnection(environment.standbyJdbcUrl(), DamengPrimaryStandbyEnvironment.USER,
                            DamengPrimaryStandbyEnvironment.PASSWORD);
                    var writer = primary.createStatement();
                    var reader = standby.createStatement()) {
                writer.execute("CREATE TABLE SYSDBA.DBZ_STANDBY_PROGRESS (ID INT PRIMARY KEY)");
                writer.execute("INSERT INTO SYSDBA.DBZ_STANDBY_PROGRESS VALUES(1)");
                final long boundary;
                try (var rows = writer.executeQuery("SELECT CUR_LSN FROM V$RLOG")) {
                    assertThat(rows.next()).isTrue();
                    boundary = rows.getLong(1);
                }
                final var evidence = new StringBuilder("primary boundary=" + boundary + "\n");
                Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
                    try (var rows = reader.executeQuery("SELECT DSC_SEQNO, RPKG_LSN FROM V$RAPPLY_PARALLEL_INFO")) {
                        assertThat(rows.next()).isTrue();
                        assertThat(rows.getInt(1)).isZero();
                        assertThat(rows.getLong(2)).isGreaterThanOrEqualTo(boundary);
                        evidence.append("replayed=").append(rows.getLong(2)).append('\n');
                        assertThat(rows.next()).isFalse();
                    }
                });
                try (var rows = reader.executeQuery("SELECT COUNT(*) FROM SYSDBA.DBZ_STANDBY_PROGRESS")) {
                    assertThat(rows.next()).isTrue();
                    assertThat(rows.getLong(1)).isEqualTo(1);
                }
                java.nio.file.Files.writeString(Path.of("target/dameng-standby-progress.txt"), evidence);
            }
        }
    }
}
