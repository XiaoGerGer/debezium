/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.EOFException;
import java.io.StreamCorruptedException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.debezium.config.Configuration;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;
import io.debezium.storage.file.history.FileSchemaHistory;

/** Real physical replication and manual switchover without a VIP or modified connector endpoints. */
class DamengPrimaryStandbyIT extends AbstractAsyncEngineConnectorTest {
    @TempDir
    Path directory;
    private final AtomicReference<Throwable> failure = new AtomicReference<>();
    private final String table = "DBZ_HA_" + UUID.randomUUID().toString().replace("-", "").substring(0, 12).toUpperCase();
    private Configuration config;

    @Override
    protected int getMaximumEnqueuedRecordCount() {
        return 64;
    }

    @Test
    void shouldRestartAnIncompleteSnapshotAfterManualSwitchover() throws Exception {
        try (var environment = new DamengPrimaryStandbyEnvironment()) {
            environment.start();
            execute(environment, "CREATE TABLE SYSDBA." + table + " (ID INT PRIMARY KEY, LABEL VARCHAR(100))");
            try (var writer = environment.primaryConnection();
                    var statement = writer.prepareStatement("INSERT INTO SYSDBA." + table + " VALUES(?,?)")) {
                writer.setAutoCommit(false);
                for (int id = 1; id <= 500; id++) {
                    statement.setInt(1, id);
                    statement.setString(2, "snapshot-" + id);
                    statement.addBatch();
                }
                assertThat(statement.executeBatch()).hasSize(500);
                writer.commit();
            }
            config = Configuration.create().with("topic.prefix", "dameng")
                    .with("database.failover.hosts", environment.endpoints())
                    .with("database.user", DamengPrimaryStandbyEnvironment.USER).with("database.password", DamengPrimaryStandbyEnvironment.PASSWORD)
                    .with("database.dbname", "DAMENG").with("table.include.list", "SYSDBA\\." + table)
                    .with("database.socket.timeout.ms", 15_000).with("database.query.timeout.ms", 10_000)
                    .with("schema.history.internal", FileSchemaHistory.class.getName())
                    .with("schema.history.internal.file.filename", directory.resolve("snapshot-history.dat").toString())
                    .with("snapshot.mode", "initial").with("snapshot.max.duration.ms", 120_000)
                    .with("max.queue.size", 64).with("max.batch.size", 32)
                    .with("poll.interval.ms", 100).with("offset.flush.interval.ms", 100)
                    .with("heartbeat.interval.ms", 100).with("retriable.restart.connector.wait.ms", 200)
                    .with("include.schema.changes", false).with("tombstones.on.delete", false).build();
            try {
                startEngine();
                Awaitility.await().atMost(Duration.ofSeconds(30)).until(() -> consumedLines.size() == 64);
                final var first = consumedLines.peek();
                assertThat(first).isNotNull();
                assertThat(first.sourceOffset().get("snapshot_completed")).isEqualTo(false);
                final var oldMagic = first.sourceOffset().get(DamengOffsetContext.DATABASE_MAGIC_KEY);
                // Keep the consumer blocked so no last snapshot row can be
                // published before the database changes its primary role.
                environment.switchover();
                final Set<Integer> rowsOnNewPrimary = new HashSet<>();
                boolean completedOnNewPrimary = false;
                final long deadline = System.nanoTime() + Duration.ofSeconds(120).toNanos();
                while (System.nanoTime() < deadline && (rowsOnNewPrimary.size() < 500 || !completedOnNewPrimary)) {
                    assertThat(failure.get()).isNull();
                    // nextRecord() uses Awaitility and costs at least 100 ms per
                    // call. Poll directly for this 500-row assertion.
                    final var record = consumedLines.poll(1, TimeUnit.SECONDS);
                    if (record == null || !record.topic().equals("dameng.SYSDBA." + table)
                            || !(record.value() instanceof Struct envelope) || envelope.schema().field("op") == null) {
                        continue;
                    }
                    envelope.validate();
                    assertThat(envelope.getString("op")).isEqualTo("r");
                    final int id = envelope.getStruct("after").getInt32("ID");
                    assertThat(envelope.getStruct("after").getString("LABEL")).isEqualTo("snapshot-" + id);
                    if (!record.sourceOffset().get(DamengOffsetContext.DATABASE_MAGIC_KEY).equals(oldMagic)) {
                        rowsOnNewPrimary.add(id);
                    }
                    if (Boolean.TRUE.equals(record.sourceOffset().get("snapshot_completed"))) {
                        assertThat(record.sourceOffset().get(DamengOffsetContext.DATABASE_MAGIC_KEY)).isNotEqualTo(oldMagic);
                        completedOnNewPrimary = true;
                    }
                }
                assertThat(rowsOnNewPrimary).hasSize(500);
                assertThat(completedOnNewPrimary).isTrue();
                execute(environment, "INSERT INTO SYSDBA." + table + " VALUES(501,'after snapshot switchover')");
                final var live = awaitInsert(501, "after snapshot switchover");
                assertThat(live.sourceOffset().get(DamengOffsetContext.DATABASE_MAGIC_KEY)).isNotEqualTo(oldMagic);
                assertThat(failure.get()).isNull();
            }
            finally {
                stopConnector();
            }
        }
    }

    @Test
    void shouldRecoverHistoricalDdlAcrossManualSwitchoverSwitchbackAndPrimaryFailure() throws Exception {
        try (var environment = new DamengPrimaryStandbyEnvironment()) {
            environment.start();
            execute(environment, "CREATE TABLE SYSDBA." + table + " (ID INT PRIMARY KEY, LABEL VARCHAR(100))",
                    "INSERT INTO SYSDBA." + table + " VALUES(1,'snapshot')");
            config = Configuration.create().with("topic.prefix", "dameng")
                    .with("database.failover.hosts", environment.endpoints())
                    .with("database.user", DamengPrimaryStandbyEnvironment.USER).with("database.password", DamengPrimaryStandbyEnvironment.PASSWORD)
                    .with("database.dbname", "DAMENG").with("table.include.list", "SYSDBA\\." + table)
                    .with("database.socket.timeout.ms", 15_000).with("database.query.timeout.ms", 10_000)
                    .with("schema.history.internal", FileSchemaHistory.class.getName())
                    .with("schema.history.internal.file.filename", directory.resolve("history.dat").toString())
                    .with("snapshot.mode", "initial").with("poll.interval.ms", 100).with("offset.flush.interval.ms", 100)
                    .with("heartbeat.interval.ms", 100).with("retriable.restart.connector.wait.ms", 200)
                    .with("include.schema.changes", false).with("tombstones.on.delete", false).build();
            try {
                startEngine();
                final var snapshot = nextRecord();
                assertThat(value(snapshot).getString("op")).isEqualTo("r");
                execute(environment, "INSERT INTO SYSDBA." + table + " VALUES(2,'before switchover')");
                final var checkpoint = nextRecord();
                assertThat(value(checkpoint).getStruct("after").getInt32("ID")).isEqualTo(2);
                awaitDurableOffset(checkpoint);
                final var oldMagic = checkpoint.sourceOffset().get(DamengOffsetContext.DATABASE_MAGIC_KEY);
                final var family = checkpoint.sourceOffset().get(DamengOffsetContext.PERMANENT_MAGIC_KEY);
                assertThat(family).isNotNull();

                stopConnector();
                execute(environment, "ALTER TABLE SYSDBA." + table + " ADD EXTRA VARCHAR(40)",
                        "INSERT INTO SYSDBA." + table + " VALUES(3,'old primary backlog','historical DDL')",
                        "UPDATE SYSDBA." + table + " SET LABEL='updated before switch',EXTRA='historical DDL' WHERE ID=2");
                environment.switchover();
                startEngine();
                final Set<Integer> replayed = new HashSet<>();
                SourceRecord resumed = null;
                while (replayed.size() < 2) {
                    resumed = nextRecord();
                    final var envelope = value(resumed);
                    assertThat(envelope.getString("op")).isNotEqualTo("r");
                    final var after = envelope.getStruct("after");
                    final int id = after.getInt32("ID");
                    if (id == 3 || (id == 2 && envelope.getString("op").equals("u"))) {
                        assertThat(after.getString("EXTRA")).isEqualTo("historical DDL");
                        replayed.add(id);
                    }
                    assertThat(resumed.sourceOffset().get(DamengOffsetContext.PERMANENT_MAGIC_KEY)).isEqualTo(family);
                    assertThat(resumed.sourceOffset().get(DamengOffsetContext.DATABASE_MAGIC_KEY)).isNotEqualTo(oldMagic);
                }
                awaitDurableOffset(resumed);
                execute(environment, "INSERT INTO SYSDBA." + table + " VALUES(4,'on new primary','live')");
                awaitInsert(4, "on new primary");
                // The same running engine must follow a second manual role change.
                environment.switchover();
                execute(environment, "INSERT INTO SYSDBA." + table + " VALUES(5,'after switchback','live')");
                final var switchedBack = awaitInsert(5, "after switchback");
                assertThat(switchedBack.sourceOffset().get(DamengOffsetContext.DATABASE_MAGIC_KEY)).isEqualTo(oldMagic);
                assertThat(switchedBack.sourceOffset().get(DamengOffsetContext.PERMANENT_MAGIC_KEY)).isEqualTo(family);
                // Fail only this fixture's current primary. The database's
                // MANUAL watcher requires an explicit, non-forced takeover.
                environment.killPrimaryAndTakeover();
                execute(environment, "INSERT INTO SYSDBA." + table + " VALUES(6,'after primary failure','live')");
                final var takenOver = awaitInsert(6, "after primary failure");
                assertThat(takenOver.sourceOffset().get(DamengOffsetContext.DATABASE_MAGIC_KEY)).isNotEqualTo(oldMagic);
                assertThat(takenOver.sourceOffset().get(DamengOffsetContext.PERMANENT_MAGIC_KEY)).isEqualTo(family);
                assertThat(failure.get()).isNull();
            }
            finally {
                stopConnector();
            }
        }
    }

    private SourceRecord awaitInsert(int id, String label) throws Exception {
        final long deadline = System.nanoTime() + Duration.ofSeconds(90).toNanos();
        while (System.nanoTime() < deadline) {
            final var record = nextRecord();
            final var envelope = value(record);
            assertThat(envelope.getString("op")).isNotEqualTo("r");
            if (envelope.getStruct("after").getInt32("ID") == id) {
                assertThat(envelope.getString("op")).isEqualTo("c");
                assertThat(envelope.getStruct("after").getString("LABEL")).isEqualTo(label);
                awaitDurableOffset(record);
                return record;
            }
        }
        throw new AssertionError("Missing primary/standby event for ID " + id);
    }

    private void startEngine() {
        start(DamengConnector.class, config, (success, message, error) -> {
            if (!success) {
                failure.set(error == null ? new IllegalStateException(message) : error);
            }
        }, null);
    }

    private SourceRecord nextRecord() throws Exception {
        final long deadline = System.nanoTime() + Duration.ofSeconds(90).toNanos();
        while (System.nanoTime() < deadline) {
            assertThat(failure.get()).isNull();
            if (waitForAvailableRecords(1, TimeUnit.SECONDS)) {
                final var record = consumeRecord();
                if (record.topic().equals("dameng.SYSDBA." + table) && record.value() instanceof Struct envelope && envelope.schema().field("op") != null) {
                    envelope.validate();
                    return record;
                }
            }
        }
        throw new AssertionError("No primary/standby data event within 90 seconds");
    }

    private void awaitDurableOffset(SourceRecord record) {
        // FileOffsetBackingStore rewrites its file while the engine is running.
        // Retry an incomplete serialized read; a permanently unreadable file
        // still fails the bounded wait and the subsequent engine restart.
        Awaitility.await().atMost(Duration.ofSeconds(30))
                .ignoreExceptionsMatching(error -> error instanceof ConnectException
                        && (error.getCause() instanceof EOFException || error.getCause() instanceof StreamCorruptedException))
                .untilAsserted(() -> {
                    final var stored = readLastCommittedOffset(config, record.sourcePartition());
                    assertThat(stored).isNotNull();
                    assertThat(((Number) stored.get("commit_lsn")).longValue())
                            .isGreaterThanOrEqualTo(((Number) record.sourceOffset().get("commit_lsn")).longValue());
                    assertThat(stored.get(DamengOffsetContext.PRIMARY_OPEN_GUID_KEY)).isEqualTo(record.sourceOffset().get(DamengOffsetContext.PRIMARY_OPEN_GUID_KEY));
                });
    }

    private static Struct value(SourceRecord record) {
        return (Struct) record.value();
    }

    private static void execute(DamengPrimaryStandbyEnvironment environment, String... commands) throws Exception {
        try (var connection = environment.primaryConnection(); var statement = connection.createStatement()) {
            statement.setQueryTimeout(15);
            for (String sql : commands) {
                statement.execute(sql);
            }
        }
    }
}
