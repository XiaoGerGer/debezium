/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import io.debezium.config.Configuration;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;
import io.debezium.storage.file.history.FileSchemaHistory;

/** Scope changes keep the existing source identity and independently activate newly included tables. */
class DamengCaptureScopeIT extends AbstractAsyncEngineConnectorTest {
    @TempDir
    Path directory;

    private Connection writer;
    private String retained;
    private String added;
    private String signal;
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
        final var suffix = UUID.randomUUID().toString().replace("-", "").substring(0, 12).toUpperCase();
        retained = "DBZ_SCOPE_A_" + suffix;
        added = "DBZ_SCOPE_B_" + suffix;
        signal = "DBZ_SCOPE_S_" + suffix;
        execute("CREATE TABLE " + qualified(retained) + " (ID INT PRIMARY KEY, NAME VARCHAR(100))");
        execute("CREATE TABLE " + qualified(added) + " (ID INT PRIMARY KEY, NAME VARCHAR(100))");
        execute("CREATE TABLE " + qualified(signal) + " (ID VARCHAR(64) PRIMARY KEY, TYPE VARCHAR(64), DATA VARCHAR(2000))");
        insert(retained, 1);
        insert(added, 1);
        writer.commit();
        config = Configuration.create().with("topic.prefix", "dameng")
                .with("database.hostname", DamengTestDatabase.hostname()).with("database.port", DamengTestDatabase.port())
                .with("database.user", DamengTestDatabase.user()).with("database.password", DamengTestDatabase.password())
                .with("database.dbname", System.getProperty("dameng.database", "DAMENG"))
                .with("table.include.list", included(false))
                .with("signal.data.collection", qualified(signal)).with("incremental.snapshot.chunk.size", 1)
                .with("schema.history.internal", FileSchemaHistory.class.getName())
                .with("schema.history.internal.file.filename", directory.resolve("history.dat").toString())
                .with("snapshot.mode", "no_data").with("poll.interval.ms", 100).with("heartbeat.interval.ms", 100)
                .with("offset.flush.interval.ms", 100).with("include.schema.changes", false).with("tombstones.on.delete", false).build();
    }

    @AfterEach
    void after() throws Exception {
        stopConnector();
        if (writer != null) {
            try {
                writer.rollback();
                for (var name : List.of(signal, added, retained)) {
                    execute("DROP TABLE " + qualified(name));
                }
                writer.commit();
            }
            finally {
                writer.close();
            }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "initial", "no_data" })
    void shouldExpandShrinkAndReAddWithoutMovingTheRetainedTableOffsetOrBackfillingRows(String snapshotMode) throws Exception {
        config = config.edit().with("snapshot.mode", snapshotMode).build();
        startAndWait();
        if (snapshotMode.equals("initial")) {
            assertRow(nextData(), retained, "r", 1, "NAME");
        }
        insert(retained, 2);
        writer.commit();
        stopAt(assertRow(nextData(), retained, "c", 2, "NAME"));

        // B's current schema differs from its old redo. Its new baseline must
        // not be used to interpret or publish changes preceding activation.
        insert(added, 2);
        writer.commit();
        execute("ALTER TABLE " + qualified(added) + " RENAME COLUMN NAME TO LABEL");
        insert(retained, 3);
        writer.commit();
        config = config.edit().with("table.include.list", included(true)).build();
        startAndWait();
        assertRow(nextData(), retained, "c", 3, "NAME");
        insert(added, 3);
        writer.commit();
        final var activated = assertRow(nextData(), added, "c", 3, "LABEL");
        assertThat(envelope(activated).getStruct("after").schema().field("NAME")).isNull();
        assertNoData();
        stopAt(activated);

        // A completed expansion must resume its original activation boundary,
        // rather than choose a newer one and lose B's changes during downtime.
        insert(added, 4);
        insert(retained, 4);
        writer.commit();
        startAndWait();
        assertRow(nextData(), added, "c", 4, "LABEL");
        final var restarted = assertRow(nextData(), retained, "c", 4, "NAME");
        assertNoData();
        stopAt(restarted);

        // Removal abandons B's unsent backlog. A still resumes its own backlog.
        insert(added, 5);
        insert(retained, 5);
        writer.commit();
        config = config.edit().with("table.include.list", included(false)).build();
        startAndWait();
        assertRow(nextData(), retained, "c", 5, "NAME");
        // Filtering B must not discard a transaction that also modifies A.
        insert(added, 6);
        insert(retained, 6);
        writer.commit();
        final var shrunk = assertRow(nextData(), retained, "c", 6, "NAME");
        assertNoData();
        stopAt(shrunk);

        // Re-adding B creates a fresh cut. Excluded-period changes and existing
        // rows stay excluded; a manual incremental snapshot can backfill them.
        insert(added, 7);
        insert(retained, 7);
        writer.commit();
        config = config.edit().with("table.include.list", included(true)).build();
        startAndWait();
        assertRow(nextData(), retained, "c", 7, "NAME");
        insert(added, 8);
        writer.commit();
        assertRow(nextData(), added, "c", 8, "LABEL");
        assertNoData();

        // A manually requested snapshot must include rows from every earlier
        // capture epoch, including B's rows written before NAME became LABEL.
        requestSnapshot(added);
        final var firstSnapshot = assertRow(nextData(), added, "r", 1, "LABEL");
        assertThat(envelope(firstSnapshot).getStruct("after").schema().field("NAME")).isNull();
        final List<Integer> snapshotted = new ArrayList<>(List.of(1));
        insert(retained, 9);
        writer.commit();
        boolean retainedCaptured = false;
        while (snapshotted.size() < 8 || !retainedCaptured) {
            final var record = nextData();
            if (record.topic().equals("dameng." + qualified(retained))) {
                assertThat(retainedCaptured).as("A's concurrent change is emitted once").isFalse();
                assertRow(record, retained, "c", 9, "NAME");
                retainedCaptured = true;
            }
            else {
                final int id = envelope(record).getStruct("after").getInt32("ID");
                assertRow(record, added, "r", id, "LABEL");
                assertThat(envelope(record).getStruct("after").schema().field("NAME")).isNull();
                assertThat(snapshotted).as("each existing B row is snapshotted once").doesNotContain(id);
                snapshotted.add(id);
            }
        }
        assertThat(snapshotted).containsExactlyInAnyOrder(1, 2, 3, 4, 5, 6, 7, 8);
        assertNoData();
    }

    @Test
    void shouldActivateTheAddedTableWhileAnOldTableTransactionPinsTheReplayOffset() throws Exception {
        startAndWait();
        insert(retained, 2);
        writer.commit();
        stopAt(assertRow(nextData(), retained, "c", 2, "NAME"));

        try (var transaction = DriverManager.getConnection(DamengTestDatabase.jdbcUrl(), DamengTestDatabase.user(), DamengTestDatabase.password())) {
            transaction.setAutoCommit(false);
            try {
                try (var statement = transaction.createStatement()) {
                    statement.execute("INSERT INTO " + qualified(retained) + " VALUES(10,'row-10')");
                }
                // An existing table's writer must not block B's schema cut. Its
                // transaction keeps the replay position before B's activation.
                config = config.edit().with("table.include.list", included(true)).build();
                startAndWait();
                insert(added, 3);
                writer.commit();
                final var activated = assertRow(nextData(), added, "c", 3, "NAME");
                assertThat(((Number) activated.sourceOffset().get("lsn")).longValue())
                        .isLessThan(((Number) activated.sourceOffset().get("change_lsn")).longValue());
                stopAt(activated);
                transaction.commit();
                startAndWait();
                assertRow(nextData(), retained, "c", 10, "NAME");
                assertNoData();
            }
            finally {
                transaction.rollback();
            }
        }
    }

    @Test
    void shouldReuseAPersistedExpansionBoundaryWhenTheOffsetCheckpointStillHasTheOldScope() throws Exception {
        startAndWait();
        insert(retained, 2);
        writer.commit();
        final var checkpoint = assertRow(nextData(), retained, "c", 2, "NAME");
        stopAt(checkpoint);

        config = config.edit().with("table.include.list", included(true)).build();
        startAndWait();
        assertNoData();
        stopConnector();
        consumedLines.clear();
        // Simulate a crash after history durably accepted the expansion but
        // before the worker acknowledged an offset containing the new scope.
        storeOffsets(config, Map.of(checkpoint.sourcePartition(), checkpoint.sourceOffset()));
        insert(added, 2);
        writer.commit();
        startAndWait();
        assertRow(nextData(), added, "c", 2, "NAME");
        assertNoData();
    }

    @Test
    void shouldRejectScopeChangesWhileAnInitialSnapshotIsIncomplete() throws Exception {
        insert(retained, 2);
        writer.commit();
        config = config.edit().with("snapshot.mode", "initial").build();
        startAndWait();
        final var checkpoint = assertRow(nextData(), retained, "r", 1, "NAME");
        assertThat(envelope(checkpoint).getStruct("source").getString("snapshot")).isIn("first", "true");
        stopAt(checkpoint);

        config = config.edit().with("table.include.list", included(true)).build();
        startEngine();
        Awaitility.await().atMost(Duration.ofSeconds(30)).until(() -> failure.get() != null);
        assertThat(failure.get()).hasStackTraceContaining("snapshot");
    }

    @Test
    void shouldDiscardRemovedTablesFromAnAcknowledgedIncrementalSnapshotAndContinueRemainingTables() throws Exception {
        insert(added, 2);
        insert(added, 3);
        writer.commit();
        config = config.edit().with("table.include.list", included(true)).build();
        startAndWait();
        insert(retained, 2);
        writer.commit();
        assertRow(nextData(), retained, "c", 2, "NAME");
        requestSnapshot(added, retained);
        final var checkpoint = assertRow(nextData(), added, "r", 1, "NAME");
        assertThat(checkpoint.sourceOffset()).containsKey("incremental_snapshot_collections");
        assertThat(checkpoint.sourceOffset().get("incremental_snapshot_collections").toString())
                .contains(qualified(added), qualified(retained));
        stopAt(checkpoint);

        config = config.edit().with("table.include.list", included(false)).build();
        startAndWait();
        assertRow(nextData(), retained, "r", 1, "NAME");
        assertRow(nextData(), retained, "r", 2, "NAME");
        insert(retained, 9);
        writer.commit();
        final var resumed = assertRow(nextData(), retained, "c", 9, "NAME");
        assertThat(String.valueOf(resumed.sourceOffset().get("incremental_snapshot_collections"))).doesNotContain(qualified(added));
        assertNoData();
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

    private void stopAt(SourceRecord checkpoint) throws Exception {
        stopConnector();
        // Use an explicitly acknowledged checkpoint even when later heartbeats
        // or snapshot chunks reached the engine before the stop completed.
        storeOffsets(config, Map.of(checkpoint.sourcePartition(), checkpoint.sourceOffset()));
        consumedLines.clear();
    }

    private SourceRecord nextData() throws Exception {
        final long deadline = System.nanoTime() + Duration.ofSeconds(90).toNanos();
        while (System.nanoTime() < deadline) {
            healthy();
            if (waitForAvailableRecords(100, TimeUnit.MILLISECONDS)) {
                final var record = consumeRecord();
                if (isBusinessRecord(record)) {
                    return record;
                }
            }
        }
        throw new AssertionError("No capture-scope change received");
    }

    private void assertNoData() throws Exception {
        final long deadline = System.nanoTime() + Duration.ofSeconds(1).toNanos();
        final List<SourceRecord> unexpected = new ArrayList<>();
        while (System.nanoTime() < deadline) {
            healthy();
            if (waitForAvailableRecords(100, TimeUnit.MILLISECONDS)) {
                final var record = consumeRecord();
                if (isBusinessRecord(record)) {
                    unexpected.add(record);
                }
            }
        }
        assertThat(unexpected).isEmpty();
    }

    private boolean isBusinessRecord(SourceRecord record) {
        return (record.topic().equals("dameng." + qualified(retained)) || record.topic().equals("dameng." + qualified(added)))
                && record.value() instanceof Struct value && value.schema().field("op") != null;
    }

    private SourceRecord assertRow(SourceRecord record, String table, String operation, int id, String nameColumn) {
        assertThat(record.topic()).isEqualTo("dameng." + qualified(table));
        final var value = envelope(record);
        assertThat(value.getString("op")).isEqualTo(operation);
        assertThat(value.getStruct("after").getInt32("ID")).isEqualTo(id);
        assertThat(value.getStruct("after").getString(nameColumn)).isEqualTo("row-" + id);
        return record;
    }

    private void requestSnapshot(String... tables) throws Exception {
        final List<String> collections = new ArrayList<>();
        for (var table : tables) {
            collections.add("\"" + qualified(table) + "\"");
        }
        execute("INSERT INTO " + qualified(signal) + " VALUES('scope-snapshot','execute-snapshot','{\"type\":\"incremental\",\"data-collections\":["
                + String.join(",", collections) + "]}')");
        writer.commit();
    }

    private String included(boolean includeAdded) {
        return DamengTestDatabase.schema() + "\\." + retained + "," + DamengTestDatabase.schema() + "\\." + signal
                + (includeAdded ? "," + DamengTestDatabase.schema() + "\\." + added : "");
    }

    private void insert(String table, int id) throws Exception {
        execute("INSERT INTO " + qualified(table) + " VALUES(" + id + ",'row-" + id + "')");
    }

    private void execute(String sql) throws Exception {
        try (var statement = writer.createStatement()) {
            statement.execute(sql);
        }
    }

    private void healthy() {
        if (failure.get() != null) {
            throw new AssertionError("Capture-scope task failed", failure.get());
        }
    }

    private static Struct envelope(SourceRecord record) {
        return (Struct) record.value();
    }

    private static String qualified(String table) {
        return DamengTestDatabase.schema() + "." + table;
    }
}
