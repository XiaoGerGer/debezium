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
import java.sql.SQLException;
import java.time.Duration;
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

import io.debezium.config.Configuration;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;
import io.debezium.storage.file.history.FileSchemaHistory;

class DamengSnapshotFilterIT extends AbstractAsyncEngineConnectorTest {
    @TempDir
    Path directory;

    private Connection connection;
    private String snapshotTable;
    private String streamingTable;
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
        connection = DriverManager.getConnection(DamengTestDatabase.jdbcUrl(), DamengTestDatabase.user(), DamengTestDatabase.password());
        connection.setAutoCommit(false);
        final String prefix = "DBZ_FILTER_" + UUID.randomUUID().toString().replace("-", "").substring(0, 12).toUpperCase();
        snapshotTable = prefix + "_A";
        streamingTable = prefix + "_B";
        execute("CREATE TABLE " + qualified(snapshotTable) + " (ID INT PRIMARY KEY, LABEL VARCHAR(100))");
        execute("CREATE TABLE " + qualified(streamingTable) + " (ID INT PRIMARY KEY, SECRET VARCHAR(100), SCORE INT, LABEL VARCHAR(100))");
        execute("INSERT INTO " + qualified(snapshotTable) + " VALUES(1,'snapshot A')");
        execute("INSERT INTO " + qualified(streamingTable) + " VALUES(1,'private initial value',10,'existing B')");
        connection.commit();
        config = Configuration.create()
                .with("topic.prefix", "dameng")
                .with("database.hostname", DamengTestDatabase.hostname())
                .with("database.port", DamengTestDatabase.port())
                .with("database.user", DamengTestDatabase.user())
                .with("database.password", DamengTestDatabase.password())
                .with("database.dbname", System.getProperty("dameng.database", "DAMENG"))
                .with("table.include.list", DamengTestDatabase.schema() + "\\.(" + snapshotTable + "|" + streamingTable + ")")
                .with("snapshot.include.collection.list", DamengTestDatabase.schema() + "\\." + snapshotTable)
                .with("column.exclude.list", DamengTestDatabase.schema() + "\\." + streamingTable + "\\.SECRET")
                .with("schema.history.internal", FileSchemaHistory.class.getName())
                .with("schema.history.internal.file.filename", directory.resolve("schema-history.dat").toString())
                .with("snapshot.mode", "initial")
                .with("poll.interval.ms", 100)
                .with("include.schema.changes", false)
                .with("tombstones.on.delete", false)
                .build();
    }

    @AfterEach
    void after() throws Exception {
        stopConnector();
        if (connection != null) {
            try {
                connection.rollback();
                execute("DROP TABLE IF EXISTS " + qualified(streamingTable));
                execute("DROP TABLE IF EXISTS " + qualified(snapshotTable));
                connection.commit();
            }
            finally {
                connection.close();
            }
        }
    }

    @Test
    void shouldStreamAndRecoverCapturedTablesExcludedFromTheRowSnapshot() throws Exception {
        startEngine();
        final var snapshot = nextDataRecord();
        assertThat(snapshot.topic()).isEqualTo(topic(snapshotTable));
        assertThat(envelope(snapshot).getString("op")).isEqualTo("r");
        assertThat(envelope(snapshot).getStruct("after").getString("LABEL")).isEqualTo("snapshot A");
        assertNoDataRecords();

        execute("INSERT INTO " + qualified(streamingTable) + " VALUES(2,'private inserted value',20,'inserted B')");
        connection.commit();
        final var inserted = nextDataRecord();
        assertThat(inserted.topic()).isEqualTo(topic(streamingTable));
        assertThat(envelope(inserted).getString("op")).isEqualTo("c");
        assertRow(envelope(inserted).getStruct("after"), 2, 20, "inserted B");

        stopConnector();
        // Restart from an acknowledged data event so B's definition and object id
        // must be recovered from the initial schema history, without a B row snapshot.
        storeOffsets(config, Map.of(inserted.sourcePartition(), inserted.sourceOffset()));
        consumedLines.clear();
        execute("UPDATE " + qualified(streamingTable) + " SET SECRET='changed secret',SCORE=21,LABEL='updated B' WHERE ID=2");
        execute("DELETE FROM " + qualified(streamingTable) + " WHERE ID=1");
        connection.commit();

        startEngine();
        final var updated = nextDataRecord();
        assertThat(updated.topic()).isEqualTo(topic(streamingTable));
        assertThat(envelope(updated).getString("op")).isEqualTo("u");
        assertRow(envelope(updated).getStruct("before"), 2, 20, "inserted B");
        assertRow(envelope(updated).getStruct("after"), 2, 21, "updated B");
        final var deleted = nextDataRecord();
        assertThat(deleted.topic()).isEqualTo(topic(streamingTable));
        assertThat(envelope(deleted).getString("op")).isEqualTo("d");
        assertRow(envelope(deleted).getStruct("before"), 1, 10, "existing B");
        assertThat(envelope(deleted).getStruct("after")).isNull();
        assertNoDataRecords();
    }

    @Test
    void shouldRejectExpandedCaptureEvenWhenThePreviouslyExcludedTableWasDropped() throws Exception {
        final Configuration expanded = config;
        config = config.edit().with("table.include.list", DamengTestDatabase.schema() + "\\." + snapshotTable).build();
        startEngine();
        nextDataRecord();
        execute("INSERT INTO " + qualified(snapshotTable) + " VALUES(2,'offset before expansion')");
        connection.commit();
        final var beforeStop = nextDataRecord();
        stopConnector();
        storeOffsets(config, Map.of(beforeStop.sourcePartition(), beforeStop.sourceOffset()));
        consumedLines.clear();

        execute("INSERT INTO " + qualified(streamingTable) + " VALUES(2,'unrecorded',20,'change before drop')");
        connection.commit();
        execute("DROP TABLE " + qualified(streamingTable));
        connection.commit();
        config = expanded;
        failure.set(null);
        start(DamengConnector.class, config, (success, message, error) -> {
            if (!success) {
                failure.set(error == null ? new IllegalStateException(message) : error);
            }
        }, null);

        Awaitility.await().atMost(Duration.ofSeconds(30)).until(() -> failure.get() != null);
        assertThat(failure.get()).hasStackTraceContaining("capture scope").hasStackTraceContaining("initial snapshot");
        assertThat(consumedLines).isEmpty();
    }

    @Test
    void shouldReplayNewAndRecreatedTablesDuringDowntimeWithoutChangingTheCaptureFilter() throws Exception {
        execute("DROP TABLE " + qualified(streamingTable));
        connection.commit();
        startEngine();
        nextDataRecord();
        execute("INSERT INTO " + qualified(snapshotTable) + " VALUES(2,'offset before create')");
        connection.commit();
        final var beforeStop = nextDataRecord();
        stopConnector();
        storeOffsets(config, Map.of(beforeStop.sourcePartition(), beforeStop.sourceOffset()));
        consumedLines.clear();

        execute("CREATE TABLE " + qualified(streamingTable) + " (ID INT PRIMARY KEY, ORIGINAL VARCHAR(50))");
        execute("INSERT INTO " + qualified(streamingTable) + " VALUES(3,'created during downtime')");
        connection.commit();
        startEngine();
        final var created = nextDataRecord();
        assertThat(created.topic()).isEqualTo(topic(streamingTable));
        assertThat(envelope(created).getString("op")).isEqualTo("c");
        assertThat(envelope(created).getStruct("after").getString("ORIGINAL")).isEqualTo("created during downtime");
        stopConnector();
        storeOffsets(config, Map.of(created.sourcePartition(), created.sourceOffset()));
        consumedLines.clear();

        execute("DROP TABLE " + qualified(streamingTable));
        execute("CREATE TABLE " + qualified(streamingTable) + " (ID INT PRIMARY KEY, REPLACEMENT INT)");
        execute("INSERT INTO " + qualified(streamingTable) + " VALUES(4,42)");
        connection.commit();
        startEngine();
        final var recreated = nextDataRecord();
        assertThat(recreated.topic()).isEqualTo(topic(streamingTable));
        assertThat(envelope(recreated).getString("op")).isEqualTo("c");
        assertThat(envelope(recreated).getStruct("after").schema().field("ORIGINAL")).isNull();
        assertThat(envelope(recreated).getStruct("after").getInt32("REPLACEMENT")).isEqualTo(42);
        assertNoDataRecords();
    }

    private void startEngine() throws Exception {
        failure.set(null);
        start(DamengConnector.class, config, (success, message, error) -> {
            if (!success) {
                failure.set(error == null ? new IllegalStateException(message) : error);
            }
        }, null);
        waitForStreamingRunning("dameng", "dameng");
    }

    private SourceRecord nextDataRecord() throws Exception {
        final long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
        while (System.nanoTime() < deadline) {
            assertConnectorHealthy();
            if (waitForAvailableRecords(1, TimeUnit.SECONDS)) {
                final var record = consumeRecord();
                if (isDataRecord(record)) {
                    envelope(record).validate();
                    ((Struct) record.key()).validate();
                    return record;
                }
            }
        }
        throw new AssertionError("No Dameng snapshot filter change event within 60 seconds");
    }

    private void assertNoDataRecords() throws Exception {
        final long deadline = System.nanoTime() + Duration.ofSeconds(1).toNanos();
        while (System.nanoTime() < deadline) {
            assertConnectorHealthy();
            if (waitForAvailableRecords(100, TimeUnit.MILLISECONDS)) {
                assertThat(isDataRecord(consumeRecord())).as("No additional snapshot or replayed data event").isFalse();
            }
        }
    }

    private void assertConnectorHealthy() {
        if (failure.get() != null) {
            throw new AssertionError("Dameng snapshot filter connector failed", failure.get());
        }
    }

    private boolean isDataRecord(SourceRecord record) {
        return record != null && (record.topic().equals(topic(snapshotTable)) || record.topic().equals(topic(streamingTable)))
                && record.value() instanceof Struct value && value.schema().field("op") != null;
    }

    private static void assertRow(Struct row, int id, int score, String label) {
        assertThat(row.schema().field("SECRET")).isNull();
        assertThat(row.getInt32("ID")).isEqualTo(id);
        assertThat(row.getInt32("SCORE")).isEqualTo(score);
        assertThat(row.getString("LABEL")).isEqualTo(label);
    }

    private static Struct envelope(SourceRecord record) {
        return (Struct) record.value();
    }

    private void execute(String sql) throws SQLException {
        try (var statement = connection.createStatement()) {
            statement.execute(sql);
        }
    }

    private static String qualified(String table) {
        return '"' + DamengTestDatabase.schema() + "\".\"" + table + '"';
    }

    private static String topic(String table) {
        return "dameng." + DamengTestDatabase.schema() + "." + table;
    }
}
