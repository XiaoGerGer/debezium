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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
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

class DamengIncrementalSnapshotIT extends AbstractAsyncEngineConnectorTest {
    @TempDir
    Path directory;

    private Connection connection;
    private String table;
    private String signalTable;
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
        table = "DBZ_INC_" + UUID.randomUUID().toString().replace("-", "").substring(0, 12).toUpperCase();
        signalTable = table + "_SIGNAL";
        execute("CREATE TABLE " + qualified(table) + " (ID INT PRIMARY KEY, NAME VARCHAR(100))");
        execute("CREATE TABLE " + qualified(signalTable) + " (ID VARCHAR(64) PRIMARY KEY, TYPE VARCHAR(64), DATA VARCHAR(2000))");
        for (int id = 1; id <= 6; id++) {
            execute("INSERT INTO " + qualified(table) + " VALUES (" + id + ",'initial')");
        }
        connection.commit();
        config = Configuration.create()
                .with("topic.prefix", "dameng")
                .with("database.hostname", DamengTestDatabase.hostname())
                .with("database.port", DamengTestDatabase.port())
                .with("database.user", DamengTestDatabase.user())
                .with("database.password", DamengTestDatabase.password())
                .with("database.dbname", System.getProperty("dameng.database", "DAMENG"))
                .with("table.include.list", DamengTestDatabase.schema() + "\\." + table)
                .with("signal.data.collection", DamengTestDatabase.schema() + "." + signalTable)
                .with("incremental.snapshot.chunk.size", 2)
                .with("schema.history.internal", FileSchemaHistory.class.getName())
                .with("schema.history.internal.file.filename", directory.resolve("schema-history.dat").toString())
                .with("snapshot.mode", "no_data")
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
                execute("DROP TABLE " + qualified(signalTable));
                execute("DROP TABLE " + qualified(table));
                connection.commit();
            }
            finally {
                connection.close();
            }
        }
    }

    @Test
    void shouldSnapshotRequestedTableInMultipleChunks() throws Exception {
        startEngine();
        requestSnapshot();
        final Set<Integer> ids = new HashSet<>();
        for (int i = 0; i < 6; i++) {
            final var record = nextDataRecord();
            final var envelope = (Struct) record.value();
            assertThat(envelope.getString("op")).isEqualTo("r");
            assertThat(envelope.getStruct("source").getString("snapshot")).isEqualTo("incremental");
            ids.add(envelope.getStruct("after").getInt32("ID"));
        }
        assertThat(ids).containsExactlyInAnyOrder(1, 2, 3, 4, 5, 6);
        try (var statement = connection.createStatement();
                var rows = statement.executeQuery("SELECT COUNT(*) FROM " + qualified(signalTable) + " WHERE TYPE='snapshot-window-open'")) {
            assertThat(rows.next()).isTrue();
            assertThat(rows.getInt(1)).isGreaterThanOrEqualTo(3);
        }
    }

    @Test
    void shouldDeduplicateRowsChangedBetweenWatermarks() throws Exception {
        // The trigger executes in the watermark transaction, placing this update
        // after the open marker and before the chunk's close marker in redo order.
        execute("CREATE TRIGGER " + qualified(table + "_TRG") + " AFTER INSERT ON " + qualified(signalTable)
                + " FOR EACH ROW BEGIN IF :NEW.TYPE = 'snapshot-window-open' THEN UPDATE " + qualified(table)
                + " SET NAME='updated' WHERE ID=1 AND NAME='initial'; END IF; END;");
        connection.commit();
        startEngine();
        requestSnapshot();
        final List<Integer> snapshotIds = new ArrayList<>();
        final List<Integer> updatedIds = new ArrayList<>();
        for (int i = 0; i < 6; i++) {
            final var envelope = (Struct) nextDataRecord().value();
            final int id = envelope.getStruct("after").getInt32("ID");
            if (envelope.getString("op").equals("u")) {
                assertThat(envelope.getStruct("after").getString("NAME")).isEqualTo("updated");
                updatedIds.add(id);
            }
            else {
                assertThat(envelope.getString("op")).isEqualTo("r");
                snapshotIds.add(id);
            }
        }
        assertThat(updatedIds).containsExactly(1);
        assertThat(snapshotIds).containsExactlyInAnyOrder(2, 3, 4, 5, 6);
    }

    @Test
    void shouldResumeFromAnAcknowledgedIncrementalSnapshotRow() throws Exception {
        startEngine();
        requestSnapshot();
        final SourceRecord first = nextDataRecord();
        assertThat(((Struct) first.value()).getString("op")).isEqualTo("r");
        assertThat(first.sourceOffset()).containsKey("incremental_snapshot_collections");
        stopConnector();
        // Model a crash after only this row was acknowledged, even if the live
        // engine had already queued a later chunk before it was stopped.
        storeOffsets(config, Map.of(first.sourcePartition(), first.sourceOffset()));
        consumedLines.clear();
        startEngine();
        final Set<Integer> ids = new HashSet<>();
        ids.add(((Struct) first.value()).getStruct("after").getInt32("ID"));
        while (ids.size() < 6) {
            final var envelope = (Struct) nextDataRecord().value();
            assertThat(envelope.getString("op")).isEqualTo("r");
            ids.add(envelope.getStruct("after").getInt32("ID"));
        }
        assertThat(ids).containsExactlyInAnyOrder(1, 2, 3, 4, 5, 6);
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void shouldSuppressStaleSnapshotRowsAfterDeleteOrKeyMovementInsideTheWatermarkWindow(boolean moveKey) throws Exception {
        final var mutation = moveKey ? "UPDATE " + qualified(table) + " SET ID=9,NAME='moved' WHERE ID=1"
                : "DELETE FROM " + qualified(table) + " WHERE ID=1";
        execute("CREATE TRIGGER " + qualified(table + "_TRG") + " AFTER INSERT ON " + qualified(signalTable)
                + " FOR EACH ROW BEGIN IF :NEW.TYPE = 'snapshot-window-open' THEN " + mutation + "; END IF; END;");
        connection.commit();
        startEngine();
        requestSnapshot();
        final List<Integer> snapshotIds = new ArrayList<>();
        final List<String> changes = new ArrayList<>();
        final Map<Integer, String> materialized = new java.util.HashMap<>();
        final List<SourceRecord> records = new ArrayList<>();
        for (int index = 0; index < (moveKey ? 7 : 6); index++) {
            records.add(nextDataRecord());
        }
        final long deadline = System.nanoTime() + Duration.ofSeconds(2).toNanos();
        while (System.nanoTime() < deadline) {
            assertThat(failure.get()).isNull();
            if (waitForAvailableRecords(100, TimeUnit.MILLISECONDS)) {
                final var record = consumeRecord();
                if (record.topic().equals("dameng." + DamengTestDatabase.schema() + "." + table)) {
                    records.add(record);
                }
            }
        }
        for (var record : records) {
            final var envelope = (Struct) record.value();
            final var operation = envelope.getString("op");
            final int id = ((Struct) record.key()).getInt32("ID");
            if (operation.equals("d")) {
                assertThat(id).isEqualTo(1);
                assertThat(envelope.getStruct("before").getString("NAME")).isEqualTo("initial");
                assertThat(envelope.get("after")).isNull();
                materialized.remove(id);
                changes.add("d:" + id);
            }
            else {
                final var row = envelope.getStruct("after");
                assertThat(row.getInt32("ID")).isEqualTo(id);
                materialized.put(id, row.getString("NAME"));
                if (operation.equals("r")) {
                    snapshotIds.add(id);
                }
                else {
                    assertThat(operation).isEqualTo("c");
                    assertThat(moveKey).isTrue();
                    assertThat(row.getString("NAME")).isEqualTo("moved");
                    changes.add("c:" + id);
                }
            }
        }
        assertThat(snapshotIds).contains(2, 3, 4, 5, 6).doesNotHaveDuplicates();
        // A moved key can enter a later chunk. That current-value snapshot is
        // legitimate; the deleted old key must never be resurrected.
        assertThat(snapshotIds).allSatisfy(id -> assertThat(id >= 2 && id <= 6 || moveKey && id == 9).isTrue());
        assertThat(changes).containsExactlyElementsOf(moveKey ? List.of("d:1", "c:9") : List.of("d:1"));
        final Map<Integer, String> expected = new java.util.HashMap<>();
        try (var statement = connection.createStatement(); var result = statement.executeQuery("SELECT ID,NAME FROM " + qualified(table))) {
            while (result.next()) {
                expected.put(result.getInt(1), result.getString(2));
            }
        }
        assertThat(materialized).containsExactlyInAnyOrderEntriesOf(expected);
        assertNoDataRecords();
    }

    @Test
    void shouldResumeCompositeKeyChunksWithoutLosingRowsWithTheSameTrailingKey() throws Exception {
        execute("DROP TABLE " + qualified(table));
        execute("CREATE TABLE " + qualified(table) + " (TENANT INT, ID VARCHAR(20), NAME VARCHAR(100), PRIMARY KEY(TENANT,ID))");
        final Set<String> expected = Set.of("-1:a", "-1:b", "0:a", "0:b", "7:a", "7:b", "7:c");
        for (var key : expected) {
            final var parts = key.split(":");
            execute("INSERT INTO " + qualified(table) + " VALUES(" + parts[0] + ",'" + parts[1] + "','" + key + "')");
        }
        connection.commit();
        startEngine();
        requestSnapshot();
        final Set<String> seen = new HashSet<>();
        final var checkpoint = nextDataRecord();
        seen.add(assertCompositeSnapshotRow(checkpoint));
        stopConnector();
        storeOffsets(config, Map.of(checkpoint.sourcePartition(), checkpoint.sourceOffset()));
        consumedLines.clear();
        startEngine();
        final long deadline = System.nanoTime() + Duration.ofSeconds(90).toNanos();
        while (seen.size() < expected.size() && System.nanoTime() < deadline) {
            seen.add(assertCompositeSnapshotRow(nextDataRecord()));
        }
        assertThat(seen).containsExactlyInAnyOrderElementsOf(expected);
        assertNoDataRecords();
    }

    private static String assertCompositeSnapshotRow(SourceRecord record) {
        final var envelope = (Struct) record.value();
        assertThat(envelope.getString("op")).isEqualTo("r");
        final var row = envelope.getStruct("after");
        final var key = (Struct) record.key();
        assertThat(key.getInt32("TENANT")).isEqualTo(row.getInt32("TENANT"));
        assertThat(key.getString("ID")).isEqualTo(row.getString("ID"));
        final var signature = key.getInt32("TENANT") + ":" + key.getString("ID");
        assertThat(row.getString("NAME")).isEqualTo(signature);
        return signature;
    }

    private void assertNoDataRecords() throws Exception {
        final long deadline = System.nanoTime() + Duration.ofSeconds(2).toNanos();
        while (System.nanoTime() < deadline) {
            assertThat(failure.get()).isNull();
            if (waitForAvailableRecords(100, TimeUnit.MILLISECONDS)) {
                final var record = consumeRecord();
                assertThat(record.topic()).isNotEqualTo("dameng." + DamengTestDatabase.schema() + "." + table);
            }
        }
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

    private void requestSnapshot() throws SQLException {
        try (var statement = connection.prepareStatement("INSERT INTO " + qualified(signalTable) + " VALUES(?,?,?)")) {
            statement.setString(1, UUID.randomUUID().toString());
            statement.setString(2, "execute-snapshot");
            statement.setString(3, "{\"type\":\"incremental\",\"data-collections\":[\"" + DamengTestDatabase.schema() + "." + table + "\"]}");
            statement.executeUpdate();
        }
        connection.commit();
    }

    private SourceRecord nextDataRecord() throws Exception {
        final long deadline = System.nanoTime() + Duration.ofSeconds(90).toNanos();
        while (System.nanoTime() < deadline) {
            if (failure.get() != null) {
                throw new AssertionError("Dameng incremental snapshot failed", failure.get());
            }
            if (waitForAvailableRecords(1, TimeUnit.SECONDS)) {
                final SourceRecord record = consumeRecord();
                if (record != null && record.topic().equals("dameng." + DamengTestDatabase.schema() + "." + table)
                        && record.value() instanceof Struct value && value.schema().field("op") != null) {
                    value.validate();
                    return record;
                }
            }
        }
        throw new AssertionError("No Dameng incremental snapshot record within 90 seconds");
    }

    private void execute(String sql) throws SQLException {
        try (var statement = connection.createStatement()) {
            statement.execute(sql);
        }
    }

    private static String qualified(String name) {
        return '"' + DamengTestDatabase.schema() + "\".\"" + name + '"';
    }
}
