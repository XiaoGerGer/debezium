/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.debezium.config.Configuration;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;
import io.debezium.storage.file.history.FileSchemaHistory;

/** Captures partition roots, including relocation redo, without inventing deletes for dropped or truncated partitions. */
class DamengPartitionTableIT extends AbstractAsyncEngineConnectorTest {
    private static final String SCHEMA = "DBZ_PART";
    private static final List<String> TABLES = List.of("RANGE_T", "LIST_T", "HASH_T", "MULTI_T", "INTERVAL_T");
    private static final String RANGE = "PARTITION BY RANGE(K)(PARTITION P1 VALUES LESS THAN(10),"
            + "PARTITION P2 VALUES LESS THAN(20),PARTITION PM VALUES LESS THAN(MAXVALUE))";
    @TempDir
    Path directory;
    private Connection writer;
    private Configuration config;
    private final AtomicReference<Throwable> failure = new AtomicReference<>();

    @BeforeAll
    static void startDatabase() throws Exception {
        assumeTrue(System.getProperty("dameng.hostname") == null, "Partition tests require a disposable database");
        assumeTrue(DamengTestDatabase.requestedCompatibilityMode() == 0, "Partition fixtures use native mode");
        DamengTestDatabase.start();
    }

    @AfterAll
    static void stopDatabase() {
        DamengTestDatabase.stop();
    }

    @AfterEach
    void closeConnectorAndWriter() throws Exception {
        stopConnector();
        if (writer != null) {
            writer.close();
        }
    }

    @Test
    void shouldCapturePartitionSnapshotsReorganizationRowsAndResumePastOptedInPartitionDataRemoval() throws Exception {
        writer = DriverManager.getConnection(DamengTestDatabase.jdbcUrl(), DamengTestDatabase.user(), DamengTestDatabase.password());
        writer.setAutoCommit(true);
        execute("CREATE SCHEMA " + SCHEMA);
        for (final var table : TABLES) {
            final var definition = switch (table) {
                case "LIST_T" -> "PARTITION BY LIST(K)(PARTITION P1 VALUES(1,2,3),PARTITION P2 VALUES(11,12),PARTITION PM VALUES(DEFAULT))";
                case "HASH_T" -> "PARTITION BY HASH(K) PARTITIONS 4";
                case "MULTI_T" -> "PARTITION BY RANGE(K) SUBPARTITION BY HASH(ID) SUBPARTITIONS 2"
                        + "(PARTITION P1 VALUES LESS THAN(10),PARTITION P2 VALUES LESS THAN(20),PARTITION PM VALUES LESS THAN(MAXVALUE))";
                case "INTERVAL_T" -> "PARTITION BY RANGE(K) INTERVAL(10)(PARTITION P1 VALUES LESS THAN(10))";
                default -> RANGE;
            };
            execute("CREATE TABLE " + qualified(table) + "(ID INT PRIMARY KEY,K INT,V VARCHAR(30)) " + definition);
            execute("ALTER TABLE " + qualified(table) + " ENABLE ROW MOVEMENT");
            execute("INSERT INTO " + qualified(table) + " VALUES(1,1,'one')");
            execute("INSERT INTO " + qualified(table) + " VALUES(2,2,'two')");
            execute("INSERT INTO " + qualified(table) + " VALUES(3,21,'three')");
            execute("INSERT INTO " + qualified(table) + " VALUES(4,11,'four')");
        }
        execute("CREATE TABLE " + qualified("MARKER") + "(ID INT PRIMARY KEY,K INT,V VARCHAR(30))");
        config = Configuration.create().with("topic.prefix", "dameng")
                .with("database.hostname", DamengTestDatabase.hostname()).with("database.port", DamengTestDatabase.port())
                .with("database.user", DamengTestDatabase.user()).with("database.password", DamengTestDatabase.password())
                .with("database.dbname", System.getProperty("dameng.database", "DAMENG"))
                .with("schema.include.list", SCHEMA).with("snapshot.mode", "initial")
                .with("schema.history.internal", FileSchemaHistory.class.getName())
                .with("schema.history.internal.file.filename", directory.resolve("history.dat").toString())
                .with("poll.interval.ms", 100).with("offset.flush.interval.ms", 100)
                .with("include.schema.changes", false).with("tombstones.on.delete", false)
                .with(DamengConnectorConfig.PARTITION_DROP_IGNORE, true).build();
        startEngine();
        final List<String> snapshots = new ArrayList<>();
        for (int index = 0; index < 20; index++) {
            snapshots.add(signature(nextDataRecord()));
        }
        final List<String> expectedSnapshots = new ArrayList<>();
        TABLES.forEach(table -> expectedSnapshots.addAll(List.of(table + ":r:1:one", table + ":r:2:two",
                table + ":r:3:three", table + ":r:4:four")));
        assertThat(snapshots).containsExactlyInAnyOrderElementsOf(expectedSnapshots);

        final List<String> expectedChanges = new ArrayList<>();
        for (final var table : TABLES) {
            execute("INSERT INTO " + qualified(table) + " VALUES(5,3,'five')");
            execute("UPDATE " + qualified(table) + " SET V='changed' WHERE ID=1");
            execute("UPDATE " + qualified(table) + " SET K=1 WHERE ID=3");
            execute("DELETE FROM " + qualified(table) + " WHERE ID=2");
            expectedChanges.addAll(List.of(table + ":c:5:five", table + ":u:1:changed", table + ":u:3:three", table + ":d:2:two"));
        }
        // Automatic interval partition creation must continue to identify the root table.
        execute("INSERT INTO " + qualified("INTERVAL_T") + " VALUES(6,35,'interval')");
        expectedChanges.add("INTERVAL_T:c:6:interval");
        marker(1);
        expectedChanges.add("MARKER:c:1:marker");
        assertThat(throughMarker(1).stream().map(DamengPartitionTableIT::signature)).containsExactlyElementsOf(expectedChanges);

        for (final var table : List.of("RANGE_T", "LIST_T", "MULTI_T")) {
            final var before = data(table);
            execute("ALTER TABLE " + qualified(table) + " MERGE PARTITIONS P1,P2 INTO PARTITION P12");
            assertThat(data(table)).containsExactlyElementsOf(before);
        }
        marker(2);
        assertRelocationInserts(throughMarker(2));
        for (final var table : List.of("RANGE_T", "LIST_T", "MULTI_T")) {
            final var before = data(table);
            execute("ALTER TABLE " + qualified(table) + " SPLIT PARTITION P12 "
                    + (table.equals("LIST_T") ? "VALUES(1,2,3)" : "AT(10)") + " INTO(PARTITION PA,PARTITION PB)");
            assertThat(data(table)).containsExactlyElementsOf(before);
        }
        marker(3);
        final var reorganized = throughMarker(3);
        assertRelocationInserts(reorganized);
        final var checkpoint = reorganized.get(reorganized.size() - 1);
        stopConnector();
        storeOffsets(config, Map.of(checkpoint.sourcePartition(), checkpoint.sourceOffset()));
        consumedLines.clear();

        // Replay maintenance from archived redo after the old partition objects have disappeared.
        for (final var table : List.of("RANGE_T", "LIST_T", "MULTI_T")) {
            execute("ALTER TABLE " + qualified(table) + " DROP PARTITION PA");
            assertThat(data(table)).containsExactly("4:11:four");
            execute("UPDATE " + qualified(table) + " SET V='restart' WHERE ID=4");
            execute((table.equals("LIST_T") ? "TRUNCATE TABLE " + qualified(table) + " PARTITION PB"
                    : "ALTER TABLE " + qualified(table) + " TRUNCATE PARTITION PB"));
            assertThat(data(table)).isEmpty();
            execute("INSERT INTO " + qualified(table) + " VALUES(4,11,'refilled')");
        }
        execute("CREATE TABLE " + qualified("NEW_T") + "(ID INT PRIMARY KEY,K INT,V VARCHAR(30)) PARTITION BY HASH(K) PARTITIONS 2");
        execute("INSERT INTO " + qualified("NEW_T") + " VALUES(1,1,'new')");
        execute("DROP TABLE " + qualified("HASH_T"));
        execute("CREATE TABLE " + qualified("SUB_T") + "(ID INT PRIMARY KEY,K INT,V VARCHAR(30))"
                + " PARTITION BY RANGE(K) SUBPARTITION BY RANGE(ID)"
                + "(PARTITION P1 VALUES LESS THAN(MAXVALUE)(SUBPARTITION S1 VALUES LESS THAN(2),SUBPARTITION S2 VALUES LESS THAN(MAXVALUE)))");
        execute("INSERT INTO " + qualified("SUB_T") + " VALUES(1,1,'sub1')");
        execute("INSERT INTO " + qualified("SUB_T") + " VALUES(2,2,'sub2')");
        execute("ALTER TABLE " + qualified("SUB_T") + " TRUNCATE SUBPARTITION(S1) REUSE STORAGE");
        assertThat(data("SUB_T")).containsExactly("2:2:sub2");
        execute("INSERT INTO " + qualified("SUB_T") + " VALUES(1,1,'refilled')");
        marker(4);
        startEngine();
        assertThat(throughMarker(4).stream().map(DamengPartitionTableIT::signature)).containsExactly(
                "RANGE_T:u:4:restart", "RANGE_T:c:4:refilled", "LIST_T:u:4:restart", "LIST_T:c:4:refilled",
                "MULTI_T:u:4:restart", "MULTI_T:c:4:refilled", "NEW_T:c:1:new",
                "SUB_T:c:1:sub1", "SUB_T:c:2:sub2", "SUB_T:c:1:refilled", "MARKER:c:4:marker");
    }

    private static void assertRelocationInserts(List<SourceRecord> records) {
        final var changes = records.subList(0, records.size() - 1);
        assertThat(changes).isNotEmpty().allSatisfy(record -> {
            final var value = (Struct) record.value();
            assertThat(value.getString("op")).isEqualTo("c");
            assertThat(value.getStruct("after").getInt32("ID")).isIn(1, 3, 4, 5);
        });
        assertThat(changes.stream().map(record -> record.topic())).contains(
                "dameng." + qualified("RANGE_T"), "dameng." + qualified("LIST_T"), "dameng." + qualified("MULTI_T"));
    }

    private void startEngine() throws Exception {
        failure.set(null);
        start(DamengConnector.class, config, (success, message, error) -> {
            if (!success) {
                failure.set(error == null ? new IllegalStateException(message) : error);
            }
        }, null);
        Awaitility.await("partition connector streams").atMost(Duration.ofSeconds(60)).pollInterval(Duration.ofMillis(100))
                .until(() -> {
                    assertThat(failure.get()).isNull();
                    return isStreamingRunning("dameng", "dameng");
                });
    }

    private SourceRecord nextDataRecord() throws Exception {
        final long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
        while (System.nanoTime() < deadline) {
            assertThat(failure.get()).isNull();
            if (waitForAvailableRecords(100, TimeUnit.MILLISECONDS)) {
                final var record = consumeRecord();
                if (record != null && record.value() instanceof Struct value && value.schema().field("op") != null) {
                    return record;
                }
            }
        }
        throw new AssertionError("No partition-table data event within 60 seconds");
    }

    private List<SourceRecord> throughMarker(int id) throws Exception {
        final List<SourceRecord> records = new ArrayList<>();
        while (true) {
            final var record = nextDataRecord();
            records.add(record);
            if (signature(record).equals("MARKER:c:" + id + ":marker")) {
                return records;
            }
        }
    }

    private static String signature(SourceRecord record) {
        final var value = (Struct) record.value();
        final var op = value.getString("op");
        final var row = value.getStruct(op.equals("d") ? "before" : "after");
        return record.topic().substring(record.topic().lastIndexOf('.') + 1) + ':' + op + ':' + row.getInt32("ID") + ':' + row.getString("V");
    }

    private List<String> data(String table) throws SQLException {
        final List<String> result = new ArrayList<>();
        try (var statement = writer.createStatement(); var rows = statement.executeQuery("SELECT ID,K,V FROM " + qualified(table) + " ORDER BY ID")) {
            while (rows.next()) {
                result.add(rows.getInt(1) + ":" + rows.getInt(2) + ":" + rows.getString(3));
            }
        }
        return result;
    }

    private void marker(int id) throws SQLException {
        execute("INSERT INTO " + qualified("MARKER") + " VALUES(" + id + ",0,'marker')");
    }

    private void execute(String sql) throws SQLException {
        try (var statement = writer.createStatement()) {
            statement.setQueryTimeout(15);
            statement.execute(sql);
        }
    }

    private static String qualified(String table) {
        return SCHEMA + '.' + table;
    }
}
