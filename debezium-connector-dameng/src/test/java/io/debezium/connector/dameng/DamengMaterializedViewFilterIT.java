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

/** Broad schema capture must exclude materialized storage while preserving ordinary base-table changes. */
class DamengMaterializedViewFilterIT extends AbstractAsyncEngineConnectorTest {
    private static final String SCHEMA = "DBZ_MVF";
    private static final String COMPLETE_BASE = "DBZ_MVF_COMPLETE_BASE";
    private static final String FAST_BASE = "DBZ_MVF_FAST_BASE";
    private static final String COMPLETE = "DBZ_MVF_COMPLETE";
    private static final String FAST = "DBZ_MVF_FAST";
    private static final String MARKER = "DBZ_MVF_MARKER";
    @TempDir
    Path directory;
    private Connection writer;
    private Configuration config;
    private final AtomicReference<Throwable> failure = new AtomicReference<>();

    @BeforeAll
    static void startDatabase() throws Exception {
        assumeTrue(System.getProperty("dameng.hostname") == null, "Broad materialized-view capture requires a test-owned database");
        assumeTrue(DamengTestDatabase.requestedCompatibilityMode() == 0, "Materialized-view fixtures use COMPATIBLE_MODE=0");
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
            try {
                writer.rollback();
            }
            finally {
                writer.close();
            }
        }
    }

    @Test
    void excludesInternalSnapshotsAndRefreshRedoAcrossRebuildsAndRestart() throws Exception {
        writer = DriverManager.getConnection(DamengTestDatabase.jdbcUrl(), DamengTestDatabase.user(), DamengTestDatabase.password());
        writer.setAutoCommit(false);
        execute("CREATE SCHEMA " + SCHEMA);
        for (final String table : List.of(COMPLETE_BASE, FAST_BASE, MARKER)) {
            execute("CREATE TABLE " + qualified(table) + "(ID INT PRIMARY KEY,V VARCHAR(40))");
        }
        for (final String base : List.of(COMPLETE_BASE, FAST_BASE)) {
            execute("INSERT INTO " + qualified(base) + " VALUES(1,'one')");
            execute("INSERT INTO " + qualified(base) + " VALUES(2,'two')");
        }
        execute("INSERT INTO " + qualified(MARKER) + " VALUES(0,'snapshot')");
        writer.commit();
        createViews(false);
        assertView(COMPLETE, List.of("1:one", "2:two"));
        assertView(FAST, List.of("1:one", "2:two"));
        config = Configuration.create().with("topic.prefix", "dameng")
                .with("database.hosts", DamengTestDatabase.hosts())
                .with("database.user", DamengTestDatabase.user()).with("database.password", DamengTestDatabase.password())
                .with("database.dbname", System.getProperty("dameng.database", "DAMENG"))
                .with("schema.include.list", SCHEMA)
                .with("schema.history.internal", FileSchemaHistory.class.getName())
                .with("schema.history.internal.file.filename", directory.resolve("history.dat").toString())
                .with("snapshot.mode", "initial").with("poll.interval.ms", 100).with("offset.flush.interval.ms", 100)
                .with("include.schema.changes", false).with("tombstones.on.delete", false).build();
        // No table include/exclude pattern: MTAB$_ and MLOG$_ match the configured schema.
        startEngine();
        final List<String> snapshots = new ArrayList<>();
        for (int index = 0; index < 5; index++) {
            snapshots.add(signature(nextDataRecord()));
        }
        assertThat(snapshots).containsExactlyInAnyOrder(COMPLETE_BASE + ":r:1:one", COMPLETE_BASE + ":r:2:two",
                FAST_BASE + ":r:1:one", FAST_BASE + ":r:2:two", MARKER + ":r:0:snapshot");
        assertNoDataRecords();

        execute("INSERT INTO " + qualified(FAST_BASE) + " VALUES(3,'three')");
        execute("UPDATE " + qualified(FAST_BASE) + " SET V='updated' WHERE ID=1");
        execute("DELETE FROM " + qualified(FAST_BASE) + " WHERE ID=2");
        writer.commit();
        execute("UPDATE " + qualified(COMPLETE_BASE) + " SET V='complete' WHERE ID=1");
        writer.commit();
        execute("REFRESH MATERIALIZED VIEW " + qualified(FAST) + " FAST");
        execute("REFRESH MATERIALIZED VIEW " + qualified(COMPLETE) + " COMPLETE USING TRUNCATE");
        assertView(FAST, List.of("1:updated", "3:three"));
        assertView(COMPLETE, List.of("1:complete", "2:two"));
        marker(1);
        assertThat(throughMarker(1)).containsExactly(FAST_BASE + ":c:3:three", FAST_BASE + ":u:1:updated",
                FAST_BASE + ":d:2:two", COMPLETE_BASE + ":u:1:complete", MARKER + ":c:1:barrier");

        execute("ALTER MATERIALIZED VIEW " + qualified(COMPLETE) + " ENABLE QUERY REWRITE");
        execute("ALTER MATERIALIZED VIEW " + qualified(FAST) + " REFRESH FAST ON DEMAND");
        dropViews();
        createViews(true);
        execute("REFRESH MATERIALIZED VIEW " + qualified(FAST) + " FAST");
        execute("REFRESH MATERIALIZED VIEW " + qualified(COMPLETE) + " COMPLETE USING TRUNCATE");
        assertView(COMPLETE, List.of("1", "2"));
        assertView(FAST, List.of("1", "3"));
        marker(2);
        final var checkpoint = throughMarkerRecord(2);

        stopConnector();
        storeOffsets(config, Map.of(checkpoint.sourcePartition(), checkpoint.sourceOffset()));
        consumedLines.clear();
        // Old and replacement MTAB objects both disappear before their redo is replayed.
        dropViews();
        createViews(false);
        execute("UPDATE " + qualified(FAST_BASE) + " SET V='offline' WHERE ID=1");
        writer.commit();
        execute("REFRESH MATERIALIZED VIEW " + qualified(FAST) + " FAST");
        assertView(FAST, List.of("1:offline", "3:three"));
        dropViews();
        execute("INSERT INTO " + qualified(COMPLETE_BASE) + " VALUES(3,'after-drop')");
        writer.commit();
        marker(3);
        startEngine();
        assertThat(throughMarker(3)).containsExactly(FAST_BASE + ":u:1:offline", COMPLETE_BASE + ":c:3:after-drop", MARKER + ":c:3:barrier");
        assertNoDataRecords();
    }

    private void createViews(boolean replacement) throws SQLException {
        execute("CREATE MATERIALIZED VIEW " + qualified(COMPLETE) + " REFRESH COMPLETE ON DEMAND AS SELECT "
                + (replacement ? "ID" : "ID,V") + " FROM " + qualified(COMPLETE_BASE));
        execute("CREATE MATERIALIZED VIEW LOG ON " + qualified(FAST_BASE) + " WITH ROWID(V) PURGE IMMEDIATE SYNCHRONOUS");
        execute("CREATE MATERIALIZED VIEW " + qualified(FAST) + " REFRESH FAST ON DEMAND WITH ROWID AS SELECT "
                + (replacement ? "ID" : "ID,V") + ",ROWID AS X FROM " + qualified(FAST_BASE));
    }

    private void dropViews() throws SQLException {
        execute("DROP MATERIALIZED VIEW " + qualified(FAST));
        execute("DROP MATERIALIZED VIEW " + qualified(COMPLETE));
        execute("DROP MATERIALIZED VIEW LOG ON " + qualified(FAST_BASE));
    }

    private void marker(int id) throws SQLException {
        execute("INSERT INTO " + qualified(MARKER) + " VALUES(" + id + ",'barrier')");
        writer.commit();
    }

    private List<String> throughMarker(int id) throws Exception {
        final List<String> changes = new ArrayList<>();
        SourceRecord record;
        do {
            record = nextDataRecord();
            changes.add(signature(record));
        } while (!isMarker(record, id));
        return changes;
    }

    private SourceRecord throughMarkerRecord(int id) throws Exception {
        final var record = nextDataRecord();
        assertThat(isMarker(record, id)).as("DDL and refreshes emit no data events before the marker").isTrue();
        return record;
    }

    private static boolean isMarker(SourceRecord record, int id) {
        return record.topic().equals(topic(MARKER)) && ((Struct) record.value()).getStruct("after").getInt32("ID") == id;
    }

    private void startEngine() throws Exception {
        failure.set(null);
        start(DamengConnector.class, config, (success, message, error) -> {
            if (!success) {
                failure.set(error == null ? new IllegalStateException(message) : error);
            }
        }, null);
        Awaitility.await("materialized-view filter connector starts streaming").atMost(Duration.ofSeconds(60))
                .pollInterval(Duration.ofMillis(100)).until(() -> {
                    if (failure.get() != null) {
                        throw new AssertionError("Connector failed during startup", failure.get());
                    }
                    return isStreamingRunning("dameng", "dameng");
                });
    }

    private SourceRecord nextDataRecord() throws Exception {
        final long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
        while (System.nanoTime() < deadline) {
            assertThat(failure.get()).as("connector remains healthy").isNull();
            if (waitForAvailableRecords(100, TimeUnit.MILLISECONDS)) {
                final var record = consumeRecord();
                if (isDataRecord(record)) {
                    assertThat(record.topic()).doesNotContain("MTAB$_", "MLOG$_");
                    assertThat(record.topic()).isIn(topic(COMPLETE_BASE), topic(FAST_BASE), topic(MARKER));
                    return record;
                }
            }
        }
        throw new AssertionError("No base-table event within 60 seconds; connector failure=" + failure.get());
    }

    private void assertNoDataRecords() throws Exception {
        final long deadline = System.nanoTime() + Duration.ofSeconds(1).toNanos();
        while (System.nanoTime() < deadline) {
            assertThat(failure.get()).isNull();
            if (waitForAvailableRecords(100, TimeUnit.MILLISECONDS)) {
                assertThat(isDataRecord(consumeRecord())).as("no additional data events from any topic").isFalse();
            }
        }
    }

    private static boolean isDataRecord(SourceRecord record) {
        return record != null && record.value() instanceof Struct value && value.schema().field("op") != null;
    }

    private static String signature(SourceRecord record) {
        final var envelope = (Struct) record.value();
        final var row = envelope.getStruct(envelope.getString("op").equals("d") ? "before" : "after");
        return record.topic().substring(record.topic().lastIndexOf('.') + 1) + ':' + envelope.getString("op") + ':' + row.getInt32("ID") + ':' + row.getString("V");
    }

    private void assertView(String name, List<String> expected) throws SQLException {
        final List<String> actual = new ArrayList<>();
        try (var statement = writer.createStatement(); var rows = statement.executeQuery("SELECT * FROM " + qualified(name) + " ORDER BY ID")) {
            while (rows.next()) {
                final boolean hasValue = rows.getMetaData().getColumnCount() > (name.equals(FAST) ? 2 : 1);
                actual.add(rows.getString(1) + (hasValue ? ":" + rows.getString(2) : ""));
            }
        }
        assertThat(actual).as("actual materialized-view rows for %s", name).containsExactlyElementsOf(expected);
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

    private static String topic(String table) {
        return "dameng." + SCHEMA + '.' + table;
    }
}
