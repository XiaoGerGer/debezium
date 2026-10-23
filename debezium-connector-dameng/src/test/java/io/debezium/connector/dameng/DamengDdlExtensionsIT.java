/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
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
import io.debezium.relational.Tables;
import io.debezium.storage.file.history.FileSchemaHistory;

/** P1 acceptance on the original DM image, including offline DDL and durable recovery. */
class DamengDdlExtensionsIT extends AbstractAsyncEngineConnectorTest {
    @TempDir
    Path directory;

    private Connection jdbc;
    private Configuration config;
    private String prefix;
    private final List<String> created = new ArrayList<>();
    private final AtomicReference<Throwable> failure = new AtomicReference<>();

    @BeforeAll
    static void startDatabase() throws Exception {
        assumeTrue(System.getProperty("dameng.hostname") == null, "Parameter tests own their database");
        DamengTestDatabase.start();
    }

    @AfterAll
    static void stopDatabase() {
        DamengTestDatabase.stop();
    }

    @BeforeEach
    void prepare() throws Exception {
        jdbc = DriverManager.getConnection(DamengTestDatabase.jdbcUrl(), DamengTestDatabase.user(), DamengTestDatabase.password());
        jdbc.setAutoCommit(true);
        prefix = "DBZ_P1_" + UUID.randomUUID().toString().replace("-", "").substring(0, 10).toUpperCase();
        config = Configuration.create()
                .with("topic.prefix", "dameng")
                .with("database.hostname", DamengTestDatabase.hostname()).with("database.port", DamengTestDatabase.port())
                .with("database.user", DamengTestDatabase.user()).with("database.password", DamengTestDatabase.password())
                .with("database.dbname", "DAMENG").with("table.include.list", "SYSDBA\\." + prefix + ".*")
                .with("schema.history.internal", FileSchemaHistory.class.getName())
                .with("schema.history.internal.file.filename", directory.resolve("history.dat").toString())
                .with("snapshot.mode", "initial").with("poll.interval.ms", 100)
                .with("include.schema.changes", false).with("tombstones.on.delete", false).build();
    }

    @AfterEach
    void cleanup() throws Exception {
        stopConnector();
        try {
            execute("SP_SET_PARA_VALUE(1,'NESTED_C_STYLE_COMMENT',0)");
            for (int index = created.size() - 1; index >= 0; index--) {
                execute("DROP TABLE " + created.get(index));
            }
        }
        finally {
            jdbc.close();
        }
    }

    @Test
    void shouldReplayHistoricalCtasLikeStorageAndPartitionDdlThenRecoverCrud() throws Exception {
        final var source = create("_S", "(ID INT PRIMARY KEY,V VARCHAR(30) DEFAULT 'source')");
        execute("INSERT INTO " + source + "(ID) VALUES(1)");
        startEngine();
        assertThat(next().value()).isInstanceOf(Struct.class);
        stopConnector();
        final List<String> copies = new ArrayList<>();
        copies.add(create("_CTAS", "AS SELECT * FROM " + source));
        copies.add(create("_LIKE", "LIKE " + source));
        copies.add(create("_CAST", "AS SELECT ID,CAST(V AS VARCHAR(50)) AS V FROM " + source + " WHERE ID=1"));
        for (var suffix : List.of("_RANGE", "_HASH", "_LIST", "_STORAGE")) {
            final var options = switch (suffix) {
                case "_RANGE" -> "PARTITION BY RANGE(ID) (PARTITION P0 VALUES LESS THAN(10),PARTITION P1 VALUES LESS THAN(MAXVALUE))";
                case "_HASH" -> "PARTITION BY HASH(ID) PARTITIONS 2";
                case "_LIST" -> "PARTITION BY LIST(ID) (PARTITION P0 VALUES(1,2),PARTITION P1 VALUES(DEFAULT))";
                default -> "STORAGE(INITIAL 1,NEXT 1,MINEXTENTS 1,ON MAIN,FILLFACTOR 80,USING LONG ROW)";
            };
            final var table = create(suffix, "(ID INT PRIMARY KEY,V VARCHAR(30) COMMENT 'value') " + options);
            execute("INSERT INTO " + table + " VALUES(1,'source')");
            copies.add(table);
        }
        // Today's source schema no longer matches the CTAS/LIKE source at its log position.
        execute("ALTER TABLE " + source + " MODIFY V VARCHAR(90)");
        execute("ALTER TABLE " + source + " ADD LATER_COLUMN INT");
        startEngine();
        final List<String> seen = new ArrayList<>();
        for (int i = 0; i < copies.size(); i++) {
            final var record = next();
            final var envelope = (Struct) record.value();
            assertThat(envelope.getString("op")).isEqualTo("c");
            final var after = envelope.getStruct("after");
            assertThat(after.schema().fields()).hasSize(2);
            assertThat(after.getInt32("ID")).isOne();
            assertThat(after.getString("V")).isEqualTo("source");
            assertThat(after.schema().field("V").schema().defaultValue()).isNull();
            if (record.topic().endsWith("_CTAS") || record.topic().endsWith("_LIKE") || record.topic().endsWith("_CAST")) {
                assertThat(record.key()).isNull();
            }
            seen.add(record.topic().substring("dameng.".length()));
        }
        assertThat(seen).containsExactlyInAnyOrderElementsOf(copies);
        stopConnector();
        for (var table : copies) {
            execute("UPDATE " + table + " SET V='updated' WHERE ID=1");
        }
        startEngine();
        for (int i = 0; i < copies.size(); i++) {
            final var envelope = (Struct) next().value();
            assertThat(envelope.getString("op")).isEqualTo("u");
            assertThat(envelope.getStruct("before").getString("V")).isEqualTo("source");
            assertThat(envelope.getStruct("after").getString("V")).isEqualTo("updated");
        }
        for (var table : copies) {
            execute("DELETE FROM " + table + " WHERE ID=1");
        }
        for (int i = 0; i < copies.size(); i++) {
            final var envelope = (Struct) next().value();
            assertThat(envelope.getString("op")).isEqualTo("d");
            assertThat(envelope.getStruct("before").getString("V")).isEqualTo("updated");
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void shouldRejectChangedLexicalSettingsDuringIdleStreamingOrRestart(boolean offline) throws Exception {
        final var table = create("_S", "(ID INT PRIMARY KEY,V VARCHAR(30))");
        execute("INSERT INTO " + table + " VALUES(1,'snapshot')");
        startEngine();
        next();
        waitForSnapshotToBeCompleted("dameng", "dameng");
        if (offline) {
            stopConnector();
        }
        execute("SP_SET_PARA_VALUE(1,'NESTED_C_STYLE_COMMENT',1)");
        if (offline) {
            startEngine();
        }
        Awaitility.await("Changed lexical settings must stop CDC").atMost(Duration.ofSeconds(30)).until(() -> failure.get() != null);
        assertThat(failure.get()).hasStackTraceContaining(offline ? "differ from the stored offset" : "changed while the task was running");
        stopConnector();
        execute("SP_SET_PARA_VALUE(1,'NESTED_C_STYLE_COMMENT',0)");
        execute("INSERT INTO " + table + " VALUES(2,'restored')");
        startEngine();
        assertThat(((Struct) next().value()).getStruct("after").getInt32("ID")).isEqualTo(2);
    }

    @ParameterizedTest
    @ValueSource(strings = { "TEMPORARY", "HUGE", "VIRTUAL" })
    void shouldRejectSnapshotTypesWhoseLogMinerValuesAreIncomplete(String kind) throws Exception {
        final var table = "SYSDBA." + prefix + "_UNSAFE";
        execute(switch (kind) {
            case "TEMPORARY" -> "CREATE GLOBAL TEMPORARY TABLE " + table + "(ID INT) ON COMMIT PRESERVE ROWS";
            case "HUGE" -> "CREATE HUGE TABLE " + table + "(ID INT)";
            default -> "CREATE TABLE " + table + "(ID INT,V INT AS(ID+1) VIRTUAL)";
        });
        created.add(table);
        try (var connection = new DamengConnection(Configuration.create().with("hostname", DamengTestDatabase.hostname())
                .with("port", DamengTestDatabase.port()).with("user", DamengTestDatabase.user()).with("password", DamengTestDatabase.password()).build())) {
            assertThatThrownBy(() -> connection.readSchema(new Tables(), null, "SYSDBA", id -> id.table().equals(prefix + "_UNSAFE"), null, false))
                    .hasStackTraceContaining(kind.equals("VIRTUAL") ? "virtual/generated column" : "unsupported storage kind");
        }
    }

    @Test
    void shouldRejectSnapshotColumnsThatDifferOnlyByCase() throws Exception {
        create("_CASE", "(\"id\" INT,\"ID\" INT)");
        try (var connection = new DamengConnection(Configuration.create().with("hostname", DamengTestDatabase.hostname())
                .with("port", DamengTestDatabase.port()).with("user", DamengTestDatabase.user()).with("password", DamengTestDatabase.password()).build())) {
            assertThatThrownBy(() -> connection.readSchema(new Tables(), null, "SYSDBA", id -> id.table().equals(prefix + "_CASE"), null, false))
                    .hasMessageContaining("columns that differ only by case");
        }
    }

    @Test
    void shouldPreserveQuotedMixedCaseTableAndColumnThroughSnapshotAndOfflineRename() throws Exception {
        final var raw = prefix + "_MiXeD";
        final var quoted = "SYSDBA.\"" + raw + "\"";
        execute("CREATE TABLE " + quoted + "(\"id\" INT PRIMARY KEY,\"v\" VARCHAR(30))");
        created.add(quoted);
        execute("INSERT INTO " + quoted + " VALUES(1,'snapshot')");
        startEngine();
        final var snapshot = (Struct) next().value();
        assertThat(snapshot.getStruct("after").getString("v")).isEqualTo("snapshot");
        stopConnector();
        execute("ALTER TABLE " + quoted + " RENAME COLUMN \"v\" TO \"renamed\"");
        execute("INSERT INTO " + quoted + " VALUES(2,'stream')");
        startEngine();
        assertThat(((Struct) next().value()).getStruct("after").getString("renamed")).isEqualTo("stream");
    }

    private String create(String suffix, String definition) throws Exception {
        final var table = "SYSDBA." + prefix + suffix;
        execute("CREATE TABLE " + table + " " + definition);
        created.add(table);
        return table;
    }

    private void execute(String sql) throws Exception {
        try (var statement = jdbc.createStatement()) {
            statement.execute(sql);
        }
    }

    private void startEngine() {
        failure.set(null);
        start(DamengConnector.class, config, (success, message, error) -> {
            if (!success) {
                failure.set(error == null ? new IllegalStateException(message) : error);
            }
        }, null);
    }

    private SourceRecord next() throws Exception {
        final long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
        while (System.nanoTime() < deadline) {
            assertThat(failure.get()).as("connector failure").isNull();
            if (waitForAvailableRecords(1, TimeUnit.SECONDS)) {
                final var record = consumeRecord();
                if (record != null && record.value() instanceof Struct envelope && envelope.schema().field("op") != null) {
                    envelope.validate();
                    return record;
                }
            }
        }
        throw new AssertionError("No change event in 60 seconds");
    }
}
