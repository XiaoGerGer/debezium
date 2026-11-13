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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import io.debezium.config.Configuration;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;
import io.debezium.storage.file.history.FileSchemaHistory;

class DamengExternalTableHistoryIT extends AbstractAsyncEngineConnectorTest {
    @TempDir
    Path directory;

    private Connection connection;
    private String ordinary;
    private String external;
    private String externalDirectory;
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
        final String prefix = "DBZ_EXT_" + UUID.randomUUID().toString().replace("-", "").substring(0, 12).toUpperCase();
        ordinary = prefix + "_A";
        external = prefix + "_B";
        externalDirectory = prefix + "_DIR";
        execute("CREATE DIRECTORY " + externalDirectory + " AS '/tmp'");
        execute("CREATE TABLE " + qualified(ordinary) + " (ID INT PRIMARY KEY)");
        execute("INSERT INTO " + qualified(ordinary) + " VALUES(1)");
        createExternal();
        connection.commit();
        config = Configuration.create()
                .with("topic.prefix", "dameng")
                .with("database.hosts", DamengTestDatabase.hosts())
                .with("database.user", DamengTestDatabase.user())
                .with("database.password", DamengTestDatabase.password())
                .with("database.dbname", System.getProperty("dameng.database", "DAMENG"))
                .with("table.include.list", DamengTestDatabase.schema() + "\\.(" + ordinary + "|" + external + ")")
                .with("schema.history.internal", FileSchemaHistory.class.getName())
                .with("schema.history.internal.file.filename", directory.resolve("schema-history.dat").toString())
                .with("snapshot.mode", "initial").with("snapshot.locking.mode", "shared")
                .with("poll.interval.ms", 100)
                .with("include.schema.changes", true)
                .build();
    }

    @AfterEach
    void after() throws Exception {
        stopConnector();
        if (connection != null) {
            try {
                connection.rollback();
                for (String table : new String[]{ external, ordinary }) {
                    execute("DROP TABLE IF EXISTS " + qualified(table));
                }
                execute("DROP DIRECTORY " + externalDirectory);
                connection.commit();
            }
            finally {
                connection.close();
            }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "initial", "no_data" })
    void shouldRestoreSnapshotExternalIdentityThenCaptureOrdinaryRecreation(String mode) throws Exception {
        config = config.edit().with("snapshot.mode", mode).build();
        startEngine();
        if (mode.equals("initial")) {
            assertRow(nextDataRecord(), ordinary, "r", 1);
        }
        final var marker = insertMarker(2);
        assertThat(java.nio.file.Files.readString(directory.resolve("schema-history.dat")))
                .contains("dameng.table.kind", "EXTERNAL", "dameng.object.id", external);
        stopAt(marker);

        // The dictionary no longer contains the external object when history is recovered.
        execute("DROP TABLE " + qualified(external));
        execute("CREATE TABLE " + qualified(external) + " (ID INT PRIMARY KEY)");
        execute("INSERT INTO " + qualified(external) + " VALUES(42)");
        connection.commit();
        startEngine();
        assertRow(nextDataRecord(), external, "c", 42);
        assertRow(insertMarker(3), ordinary, "c", 3);
    }

    @Test
    void shouldRestoreStreamingExternalCreateAndReplayDrop() throws Exception {
        execute("DROP TABLE " + qualified(external));
        connection.commit();
        startEngine();
        assertRow(nextDataRecord(), ordinary, "r", 1);
        createExternal();
        execute("CREATE EXTERNAL TABLE IF NOT EXISTS " + qualified(external) + " (ID INT) FROM DATAFILE DEFAULT DIRECTORY "
                + externalDirectory + " LOCATION ('unused.csv')");
        connection.commit();
        stopAt(insertMarker(2));

        execute("DROP TABLE " + qualified(external));
        connection.commit();
        startEngine();
        stopAt(insertMarker(3));
        startEngine();
        assertRow(insertMarker(4), ordinary, "c", 4);
    }

    @Test
    void shouldCompleteAnExternalOnlySnapshotAndCaptureFutureOrdinaryTables() throws Exception {
        execute("DROP TABLE " + qualified(ordinary));
        connection.commit();
        startEngine();
        execute("DROP TABLE " + qualified(external));
        execute("CREATE TABLE " + qualified(ordinary) + " (ID INT PRIMARY KEY)");
        assertRow(insertMarker(2), ordinary, "c", 2);
    }

    private void createExternal() throws SQLException {
        // No such file exists: any accidental snapshot scan fails in the database.
        execute("CREATE EXTERNAL TABLE " + qualified(external) + " (ID INT) FROM DATAFILE DEFAULT DIRECTORY "
                + externalDirectory + " LOCATION ('" + external + "_missing.csv') PARMS(CHARACTER_CODE 'UTF-8')");
    }

    @ParameterizedTest
    @CsvSource({ "initial,DELETE", "initial,PRESERVE", "no_data,DELETE", "no_data,PRESERVE" })
    void shouldSkipTemporarySnapshotAndRestoreItsIdentityBeforeOrdinaryRecreation(String mode, String policy) throws Exception {
        execute("DROP TABLE " + qualified(external));
        createTemporary(policy);
        execute("INSERT INTO " + qualified(external) + " VALUES(7)");
        connection.commit();
        config = config.edit().with("snapshot.mode", mode).build();
        startEngine();
        if (mode.equals("initial")) {
            assertRow(nextDataRecord(), ordinary, "r", 1);
        }
        final var marker = insertMarker(2);
        assertThat(java.nio.file.Files.readString(directory.resolve("schema-history.dat")))
                .contains("TEMPORARY", "dameng.table.kind", "dameng.object.id", external);
        stopAt(marker);

        execute("ALTER TABLE " + qualified(external) + " ADD V VARCHAR(30)");
        execute("DROP TABLE " + qualified(external));
        execute("CREATE TABLE " + qualified(external) + " (ID INT PRIMARY KEY)");
        execute("INSERT INTO " + qualified(external) + " VALUES(42)");
        connection.commit();
        startEngine();
        assertRow(nextDataRecord(), external, "c", 42);
        assertRow(insertMarker(3), ordinary, "c", 3);
    }

    @ParameterizedTest
    @ValueSource(strings = { "DELETE", "PRESERVE" })
    void shouldSkipStreamingTemporaryChangesAndRecoverTheMarker(String policy) throws Exception {
        execute("DROP TABLE " + qualified(external));
        connection.commit();
        startEngine();
        assertRow(nextDataRecord(), ordinary, "r", 1);
        createTemporary(policy);
        execute("INSERT INTO " + qualified(external) + " VALUES(7)");
        execute("UPDATE " + qualified(external) + " SET ID=8 WHERE ID=7");
        execute("DELETE FROM " + qualified(external) + " WHERE ID=8");
        connection.commit();
        execute("ALTER TABLE " + qualified(external) + " ADD V VARCHAR(30)");
        execute("TRUNCATE TABLE " + qualified(external));
        connection.commit();
        stopAt(insertMarker(2));
        assertThat(java.nio.file.Files.readString(directory.resolve("schema-history.dat"))).contains("TEMPORARY", external);

        startEngine();
        assertRow(insertMarker(3), ordinary, "c", 3);
        execute("DROP TABLE " + qualified(external));
        connection.commit();
        assertRow(insertMarker(4), ordinary, "c", 4);
    }

    @ParameterizedTest
    @ValueSource(strings = { "DELETE", "PRESERVE" })
    void shouldCompleteATemporaryOnlySnapshotAndCaptureFutureOrdinaryTables(String policy) throws Exception {
        execute("DROP TABLE " + qualified(external));
        createTemporary(policy);
        execute("DROP TABLE " + qualified(ordinary));
        connection.commit();
        startEngine();
        execute("CREATE TABLE " + qualified(ordinary) + " (ID INT PRIMARY KEY)");
        assertRow(insertMarker(2), ordinary, "c", 2);
    }

    private void createTemporary(String policy) throws SQLException {
        execute("CREATE GLOBAL TEMPORARY TABLE " + qualified(external) + " (ID INT) ON COMMIT " + policy + " ROWS");
    }

    private SourceRecord insertMarker(int id) throws Exception {
        execute("INSERT INTO " + qualified(ordinary) + " VALUES(" + id + ")");
        connection.commit();
        final var record = nextDataRecord();
        assertRow(record, ordinary, "c", id);
        return record;
    }

    private void stopAt(SourceRecord marker) throws Exception {
        stopConnector();
        storeOffsets(config, Map.of(marker.sourcePartition(), marker.sourceOffset()));
        consumedLines.clear();
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
            if (failure.get() != null) {
                throw new AssertionError("Dameng external table capture failed", failure.get());
            }
            if (waitForAvailableRecords(1, TimeUnit.SECONDS)) {
                final var record = consumeRecord();
                if (record.value() instanceof Struct value) {
                    if (value.schema().field("op") != null) {
                        value.validate();
                        return record;
                    }
                    if (value.schema().field("ddl") != null) {
                        final var ddl = value.getString("ddl");
                        assertThat(ddl == null ? "" : ddl).doesNotContain("CREATE EXTERNAL", "CREATE GLOBAL TEMPORARY", "DROP TABLE " + qualified(external));
                        // Snapshot CREATE events carry no DDL, so inspect their table models too.
                        assertThat(value.toString()).doesNotContain("EXTERNAL", "TEMPORARY");
                    }
                }
            }
        }
        throw new AssertionError("No Dameng data event within 60 seconds");
    }

    private static void assertRow(SourceRecord record, String table, String operation, int id) {
        assertThat(record.topic()).isEqualTo("dameng." + DamengTestDatabase.schema() + "." + table);
        final var value = (Struct) record.value();
        assertThat(value.getString("op")).isEqualTo(operation);
        assertThat(value.getStruct("after").getInt32("ID")).isEqualTo(id);
    }

    private void execute(String sql) throws SQLException {
        try (var statement = connection.createStatement()) {
            statement.execute(sql);
        }
    }

    private static String qualified(String table) {
        return '"' + DamengTestDatabase.schema() + "\".\"" + table + '"';
    }
}
