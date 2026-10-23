/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.nio.file.Path;
import java.sql.DriverManager;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import io.debezium.config.Configuration;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;
import io.debezium.storage.file.history.FileSchemaHistory;

/** Real initialized databases are required; CASE_SENSITIVE cannot be changed on a live database. */
class DamengCaseSensitivityIT extends AbstractAsyncEngineConnectorTest {
    @TempDir
    Path directory;
    private final AtomicReference<Throwable> failure = new AtomicReference<>();

    @ParameterizedTest(name = "snapshot/history/CRUD/signals CASE_SENSITIVE={0}")
    @ValueSource(ints = { 0, 1 })
    void shouldKeepSnapshotStreamingDdlAndSignalsConsistent(int sensitive) throws Exception {
        assumeTrue(System.getProperty("dameng.hostname") == null, "The case matrix owns its database");
        DamengTestDatabase.start(0, 0, sensitive);
        final var schemaName = canonical("Dbz_App", sensitive);
        final var tableName = canonical("Dbz_Case", sensitive);
        final var valueName = canonical("VaLuE", sensitive);
        final var idName = canonical("Id", sensitive);
        final var renamedValue = canonical("NewValue", sensitive);
        final var signal = sensitive == 0 ? "dbz_app.DbZ_sIgNaL" : "Dbz_App.Dbz_Signal";
        final var qualified = "\"Dbz_App\".\"Dbz_Case\"";
        try (var jdbc = DriverManager.getConnection(DamengTestDatabase.jdbcUrl(), DamengTestDatabase.user(), DamengTestDatabase.password());
                var sql = jdbc.createStatement()) {
            sql.execute("CREATE SCHEMA \"Dbz_App\" AUTHORIZATION SYSDBA");
            sql.execute("CREATE TABLE " + qualified + " (\"Id\" INT, \"VaLuE\" VARCHAR(30) DEFAULT 'AbC', \"Skip\" INT, CONSTRAINT \"Pk_Mixed\" PRIMARY KEY (\"Id\"))");
            sql.execute("CREATE TABLE \"Dbz_App\".\"Dbz_NoPk\" (\"Id\" INT, \"VaLuE\" VARCHAR(30))");
            sql.execute("INSERT INTO " + qualified + " VALUES (1, 'AbC', 9)");
            final var config = Configuration.create().with("topic.prefix", "case")
                    .with("database.hostname", DamengTestDatabase.hostname()).with("database.port", DamengTestDatabase.port())
                    .with("database.user", DamengTestDatabase.user()).with("database.password", DamengTestDatabase.password())
                    .with("database.dbname", "DAMENG")
                    .with("schema.include.list", "(?-i)" + schemaName)
                    .with("table.include.list",
                            "(?-i)" + schemaName + "\\.(" + tableName + "|" + canonical("Dbz_NoPk", sensitive) + "|" + canonical("Dbz_Renamed", sensitive)
                                    + "|" + canonical("Dbz_TextKey", sensitive) + ")")
                    .with("column.exclude.list", "(?-i)" + schemaName + "\\." + tableName + "\\." + canonical("Skip", sensitive))
                    .with("signal.data.collection", signal)
                    .with("incremental.snapshot.chunk.size", 1)
                    .with("schema.history.internal", FileSchemaHistory.class.getName())
                    .with("schema.history.internal.file.filename", directory.resolve("history.dat").toString())
                    .with("snapshot.mode", "initial").with("poll.interval.ms", 100)
                    .with("include.schema.changes", false).with("tombstones.on.delete", false).build();
            startEngine(config);
            final var snapshot = nextData(schemaName + "." + tableName, "r");
            final var row = ((Struct) snapshot.value()).getStruct("after");
            assertThat(row.getInt32(idName)).isEqualTo(1);
            assertThat(row.getString(valueName)).isEqualTo("AbC");
            assertThat(row.schema().fields()).hasSize(2);
            assertThat(((Struct) snapshot.key()).getInt32(idName)).isEqualTo(1);
            stopConnector();
            consumedLines.clear();
            final var reference = sensitive == 0 ? "\"dbz_app\".\"dbz_case\"" : qualified;
            final var valueReference = sensitive == 0 ? "\"value\"" : "\"VaLuE\"";
            sql.execute("ALTER TABLE " + reference + " ADD \"Extra\" VARCHAR(20) DEFAULT 'xYz'");
            sql.execute("ALTER TABLE " + reference + " RENAME COLUMN " + valueReference + " TO \"NewValue\"");
            sql.execute("INSERT INTO " + reference + " (\"Id\", \"NewValue\") VALUES (2, 'dEf')");
            startEngine(config);
            final var inserted = (Struct) nextData(schemaName + "." + tableName, "c").value();
            assertThat(inserted.getStruct("after").getString(renamedValue)).isEqualTo("dEf");
            assertThat(inserted.getStruct("after").getString(canonical("Extra", sensitive))).isEqualTo("xYz");
            assertThat(inserted.getStruct("after").schema().fields()).hasSize(3);
            sql.execute("UPDATE " + reference + " SET \"NewValue\"='gHi' WHERE \"Id\"=2");
            final var update = (Struct) nextData(schemaName + "." + tableName, "u").value();
            assertThat(update.getStruct("before").getString(renamedValue)).isEqualTo("dEf");
            assertThat(update.getStruct("after").getString(renamedValue)).isEqualTo("gHi");
            sql.execute("DELETE FROM " + reference + " WHERE \"Id\"=2");
            assertThat(((Struct) nextData(schemaName + "." + tableName, "d").value())
                    .getStruct("before").getString(renamedValue)).isEqualTo("gHi");
            // Signal spelling differs from JDBC metadata in case-insensitive mode.
            final var signalSql = sensitive == 0 ? "\"DBZ_APP\".\"dbz_signal\"" : "\"Dbz_App\".\"Dbz_Signal\"";
            try (var insert = jdbc.prepareStatement("INSERT INTO " + signalSql + " VALUES (?,?,?)")) {
                insert.setString(1, "case-snapshot");
                insert.setString(2, "execute-snapshot");
                insert.setString(3, "{\"data-collections\":[\"" + schemaName + "." + tableName + "\"],\"type\":\"incremental\"}");
                insert.executeUpdate();
            }
            final var incremental = (Struct) nextData(schemaName + "." + tableName, "r").value();
            assertThat(incremental.getStruct("source").getString("snapshot")).isEqualTo("incremental");
            assertThat(incremental.getStruct("after").getString(renamedValue)).isEqualTo("AbC");
            sql.execute("DROP TABLE \"Dbz_App\".\"Dbz_NoPk\"");
            // Replay a CREATE too: case-insensitive JDBC preserves unquoted spelling,
            // whereas the historical model must canonicalize the archived SQL.
            sql.execute(sensitive == 0 ? "CREATE TABLE dbz_app.Dbz_NoPk (Id INT, VaLuE VARCHAR(30))"
                    : "CREATE TABLE \"Dbz_App\".\"Dbz_NoPk\" (\"Id\" INT, \"VaLuE\" VARCHAR(30))");
            sql.execute("INSERT INTO \"Dbz_App\".\"Dbz_NoPk\" VALUES (3, 'NoPk')");
            assertThat(nextData(schemaName + "." + canonical("Dbz_NoPk", sensitive), "c").key()).isNull();
            final var textTable = schemaName + "." + canonical("Dbz_TextKey", sensitive);
            sql.execute("CREATE TABLE \"Dbz_App\".\"Dbz_TextKey\" (\"Id\" VARCHAR(20) PRIMARY KEY, \"Payload\" VARCHAR(20))");
            // Database collation also affects chunk ordering. Keep key values verbatim,
            // including a key change that differs only in case.
            for (var key : List.of("a", "B", "c")) {
                sql.execute("INSERT INTO \"Dbz_App\".\"Dbz_TextKey\" VALUES ('" + key + "', 'AbC')");
                assertThat(((Struct) nextData(textTable, "c").key()).getString(idName)).isEqualTo(key);
            }
            sql.execute("UPDATE \"Dbz_App\".\"Dbz_TextKey\" SET \"Id\"='A' WHERE \"Id\"='a'");
            assertThat(((Struct) nextData(textTable, "d").key()).getString(idName)).isEqualTo("a");
            assertThat(((Struct) nextData(textTable, "c").key()).getString(idName)).isEqualTo("A");
            try (var insert = jdbc.prepareStatement("INSERT INTO " + signalSql + " VALUES (?,?,?)")) {
                insert.setString(1, "text-key-snapshot");
                insert.setString(2, "execute-snapshot");
                insert.setString(3, "{\"data-collections\":[\"" + textTable + "\"],\"type\":\"incremental\"}");
                insert.executeUpdate();
            }
            final Set<String> keys = new HashSet<>();
            for (int index = 0; index < 3; index++) {
                final var record = nextData(textTable, "r");
                assertThat(((Struct) record.value()).getStruct("after").getString(canonical("Payload", sensitive))).isEqualTo("AbC");
                assertThat(keys.add(((Struct) record.key()).getString(idName))).isTrue();
            }
            assertThat(keys).containsExactlyInAnyOrder("A", "B", "c");
            stopConnector();
            consumedLines.clear();
            sql.execute("ALTER TABLE " + reference + " DROP CONSTRAINT \"" + (sensitive == 0 ? "pk_mixed" : "Pk_Mixed") + "\"");
            sql.execute("ALTER TABLE " + reference + " RENAME TO \"Dbz_Renamed\"");
            sql.execute("INSERT INTO \"Dbz_App\".\"Dbz_Renamed\" (\"Id\", \"NewValue\") VALUES (4, 'jKl')");
            startEngine(config);
            final var renamed = nextData(schemaName + "." + canonical("Dbz_Renamed", sensitive), "c");
            assertThat(renamed.key()).isNull();
            assertThat(((Struct) renamed.value()).getStruct("after").getString(renamedValue)).isEqualTo("jKl");
        }
        finally {
            try {
                stopConnector();
            }
            finally {
                DamengTestDatabase.stop();
            }
        }
    }

    private static String canonical(String name, int sensitive) {
        return sensitive == 0 ? name.toUpperCase(Locale.ROOT) : name;
    }

    private void startEngine(Configuration config) {
        failure.set(null);
        start(DamengConnector.class, config, (success, message, error) -> {
            if (!success) {
                failure.set(error != null ? error : new IllegalStateException(message));
            }
        }, null);
    }

    private SourceRecord nextData(String table, String operation) throws Exception {
        final long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
        while (System.nanoTime() < deadline) {
            assertThat(failure.get()).as("connector failure").isNull();
            if (waitForAvailableRecords(1, TimeUnit.SECONDS)) {
                final var record = consumeRecord();
                if (record != null && record.topic().equals("case." + table)) {
                    final var envelope = (Struct) record.value();
                    envelope.validate();
                    assertThat(envelope.getString("op")).isEqualTo(operation);
                    return record;
                }
            }
        }
        throw new AssertionError("No " + operation + " event for " + table);
    }

    @ParameterizedTest
    @ValueSource(ints = { 0, 1 })
    void shouldReadIdentifierSpellingFromJdbcAndArchivedDdl(int sensitive) throws Exception {
        assumeTrue(System.getProperty("dameng.hostname") == null, "The case matrix owns its database");
        DamengTestDatabase.start(0, 0, sensitive);
        final var evidence = new StringBuilder("CASE_SENSITIVE=").append(sensitive).append('\n');
        try (var connection = new DamengConnection(Configuration.create()
                .with("hostname", DamengTestDatabase.hostname()).with("port", DamengTestDatabase.port())
                .with("user", DamengTestDatabase.user()).with("password", DamengTestDatabase.password()).build())) {
            final long start = connection.currentLsn();
            connection.execute("CREATE TABLE Dbz_Mixed (Id INT PRIMARY KEY, VaLuE VARCHAR(30))",
                    "CREATE TABLE \"Dbz_Quoted\" (\"Id\" INT PRIMARY KEY, \"VaLuE\" VARCHAR(30))",
                    "INSERT INTO Dbz_Mixed VALUES (1, 'AbC')", "INSERT INTO \"Dbz_Quoted\" VALUES (1, 'AbC')");
            final var physical = new io.debezium.relational.Tables();
            connection.readSchema(physical, null, "SYSDBA", id -> id.table().equalsIgnoreCase("DBZ_MIXED")
                    || id.table().equalsIgnoreCase("DBZ_QUOTED"), null, false);
            final var historical = new io.debezium.relational.Tables();
            final var parser = connection.createDdlParser();
            parser.parse("CREATE TABLE Dbz_Mixed (Id INT PRIMARY KEY, VaLuE VARCHAR(30))", historical, "SYSDBA");
            parser.parse("CREATE TABLE \"Dbz_Quoted\" (\"Id\" INT PRIMARY KEY, \"VaLuE\" VARCHAR(30))", historical, "SYSDBA");
            assertThat(historical.tableIds()).containsExactlyInAnyOrderElementsOf(physical.tableIds());
            for (var id : physical.tableIds()) {
                assertThat(historical.forTable(id).retrieveColumnNames()).isEqualTo(physical.forTable(id).retrieveColumnNames());
                assertThat(historical.forTable(id).primaryKeyColumnNames()).isEqualTo(physical.forTable(id).primaryKeyColumnNames());
            }
            final var metadata = connection.connection().getMetaData();
            try (var columns = metadata.getColumns(null, "SYSDBA", "%", null)) {
                while (columns.next()) {
                    if (columns.getString("TABLE_NAME").toUpperCase(Locale.ROOT).startsWith("DBZ_")) {
                        evidence.append("COLUMN ").append(columns.getString("TABLE_NAME")).append('.')
                                .append(columns.getString("COLUMN_NAME")).append('\n');
                    }
                }
            }
            if (sensitive == 0) {
                connection.execute("ALTER TABLE \"dbz_quoted\" ADD \"Extra\" INT DEFAULT 7",
                        "ALTER TABLE DBZ_QUOTED RENAME COLUMN \"value\" TO \"NeW_Value\"",
                        "UPDATE dbz_QUOTED SET \"new_value\"='xYz' WHERE \"id\"=1");
                connection.query("SELECT \"NEW_VALUE\", extra FROM \"DBZ_QUOTED\"", rows -> {
                    assertThat(rows.next()).isTrue();
                    assertThat(rows.getString(1)).isEqualTo("xYz");
                    assertThat(rows.getInt(2)).isEqualTo(7);
                });
            }
            final long end = connection.currentLsn();
            Awaitility.await().atMost(Duration.ofSeconds(30)).until(() -> connection.latestArchivedLsn() >= end);
            connection.startLogMining(start, end, false);
            try {
                connection.query("SELECT OPERATION_CODE, SQL_REDO FROM V$LOGMNR_CONTENTS WHERE SCN>" + start + " AND SCN<=" + end
                        + " ORDER BY SCN", rows -> {
                            while (rows.next()) {
                                evidence.append("REDO ").append(rows.getInt(1)).append(' ').append(rows.getString(2)).append('\n');
                            }
                        });
            }
            finally {
                connection.endLogMining();
            }
        }
        finally {
            final var directory = Path.of("target", "dameng-case-sensitivity-evidence");
            java.nio.file.Files.createDirectories(directory);
            java.nio.file.Files.writeString(directory.resolve("case-" + sensitive + ".txt"), evidence);
            DamengTestDatabase.stop();
        }
    }
}
