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
import java.sql.SQLException;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import io.debezium.config.Configuration;
import io.debezium.connector.dameng.ddl.DamengSqlLexer;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;
import io.debezium.storage.file.history.FileSchemaHistory;

/** Each matrix row owns its database; static parameters take effect before any test DDL is executed. */
class DamengLexicalSettingsIT extends AbstractAsyncEngineConnectorTest {
    private static final String TABLE = "SYSDBA.DBZ_LEXICAL";
    private static final String DEFAULT = "a\\n\\\\b'c;--/*//";
    private static final String LITERAL = "'a\\n\\\\b''c;--/*//'";
    private static final String BOUND = "中文\\0\\n\\r\\t\\b\\f\\v\\x\\u\\z\\\\'\";--/*//\n\r\t\0";
    private final AtomicReference<Throwable> failure = new AtomicReference<>();
    private Connection jdbc;
    private Configuration config;
    private boolean databaseStarted;

    @TempDir
    Path temporaryDirectory;

    @AfterEach
    void cleanup() throws Exception {
        try {
            stopConnector();
        }
        finally {
            try {
                if (jdbc != null) {
                    jdbc.close();
                }
            }
            finally {
                if (databaseStarted) {
                    DamengTestDatabase.stop();
                }
            }
        }
    }

    @ParameterizedTest(name = "BACKSLASH_ESCAPE={0}, NESTED_C_STYLE_COMMENT={1}")
    @CsvSource({ "0,0", "0,1", "1,0", "1,1" })
    void shouldMatchDatabaseValuesThroughSnapshotArchivedDdlCrudAndRecovery(int backslash, int nested) throws Exception {
        assumeTrue(System.getProperty("dameng.hostname") == null, "The lexical matrix only changes disposable databases");
        DamengTestDatabase.start(backslash, nested);
        databaseStarted = true;
        jdbc = DriverManager.getConnection(DamengTestDatabase.jdbcUrl(), DamengTestDatabase.user(), DamengTestDatabase.password());
        jdbc.setAutoCommit(true);
        final var evidenceDirectory = Path.of("target", "dameng-lexical-evidence");
        java.nio.file.Files.createDirectories(evidenceDirectory);
        final var prefix = "native-" + backslash + "-" + nested;
        try (var statement = jdbc.createStatement();
                var result = statement.executeQuery("SELECT PARA_NAME, PARA_VALUE, SESS_VALUE, FILE_VALUE FROM V$DM_INI "
                        + "WHERE PARA_NAME IN ('COMPATIBLE_MODE','BACKSLASH_ESCAPE','NESTED_C_STYLE_COMMENT') ORDER BY PARA_NAME")) {
            final var evidence = new StringBuilder("version=").append(jdbc.getMetaData().getDatabaseProductVersion()).append('\n')
                    .append("driver=").append(jdbc.getMetaData().getDriverVersion()).append('\n');
            while (result.next()) {
                evidence.append(result.getString(1)).append('=').append(result.getString(2)).append('/')
                        .append(result.getString(3)).append('/').append(result.getString(4)).append('\n');
            }
            java.nio.file.Files.writeString(evidenceDirectory.resolve(prefix + "-environment.txt"), evidence);
        }
        verifyLiteralSemantics(nested);
        final var disql = DamengTestDatabase.executeDisql(
                "SELECT CASE WHEN LENGTH('A\\nB')=4 AND LENGTH(N'A\\nB')=4 AND LENGTH(q'[A\\nB]')=4 "
                        + "THEN 'DBZ_RAW_BACKSLASH_OK' ELSE 'DBZ_ESCAPE_CHANGED' END FROM DUAL;");
        java.nio.file.Files.writeString(evidenceDirectory.resolve(prefix + "-disql.txt"), disql);
        // DISQL echoes the input SQL, including both CASE branches. Assert the result row.
        assertThat(disql).containsPattern("(?m)^1\\s+DBZ_RAW_BACKSLASH_OK\\s*$");

        final var comment = nested == 1 ? "/* outer /* ; DROP TABLE X; */ outer */" : "/* outer /* text */";
        final var expressionComment = nested == 1 ? "/* outer /* inner */ -- hidden */" : "/* -- hidden */";
        execute("CREATE" + comment + "TABLE " + TABLE + " (ID INT PRIMARY KEY, V VARCHAR(300) DEFAULT " + LITERAL
                + ", N VARCHAR(300) DEFAULT N'中文\\n', Q VARCHAR(300) DEFAULT q'[it''s\\n;--/*//]', "
                + "S_EXPR VARCHAR(10) DEFAULT 'x' " + expressionComment + " || 'y')");
        execute("INSERT INTO " + TABLE + "(ID) VALUES(1)");
        final var snapshotValues = databaseRow(1);
        config = Configuration.create()
                .with("topic.prefix", "dameng")
                .with("database.hostname", DamengTestDatabase.hostname())
                .with("database.port", DamengTestDatabase.port())
                .with("database.user", DamengTestDatabase.user())
                .with("database.password", DamengTestDatabase.password())
                .with("database.dbname", "DAMENG")
                .with("table.include.list", "SYSDBA\\.DBZ_LEXICAL")
                .with("schema.history.internal", FileSchemaHistory.class.getName())
                .with("schema.history.internal.file.filename", temporaryDirectory.resolve("history.dat").toString())
                .with("snapshot.mode", "initial")
                .with("poll.interval.ms", 100)
                .with("include.schema.changes", false)
                .with("tombstones.on.delete", false)
                .build();
        startEngine();
        final var snapshot = nextData("r").getStruct("after");
        assertValues(snapshot, snapshotValues);
        assertThat(snapshot.schema().field("V").schema().defaultValue()).isEqualTo(DEFAULT);
        assertThat(snapshot.getString("S_EXPR")).isEqualTo("xy");
        // JDBC may fold the constant expression. It must never report just its first literal.
        assertThat(snapshot.schema().field("S_EXPR").schema().defaultValue()).isIn(null, "xy");
        stopConnector();

        // The connector must replay the historical DDL after downtime, not query today's table instead.
        execute("ALTER" + comment + "TABLE " + TABLE + " ADD (EXTRA VARCHAR(300) DEFAULT " + LITERAL
                + ", EXPR VARCHAR(10) DEFAULT 'x' " + expressionComment + " || 'y')");
        execute("COMMENT" + comment + "ON COLUMN " + TABLE + ".EXTRA IS q'[; DROP TABLE X; /*text*/]'");
        execute("INSERT INTO " + TABLE + "(ID) VALUES(2)");
        final var insertedValues = databaseRow(2);
        startEngine();
        final var inserted = nextData("c").getStruct("after");
        assertValues(inserted, insertedValues);
        assertThat(inserted.schema().field("EXTRA").schema().defaultValue()).isEqualTo(DEFAULT);
        assertThat(inserted.getString("EXPR")).isEqualTo("xy");
        // A compound expression must not be mistaken for the first string constant
        // when history/default conversion no longer has the original lexical settings.
        assertThat(inserted.schema().field("EXPR").schema().defaultValue()).isNull();
        stopConnector();

        try (var statement = jdbc.prepareStatement("UPDATE " + TABLE + " SET V=?, EXTRA=? WHERE ID=2")) {
            statement.setString(1, BOUND);
            statement.setString(2, BOUND);
            assertThat(statement.executeUpdate()).isOne();
        }
        final var updatedValues = databaseRow(2);
        startEngine();
        final var update = nextData("u");
        assertValues(update.getStruct("before"), insertedValues);
        assertValues(update.getStruct("after"), updatedValues);
        assertThat(update.getStruct("after").schema()).isEqualTo(inserted.schema());
        assertThat(update.getStruct("after").getString("V")).isEqualTo(BOUND);
        execute("DELETE FROM " + TABLE + " WHERE ID=2");
        assertValues(nextData("d").getStruct("before"), updatedValues);
    }

    private void verifyLiteralSemantics(int nested) throws Exception {
        final var lexer = new DamengSqlLexer(nested == 1);
        for (var prefix : List.of("'", "N'", "q'!")) {
            for (char escaped : "0abfnrtvzZ1234789xu\"\\%_中".toCharArray()) {
                final var literal = prefix + "A\\" + escaped + "B" + (prefix.startsWith("q") ? "!'" : "'");
                try (var statement = jdbc.createStatement(); var row = statement.executeQuery("SELECT " + literal + " FROM DUAL")) {
                    assertThat(row.next()).isTrue();
                    assertThat(row.getString(1)).as("native literal %s", literal).isEqualTo("A\\" + escaped + "B");
                    assertThat(lexer.tokenize(literal).get(0).stringValue()).isEqualTo(row.getString(1));
                }
            }
        }
        assertThatThrownBy(() -> execute("SELECT 'a\\'b' FROM DUAL")).isInstanceOf(SQLException.class);
        if (nested == 1) {
            assertThatThrownBy(() -> execute("SELECT /* outer /* inner */ 1 FROM DUAL"))
                    .isInstanceOf(SQLException.class);
        }
        else {
            assertThatThrownBy(() -> execute("SELECT /* outer /* inner */ tail */ 1 FROM DUAL"))
                    .isInstanceOf(SQLException.class);
        }
    }

    private Map<String, Object> databaseRow(int id) throws SQLException {
        final Map<String, Object> values = new LinkedHashMap<>();
        try (var statement = jdbc.createStatement(); var row = statement.executeQuery("SELECT * FROM " + TABLE + " WHERE ID=" + id)) {
            assertThat(row.next()).isTrue();
            for (int index = 1; index <= row.getMetaData().getColumnCount(); index++) {
                values.put(row.getMetaData().getColumnName(index), row.getObject(index));
            }
            assertThat(row.next()).isFalse();
        }
        return values;
    }

    private void assertValues(Struct actual, Map<String, Object> expected) {
        actual.validate();
        assertThat(actual.schema().fields()).hasSize(expected.size());
        expected.forEach((name, value) -> assertThat(actual.get(name)).as(name).isEqualTo(value));
    }

    private void execute(String sql) throws SQLException {
        try (var statement = jdbc.createStatement()) {
            statement.execute(sql);
        }
    }

    private void startEngine() {
        failure.set(null);
        start(DamengConnector.class, config, (success, message, error) -> {
            if (!success) {
                failure.set(error != null ? error : new IllegalStateException(message));
            }
        }, null);
    }

    private Struct nextData(String operation) throws Exception {
        final long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
        while (System.nanoTime() < deadline) {
            assertThat(failure.get()).as("connector failure").isNull();
            if (waitForAvailableRecords(1, TimeUnit.SECONDS)) {
                final SourceRecord record = consumeRecord();
                if (record != null && record.topic().equals("dameng." + TABLE)) {
                    final var envelope = (Struct) record.value();
                    envelope.validate();
                    assertThat(envelope.getString("op")).isEqualTo(operation);
                    return envelope;
                }
            }
        }
        throw new AssertionError("No " + operation + " event in 60 seconds");
    }
}
