/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.math.BigDecimal;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import io.debezium.config.Configuration;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;
import io.debezium.storage.file.history.FileSchemaHistory;

/** PostgreSQL-mode snapshot, archived DDL and CRUD must agree across restarts. */
@EnabledIfSystemProperty(named = "dameng.compatible.mode", matches = "7")
class DamengPostgresCompatibilityIT extends AbstractAsyncEngineConnectorTest {
    private static final String TABLE = "SYSDBA.\"Dbz_Pg\"";
    private static final String DEFAULT = "汉字\\n'a;--/*";
    private static final String LITERAL = "E'汉字\\n''a;--/*'";

    @TempDir
    Path directory;

    private final AtomicReference<Throwable> failure = new AtomicReference<>();
    private Connection jdbc;
    private Configuration config;
    private boolean databaseStarted;

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

    @ParameterizedTest(name = "PostgreSQL BACKSLASH_ESCAPE={0}, NESTED_C_STYLE_COMMENT={1}")
    @CsvSource({ "0,0", "0,1" })
    void shouldCaptureSnapshotHistoricalDdlCrudAndRecovery(int backslash, int nested) throws Exception {
        assumeTrue(System.getProperty("dameng.hostname") == null, "The lexical matrix owns its disposable databases");
        DamengTestDatabase.start(backslash, nested);
        databaseStarted = true;
        jdbc = DriverManager.getConnection(DamengTestDatabase.jdbcUrl(), DamengTestDatabase.user(), DamengTestDatabase.password());
        jdbc.setAutoCommit(true);
        execute("CREATE TABLE " + TABLE + "(\"Id\" INT PRIMARY KEY, v VARCHAR(120) DEFAULT " + LITERAL
                + ", d DATE, ts TIMESTAMP(6) WITHOUT TIME ZONE, t TIME(3) WITHOUT TIME ZONE, n NUMERIC(12,2))");
        insert(1);
        config = Configuration.create()
                .with("topic.prefix", "pg")
                .with("database.hostname", DamengTestDatabase.hostname())
                .with("database.port", DamengTestDatabase.port())
                .with("database.user", DamengTestDatabase.user())
                .with("database.password", DamengTestDatabase.password())
                .with("database.dbname", "DAMENG")
                .with("table.include.list", "(?-i)SYSDBA\\.Dbz_Pg")
                .with("schema.history.internal", FileSchemaHistory.class.getName())
                .with("schema.history.internal.file.filename", directory.resolve("history.dat").toString())
                .with("snapshot.mode", "initial")
                .with("poll.interval.ms", 100)
                .with("include.schema.changes", false)
                .with("tombstones.on.delete", false)
                .build();
        startEngine();
        final var snapshot = nextData("r", 1).getStruct("after");
        assertValues(snapshot, DEFAULT);
        assertThat(snapshot.schema().field("V").schema().defaultValue()).isEqualTo(DEFAULT);
        stopConnector();

        final var comment = nested == 1 ? "/* outer /* nested */ outer */" : "/* regular */";
        execute("ALTER" + comment + "TABLE " + TABLE + " ADD (extra VARCHAR(120) DEFAULT " + LITERAL
                + ", extra_ts TIMESTAMP(3) WITHOUT TIME ZONE DEFAULT TIMESTAMP'2001-01-01 01:02:03.123')");
        execute("ALTER TABLE " + TABLE + " RENAME COLUMN v TO \"NewValue\"");
        insert(2);
        execute("UPDATE " + TABLE + " SET \"NewValue\"='changed' WHERE \"Id\"=2");
        execute("DELETE FROM " + TABLE + " WHERE \"Id\"=2");

        // The row is already gone from the live table; only historical redo can
        // supply its values and the schema that applied to each operation.
        startEngine();
        final var created = nextData("c", 2).getStruct("after");
        assertValues(created, DEFAULT);
        assertThat(created.getString("EXTRA")).isEqualTo(DEFAULT);
        assertThat(created.schema().field("EXTRA").schema().defaultValue()).isEqualTo(DEFAULT);
        assertThat(created.getString("EXTRA_TS")).isEqualTo("2001-01-01 01:02:03.123");
        final var updated = nextData("u", 2);
        assertValues(updated.getStruct("before"), DEFAULT);
        assertValues(updated.getStruct("after"), "changed");
        assertValues(nextData("d", 2).getStruct("before"), "changed");
        stopConnector();

        insert(3);
        startEngine();
        final var recovered = nextData("c", 3).getStruct("after");
        assertValues(recovered, DEFAULT);
        assertThat(recovered.schema()).isEqualTo(created.schema());
    }

    private void assertValues(Struct value, String text) {
        value.validate();
        assertThat(value.getString(value.schema().field("V") == null ? "NewValue" : "V")).isEqualTo(text);
        assertThat(value.getString("D")).isEqualTo("2000-02-29");
        assertThat(value.getString("TS")).isEqualTo("2000-02-29 12:34:56.123456");
        assertThat(value.getString("T")).isEqualTo("12:34:56.123");
        assertThat(value.get("N")).isEqualTo(new BigDecimal("12.34"));
    }

    private void insert(int id) throws Exception {
        execute("INSERT INTO " + TABLE + "(\"Id\",d,ts,t,n) VALUES(" + id
                + ",DATE'2000-02-29',TIMESTAMP'2000-02-29 12:34:56.123456',TIME'12:34:56.123',12.34)");
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
                failure.set(error != null ? error : new IllegalStateException(message));
            }
        }, null);
    }

    private Struct nextData(String operation, int id) throws Exception {
        final long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
        while (System.nanoTime() < deadline) {
            assertThat(failure.get()).as("connector failure").isNull();
            if (waitForAvailableRecords(1, TimeUnit.SECONDS)) {
                final SourceRecord record = consumeRecord();
                if (record != null && record.topic().equals("pg.SYSDBA.Dbz_Pg")) {
                    assertThat(((Struct) record.key()).getInt32("Id")).isEqualTo(id);
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
