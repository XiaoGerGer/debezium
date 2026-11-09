/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import io.debezium.config.Configuration;
import io.debezium.data.VariableScaleDecimal;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;
import io.debezium.storage.file.history.FileSchemaHistory;

/** Decimal FLOAT must retain the database's stored values throughout archived replay. */
@EnabledIfSystemProperty(named = "dameng.compatible.mode", matches = "2")
@EnabledIfSystemProperty(named = "dameng.test.ini.NUMBER_MODE", matches = "1")
class DamengOracleNumberModeIT extends AbstractAsyncEngineConnectorTest {
    private static final String TABLE = "SYSDBA.DBZ_ORA_NUM";
    private static final String LATE_TABLE = "SYSDBA.DBZ_ORA_NUM_LATE";

    @TempDir
    Path directory;

    private final AtomicReference<Throwable> failure = new AtomicReference<>();
    private final List<String> createdTables = new ArrayList<>();
    private Connection jdbc;
    private Configuration config;
    private boolean databaseStarted;
    private String decimalMode;

    @BeforeAll
    static void startDatabase() throws Exception {
        DamengTestDatabase.start();
    }

    @AfterAll
    static void stopDatabase() {
        DamengTestDatabase.stop();
    }

    @AfterEach
    void cleanup() throws Exception {
        try {
            stopConnector();
        }
        finally {
            try {
                if (jdbc != null) {
                    try {
                        for (var table : createdTables) {
                            execute("DROP TABLE IF EXISTS " + table);
                        }
                    }
                    finally {
                        jdbc.close();
                    }
                }
            }
            finally {
                if (databaseStarted) {
                    DamengTestDatabase.stop();
                }
            }
        }
    }

    @ParameterizedTest(name = "Oracle NUMBER_MODE=1 snapshot/history/CRUD/recovery decimal.handling.mode={0}")
    @ValueSource(strings = { "precise", "string", "double" })
    void shouldCaptureDecimalFloatsDefaultsTypeChangesAndRecovery(String decimalMode) throws Exception {
        this.decimalMode = decimalMode;
        DamengTestDatabase.start();
        databaseStarted = true;
        jdbc = DriverManager.getConnection(DamengTestDatabase.jdbcUrl(), DamengTestDatabase.user(), DamengTestDatabase.password());
        jdbc.setAutoCommit(true);
        createTable(TABLE);
        execute("INSERT INTO " + TABLE + "(id) VALUES(1)");
        final var initial = readValues(TABLE, 1);
        config = Configuration.create()
                .with("topic.prefix", "ora_num")
                .with("database.hosts", DamengTestDatabase.hosts())
                .with("database.user", DamengTestDatabase.user())
                .with("database.password", DamengTestDatabase.password())
                .with("database.dbname", "DAMENG")
                .with("table.include.list", "SYSDBA\\.DBZ_ORA_NUM(_LATE)?")
                .with("schema.history.internal", FileSchemaHistory.class.getName())
                .with("schema.history.internal.file.filename", directory.resolve("history.dat").toString())
                .with("snapshot.mode", "initial")
                .with("decimal.handling.mode", decimalMode)
                .with("poll.interval.ms", 100)
                .with("include.schema.changes", false)
                .with("tombstones.on.delete", false)
                .build();
        startEngine();
        final var snapshot = nextData(TABLE, "r", 1).getStruct("after");
        assertValues(snapshot, initial);
        initial.forEach((name, expected) -> assertDefault(snapshot.schema().field(name).schema(), expected));
        stopConnector();

        execute("INSERT INTO " + TABLE + "(id) VALUES(2)");
        final var inserted = readValues(TABLE, 2);
        execute("UPDATE " + TABLE + " SET " + initial.keySet().stream().map(name -> name + "=NULL").collect(Collectors.joining(",")) + " WHERE id=2");
        final var nulled = readValues(TABLE, 2);
        execute("UPDATE " + TABLE + " SET n=1E-120, fixed=-98.76543210, whole=-12345678901234567890123456789012345678, "
                + "plain=-9.876543210987654321E120, low=-9E120, small=1.234567890123, medium=-1.234567890123456789, "
                + "wide=-0.00000012345678901234567890123456789, real_num=1.2345678901234567890123, double_num=1E-120, "
                + "numeric_fixed=-9.87, decimal_fixed=-6.54, unbounded_dec=9.876543210987654321E120 WHERE id=2");
        final var updated = readValues(TABLE, 2);
        execute("DELETE FROM " + TABLE + " WHERE id=2");
        execute("ALTER TABLE " + TABLE + " MODIFY small FLOAT(126)");
        execute("ALTER TABLE " + TABLE + " RENAME COLUMN plain TO renamed_plain");
        execute("ALTER TABLE " + TABLE + " ADD extra REAL DEFAULT 1.234567890123456");
        execute("INSERT INTO " + TABLE + "(id,small) VALUES(3,1.2345678901234567890123456789)");
        final var altered = readValues(TABLE, 3);
        execute("DELETE FROM " + TABLE + " WHERE id=3");
        createTable(LATE_TABLE);
        execute("INSERT INTO " + LATE_TABLE + "(id) VALUES(4)");
        final var historical = readValues(LATE_TABLE, 4);
        execute("DELETE FROM " + LATE_TABLE + " WHERE id=4");

        // No live row can supply these values when the engine replays the archive.
        startEngine();
        assertValues(nextData(TABLE, "c", 2).getStruct("after"), inserted);
        final var nullUpdate = nextData(TABLE, "u", 2);
        assertValues(nullUpdate.getStruct("before"), inserted);
        assertValues(nullUpdate.getStruct("after"), nulled);
        final var valueUpdate = nextData(TABLE, "u", 2);
        assertValues(valueUpdate.getStruct("before"), nulled);
        assertValues(valueUpdate.getStruct("after"), updated);
        assertValues(nextData(TABLE, "d", 2).getStruct("before"), updated);
        final var afterDdl = nextData(TABLE, "c", 3).getStruct("after");
        assertValues(afterDdl, altered);
        assertDefault(afterDdl.schema().field("EXTRA").schema(), altered.get("EXTRA"));
        assertValues(nextData(TABLE, "d", 3).getStruct("before"), altered);
        final var late = nextData(LATE_TABLE, "c", 4).getStruct("after");
        assertValues(late, historical);
        historical.forEach((name, expected) -> assertDefault(late.schema().field(name).schema(), expected));
        assertValues(nextData(LATE_TABLE, "d", 4).getStruct("before"), historical);
        stopConnector();

        execute("INSERT INTO " + TABLE + "(id,small) VALUES(5,1E-120)");
        final var recovered = readValues(TABLE, 5);
        startEngine();
        final var resumed = nextData(TABLE, "c", 5).getStruct("after");
        assertValues(resumed, recovered);
        assertThat(resumed.schema()).isEqualTo(afterDdl.schema());
    }

    private void createTable(String table) throws Exception {
        execute("CREATE TABLE " + table + "(id PLS_INTEGER PRIMARY KEY, "
                + "n NUMBER DEFAULT 1.2345678901234567890123456789, fixed NUMBER(38,8) DEFAULT 12.34000000, "
                + "whole NUMBER(38) DEFAULT 12345678901234567890123456789012345678, "
                + "plain FLOAT DEFAULT 1.2345678901234567890123456789, low FLOAT(1) DEFAULT 1E-120, "
                + "small FLOAT(24) DEFAULT 1.25, medium FLOAT(53) DEFAULT 1.23456789012345, "
                + "wide FLOAT(126) DEFAULT 12345678901234567890.123456, real_num REAL DEFAULT 1.234567890123456, "
                + "double_num DOUBLE PRECISION DEFAULT 1.2345678901234567890123456789, "
                + "numeric_fixed NUMERIC(12,2) DEFAULT 12.34, decimal_fixed DECIMAL(12,2) DEFAULT 56.78, "
                + "unbounded_dec DEC DEFAULT 1.2345678901234567890123456789)");
        createdTables.add(table);
    }

    private Map<String, BigDecimal> readValues(String table, int id) throws Exception {
        try (var statement = jdbc.createStatement(); var result = statement.executeQuery("SELECT * FROM " + table + " WHERE id=" + id)) {
            assertThat(result.next()).isTrue();
            final Map<String, BigDecimal> values = new LinkedHashMap<>();
            final var metadata = result.getMetaData();
            for (int index = 2; index <= metadata.getColumnCount(); index++) {
                values.put(metadata.getColumnName(index), result.getBigDecimal(index));
            }
            return values;
        }
    }

    private void assertValues(Struct value, Map<String, BigDecimal> expected) {
        value.validate();
        assertThat(value.schema().fields()).hasSize(expected.size() + 1);
        expected.forEach((name, number) -> assertNumber(value.getWithoutDefault(name), value.schema().field(name).schema(), number));
    }

    private void assertNumber(Object value, Schema schema, BigDecimal expected) {
        if (expected == null) {
            assertThat(value).isNull();
        }
        else if (decimalMode.equals("precise")) {
            if (VariableScaleDecimal.LOGICAL_NAME.equals(schema.name())) {
                assertThat(VariableScaleDecimal.toLogical((Struct) value).getDecimalValue().orElseThrow()).isEqualByComparingTo(expected);
            }
            else {
                assertThat(value).isEqualTo(expected);
            }
        }
        else if (decimalMode.equals("string")) {
            assertThat(schema.type()).isEqualTo(Schema.Type.STRING);
            assertThat(new BigDecimal((String) value)).isEqualByComparingTo(expected);
        }
        else {
            assertThat(schema.type()).isEqualTo(Schema.Type.FLOAT64);
            assertThat(value).isEqualTo(expected.doubleValue());
        }
    }

    private void assertDefault(Schema schema, BigDecimal expected) {
        if (VariableScaleDecimal.LOGICAL_NAME.equals(schema.name())) {
            assertThat(schema.defaultValue()).isNull();
        }
        else {
            assertNumber(schema.defaultValue(), schema, expected);
        }
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

    private Struct nextData(String table, String operation, int id) throws Exception {
        final long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
        while (System.nanoTime() < deadline) {
            assertThat(failure.get()).as("connector failure").isNull();
            if (waitForAvailableRecords(1, TimeUnit.SECONDS)) {
                final var record = consumeRecord();
                if (record != null && record.topic().equals("ora_num." + table)) {
                    assertThat(((Struct) record.key()).getInt32("ID")).isEqualTo(id);
                    final var envelope = (Struct) record.value();
                    envelope.validate();
                    assertThat(envelope.getString("op")).isEqualTo(operation);
                    return envelope;
                }
            }
        }
        throw new AssertionError("No " + operation + " event for " + table + " in 60 seconds");
    }
}
