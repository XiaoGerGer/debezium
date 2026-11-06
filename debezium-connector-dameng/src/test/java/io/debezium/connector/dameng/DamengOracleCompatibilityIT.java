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
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
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

/** Oracle-mode DATE and numeric aliases must agree in snapshots and archived replay. */
@EnabledIfSystemProperty(named = "dameng.compatible.mode", matches = "2")
class DamengOracleCompatibilityIT extends AbstractAsyncEngineConnectorTest {
    private static final String TABLE = "SYSDBA.DBZ_ORA";
    private static final String LATE_TABLE = "SYSDBA.DBZ_ORA_LATE";
    private static final String DATE = "2000-02-29 12:34:56";
    private static final String UPDATED_DATE = "1965-02-03 04:05:06";
    private static final String ANCIENT = "-0001-01-01 12:34:56";
    private static final String DATE_FUNCTION = "TO_DATE('" + DATE + "','YYYY-MM-DD HH24:MI:SS')";

    @TempDir
    Path directory;

    private final AtomicReference<Throwable> failure = new AtomicReference<>();
    private final List<String> createdTables = new ArrayList<>();
    private Connection jdbc;
    private Configuration config;
    private boolean databaseStarted;
    private String precision;

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

    @ParameterizedTest(name = "Oracle snapshot/history/CRUD/recovery time.precision.mode={0}")
    @ValueSource(strings = { "sqlstring", "adaptive" })
    void shouldCaptureOracleDatesAliasesHistoricalDdlAndRecovery(String precision) throws Exception {
        this.precision = precision;
        DamengTestDatabase.start();
        databaseStarted = true;
        jdbc = DriverManager.getConnection(DamengTestDatabase.jdbcUrl(), DamengTestDatabase.user(), DamengTestDatabase.password());
        jdbc.setAutoCommit(true);
        createTable(TABLE);
        insert(TABLE, 1);
        config = Configuration.create()
                .with("topic.prefix", "ora")
                .with("database.hostname", DamengTestDatabase.hostname())
                .with("database.port", DamengTestDatabase.port())
                .with("database.user", DamengTestDatabase.user())
                .with("database.password", DamengTestDatabase.password())
                .with("database.dbname", "DAMENG")
                .with("table.include.list", "SYSDBA\\.DBZ_ORA(_LATE)?")
                .with("schema.history.internal", FileSchemaHistory.class.getName())
                .with("schema.history.internal.file.filename", directory.resolve("history.dat").toString())
                .with("snapshot.mode", "initial")
                .with("time.precision.mode", precision)
                .with("poll.interval.ms", 100)
                .with("include.schema.changes", false)
                .with("tombstones.on.delete", false)
                .build();
        startEngine();
        final var snapshot = nextData(TABLE, "r", 1).getStruct("after");
        assertValues(snapshot);
        assertThat(snapshot.schema().field("D").schema().defaultValue()).isEqualTo(dateValue(DATE));
        assertThat(snapshot.schema().field("MIDNIGHT").schema().defaultValue()).isEqualTo(dateValue("2000-02-29 00:00:00"));
        stopConnector();

        execute("ALTER TABLE " + TABLE + " ADD extra DATE DEFAULT " + DATE_FUNCTION);
        execute("ALTER TABLE " + TABLE + " RENAME COLUMN d TO \"NewDate\"");
        insert(TABLE, 2);
        execute("UPDATE " + TABLE + " SET \"NewDate\"=NULL, ancient=NULL, v=NULL WHERE id=2");
        execute("UPDATE " + TABLE + " SET \"NewDate\"=TO_DATE('" + UPDATED_DATE
                + "','YYYY-MM-DD HH24:MI:SS'), ancient=TIMESTAMP'" + ANCIENT + "', v='汉字' WHERE id=2");
        execute("DELETE FROM " + TABLE + " WHERE id=2");
        createTable(LATE_TABLE);
        insert(LATE_TABLE, 4);
        execute("DELETE FROM " + LATE_TABLE + " WHERE id=4");

        // Both rows are gone before the engine resumes. Only archived redo and
        // historical CREATE/ALTER definitions can reconstruct these records.
        startEngine();
        final var created = nextData(TABLE, "c", 2).getStruct("after");
        assertValues(created);
        assertThat(created.getWithoutDefault("EXTRA")).isEqualTo(dateValue(DATE));
        assertThat(created.schema().field("EXTRA").schema().defaultValue()).isEqualTo(dateValue(DATE));
        final var nulled = nextData(TABLE, "u", 2);
        assertValues(nulled.getStruct("before"));
        assertNulls(nulled.getStruct("after"));
        final var restored = nextData(TABLE, "u", 2);
        assertNulls(restored.getStruct("before"));
        assertValues(restored.getStruct("after"), UPDATED_DATE);
        assertValues(nextData(TABLE, "d", 2).getStruct("before"), UPDATED_DATE);
        final var historical = nextData(LATE_TABLE, "c", 4).getStruct("after");
        assertValues(historical);
        assertThat(historical.schema().field("D").schema()).isEqualTo(snapshot.schema().field("D").schema());
        assertValues(nextData(LATE_TABLE, "d", 4).getStruct("before"));
        stopConnector();

        insert(TABLE, 3);
        startEngine();
        final var recovered = nextData(TABLE, "c", 3).getStruct("after");
        assertValues(recovered);
        assertThat(recovered.schema()).isEqualTo(created.schema());
    }

    private void createTable(String table) throws Exception {
        execute("CREATE TABLE " + table + "(id PLS_INTEGER PRIMARY KEY, d DATE DEFAULT " + DATE_FUNCTION
                + ", midnight DATE DEFAULT DATE'2000-02-29', ancient DATE, v NVARCHAR2(30), "
                + "n NUMBER, fixed NUMBER(12,2), f FLOAT(24), wide FLOAT(126), bf BINARY_FLOAT, bd BINARY_DOUBLE, raw RAW(8))");
        createdTables.add(table);
    }

    private void insert(String table, int id) throws Exception {
        execute("INSERT INTO " + table + "(id,ancient,v,n,fixed,f,wide,bf,bd,raw) VALUES(" + id
                + ",TIMESTAMP'" + ANCIENT + "','汉字',12345678901234567890.123456,12.34,1.25,2.5,3.5,4.5,HEXTORAW('00ABFF'))");
    }

    private void assertValues(Struct value) {
        assertValues(value, DATE);
    }

    private void assertValues(Struct value, String date) {
        value.validate();
        assertThat(value.getWithoutDefault(value.schema().field("D") == null ? "NewDate" : "D")).isEqualTo(dateValue(date));
        assertThat(value.getWithoutDefault("MIDNIGHT")).isEqualTo(dateValue("2000-02-29 00:00:00"));
        assertThat(value.get("ANCIENT")).isEqualTo(dateValue(ANCIENT));
        assertThat(value.getString("V")).isEqualTo("汉字");
        assertThat(VariableScaleDecimal.toLogical(value.getStruct("N")).getDecimalValue())
                .contains(new BigDecimal("12345678901234567890.123456"));
        assertThat(value.get("FIXED")).isEqualTo(new BigDecimal("12.34"));
        if ("1".equals(System.getProperty("dameng.test.ini.NUMBER_MODE", "0"))) {
            assertThat(VariableScaleDecimal.toLogical(value.getStruct("F")).getDecimalValue()).contains(new BigDecimal("1.25"));
            assertThat(VariableScaleDecimal.toLogical(value.getStruct("WIDE")).getDecimalValue()).contains(new BigDecimal("2.5"));
        }
        else {
            assertThat(value.getFloat32("F")).isEqualTo(1.25f);
            assertThat(value.getFloat64("WIDE")).isEqualTo(2.5d);
        }
        assertThat(value.getFloat32("BF")).isEqualTo(3.5f);
        assertThat(value.getFloat64("BD")).isEqualTo(4.5d);
        assertThat(value.getBytes("RAW")).containsExactly(0, (byte) 0xAB, (byte) 0xFF);
        assertThat(value.schema().field("ANCIENT").schema().type()).isEqualTo(precision.equals("sqlstring") ? Schema.Type.STRING : Schema.Type.INT64);
    }

    private void assertNulls(Struct value) {
        value.validate();
        assertThat(value.getWithoutDefault("NewDate")).isNull();
        assertThat(value.getWithoutDefault("ANCIENT")).isNull();
        assertThat(value.getWithoutDefault("V")).isNull();
    }

    private Object dateValue(String value) {
        return precision.equals("sqlstring") ? value : LocalDateTime.parse(value.replace(' ', 'T')).toInstant(ZoneOffset.UTC).toEpochMilli();
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
                final SourceRecord record = consumeRecord();
                if (record != null && record.topic().equals("ora." + table)) {
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
