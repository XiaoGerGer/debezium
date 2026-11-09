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
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

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
    private final Map<String, String> originalParameters = new HashMap<>();
    private Connection jdbc;
    private Configuration config;
    private boolean databaseStarted;
    private boolean decimalFloats;
    private String precision;

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
                try {
                    if (databaseStarted) {
                        DamengTestDatabase.stop();
                    }
                }
                finally {
                    originalParameters.forEach((name, value) -> {
                        if (value == null) {
                            System.clearProperty(name);
                        }
                        else {
                            System.setProperty(name, value);
                        }
                    });
                }
            }
        }
    }

    static Stream<Arguments> parameterCombinations() {
        return IntStream.range(0, 16).mapToObj(value -> Arguments.of(value & 1, (value >> 1) & 1, (value >> 2) & 1, (value >> 3) & 1));
    }

    @ParameterizedTest(name = "Oracle FLOAT_MODE={0}, NUMBER_MODE={1}, ORA_DATE_FMT={2}, BACKSLASH_ESCAPE={3}")
    @MethodSource("parameterCombinations")
    void shouldCaptureOracleDatesAliasesHistoricalDdlAndRecovery(int floatMode, int numberMode, int dateFormat, int backslash) throws Exception {
        assumeTrue(System.getProperty("dameng.hostname") == null, "The parameter matrix owns its disposable databases");
        setParameter("FLOAT_MODE", floatMode);
        setParameter("NUMBER_MODE", numberMode);
        setParameter("ORA_DATE_FMT", dateFormat);
        final int nested = floatMode ^ dateFormat;
        precision = nested == 0 ? "sqlstring" : "adaptive";
        decimalFloats = numberMode == 1;
        DamengTestDatabase.start(backslash, nested);
        databaseStarted = true;
        jdbc = DriverManager.getConnection(DamengTestDatabase.jdbcUrl(), DamengTestDatabase.user(), DamengTestDatabase.password());
        jdbc.setAutoCommit(true);
        createTable(TABLE);
        insert(TABLE, 1);
        final var initialStrings = stringValues(TABLE, 1);
        final var initialDefaults = stringDefaults(TABLE);
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
                .with("include.schema.comments", true)
                .with("tombstones.on.delete", false)
                .build();
        startEngine();
        final var snapshot = nextData(TABLE, "r", 1).getStruct("after");
        assertValues(snapshot);
        assertStrings(snapshot, initialStrings);
        assertDefaults(snapshot, initialDefaults);
        assertThat(snapshot.schema().field("D").schema().defaultValue()).isEqualTo(dateValue(DATE));
        assertThat(snapshot.schema().field("MIDNIGHT").schema().defaultValue()).isEqualTo(dateValue("2000-02-29 00:00:00"));
        stopConnector();

        execute("ALTER TABLE " + TABLE + " ADD extra DATE DEFAULT " + DATE_FUNCTION);
        final var comment = nested == 1 ? "/* outer /* nested */ outer */" : "/* regular */";
        execute("ALTER" + comment + "TABLE " + TABLE + " ADD (added_value VARCHAR(100) DEFAULT Q'[a\\nb]', marker INT)");
        execute("ALTER TABLE " + TABLE + " ALTER COLUMN escaped_value SET DEFAULT E'new\\t\\qb'");
        execute("ALTER TABLE " + TABLE + " RENAME COLUMN d TO \"NewDate\"");
        final var alteredDefaults = stringDefaults(TABLE);
        insert(TABLE, 2);
        final var insertedStrings = stringValues(TABLE, 2);
        execute("UPDATE " + TABLE + " SET \"NewDate\"=NULL, ancient=NULL, v=NULL, escaped_value=NULL WHERE id=2");
        final var nulledStrings = stringValues(TABLE, 2);
        try (var statement = jdbc.prepareStatement("UPDATE " + TABLE + " SET \"NewDate\"=TO_DATE('" + UPDATED_DATE
                + "','YYYY-MM-DD HH24:MI:SS'), ancient=TIMESTAMP'" + ANCIENT + "', v='汉字', escaped_value=? WHERE id=2")) {
            // JDBC-bound strings already contain literal backslashes; redo must retain them.
            statement.setString(1, "literal\\n\\path");
            statement.executeUpdate();
        }
        final var restoredStrings = stringValues(TABLE, 2);
        execute("DELETE FROM " + TABLE + " WHERE id=2");
        createTable(LATE_TABLE);
        insert(LATE_TABLE, 4);
        final var lateStrings = stringValues(LATE_TABLE, 4);
        final var lateDefaults = stringDefaults(LATE_TABLE);
        final var lateComment = columnComment(LATE_TABLE);
        execute("DELETE FROM " + LATE_TABLE + " WHERE id=4");

        // Both rows are gone before the engine resumes. Only archived redo and
        // historical CREATE/ALTER definitions can reconstruct these records.
        startEngine();
        final var created = nextData(TABLE, "c", 2).getStruct("after");
        assertValues(created);
        assertStrings(created, insertedStrings);
        assertDefaults(created, alteredDefaults);
        assertThat(created.getWithoutDefault("EXTRA")).isEqualTo(dateValue(DATE));
        assertThat(created.schema().field("EXTRA").schema().defaultValue()).isEqualTo(dateValue(DATE));
        final var nulled = nextData(TABLE, "u", 2);
        assertValues(nulled.getStruct("before"));
        assertNulls(nulled.getStruct("after"));
        assertStrings(nulled.getStruct("before"), insertedStrings);
        assertStrings(nulled.getStruct("after"), nulledStrings);
        final var restored = nextData(TABLE, "u", 2);
        assertNulls(restored.getStruct("before"));
        assertValues(restored.getStruct("after"), UPDATED_DATE);
        assertStrings(restored.getStruct("before"), nulledStrings);
        assertStrings(restored.getStruct("after"), restoredStrings);
        final var deleted = nextData(TABLE, "d", 2).getStruct("before");
        assertValues(deleted, UPDATED_DATE);
        assertStrings(deleted, restoredStrings);
        final var historical = nextData(LATE_TABLE, "c", 4).getStruct("after");
        assertValues(historical);
        assertStrings(historical, lateStrings);
        assertDefaults(historical, lateDefaults);
        assertThat(historical.schema().field("E_VALUE").schema().doc()).isEqualTo(lateComment);
        assertThat(historical.schema().field("D").schema()).isEqualTo(snapshot.schema().field("D").schema());
        final var lateDeleted = nextData(LATE_TABLE, "d", 4).getStruct("before");
        assertValues(lateDeleted);
        assertStrings(lateDeleted, lateStrings);
        stopConnector();

        insert(TABLE, 3);
        final var recoveredStrings = stringValues(TABLE, 3);
        startEngine();
        final var recovered = nextData(TABLE, "c", 3).getStruct("after");
        assertValues(recovered);
        assertStrings(recovered, recoveredStrings);
        assertDefaults(recovered, alteredDefaults);
        assertThat(recovered.schema()).isEqualTo(created.schema());
    }

    private void createTable(String table) throws Exception {
        execute("CREATE TABLE " + table + "(id PLS_INTEGER PRIMARY KEY, d DATE DEFAULT " + DATE_FUNCTION
                + ", midnight DATE DEFAULT DATE'2000-02-29', ancient DATE, v NVARCHAR2(30), "
                + "n NUMBER, fixed NUMBER(12,2), f FLOAT(24), wide FLOAT(126), bf BINARY_FLOAT, bd BINARY_DOUBLE, raw RAW(8), "
                + "escaped_value VARCHAR(200) DEFAULT 'a\\n\\r\\t\\b\\f\\q\\0\\Z\\%\\_\\\\b',"
                + "q_value VARCHAR(100) DEFAULT Q'[a\\nb]',n_value VARCHAR(100) DEFAULT N'a\\tb',"
                + "e_value VARCHAR(100) DEFAULT E'a\\rb' COMMENT 'a\\tb')");
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
        if (decimalFloats) {
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

    private Map<String, String> stringValues(String table, int id) throws Exception {
        final Map<String, String> values = new LinkedHashMap<>();
        try (var statement = jdbc.createStatement(); var result = statement.executeQuery("SELECT * FROM " + table + " WHERE id=" + id)) {
            assertThat(result.next()).as("stored row %s", id).isTrue();
            for (int index = 1; index <= result.getMetaData().getColumnCount(); index++) {
                final var column = result.getMetaData().getColumnName(index);
                if (column.endsWith("_VALUE")) {
                    values.put(column, result.getString(index));
                }
            }
        }
        assertThat(values.size()).isGreaterThanOrEqualTo(4);
        return values;
    }

    private Map<String, String> stringDefaults(String table) throws Exception {
        final Map<String, String> defaults = new LinkedHashMap<>();
        final var tableName = table.substring(table.indexOf('.') + 1);
        try (var columns = jdbc.getMetaData().getColumns(null, "SYSDBA", null, null)) {
            while (columns.next()) {
                final var column = columns.getString("COLUMN_NAME");
                if (tableName.equals(columns.getString("TABLE_NAME")) && column.endsWith("_VALUE")) {
                    // Compiled metadata defaults and stored row defaults can differ
                    // after a multi-column ADD. Check the schema independently.
                    final var literal = columns.getString("COLUMN_DEF").trim();
                    assertThat(literal).startsWith("'").endsWith("'");
                    defaults.put(column, literal.substring(1, literal.length() - 1).replace("''", "'"));
                }
            }
        }
        assertThat(defaults.size()).isGreaterThanOrEqualTo(4);
        return defaults;
    }

    private String columnComment(String table) throws Exception {
        final var tableName = table.substring(table.indexOf('.') + 1);
        try (var statement = jdbc.createStatement();
                var result = statement.executeQuery("SELECT COMMENTS FROM ALL_COL_COMMENTS "
                        + "WHERE OWNER='SYSDBA' AND TABLE_NAME='" + tableName + "' AND COLUMN_NAME='E_VALUE'")) {
            assertThat(result.next()).isTrue();
            return result.getString(1);
        }
    }

    private void assertStrings(Struct value, Map<String, String> expected) {
        expected.forEach((column, stored) -> assertThat(value.getWithoutDefault(column)).as(column).isEqualTo(stored));
    }

    private void assertDefaults(Struct value, Map<String, String> expected) {
        expected.forEach((column, stored) -> assertThat(value.schema().field(column).schema().defaultValue()).as(column).isEqualTo(stored));
    }

    private void setParameter(String name, int value) {
        final var property = "dameng.test.ini." + name;
        originalParameters.put(property, System.getProperty(property));
        System.setProperty(property, Integer.toString(value));
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
