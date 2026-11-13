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
import java.sql.Types;
import java.time.Duration;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;
import java.util.stream.Stream;

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

/** Native-mode snapshot, archived DDL and CRUD must agree across restarts. */
@EnabledIfSystemProperty(named = "dameng.compatible.mode", matches = "0")
class DamengNativeCompatibilityIT extends AbstractAsyncEngineConnectorTest {
    private static final String TABLE = "SYSDBA.\"Dbz_Native\"";
    private static final String LATE_TABLE = "SYSDBA.\"Dbz_Native_Late\"";
    private static final String DEFAULT = "汉字\\n'a;--/*";
    private static final String LITERAL = "E'汉字\\n''a;--/*'";

    @TempDir
    Path directory;

    private final AtomicReference<Throwable> failure = new AtomicReference<>();
    private Connection jdbc;
    private Configuration config;
    private boolean databaseStarted;
    private boolean insensitive;
    private boolean decimalFloats;
    private final Map<String, String> originalParameters = new HashMap<>();

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

    @ParameterizedTest(name = "Native FLOAT_MODE={0}, NUMBER_MODE={1}, ORA_DATE_FMT={2}, BACKSLASH_ESCAPE={3}")
    @MethodSource("parameterCombinations")
    void shouldCaptureSnapshotHistoricalDdlCrudAndRecovery(int floatMode, int numberMode, int dateFormat, int backslash) throws Exception {
        assumeTrue(System.getProperty("dameng.hostname") == null, "The parameter matrix owns its disposable databases");
        setParameter("FLOAT_MODE", floatMode);
        setParameter("NUMBER_MODE", numberMode);
        setParameter("ORA_DATE_FMT", dateFormat);
        final int nested = floatMode ^ dateFormat;
        final int caseSensitive = numberMode ^ backslash;
        insensitive = caseSensitive == 0;
        decimalFloats = numberMode == 1;
        DamengTestDatabase.start(backslash, nested, caseSensitive);
        databaseStarted = true;
        jdbc = DriverManager.getConnection(DamengTestDatabase.jdbcUrl(), DamengTestDatabase.user(), DamengTestDatabase.password());
        jdbc.setAutoCommit(true);
        createTable(TABLE);
        final var initialDefaults = storedDefaults(TABLE);
        final var initial = insert(TABLE, 1);
        final boolean escaped = backslash == 1 && !"8.1.2.128".equals(jdbc.getMetaData().getDatabaseProductVersion());
        assertThat(initial.get("V")).isEqualTo(escaped ? DEFAULT.replace("\\n", "\n") : DEFAULT);
        config = Configuration.create()
                .with("topic.prefix", "native")
                .with("database.hosts", DamengTestDatabase.hosts())
                .with("database.user", DamengTestDatabase.user())
                .with("database.password", DamengTestDatabase.password())
                .with("database.dbname", "DAMENG")
                .with("table.include.list", "SYSDBA\\.DBZ_NATIVE(_LATE)?")
                .with("schema.history.internal", FileSchemaHistory.class.getName())
                .with("schema.history.internal.file.filename", directory.resolve("history.dat").toString())
                .with("snapshot.mode", "initial").with("snapshot.locking.mode", "shared")
                .with("poll.interval.ms", 100)
                .with("include.schema.changes", false)
                .with("tombstones.on.delete", false)
                .build();
        startEngine();
        final var snapshot = nextData(false, "r", 1).getStruct("after");
        assertValues(snapshot, initial);
        assertThat(snapshot.schema().field("V").schema().defaultValue()).isEqualTo(initial.get("V"));
        assertDefaults(snapshot, initialDefaults);
        stopConnector();

        final var comment = nested == 1 ? "/* outer /* nested */ outer */" : "/* regular */";
        execute("ALTER" + comment + "TABLE " + TABLE + " ADD (extra VARCHAR(120) DEFAULT " + LITERAL
                + ", extra_ts TIMESTAMP(3) WITHOUT TIME ZONE DEFAULT TIMESTAMP'2001-01-01 01:02:03.123')");
        execute("ALTER TABLE " + TABLE + " RENAME COLUMN v TO \"NewValue\"");
        execute("ALTER TABLE " + TABLE + " ALTER COLUMN escaped_value SET DEFAULT E'new\\t\\qb'");
        execute("ALTER TABLE " + TABLE + " MODIFY f_small FLOAT(53)");
        final var alteredDefaults = storedDefaults(TABLE);
        final var inserted = insert(TABLE, 2);
        execute("UPDATE " + TABLE + " SET \"NewValue\"=NULL,d=NULL,ts=NULL,t=NULL,n=NULL,f_plain=NULL,f_small=NULL WHERE \"Id\"=2");
        final var nulled = stored(TABLE, 2);
        try (var statement = jdbc.prepareStatement("UPDATE " + TABLE + " SET \"NewValue\"='changed',d=DATE'1965-02-03',"
                + "ts=TIMESTAMP'1965-02-03 04:05:06.789123',t=TIME'04:05:06.789',n=-12.34,"
                + "f_plain=" + (decimalFloats ? "-9.876543210987654321E120" : "-1.25") + ","
                + "f_small=" + (decimalFloats ? "1E-120" : "1.25") + ",escaped_value=? WHERE \"Id\"=2")) {
            // Bound values already contain literal backslashes; redo must retain them.
            statement.setString(1, "literal\\n\\path");
            statement.executeUpdate();
        }
        final var restored = stored(TABLE, 2);
        execute("DELETE FROM " + TABLE + " WHERE \"Id\"=2");
        createTable(LATE_TABLE);
        final var lateDefaults = storedDefaults(LATE_TABLE);
        final var late = insert(LATE_TABLE, 1);
        execute("DELETE FROM " + LATE_TABLE + " WHERE \"Id\"=1");

        // JDBC expectations are captured before deletion. Replay must reconstruct
        // the historical schema and values from archived CREATE/ALTER and redo.
        startEngine();
        final var created = nextData(false, "c", 2).getStruct("after");
        assertValues(created, inserted);
        assertThat(created.schema().field("EXTRA").schema().defaultValue()).isEqualTo(initial.get("V"));
        assertDefaults(created, alteredDefaults);
        assertImages(nextData(false, "u", 2), inserted, nulled);
        assertImages(nextData(false, "u", 2), nulled, restored);
        assertValues(nextData(false, "d", 2).getStruct("before"), restored);
        final var lateCreated = nextData(true, "c", 1).getStruct("after");
        assertValues(lateCreated, late);
        assertDefaults(lateCreated, lateDefaults);
        assertValues(nextData(true, "d", 1).getStruct("before"), late);
        stopConnector();

        final var afterRestart = insert(TABLE, 3);
        startEngine();
        final var recovered = nextData(false, "c", 3).getStruct("after");
        assertValues(recovered, afterRestart);
        assertDefaults(recovered, alteredDefaults);
        assertThat(recovered.schema()).isEqualTo(created.schema());
    }

    private void createTable(String table) throws Exception {
        execute("CREATE TABLE " + table + "(\"Id\" INT PRIMARY KEY, v VARCHAR(120) DEFAULT " + LITERAL
                + ",d DATE,ts TIMESTAMP(6) WITHOUT TIME ZONE,t TIME(3) WITHOUT TIME ZONE,n NUMERIC(12,2),"
                + "f_plain FLOAT DEFAULT " + (decimalFloats ? "1.2345678901234567890123456789" : "1.25")
                + ",f_small FLOAT(24) DEFAULT " + (decimalFloats ? "1.234567890123456789" : "1.25")
                + ",f_real REAL DEFAULT " + (decimalFloats ? "1E-120" : "1.25")
                + ",f_double DOUBLE PRECISION DEFAULT " + (decimalFloats ? "-9.876543210987654321E120" : "1.25")
                + ",binary_float BINARY_FLOAT DEFAULT 1.25,binary_double DOUBLE DEFAULT 1.25,"
                + "escaped_value VARCHAR(200) DEFAULT 'a\\n\\r\\t\\b\\f\\q\\0\\Z\\%\\_\\\\b',"
                + "q_value VARCHAR(100) DEFAULT Q'[a\\nb]',n_value VARCHAR(100) DEFAULT N'a\\tb',e_value VARCHAR(100) DEFAULT E'a\\rb')");
    }

    private Map<String, Object> insert(String table, int id) throws Exception {
        execute("INSERT INTO " + table + "(\"Id\",d,ts,t,n) VALUES(" + id
                + ",DATE'2000-02-29',TIMESTAMP'2000-02-29 12:34:56.123456',TIME'12:34:56.123',12.34)");
        return stored(table, id);
    }

    private Map<String, Object> stored(String table, int id) throws Exception {
        final Map<String, Object> values = new LinkedHashMap<>();
        try (var statement = jdbc.createStatement(); var result = statement.executeQuery("SELECT * FROM " + table + " WHERE \"Id\"=" + id)) {
            assertThat(result.next()).as("stored row %s", id).isTrue();
            final var metadata = result.getMetaData();
            for (int index = 1; index <= metadata.getColumnCount(); index++) {
                final Object value;
                if (result.getObject(index) == null) {
                    value = null;
                }
                else {
                    value = switch (metadata.getColumnType(index)) {
                        case Types.INTEGER -> result.getInt(index);
                        case Types.DECIMAL, Types.NUMERIC -> result.getBigDecimal(index);
                        case Types.FLOAT -> result.getObject(index) instanceof BigDecimal ? result.getBigDecimal(index) : result.getDouble(index);
                        case Types.REAL -> result.getFloat(index);
                        case Types.DOUBLE -> result.getDouble(index);
                        default -> result.getString(index);
                    };
                }
                values.put(name(metadata.getColumnName(index)), value);
            }
        }
        return values;
    }

    private void assertImages(Struct envelope, Map<String, Object> before, Map<String, Object> after) {
        assertValues(envelope.getStruct("before"), before);
        assertValues(envelope.getStruct("after"), after);
    }

    private void assertValues(Struct value, Map<String, Object> expected) {
        value.validate();
        expected.forEach((column, stored) -> {
            final var actual = value.getWithoutDefault(column);
            if (actual instanceof Struct decimal && VariableScaleDecimal.LOGICAL_NAME.equals(decimal.schema().name())) {
                assertThat(VariableScaleDecimal.toLogical(decimal).getDecimalValue().orElseThrow()).as(column).isEqualByComparingTo((BigDecimal) stored);
            }
            else {
                assertThat(actual).as(column).isEqualTo(stored);
            }
        });
    }

    private Map<String, String> storedDefaults(String table) throws Exception {
        final Map<String, String> defaults = new LinkedHashMap<>();
        final var tableName = name(table.substring(table.indexOf('.') + 1).replace("\"", ""));
        try (var columns = jdbc.getMetaData().getColumns(null, "SYSDBA", null, null)) {
            while (columns.next()) {
                final var column = name(columns.getString("COLUMN_NAME"));
                if (name(columns.getString("TABLE_NAME")).equals(tableName) && List.of("ESCAPED_VALUE", "Q_VALUE", "N_VALUE", "E_VALUE").contains(column)) {
                    // These fixture defaults are compiled string literals. Compare
                    // schema defaults with metadata independently of stored row values.
                    final var literal = columns.getString("COLUMN_DEF").trim();
                    assertThat(literal).startsWith("'").endsWith("'");
                    defaults.put(column, literal.substring(1, literal.length() - 1).replace("''", "'"));
                }
            }
        }
        assertThat(defaults).hasSize(4);
        return defaults;
    }

    private void assertDefaults(Struct value, Map<String, String> expected) {
        for (var column : List.of("ESCAPED_VALUE", "Q_VALUE", "N_VALUE", "E_VALUE")) {
            assertThat(value.schema().field(column).schema().defaultValue()).as(column).isEqualTo(expected.get(column));
        }
    }

    private void setParameter(String name, int value) {
        final var property = "dameng.test.ini." + name;
        originalParameters.put(property, System.getProperty(property));
        System.setProperty(property, Integer.toString(value));
    }

    private String name(String value) {
        return insensitive ? value.toUpperCase(Locale.ROOT) : value;
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

    private Struct nextData(boolean late, String operation, int id) throws Exception {
        final var topic = "native.SYSDBA." + name(late ? "Dbz_Native_Late" : "Dbz_Native");
        final long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
        while (System.nanoTime() < deadline) {
            assertThat(failure.get()).as("connector failure").isNull();
            if (waitForAvailableRecords(1, TimeUnit.SECONDS)) {
                final SourceRecord record = consumeRecord();
                if (record != null && record.topic().equals(topic)) {
                    assertThat(((Struct) record.key()).getInt32(name("Id"))).isEqualTo(id);
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
