/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Types;
import java.time.Duration;
import java.util.ArrayList;
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

/** MySQL-mode snapshots and archived backtick DDL must agree across restarts. */
@EnabledIfSystemProperty(named = "dameng.compatible.mode", matches = "4")
class DamengMySqlCompatibilityIT extends AbstractAsyncEngineConnectorTest {
    private static final String TABLE = "SYSDBA.`Dbz_My`";
    private static final String LATE_TABLE = "SYSDBA.`Dbz_My_Late`";
    private static final String DEFAULT = "汉字\\n'a;--/*";
    private static final String LITERAL = "'汉字\\n''a;--/*'";

    @TempDir
    Path directory;

    private final AtomicReference<Throwable> failure = new AtomicReference<>();
    private final List<String> createdTables = new ArrayList<>();
    private Connection jdbc;
    private Configuration config;
    private boolean databaseStarted;
    private boolean insensitive;
    private boolean backslashEscape;
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

    @ParameterizedTest(name = "MySQL FLOAT_MODE={0}, NUMBER_MODE={1}, ORA_DATE_FMT={2}, BACKSLASH_ESCAPE={3}")
    @MethodSource("parameterCombinations")
    void shouldCaptureBackticksAliasesAutoIncrementHistoricalDdlCrudAndRecovery(int floatMode, int numberMode, int dateFormat, int backslash) throws Exception {
        assumeTrue(System.getProperty("dameng.hostname") == null, "The parameter matrix owns its disposable databases");
        setParameter("FLOAT_MODE", floatMode);
        setParameter("NUMBER_MODE", numberMode);
        setParameter("ORA_DATE_FMT", dateFormat);
        setParameter("MY_STRICT_TABLES", Integer.getInteger("dameng.test.ini.MY_STRICT_TABLES", (floatMode + numberMode + dateFormat + backslash) % 2));
        final int nested = floatMode ^ dateFormat;
        final int caseSensitive = numberMode ^ backslash;
        insensitive = caseSensitive == 0;
        backslashEscape = backslash == 1;
        decimalFloats = numberMode == 1;
        DamengTestDatabase.start(backslash, nested, caseSensitive);
        databaseStarted = true;
        jdbc = DriverManager.getConnection(DamengTestDatabase.jdbcUrl(), DamengTestDatabase.user(), DamengTestDatabase.password());
        jdbc.setAutoCommit(true);
        createTable(TABLE);
        final var initial = insert(TABLE, 1);
        assertThat(initial.get(name("v"))).isEqualTo(backslashEscape ? DEFAULT.replace("\\n", "\n") : DEFAULT);
        config = Configuration.create()
                .with("topic.prefix", "mysql")
                .with("database.hosts", DamengTestDatabase.hosts())
                .with("database.user", DamengTestDatabase.user())
                .with("database.password", DamengTestDatabase.password())
                .with("database.dbname", "DAMENG")
                .with("table.include.list", "SYSDBA\\.DBZ_MY(_LATE)?")
                .with("schema.history.internal", FileSchemaHistory.class.getName())
                .with("schema.history.internal.file.filename", directory.resolve("history.dat").toString())
                .with("snapshot.mode", "initial").with("snapshot.locking.mode", "shared")
                .with("poll.interval.ms", 100)
                .with("include.schema.changes", false)
                .with("tombstones.on.delete", false)
                .build();
        startEngine();
        final var snapshot = nextData(false, "r", 1).getStruct("after");
        assertValues(snapshot, initial, true);
        assertThat(snapshot.schema().field(name("v")).schema().defaultValue()).isEqualTo(initial.get(name("v")));
        assertDefaults(snapshot, initial);
        stopConnector();

        final var comment = nested == 1 ? "/* outer /* nested */ outer */" : "/* regular */";
        execute("ALTER" + comment + "TABLE " + TABLE + " MODIFY `v` VARCHAR(200)");
        execute("ALTER TABLE " + TABLE + " RENAME COLUMN `v` TO `New``Value`");
        execute("ALTER TABLE " + TABLE + " ADD `Extra` VARCHAR(120) DEFAULT " + LITERAL);
        execute("ALTER TABLE " + TABLE + " ALTER COLUMN escaped_value SET DEFAULT 'new\\t\\qb'");
        execute("ALTER TABLE " + TABLE + " MODIFY f_small FLOAT(53)");
        final var inserted = insert(TABLE, 2);
        execute("UPDATE " + TABLE + " SET `New``Value`=NULL,dt=NULL,n=NULL,raw_value=NULL WHERE `Id`=2");
        final var nulled = stored(TABLE, 2);
        // Bind literal backslash+n so redo must not apply SQL escape rules a second time.
        try (var statement = jdbc.prepareStatement("UPDATE " + TABLE + " SET `New``Value`='changed',dt=TIMESTAMP'1965-02-03 04:05:06.789',"
                + "n=-12345678901234567890.654321,raw_value=0xFF800100,escaped_value=? WHERE `Id`=2")) {
            statement.setString(1, "literal\\n\\path");
            statement.executeUpdate();
        }
        final var restored = stored(TABLE, 2);
        execute("DELETE FROM " + TABLE + " WHERE `Id`=2");
        createTable(LATE_TABLE);
        final var late = insert(LATE_TABLE, 1);
        execute("DELETE FROM " + LATE_TABLE + " WHERE `Id`=1");

        // Expected images were read independently through JDBC before both rows
        // were deleted. Replay must use historical CREATE/ALTER and archived redo.
        startEngine();
        final var created = nextData(false, "c", 2).getStruct("after");
        assertValues(created, inserted, true);
        assertThat(created.schema().field(name("Extra")).schema().defaultValue()).isEqualTo(initial.get(name("v")));
        assertDefaults(created, inserted);
        assertImages(nextData(false, "u", 2), inserted, nulled);
        assertImages(nextData(false, "u", 2), nulled, restored);
        assertValues(nextData(false, "d", 2).getStruct("before"), restored, false);
        assertValues(nextData(true, "c", 1).getStruct("after"), late, true);
        assertValues(nextData(true, "d", 1).getStruct("before"), late, false);
        stopConnector();

        final var afterRestart = insert(TABLE, 3);
        startEngine();
        final var recovered = nextData(false, "c", 3).getStruct("after");
        assertValues(recovered, afterRestart, true);
        assertThat(recovered.schema()).isEqualTo(created.schema());
    }

    private void createTable(String table) throws Exception {
        execute("CREATE TABLE " + table + "(`Id` INT AUTO_INCREMENT PRIMARY KEY, `v` VARCHAR(120) DEFAULT " + LITERAL
                + ",d DATE,dt DATETIME(3) DEFAULT TIMESTAMP'2000-02-29 12:34:56.123',n DECIMAL(38,6),raw_value VARBINARY(8),"
                + "mt MEDIUMTEXT,lt LONGTEXT,mb MEDIUMBLOB,lb LONGBLOB,"
                + "f_plain FLOAT DEFAULT " + (decimalFloats ? "1.2345678901234567890123456789" : "1.25")
                + ", f_small FLOAT(24) DEFAULT " + (decimalFloats ? "1.234567890123456789" : "1.25")
                + ", f_real REAL DEFAULT " + (decimalFloats ? "1E-120" : "1.25")
                + ", f_double DOUBLE PRECISION DEFAULT " + (decimalFloats ? "-9.876543210987654321E120" : "1.25") + ","
                + "escaped_value VARCHAR(200) DEFAULT 'a\\n\\r\\t\\b\\f\\q\\0\\Z\\%\\_\\\\b',"
                + "q_value VARCHAR(100) DEFAULT Q'[a\\nb]', n_value VARCHAR(100) DEFAULT N'a\\tb', e_value VARCHAR(100) DEFAULT E'a\\rb')");
        createdTables.add(table);
    }

    private Map<String, Object> insert(String table, int expectedId) throws Exception {
        execute("INSERT INTO " + table + "(d,n,raw_value,mt,lt,mb,lb) VALUES(DATE'2000-02-29',12345678901234567890.123456,"
                + "0x000180FF,'medium 汉字','long 汉字',0x00FF,0x01FE)");
        return stored(table, expectedId);
    }

    private Map<String, Object> stored(String table, int id) throws Exception {
        final Map<String, Object> values = new LinkedHashMap<>();
        try (var statement = jdbc.createStatement(); var result = statement.executeQuery("SELECT * FROM " + table + " WHERE `Id`=" + id)) {
            assertThat(result.next()).as("database-generated identity %s", id).isTrue();
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
                        case Types.BLOB -> {
                            final var blob = result.getBlob(index);
                            try {
                                yield blob.getBytes(1, (int) blob.length());
                            }
                            finally {
                                blob.free();
                            }
                        }
                        case Types.VARBINARY -> result.getBytes(index);
                        default -> result.getString(index);
                    };
                }
                values.put(name(metadata.getColumnName(index)), value);
            }
        }
        return values;
    }

    private void assertImages(Struct envelope, Map<String, Object> before, Map<String, Object> after) {
        assertValues(envelope.getStruct("before"), before, false);
        assertValues(envelope.getStruct("after"), after, false);
    }

    private void assertValues(Struct value, Map<String, Object> expected, boolean includeLobs) {
        value.validate();
        expected.forEach((column, stored) -> {
            // DM omits unchanged LOBs from UPDATE and DELETE row images.
            if (includeLobs || !List.of("MT", "LT", "MB", "LB").contains(column)) {
                final var actual = value.getWithoutDefault(column);
                if (stored instanceof byte[] bytes) {
                    assertThat(bytes(actual)).as(column).containsExactly(bytes);
                }
                else if (actual instanceof Struct decimal && VariableScaleDecimal.LOGICAL_NAME.equals(decimal.schema().name())) {
                    assertThat(VariableScaleDecimal.toLogical(decimal).getDecimalValue().orElseThrow()).as(column).isEqualByComparingTo((BigDecimal) stored);
                }
                else {
                    assertThat(actual).as(column).isEqualTo(stored);
                }
            }
            else {
                final var unavailable = config.getString("unavailable.value.placeholder", "__debezium_unavailable_value");
                if (column.equals("MT") || column.equals("LT")) {
                    assertThat(value.getWithoutDefault(column)).as(column).isEqualTo(unavailable);
                }
                else {
                    assertThat(bytes(value.getWithoutDefault(column))).as(column).containsExactly(unavailable.getBytes(StandardCharsets.UTF_8));
                }
            }
        });
    }

    private void assertDefaults(Struct value, Map<String, Object> expected) {
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

    private static byte[] bytes(Object value) {
        if (value instanceof byte[] bytes) {
            return bytes;
        }
        final var buffer = ((ByteBuffer) value).duplicate();
        final byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return bytes;
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
        final var topic = "mysql.SYSDBA." + name(late ? "Dbz_My_Late" : "Dbz_My");
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
        throw new AssertionError("No " + operation + " event for " + topic + " in 60 seconds");
    }
}
