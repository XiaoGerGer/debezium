/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Types;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.source.SourceRecord;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import io.debezium.config.Configuration;
import io.debezium.data.VariableScaleDecimal;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;
import io.debezium.storage.file.history.FileSchemaHistory;

/** Native DM type mappings through the complete snapshot and streaming pipelines. */
class DamengDatatypesIT extends AbstractAsyncEngineConnectorTest {
    @TempDir
    Path temporaryDirectory;

    private Connection connection;
    private String table;
    private boolean tableCreated;
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
        table = "DBZ_TYPES_" + UUID.randomUUID().toString().replace("-", "").substring(0, 12).toUpperCase();
        config = Configuration.create()
                .with("topic.prefix", "dameng")
                .with("database.hostname", DamengTestDatabase.hostname())
                .with("database.port", DamengTestDatabase.port())
                .with("database.user", DamengTestDatabase.user())
                .with("database.password", DamengTestDatabase.password())
                .with("database.dbname", System.getProperty("dameng.database", "DAMENG"))
                .with("table.include.list", DamengTestDatabase.schema() + "\\." + table)
                .with("schema.history.internal", FileSchemaHistory.class.getName())
                .with("schema.history.internal.file.filename", temporaryDirectory.resolve("history.dat").toString())
                .with("snapshot.mode", "initial")
                .with("poll.interval.ms", 100)
                .with("include.schema.changes", false)
                .build();
    }

    @AfterEach
    void after() throws Exception {
        stopConnector();
        if (connection != null) {
            try {
                if (tableCreated) {
                    execute("DROP TABLE " + tableName());
                }
            }
            finally {
                connection.close();
            }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "precise", "string", "double" })
    void shouldPreserveNativeNumericTypes(String decimalMode) throws Exception {
        config = config.edit().with("decimal.handling.mode", decimalMode).build();
        createTable("INT_VALUE INT, SMALL_VALUE SMALLINT, TINY_VALUE TINYINT, BIG_VALUE BIGINT, "
                + "NUMBER_VALUE NUMBER, DECIMAL_VALUE DECIMAL(30,6), REAL_VALUE REAL, FLOAT_VALUE FLOAT(24), "
                + "DOUBLE_VALUE DOUBLE, BIT_VALUE BIT");
        final var values = "-2147483648,-32768,-128,9223372036854775807,12345.6789,12345678901234567890.123456,1.25,-0.5,2.25,1";
        final var snapshot = snapshotAndStream(values);
        assertThat(snapshot.get("INT_VALUE")).isEqualTo(Integer.MIN_VALUE);
        assertThat(snapshot.get("SMALL_VALUE")).isEqualTo(Short.MIN_VALUE);
        assertThat(snapshot.get("TINY_VALUE")).isEqualTo(Byte.MIN_VALUE);
        assertThat(snapshot.get("BIG_VALUE")).isEqualTo(Long.MAX_VALUE);
        assertThat(snapshot.get("REAL_VALUE")).isEqualTo(1.25f);
        assertThat(snapshot.get("FLOAT_VALUE")).isEqualTo(-0.5f);
        assertThat(snapshot.get("DOUBLE_VALUE")).isEqualTo(2.25d);
        assertThat(snapshot.get("BIT_VALUE")).isEqualTo(true);
        if (decimalMode.equals("precise")) {
            assertThat(VariableScaleDecimal.toLogical(snapshot.getStruct("NUMBER_VALUE")).getDecimalValue())
                    .contains(new BigDecimal("12345.6789"));
            assertThat(snapshot.get("DECIMAL_VALUE")).isEqualTo(new BigDecimal("12345678901234567890.123456"));
        }
        else if (decimalMode.equals("string")) {
            assertThat(snapshot.getString("NUMBER_VALUE")).isEqualTo("12345.6789");
            assertThat(snapshot.getString("DECIMAL_VALUE")).isEqualTo("12345678901234567890.123456");
        }
        else {
            assertThat(snapshot.get("NUMBER_VALUE")).isEqualTo(12345.6789d);
        }
    }

    @ParameterizedTest(name = "{0}: {1}")
    @MethodSource("floatingPointValues")
    void shouldPreserveStoredFloatingPointBitsInSnapshotAndStreaming(String type, String literal) throws Exception {
        createTable("VALUE " + type);
        insertFloatingValue(1, type, literal);
        final var expected = storedValues(1);
        startEngine();
        final var snapshot = nextRecord();
        assertThat(((Struct) snapshot.value()).getString("op")).isEqualTo("r");
        assertStoredValues(((Struct) snapshot.value()).getStruct("after"), expected);
        insertFloatingValue(2, type, literal);
        final var streamed = nextRecord();
        assertThat(((Struct) streamed.value()).getString("op")).isEqualTo("c");
        // The independent JDBC baseline is the value actually stored by DM,
        // not the input literal or another invocation of the connector converter.
        assertStoredValues(((Struct) streamed.value()).getStruct("after"), storedValues(2));
    }

    static Stream<Arguments> floatingPointValues() {
        final var ordinary = Stream.of("REAL", "FLOAT(24)", "FLOAT", "FLOAT(53)", "DOUBLE")
                .flatMap(type -> Stream.of("1.23456789012345", "-1.23456789012345", "0.000000123456789", "-1.23456789012345E20", "0", "-0.0")
                        .map(value -> Arguments.of(type, value)));
        return Stream.concat(ordinary, Stream.of(
                Arguments.of("DOUBLE", Double.toString(Double.MAX_VALUE)),
                Arguments.of("DOUBLE", Double.toString(Double.MIN_NORMAL)),
                Arguments.of("DOUBLE", Double.toString(Double.MIN_VALUE)),
                Arguments.of("DOUBLE", Double.toString(Math.nextUp(1.0d))),
                Arguments.of("DOUBLE", Double.toString(Math.nextDown(1.0d))),
                // DM documents REAL's upper bound as 3.4E38, below Float.MAX_VALUE.
                Arguments.of("REAL", Float.toString(3.4e38f)),
                Arguments.of("REAL", Float.toString(Float.MIN_NORMAL)),
                Arguments.of("REAL", Float.toString(Float.MIN_VALUE))));
    }

    private void insertFloatingValue(int id, String type, String value) throws Exception {
        try (var statement = connection.prepareStatement("INSERT INTO " + tableName() + " VALUES(?,?)")) {
            statement.setInt(1, id);
            if (type.equals("REAL") || type.equals("FLOAT(24)")) {
                statement.setFloat(2, Float.parseFloat(value));
            }
            else {
                statement.setDouble(2, Double.parseDouble(value));
            }
            assertThat(statement.executeUpdate()).isEqualTo(1);
        }
    }

    @ParameterizedTest(name = "{0}: {1}")
    @MethodSource("decimalBoundaries")
    void shouldPreserveStoredDecimalBoundariesAndUpdateBeforeImages(String type, String literal) throws Exception {
        createTable("VALUE " + type);
        execute("INSERT INTO " + tableName() + " VALUES(1," + literal + ")");
        final var expectedSnapshot = storedValues(1);
        startEngine();
        assertStoredValues(((Struct) nextRecord().value()).getStruct("after"), expectedSnapshot);
        execute("INSERT INTO " + tableName() + " VALUES(2," + literal + ")");
        final var before = storedValues(2);
        assertImages(nextRecord(), "c", null, before);
        execute("UPDATE " + tableName() + " SET VALUE=-VALUE WHERE ID=2");
        final var after = storedValues(2);
        assertImages(nextRecord(), "u", before, after);
        execute("DELETE FROM " + tableName() + " WHERE ID=2");
        assertImages(nextRecord(), "d", after, null);
    }

    static Stream<Arguments> decimalBoundaries() {
        return Stream.of(
                Arguments.of("DECIMAL(38,0)", "99999999999999999999999999999999999999"),
                Arguments.of("DECIMAL(38,18)", "99999999999999999999.999999999999999999"),
                Arguments.of("DECIMAL(38,18)", "0.000000000000000001"),
                Arguments.of("DECIMAL(12,6)", "123.000000"),
                Arguments.of("DECIMAL(12,6)", "123.1234567"),
                Arguments.of("NUMBER", "1.234567890123456789E-30"),
                Arguments.of("NUMBER", "12345678901234567890123456789012345678"),
                Arguments.of("NUMERIC(12,0)", "0"));
    }

    @Test
    void shouldPreserveNativeCrudImagesNullTransitionsAndOfflineReplay() throws Exception {
        config = config.edit().with("tombstones.on.delete", false).build();
        createTable("INT_VALUE INT, SMALL_VALUE SMALLINT, TINY_VALUE TINYINT, BIG_VALUE BIGINT, NUMBER_VALUE NUMBER, "
                + "DECIMAL_VALUE DECIMAL(30,6), REAL_VALUE REAL, FLOAT_VALUE FLOAT(24), DOUBLE_VALUE DOUBLE, BIT_VALUE BIT, "
                + "DATE_VALUE DATE, TIME_VALUE TIME(6), STAMP_VALUE TIMESTAMP(6), ZONE_VALUE TIMESTAMP(6) WITH TIME ZONE, "
                + "ZONE_TIME TIME(0) WITH TIME ZONE, CHAR_VALUE CHAR(8), NCHAR_VALUE NCHAR(8), TEXT_VALUE VARCHAR(100), "
                + "NATIONAL_VALUE NVARCHAR(100), BINARY_VALUE BINARY(8), RAW_VALUE RAW(8), VARBINARY_VALUE VARBINARY(8)");
        final var original = "-2147483648,-32768,-128,-9223372036854775808,12345.6789,12345678901234567890.123456,1.25,-0.5,2.25,0,"
                + "DATE'1965-02-03',TIME'00:00:00.000001',TIMESTAMP'1965-02-03 23:59:59.123456',"
                + "TIMESTAMP'2026-09-24 00:00:00.123456 +08:00',TIME'00:00:00 -05:30',"
                + "'abc','达梦','O''Brien, null','中文',0x000180FF,0x000180FF,0x000180FF";
        final var changed = "2147483647,32767,127,9223372036854775807,-0.000123,-12345678901234567890.654321,-2.5,0.125,-4.75,1,"
                + "DATE'2024-02-29',TIME'23:59:59.999999',TIMESTAMP'2024-02-29 00:00:00.000001',"
                + "TIMESTAMP'2026-09-24 23:59:59.654321 -05:30',TIME'23:59:59 +08:00',"
                + "'xy ','测试','line one' || CHR(10) || 'line two','空格 ',0xFF800100,0xFF800100,0xFF800100";
        execute("INSERT INTO " + tableName() + " VALUES(1," + original + ")");
        final var originalSnapshot = storedValues(1);
        startEngine();
        assertStoredValues(((Struct) nextRecord().value()).getStruct("after"), originalSnapshot);

        execute("INSERT INTO " + tableName() + " VALUES(2," + original + ")");
        final var insertedValues = storedValues(2);
        assertStoredValues(((Struct) nextRecord().value()).getStruct("after"), insertedValues);
        updateValues(changed);
        final var changedValues = storedValues(2);
        assertImages(nextRecord(), "u", insertedValues, changedValues);

        execute("UPDATE " + tableName() + " SET TEXT_VALUE='only this column changes' WHERE ID=2");
        final var partialValues = storedValues(2);
        assertImages(nextRecord(), "u", changedValues, partialValues);
        execute("UPDATE " + tableName() + " SET STAMP_VALUE=TIMESTAMP'1969-12-31 23:59:59.999999' WHERE ID=2");
        final var selectiveValues = storedValues(2);
        assertImages(nextRecord(), "u", partialValues, selectiveValues);

        updateValues("NULL,".repeat(changedValues.size() - 2) + "NULL");
        final var nullValues = storedValues(2);
        final var checkpoint = nextRecord();
        assertImages(checkpoint, "u", selectiveValues, nullValues);
        stopConnector();
        storeOffsets(config, Map.of(checkpoint.sourcePartition(), checkpoint.sourceOffset()));
        consumedLines.clear();
        updateValues(original);
        final var restoredValues = storedValues(2);
        execute("DELETE FROM " + tableName() + " WHERE ID=2");
        startEngine();
        assertImages(nextRecord(), "u", nullValues, restoredValues);
        assertImages(nextRecord(), "d", restoredValues, null);
    }

    private void updateValues(String literals) throws Exception {
        // Split this fixture's comma-separated values outside SQL string literals.
        // Build assignments from the JDBC column order; SQL expressions containing
        // commas are deliberately absent from these fixed fixture values.
        final var assignments = new StringBuilder();
        final var tokens = literals.split(",(?=(?:[^']*'[^']*')*[^']*$)", -1);
        try (var statement = connection.createStatement(); var result = statement.executeQuery("SELECT * FROM " + tableName() + " WHERE ID=2")) {
            final var metadata = result.getMetaData();
            assertThat(tokens).hasSize(metadata.getColumnCount() - 1);
            for (int index = 2; index <= metadata.getColumnCount(); index++) {
                if (index > 2) {
                    assignments.append(',');
                }
                assignments.append(metadata.getColumnName(index)).append('=').append(tokens[index - 2]);
            }
        }
        execute("UPDATE " + tableName() + " SET " + assignments + " WHERE ID=2");
    }

    private Map<String, Object> storedValues(int id) throws Exception {
        final Map<String, Object> values = new LinkedHashMap<>();
        try (var statement = connection.createStatement(); var result = statement.executeQuery("SELECT * FROM " + tableName() + " WHERE ID=" + id)) {
            assertThat(result.next()).isTrue();
            final var metadata = result.getMetaData();
            for (int index = 1; index <= metadata.getColumnCount(); index++) {
                final Object value;
                if (result.getObject(index) == null) {
                    value = null;
                }
                else {
                    value = switch (metadata.getColumnType(index)) {
                        case Types.TINYINT -> result.getByte(index);
                        case Types.SMALLINT -> result.getShort(index);
                        case Types.INTEGER -> result.getInt(index);
                        case Types.BIGINT -> result.getLong(index);
                        case Types.NUMERIC, Types.DECIMAL -> result.getBigDecimal(index);
                        case Types.REAL -> result.getFloat(index);
                        case Types.FLOAT, Types.DOUBLE -> result.getDouble(index);
                        case Types.BOOLEAN, Types.BIT -> result.getBoolean(index);
                        case Types.DATE -> (int) result.getDate(index).toLocalDate().toEpochDay();
                        case Types.TIME -> LocalTime.parse(result.getString(index)).toNanoOfDay() / 1_000;
                        case Types.TIMESTAMP -> {
                            final var time = result.getTimestamp(index).toLocalDateTime();
                            yield time.toEpochSecond(ZoneOffset.UTC) * 1_000_000 + time.getNano() / 1_000;
                        }
                        case Types.TIMESTAMP_WITH_TIMEZONE -> OffsetDateTime.parse(result.getString(index)
                                .replaceFirst(" ", "T").replace(" +", "+").replace(" -", "-"))
                                .withOffsetSameInstant(ZoneOffset.UTC).format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
                        case Types.TIME_WITH_TIMEZONE -> OffsetTime.parse(result.getString(index).replace(" ", ""))
                                .withOffsetSameInstant(ZoneOffset.UTC).format(DateTimeFormatter.ISO_OFFSET_TIME);
                        case Types.BINARY, Types.VARBINARY, Types.LONGVARBINARY -> result.getBytes(index);
                        default -> result.getString(index);
                    };
                }
                values.put(metadata.getColumnName(index), value);
            }
            assertThat(result.next()).isFalse();
        }
        return values;
    }

    private static void assertImages(SourceRecord record, String operation, Map<String, Object> before, Map<String, Object> after) {
        final var envelope = (Struct) record.value();
        assertThat(envelope.getString("op")).isEqualTo(operation);
        assertStoredValues(envelope.getStruct("before"), before);
        assertStoredValues(envelope.getStruct("after"), after);
    }

    private static void assertStoredValues(Struct row, Map<String, Object> expected) {
        assertStoredColumns(row, expected, "Connect row");
        if (row != null) {
            final var converter = new JsonConverter();
            converter.configure(Map.of("schemas.enable", true), false);
            final var encoded = converter.fromConnectData("dameng.types", row.schema(), row);
            final var decoded = converter.toConnectData("dameng.types", encoded);
            assertThat(decoded.schema()).as("native row schema survives JSON serialization").isEqualTo(row.schema());
            assertStoredColumns((Struct) decoded.value(), expected, "JSON-decoded row");
        }
    }

    private static void assertStoredColumns(Struct row, Map<String, Object> expected, String stage) {
        if (expected == null) {
            assertThat(row).isNull();
            return;
        }
        assertThat(row).isNotNull();
        assertThat(row.schema().fields()).hasSize(expected.size());
        expected.forEach((name, value) -> {
            final var actual = row.get(name);
            if (value instanceof byte[] bytes) {
                assertThat(bytes(row, name)).as(name).containsExactly(bytes);
            }
            else if (value instanceof Float number) {
                assertThat(actual).as(name + " Java type").isInstanceOf(Float.class);
                assertThat(Float.floatToRawIntBits((Float) actual)).as("%s: %s stored bits: JDBC=%s, event=%s", stage, name, number, actual)
                        .isEqualTo(Float.floatToRawIntBits(number));
            }
            else if (value instanceof Double number) {
                assertThat(actual).as(name + " Java type").isInstanceOf(Double.class);
                assertThat(Double.doubleToRawLongBits((Double) actual)).as("%s: %s stored bits: JDBC=%s, event=%s", stage, name, number, actual)
                        .isEqualTo(Double.doubleToRawLongBits(number));
            }
            else if (value instanceof BigDecimal decimal) {
                final var converted = actual instanceof Struct struct
                        ? VariableScaleDecimal.toLogical(struct).getDecimalValue().orElseThrow()
                        : (BigDecimal) actual;
                assertThat(converted).as(name).isEqualByComparingTo(decimal);
                if (!(actual instanceof Struct)) {
                    assertThat(converted.scale()).as(name + " declared decimal scale").isEqualTo(decimal.scale());
                }
            }
            else {
                assertThat(actual).as(name).isEqualTo(value);
            }
        });
    }

    @Test
    void shouldPreserveNativeDateAndTimePrecisions() throws Exception {
        createTable("DATE_VALUE DATE, TIME_ZERO TIME(0), TIME_MILLI TIME(3), TIME_MICRO TIME(6), "
                + "STAMP_ZERO TIMESTAMP(0), STAMP_MILLI TIMESTAMP(3), STAMP_MICRO TIMESTAMP(6), "
                + "ZONE_POS TIMESTAMP(6) WITH TIME ZONE, ZONE_NEG TIMESTAMP(6) WITH TIME ZONE, "
                + "ZONE_TIME TIME(0) WITH TIME ZONE, ZONE_TIME_NEG TIME(0) WITH TIME ZONE");
        final var snapshot = snapshotAndStream("DATE'1965-02-03',TIME'12:34:56',TIME'12:34:56.123',TIME'12:34:56.123456',"
                + "TIMESTAMP'2026-09-24 12:34:56',TIMESTAMP'2026-09-24 12:34:56.123',TIMESTAMP'2026-09-24 12:34:56.123456',"
                + "TIMESTAMP'2026-09-24 12:34:56.123456 +08:00',TIMESTAMP'2026-09-24 12:34:56.123456 -05:30',"
                + "TIME'12:34:56 +08:00',TIME'12:34:56 -05:30'");

        assertThat(snapshot.get("DATE_VALUE")).isEqualTo((int) LocalDate.of(1965, 2, 3).toEpochDay());
        assertThat(snapshot.get("TIME_ZERO")).isEqualTo((int) (LocalTime.of(12, 34, 56).toNanoOfDay() / 1_000_000));
        assertThat(snapshot.get("TIME_MILLI")).isEqualTo((int) (LocalTime.of(12, 34, 56, 123_000_000).toNanoOfDay() / 1_000_000));
        assertThat(snapshot.get("TIME_MICRO")).isEqualTo(LocalTime.of(12, 34, 56, 123_456_000).toNanoOfDay() / 1_000);
        assertThat(snapshot.get("STAMP_ZERO")).isEqualTo(LocalDateTime.of(2026, 9, 24, 12, 34, 56).toInstant(ZoneOffset.UTC).toEpochMilli());
        assertThat(snapshot.get("STAMP_MILLI")).isEqualTo(LocalDateTime.of(2026, 9, 24, 12, 34, 56, 123_000_000).toInstant(ZoneOffset.UTC).toEpochMilli());
        assertThat(snapshot.get("STAMP_MICRO")).isEqualTo(LocalDateTime.of(2026, 9, 24, 12, 34, 56).toEpochSecond(ZoneOffset.UTC) * 1_000_000 + 123_456);
        assertThat(snapshot.getString("ZONE_POS")).isEqualTo("2026-09-24T04:34:56.123456Z");
        assertThat(snapshot.getString("ZONE_NEG")).isEqualTo("2026-09-24T18:04:56.123456Z");
        assertThat(snapshot.getString("ZONE_TIME")).isEqualTo("04:34:56Z");
        assertThat(snapshot.getString("ZONE_TIME_NEG")).isEqualTo("18:04:56Z");
        assertThat(snapshot.schema().field("TIME_MICRO").schema().name()).isEqualTo("io.debezium.time.MicroTime");
        assertThat(snapshot.schema().field("STAMP_MICRO").schema().name()).isEqualTo("io.debezium.time.MicroTimestamp");
    }

    @Test
    void shouldPreserveCharacterPaddingUnicodeAndBinaryBytes() throws Exception {
        createTable("CHAR_VALUE CHAR(8), NCHAR_VALUE NCHAR(8), UNICODE_CHAR CHAR(8), VARCHAR_VALUE VARCHAR(80), NVARCHAR_VALUE NVARCHAR(80), "
                + "BINARY_VALUE BINARY(8), RAW_VALUE RAW(8), VARBINARY_VALUE VARBINARY(8)");
        final var snapshot = snapshotAndStream("'abc','DM','达梦','达梦 O''Brien, null','Unicode 中文',0x000180FF,0x000180FF,0x000180FF");
        assertThat(snapshot.getString("CHAR_VALUE")).isEqualTo("abc     ");
        assertThat(snapshot.getString("NCHAR_VALUE")).isEqualTo("DM      ");
        assertThat(snapshot.getString("UNICODE_CHAR")).startsWith("达梦");
        try (var statement = connection.createStatement(); var result = statement.executeQuery("SELECT UNICODE_CHAR FROM " + tableName() + " WHERE ID=1")) {
            assertThat(result.next()).isTrue();
            assertThat(snapshot.getString("UNICODE_CHAR")).isEqualTo(result.getString(1));
        }
        assertThat(snapshot.getString("VARCHAR_VALUE")).isEqualTo("达梦 O'Brien, null");
        assertThat(snapshot.getString("NVARCHAR_VALUE")).isEqualTo("Unicode 中文");
        assertThat(bytes(snapshot, "BINARY_VALUE")).containsExactly(0, 1, (byte) 0x80, (byte) 0xff, 0, 0, 0, 0);
        assertThat(bytes(snapshot, "RAW_VALUE")).containsExactly(0, 1, (byte) 0x80, (byte) 0xff);
        assertThat(bytes(snapshot, "VARBINARY_VALUE")).containsExactly(0, 1, (byte) 0x80, (byte) 0xff);
    }

    @Test
    void shouldPreserveNullsAcrossNativeTypes() throws Exception {
        createTable("NUMBER_VALUE NUMBER, DECIMAL_VALUE DECIMAL(12,3), TINY_VALUE TINYINT, BIT_VALUE BIT, "
                + "DATE_VALUE DATE, TIME_VALUE TIME(6), STAMP_VALUE TIMESTAMP(6), ZONE_VALUE TIMESTAMP(6) WITH TIME ZONE, "
                + "TEXT_VALUE VARCHAR(20), BINARY_VALUE VARBINARY(20)");
        final var snapshot = snapshotAndStream("NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL");
        for (var field : snapshot.schema().fields()) {
            if (!field.name().equals("ID")) {
                assertThat(snapshot.get(field)).as(field.name()).isNull();
            }
        }
    }

    @Test
    void shouldApplyHistoricalCharacterSemanticsForColumnsAddedWhileStreaming() throws Exception {
        createTable("TEXT_VALUE VARCHAR(20)");
        execute("INSERT INTO " + tableName() + " VALUES(1,'snapshot')");
        startEngine();
        nextRecord();
        execute("ALTER TABLE " + tableName() + " ADD (FIXED_BYTES CHAR(8 BYTE), FIXED_CHARS CHAR(8 CHAR), FIXED_NATIONAL NCHAR(8))");
        execute("INSERT INTO " + tableName() + " VALUES(2,'streamed','达梦','达梦','达梦')");
        final var values = ((Struct) nextRecord().value()).getStruct("after");
        try (var statement = connection.createStatement();
                var result = statement.executeQuery(
                        "SELECT FIXED_BYTES,FIXED_CHARS,FIXED_NATIONAL FROM " + tableName() + " WHERE ID=2")) {
            assertThat(result.next()).isTrue();
            assertThat(values.getString("FIXED_BYTES")).isEqualTo(result.getString(1));
            assertThat(values.getString("FIXED_CHARS")).isEqualTo(result.getString(2));
            assertThat(values.getString("FIXED_NATIONAL")).isEqualTo(result.getString(3));
        }
    }

    @Test
    void shouldReportUnsupportedNativeIntervalColumns() throws Exception {
        createTable("YEAR_MONTH INTERVAL YEAR(3) TO MONTH");
        execute("INSERT INTO " + tableName() + " VALUES(1,INTERVAL '012-03' YEAR(3) TO MONTH)");
        startEngine();
        Awaitility.await("unsupported interval type is reported").atMost(Duration.ofSeconds(30))
                .until(() -> failure.get() != null);
        Throwable cause = failure.get();
        while (cause.getCause() != null) {
            cause = cause.getCause();
        }
        assertThat(cause.getMessage()).contains("Unsupported Dameng column type", "YEAR_MONTH", "column.exclude.list");
    }

    @Test
    void shouldAllowExcludingUnsupportedIntervalColumns() throws Exception {
        createTable("YEAR_MONTH INTERVAL YEAR(3) TO MONTH, DAY_SECOND INTERVAL DAY(3) TO SECOND(6), TEXT_VALUE VARCHAR(30)");
        config = config.edit().with("column.exclude.list", DamengTestDatabase.schema() + "\\." + table + "\\.(YEAR_MONTH|DAY_SECOND)").build();
        final var snapshot = snapshotAndStream("INTERVAL '012-03' YEAR(3) TO MONTH,INTERVAL '002 03:04:05.123456' DAY(3) TO SECOND(6),'kept column'");
        assertThat(snapshot.schema().field("YEAR_MONTH")).isNull();
        assertThat(snapshot.schema().field("DAY_SECOND")).isNull();
        assertThat(snapshot.getString("TEXT_VALUE")).isEqualTo("kept column");
    }

    private Struct snapshotAndStream(String values) throws Exception {
        execute("INSERT INTO " + tableName() + " VALUES(1," + values + ")");
        startEngine();
        final var snapshotRecord = nextRecord();
        assertThat(((Struct) snapshotRecord.value()).getString("op")).isEqualTo("r");
        final var snapshot = ((Struct) snapshotRecord.value()).getStruct("after");
        execute("INSERT INTO " + tableName() + " VALUES(2," + values + ")");
        final var streamed = (Struct) nextRecord().value();
        assertThat(streamed.getString("op")).isEqualTo("c");
        final var after = streamed.getStruct("after");
        for (var field : snapshot.schema().fields()) {
            if (!field.name().equals("ID")) {
                assertThat(after.schema().field(field.name()).schema()).as(field.name() + " schema").isEqualTo(field.schema());
                assertThat(after.get(field.name())).as(field.name() + " streamed value").isEqualTo(snapshot.get(field));
            }
        }
        return snapshot;
    }

    private void createTable(String columns) throws Exception {
        execute("CREATE TABLE " + tableName() + " (ID INT PRIMARY KEY," + columns + ")");
        tableCreated = true;
    }

    private void startEngine() {
        failure.set(null);
        start(DamengConnector.class, config, (success, message, error) -> {
            if (!success) {
                failure.set(error != null ? error : new IllegalStateException(message));
            }
        }, null);
    }

    private SourceRecord nextRecord() throws Exception {
        final long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
        while (System.nanoTime() < deadline) {
            if (failure.get() != null) {
                throw new AssertionError("Dameng connector failed", failure.get());
            }
            if (waitForAvailableRecords(1, TimeUnit.SECONDS)) {
                final var record = consumeRecord();
                if (record != null && record.value() instanceof Struct value && value.schema().field("op") != null) {
                    value.validate();
                    return record;
                }
            }
        }
        throw new AssertionError("No Dameng data event received within 60 seconds");
    }

    private void execute(String sql) throws Exception {
        try (var statement = connection.createStatement()) {
            statement.execute(sql);
        }
    }

    private String tableName() {
        return "\"" + DamengTestDatabase.schema().replace("\"", "\"\"") + "\".\"" + table + "\"";
    }

    private static byte[] bytes(Struct value, String field) {
        final var data = value.get(field);
        if (data instanceof byte[] array) {
            return array;
        }
        final var buffer = ((ByteBuffer) data).duplicate();
        final byte[] result = new byte[buffer.remaining()];
        buffer.get(result);
        return result;
    }
}
