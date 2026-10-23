/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.SQLException;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.stream.Stream;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.errors.DataException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import io.debezium.connector.dameng.logminer.parser.LogMinerDmlParser.SqlExpression;
import io.debezium.relational.Column;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.relational.ValueConverter;

class DamengValueConvertersTest {
    private final DamengValueConverters converters = new DamengValueConverters(
            new DamengConnectorConfig(DamengConnectorConfigTest.configuration().build()));

    @ParameterizedTest
    @MethodSource("numbers")
    void shouldConvertRedoNumbers(int type, String value, Object expected) {
        assertThat(convert(column(type), value)).isEqualTo(expected);
    }

    static Stream<Arguments> numbers() {
        return Stream.of(
                Arguments.of(Types.TINYINT, "127", (byte) 127),
                Arguments.of(Types.SMALLINT, "-32768", (short) -32768),
                Arguments.of(Types.INTEGER, "2147483647", Integer.MAX_VALUE),
                Arguments.of(Types.BIGINT, "9223372036854775807", Long.MAX_VALUE),
                Arguments.of(Types.DECIMAL, "1234567890123456.123456", new BigDecimal("1234567890123456.123456")),
                Arguments.of(Types.DOUBLE, "-1.25e10", -1.25e10));
    }

    @Test
    void shouldPreserveNumericAndFunctionLookingText() {
        assertThat(convert(column(Types.VARCHAR), "000123.4500")).isEqualTo("000123.4500");
        assertThat(convert(column(Types.VARCHAR), "null")).isEqualTo("null");
        assertThat(convert(column(Types.VARCHAR), "HEXTORAW('ABCD')")).isEqualTo("HEXTORAW('ABCD')");
    }

    @Test
    void shouldConvertSqlNull() {
        assertThat(convert(column(Types.VARCHAR), null)).isNull();
        assertThat(convert(column(Types.DECIMAL), null)).isNull();
    }

    @Test
    void shouldDecodeHexBinary() {
        final var converted = (ByteBuffer) convert(column(Types.VARBINARY), new SqlExpression("HEXTORAW('00ffAB')"));
        final var bytes = new byte[converted.remaining()];
        converted.duplicate().get(bytes);
        assertThat(bytes).containsExactly((byte) 0, (byte) 0xff, (byte) 0xab);
    }

    @Test
    void shouldDecodeDate() {
        assertThat(convert(column(Types.DATE), new SqlExpression("TO_DATE('2026-09-24','YYYY-MM-DD')")))
                .isEqualTo((int) LocalDate.of(2026, 9, 24).toEpochDay());
    }

    @ParameterizedTest
    @ValueSource(ints = { Types.BINARY, Types.VARBINARY, Types.LONGVARBINARY })
    void shouldDecodeQuotedEmptyBinaryOnlyForBinaryColumns(int jdbcType) {
        assertThat((ByteBuffer) convert(column(jdbcType), "0x")).isEqualTo(ByteBuffer.wrap(new byte[0]));
        assertThat(convert(column(Types.VARCHAR), "0x")).isEqualTo("0x");
    }

    @ParameterizedTest
    @ValueSource(ints = { Types.REAL, Types.FLOAT, Types.DOUBLE })
    void shouldPreserveFloatingPointNaNAndNegativeZeroInConnectValues(int jdbcType) {
        final var negativeZero = (Number) convert(column(jdbcType), "-0.000000");
        assertThat(Double.doubleToRawLongBits(negativeZero.doubleValue())).isEqualTo(Double.doubleToRawLongBits(-0.0));
        final var nan = (Number) convert(column(jdbcType), new SqlExpression("nan"));
        assertThat(Double.isNaN(nan.doubleValue())).isTrue();
        assertThatThrownBy(() -> convert(column(Types.DECIMAL), new SqlExpression("nan")))
                .isInstanceOf(DataException.class).hasMessageContaining("floating-point column");
    }

    @ParameterizedTest
    @ValueSource(strings = { "1-01-01", "99-12-31", "999-02-28", "-1-01-01", "-4712-01-01", "1582-10-10", "9999-12-31" })
    void shouldParseSignedAndUnpaddedYearsFromRedo(String value) {
        final var parts = value.substring(value.startsWith("-") ? 1 : 0).split("-");
        final int year = Integer.parseInt(parts[0]) * (value.startsWith("-") ? -1 : 1);
        final var date = LocalDate.of(year, Integer.parseInt(parts[1]), Integer.parseInt(parts[2]));
        assertThat(convert(column(Types.DATE), new SqlExpression("DATE'" + value + "'"))).isEqualTo((int) date.toEpochDay());
        final var timestamp = date.atTime(12, 34, 56, 123456000);
        assertThat(convert(column(Types.TIMESTAMP), new SqlExpression("TIMESTAMP'" + value + " 12:34:56.123456'")))
                .isEqualTo(timestamp.toEpochSecond(ZoneOffset.UTC) * 1_000_000 + 123456);
        assertThat(converters.convertLogMinerValue(new SqlExpression("TIMESTAMP'" + value + " 12:34:56.123456 +08:00'"),
                column(Types.TIMESTAMP_WITH_TIMEZONE), input -> input))
                .isEqualTo(timestamp.atOffset(ZoneOffset.ofHours(8)));
    }

    @Test
    void shouldRejectInvalidCalendarDatesInsteadOfNormalizingThem() {
        assertThatThrownBy(() -> convert(column(Types.DATE), new SqlExpression("DATE'1-02-29'")))
                .isInstanceOf(DataException.class).hasMessageContaining("Invalid LogMiner value");
    }

    @Test
    void shouldRejectLocalTimezoneSchemaInsteadOfSilentlyPublishingAReaderDependentTimestamp() {
        final var column = Column.editor().name("LOCAL_TS").jdbcType(Types.TIMESTAMP).type("TIMESTAMP WITH LOCAL TIME ZONE")
                .length(36).scale(6).optional(true).create();
        assertThatThrownBy(() -> converters.schemaBuilder(column)).isInstanceOf(DataException.class)
                .hasMessageContaining("LOCAL_TS", "source session time zone", "column.exclude.list");
    }

    @Test
    void shouldDecodeMicrosecondTimestamp() {
        final var instant = LocalDateTime.of(2026, 9, 24, 12, 34, 56, 123456000).toInstant(ZoneOffset.UTC);
        assertThat(convert(column(Types.TIMESTAMP), new SqlExpression("TO_TIMESTAMP('2026-09-24 12:34:56.123456','YYYY-MM-DD HH24:MI:SS.FF6')")))
                .isEqualTo(instant.getEpochSecond() * 1_000_000 + 123456);
    }

    @Test
    void shouldDecodeUnicodeExpression() {
        assertThat(convert(column(Types.VARCHAR), new SqlExpression("UNISTR('\\4F60\\597D')"))).isEqualTo("你好");
    }

    @Test
    void shouldDecodeDirectTemporalRedoLiterals() {
        assertThat(converters.convertLogMinerValue("2026-09-24", column(Types.DATE), value -> value))
                .isEqualTo(LocalDate.of(2026, 9, 24));
        assertThat(converters.convertLogMinerValue("2026-09-24 12:34:56.123456", column(Types.TIMESTAMP), value -> value))
                .isEqualTo(LocalDateTime.of(2026, 9, 24, 12, 34, 56, 123456000));
        assertThat(converters.convertLogMinerValue("12:34:56", column(Types.TIME), value -> value))
                .isEqualTo(LocalTime.of(12, 34, 56));
    }

    @Test
    void shouldDecodeBooleanRedoLiteralsAndRejectAmbiguousValues() {
        final var booleanColumn = column(Types.BOOLEAN);
        assertThat(converters.convertLogMinerValue("0", booleanColumn, value -> value)).isEqualTo(false);
        assertThat(converters.convertLogMinerValue("false", booleanColumn, value -> value)).isEqualTo(false);
        assertThat(converters.convertLogMinerValue("1", booleanColumn, value -> value)).isEqualTo(true);
        assertThat(converters.convertLogMinerValue("TRUE", booleanColumn, value -> value)).isEqualTo(true);
        assertThatThrownBy(() -> converters.convertLogMinerValue("yes", booleanColumn, value -> value))
                .isInstanceOf(DataException.class).hasMessageContaining("Invalid Dameng boolean literal");
    }

    @Test
    void shouldDecodeStandardTemporalAndEmptyLobExpressions() {
        assertThat(converters.convertLogMinerValue(new SqlExpression("TO_NUMBER('42')"), column(Types.INTEGER), value -> value))
                .isEqualTo(42);
        assertThat(converters.convertLogMinerValue(new SqlExpression("DATE '2026-09-24'"), column(Types.DATE), value -> value))
                .isEqualTo(LocalDate.of(2026, 9, 24));
        assertThat(converters.convertLogMinerValue(new SqlExpression("TIMESTAMP '2026-09-24 12:34:56'"), column(Types.TIMESTAMP), value -> value))
                .isEqualTo(LocalDateTime.of(2026, 9, 24, 12, 34, 56));
        assertThat((byte[]) converters.convertLogMinerValue(new SqlExpression("EMPTY_BLOB()"), column(Types.BLOB), value -> value))
                .isEmpty();
        assertThat(converters.convertLogMinerValue(new SqlExpression("EMPTY_CLOB()"), column(Types.CLOB), value -> value))
                .isEqualTo("");
    }

    @Test
    void shouldRejectMalformedRedoExpressionsWithoutSkippingTheColumn() {
        assertThatThrownBy(() -> converters.convertLogMinerValue(new SqlExpression("HEXTORAW('not-hex')"), column(Types.BLOB), value -> value))
                .isInstanceOf(DataException.class).hasMessageContaining("Invalid LogMiner value expression");
        assertThatThrownBy(() -> converters.convertLogMinerValue(new SqlExpression("SYSDATE"), column(Types.DATE), value -> value))
                .isInstanceOf(DataException.class).hasMessageContaining("Unsupported LogMiner value expression");
    }

    @Test
    void shouldSurfaceLobReadFailuresAsConversionErrors() throws SQLException {
        final var clob = mock(Clob.class);
        when(clob.length()).thenThrow(new SQLException("LOB locator expired"));
        assertThatThrownBy(() -> converters.convertLogMinerValue(clob, column(Types.CLOB), value -> value))
                .isInstanceOf(DataException.class).hasMessageContaining("Could not read Dameng LOB column")
                .hasCauseInstanceOf(SQLException.class);

        final var blob = mock(Blob.class);
        when(blob.length()).thenThrow(new SQLException("LOB locator expired"));
        assertThatThrownBy(() -> converters.convertLogMinerValue(blob, column(Types.BLOB), value -> value))
                .isInstanceOf(DataException.class).hasMessageContaining("Could not read Dameng LOB column")
                .hasCauseInstanceOf(SQLException.class);
    }

    @ParameterizedTest
    @MethodSource("fixedCharacters")
    void shouldPadFixedCharactersUsingTheHistoricalLengthSemantics(String charset, String expression, String value, String expected) {
        final var column = Column.editor().name("VALUE").jdbcType(Types.CHAR).type("CHAR", expression)
                .length(8).charsetName(charset).optional(true).create();
        final var converter = converter(column);
        assertThat(converter.convert(value)).isEqualTo(expected);
        assertThat(converter.convert(expected)).isEqualTo(expected);
        assertThat(converter.convert(null)).isNull();
        assertThat(converter.convert(value)).isEqualTo(expected);
    }

    static Stream<Arguments> fixedCharacters() {
        return Stream.of(
                Arguments.of("GB18030", "CHAR(8 BYTE)", "达梦", "达梦    "),
                Arguments.of("UTF-8", "CHAR(8 BYTE)", "达梦", "达梦  "),
                Arguments.of("UTF-8", "CHAR(8 CHAR)", "达梦", "达梦      "),
                Arguments.of("UTF-8", "CHAR(8 CHAR)", "😀", "😀       "),
                Arguments.of("UTF-8", "CHAR(8 BYTE)", "abc ", "abc     "),
                Arguments.of("EUC-KR", "CHAR(8 BYTE)", "가", "가      "));
    }

    @Test
    void shouldUseDatabaseCharsetForColumnsAddedByDdl() {
        final var converter = new DamengValueConverters(new DamengConnectorConfig(DamengConnectorConfigTest.configuration().build()), Charset.forName("GB18030"));
        final var column = Column.editor().name("VALUE").jdbcType(Types.CHAR).type("CHAR", "CHAR(8)").length(8).optional(true).create();
        final var field = new Field(column.name(), 0, converter.schemaBuilder(column).optional().build());
        assertThat(converter.converter(column, field).convert("达梦")).isEqualTo("达梦    ");
        assertThat(converter.converter(column, field).convert(new SqlExpression("UNISTR('\\8FBE\\68A6')"))).isEqualTo("达梦    ");
    }

    @Test
    void shouldPersistResolvedLengthSemanticsInHistoricalDdl() {
        final var tables = new Tables();
        final var legacy = new DamengDdlParser(Charset.forName("GB18030"), false, false, true);
        legacy.parse("CREATE TABLE APP.T (A CHAR(8 CHAR), B NCHAR(8))", tables, "APP");
        final var id = new TableId(null, "APP", "T");
        assertThat(convert(tables.forTable(id).columnWithName("A"), "达梦")).isEqualTo("达梦    ");
        assertThat(convert(tables.forTable(id).columnWithName("B"), "达梦")).isEqualTo("达梦    ");
        final var modern = new DamengDdlParser(Charset.forName("UTF-8"), true, false, true);
        modern.parse("CREATE TABLE APP.U (A CHAR(8 CHAR), B NCHAR(8), C CHAR(8 BYTE))", tables, "APP");
        final var current = tables.forTable(new TableId(null, "APP", "U"));
        assertThat(convert(current.columnWithName("A"), "达梦")).isEqualTo("达梦      ");
        assertThat(convert(current.columnWithName("B"), "达梦")).isEqualTo("达梦      ");
        assertThat(convert(current.columnWithName("C"), "达梦")).isEqualTo("达梦  ");
    }

    @ParameterizedTest
    @ValueSource(ints = { Types.CHAR, Types.NCHAR })
    void shouldKeepReusablePaddingBoundToEachHistoricalColumn(int jdbcType) {
        final var original = Column.editor().name("VALUE").jdbcType(jdbcType).type("CHAR", "CHAR(8 BYTE)")
                .length(8).charsetName("GB18030").optional(true).create();
        final var originalConverter = converter(original);
        final var characterConverter = converter(original.edit().type("CHAR", "CHAR(8 CHAR)").charsetName("UTF-8").create());
        final var extendedConverter = converter(original.edit().length(12).type("CHAR", "CHAR(12 BYTE)").create());
        final var otherTableConverter = converter(original.edit().charsetName("UTF-8").create());
        for (int i = 0; i < 3; i++) {
            assertThat(originalConverter.convert("达梦")).isEqualTo("达梦    ");
            assertThat(characterConverter.convert("达梦")).isEqualTo("达梦      ");
            assertThat(extendedConverter.convert(new SqlExpression("UNISTR('\\8FBE\\68A6')"))).isEqualTo("达梦        ");
            assertThat(otherTableConverter.convert("达梦")).isEqualTo("达梦  ");
            assertThat(characterConverter.convert("😀")).isEqualTo("😀       ");
            assertThat(originalConverter.convert(null)).isNull();
            assertThat(originalConverter.convert("longer than eight")).isEqualTo("longer than eight");
        }
    }

    private Object convert(Column column, Object value) {
        return converter(column).convert(value);
    }

    private ValueConverter converter(Column column) {
        final var field = new Field(column.name(), 0, converters.schemaBuilder(column).optional().build());
        return converters.converter(column, field);
    }

    private Column column(int jdbcType) {
        final int length = jdbcType == Types.TIMESTAMP || jdbcType == Types.TIMESTAMP_WITH_TIMEZONE ? 26 : 38;
        return Column.editor().name("VALUE").jdbcType(jdbcType).type("VALUE_TYPE").length(length).scale(6).optional(true).create();
    }
}
