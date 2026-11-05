/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.Types;
import java.util.HexFormat;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import io.debezium.connector.dameng.antlr.DamengDdlParser;
import io.debezium.relational.Column;
import io.debezium.relational.Tables;

class DamengDefaultValueConverterTest {
    private final DamengDefaultValueConverter defaults = new DamengDefaultValueConverter(
            new DamengValueConverters(new DamengConnectorConfig(DamengTestConfiguration.configuration().build())));

    @Test
    void shouldDecodeQuotedCharacterDefaultsWithoutEvaluatingThemAsSql() {
        final var column = column(Types.VARCHAR);

        assertThat(defaults.parseDefaultValue(column, "'O''Brien'")).contains("O'Brien");
        assertThat(defaults.parseDefaultValue(column, "N'达梦'")).contains("达梦");
        assertThat(defaults.parseDefaultValue(column, "n'达梦'")).contains("达梦");
        assertThat(defaults.parseDefaultValue(column, "q'[it's;--/*]'")).contains("it's;--/*");
        assertThat(defaults.parseDefaultValue(column, "Q'!a''b!'")).contains("a''b");
        assertThat(defaults.parseDefaultValue(column, "'CURRENT_TIMESTAMP'")).contains("CURRENT_TIMESTAMP");
    }

    @ParameterizedTest
    @MethodSource("unicodeStringDefaults")
    void shouldPreserveUnicodeAndBackslashesInStringDefaults(String expression, String value) {
        assertThat(defaults.parseDefaultValue(column(Types.VARCHAR), expression)).contains(value);
    }

    static Stream<Arguments> unicodeStringDefaults() {
        return Stream.of(
                Arguments.of("'汉😀字\\n\\tmp'", "汉😀字\\n\\tmp"),
                Arguments.of("N'😀 O''Brien C:\\tmp'", "😀 O'Brien C:\\tmp"),
                Arguments.of("q'界😀 it's C:\\tmp界'", "😀 it's C:\\tmp"),
                Arguments.of("q'aA'😀a'", "A'😀"),
                Arguments.of("q'[]'", ""));
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "'literal' /* ignored trailing comment */",
            "N'literal' -- ignored trailing comment",
            "q'界literal界' // ignored trailing comment"
    })
    void shouldDecodeASingleStringLiteralFollowedByComments(String expression) {
        assertThat(defaults.parseDefaultValue(column(Types.VARCHAR), expression)).contains("literal");
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "'x' /* keep following operator */ || 'y'",
            "N'x' -- keep following operator\n || LOWER('Y')",
            "q'[x]' /* keep following function */ || CONCAT('y', 'z')",
            "CONCAT('x', 'y')",
            "REPLACE(q'[x]', 'x', 'y')"
    })
    void shouldNotDecodeCommentSeparatedConcatenationsOrFunctionsAsConstants(String expression) {
        assertThat(defaults.parseDefaultValue(column(Types.VARCHAR), expression)).isEmpty();
    }

    @Test
    void shouldConvertNumericConstantsUsingTheColumnMapping() {
        assertThat(defaults.parseDefaultValue(column(Types.INTEGER), " -42 ")).contains(-42);
        assertThat(defaults.parseDefaultValue(column(Types.INTEGER), "+7")).contains(7);
    }

    @ParameterizedTest
    @ValueSource(ints = { Types.BINARY, Types.VARBINARY, Types.LONGVARBINARY, Types.BLOB })
    void shouldDecodeEveryByteOfHexBinaryDefaults(int jdbcType) {
        final var bytes = new byte[256];
        for (int index = 0; index < bytes.length; index++) {
            bytes[index] = (byte) index;
        }
        final var expression = "0x" + HexFormat.of().formatHex(bytes);
        final var column = column(jdbcType).edit().length(bytes.length).create();

        assertThat(defaults.parseDefaultValue(column, expression)).contains(ByteBuffer.wrap(bytes));
    }

    @ParameterizedTest
    @ValueSource(strings = { "0x007f80aBFF", "0X007F80Abff", "  0x007F80ABFF  ", "0x007f80abff /*😀*/" })
    void shouldPreserveLeadingZerosAndUnsignedBytesInHexBinaryDefaults(String expression) {
        assertThat(defaults.parseDefaultValue(column(Types.VARBINARY), expression))
                .contains(ByteBuffer.wrap(new byte[]{ 0, 0x7f, (byte) 0x80, (byte) 0xab, (byte) 0xff }));
    }

    @ParameterizedTest
    @MethodSource("oddHexBinaryDefaults")
    void shouldSupplyAZeroHighNibbleForOddHexBinaryDefaults(String expression, byte[] expected) {
        assertThat(defaults.parseDefaultValue(column(Types.VARBINARY), expression)).contains(ByteBuffer.wrap(expected));
    }

    static Stream<Arguments> oddHexBinaryDefaults() {
        return Stream.of(
                Arguments.of("0xF", new byte[]{ 0x0f }),
                Arguments.of("0x123", new byte[]{ 0x01, 0x23 }),
                Arguments.of("0xFFFEE", new byte[]{ 0x0f, (byte) 0xff, (byte) 0xee }),
                Arguments.of("0X0", new byte[]{ 0 }),
                Arguments.of("0x0000f", new byte[]{ 0, 0, 0x0f }),
                Arguments.of("0x080ff", new byte[]{ 0, (byte) 0x80, (byte) 0xff }));
    }

    @ParameterizedTest
    @MethodSource("fixedHexBinaryDefaults")
    void shouldRightPadFixedLengthBinaryDefaults(String expression, byte[] expected) {
        final var column = column(Types.BINARY).edit().type("BINARY").length(4).create();

        assertThat(defaults.parseDefaultValue(column, expression)).contains(ByteBuffer.wrap(expected));
    }

    static Stream<Arguments> fixedHexBinaryDefaults() {
        return Stream.of(
                Arguments.of("0xF", new byte[]{ 0x0f, 0, 0, 0 }),
                Arguments.of("0X123", new byte[]{ 0x01, 0x23, 0, 0 }),
                Arguments.of("0x00ff", new byte[]{ 0, (byte) 0xff, 0, 0 }),
                Arguments.of("0x007f80ff", new byte[]{ 0, 0x7f, (byte) 0x80, (byte) 0xff }));
    }

    @ParameterizedTest
    @ValueSource(strings = { "VARBINARY", "RAW" })
    void shouldKeepVariableLengthBinaryDefaultsUnpadded(String type) {
        final var column = column(Types.VARBINARY).edit().type(type).length(4).create();

        assertThat(defaults.parseDefaultValue(column, "0xF")).contains(ByteBuffer.wrap(new byte[]{ 0x0f }));
        assertThat(defaults.parseDefaultValue(column, "0x123")).contains(ByteBuffer.wrap(new byte[]{ 0x01, 0x23 }));
    }

    @ParameterizedTest
    @MethodSource("fixedBinaryHandlingModes")
    void shouldPadFixedBinaryDefaultsBeforeApplyingConfiguredBinaryHandling(String mode, String expected) {
        final var converters = new DamengValueConverters(new DamengConnectorConfig(
                DamengTestConfiguration.configuration().with("binary.handling.mode", mode).build()));
        final var converter = new DamengDefaultValueConverter(converters);
        final var column = column(Types.BINARY).edit().type("BINARY").length(4).create();

        assertThat(converter.parseDefaultValue(column, "0xF")).contains(expected);
    }

    static Stream<Arguments> fixedBinaryHandlingModes() {
        return Stream.of(
                Arguments.of("base64", "DwAAAA=="),
                Arguments.of("base64-url-safe", "DwAAAA=="),
                Arguments.of("hex", "0f000000"));
    }

    @ParameterizedTest
    @MethodSource("binaryHandlingModes")
    void shouldApplyConfiguredBinaryHandlingToHexBinaryDefaults(String mode, String expected) {
        final var converters = new DamengValueConverters(new DamengConnectorConfig(
                DamengTestConfiguration.configuration().with("binary.handling.mode", mode).build()));
        final var converter = new DamengDefaultValueConverter(converters);

        assertThat(converter.parseDefaultValue(column(Types.VARBINARY), "0x00fbff")).contains(expected);
    }

    static Stream<Arguments> binaryHandlingModes() {
        return Stream.of(
                Arguments.of("base64", "APv/"),
                Arguments.of("base64-url-safe", "APv_"),
                Arguments.of("hex", "00fbff"));
    }

    @ParameterizedTest
    @ValueSource(strings = { "0x00ff || 0x12", "0x00ff /* expression */ + 1", "HEXTORAW('00ff')" })
    void shouldNotEvaluateBinaryExpressionsAsConstants(String expression) {
        assertThat(defaults.parseDefaultValue(column(Types.VARBINARY), expression)).isEmpty();
    }

    @Test
    void shouldKeepHexBinaryLiteralsSeparateFromCharacterDefaults() {
        assertThat(defaults.parseDefaultValue(column(Types.VARCHAR), "0x00ff")).isEmpty();
        assertThat(defaults.parseDefaultValue(column(Types.VARCHAR), "'0x00ff'")).contains("0x00ff");
        assertThat(defaults.parseDefaultValue(column(Types.INTEGER), "0x00ff")).isEmpty();
    }

    @ParameterizedTest
    @ValueSource(strings = { "0x", "0X", "0xG", "0x12ZZ", "0XFF_", "0x12$", "0x12表" })
    void shouldRejectMalformedHexBinaryDefaults(String expression) {
        assertThatThrownBy(() -> defaults.parseDefaultValue(column(Types.VARBINARY), expression))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("character 0", "line 1, column 1");
    }

    @Test
    void shouldIgnoreNullAndDatabaseExpressionsThatCannotBeSafelyDecoded() {
        final var column = column(Types.VARCHAR);

        assertThat(defaults.parseDefaultValue(column, null)).isEmpty();
        assertThat(defaults.parseDefaultValue(column, "NULL")).isEmpty();
        assertThat(defaults.parseDefaultValue(column, "CURRENT_TIMESTAMP")).isEmpty();
        assertThat(defaults.parseDefaultValue(column, "TO_CHAR(SYSDATE)")).isEmpty();
        assertThat(defaults.parseDefaultValue(column, "'a' || 'b'")).isEmpty();
        assertThat(defaults.parseDefaultValue(column, "q'[a]' || q'[b]'")).isEmpty();
    }

    @Test
    void shouldNotTurnAnExpressionIntoAConstantWhenRecoveringNestedComments() {
        final var tables = new Tables();
        new DamengDdlParser(StandardCharsets.UTF_8, false, false, false, true).parse(
                "CREATE TABLE APP.T (V VARCHAR(10) DEFAULT 'x' /* outer /* inner */ -- hidden */ || 'y')", tables, "APP");
        final var column = tables.forTable(null, "APP", "T").columnWithName("V");
        assertThat(defaults.parseDefaultValue(column, column.defaultValueExpression().orElseThrow())).isEmpty();
    }

    private Column column(int jdbcType) {
        return Column.editor().name("VALUE").jdbcType(jdbcType).type("VALUE_TYPE").length(100).optional(true).create();
    }
}
