/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.sql.Types;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
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
import io.debezium.time.ZonedTimestamp;

class DamengTemporalValueConvertersTest {
    private final DamengValueConverters converters = new DamengValueConverters(
            new DamengConnectorConfig(DamengConnectorConfigTest.configuration().build()));

    @ParameterizedTest
    @MethodSource("zonedValues")
    void shouldNormalizeTimestampInstantToUtcAndPreserveFractionalPrecision(Object value, String expected) {
        assertThat(convert(Types.TIMESTAMP_WITH_TIMEZONE, value)).isEqualTo(expected);
    }

    static Stream<Arguments> zonedValues() {
        return Stream.of(
                Arguments.of("2026-09-24 12:34:56.123456 +08:00", "2026-09-24T04:34:56.123456Z"),
                Arguments.of(OffsetDateTime.parse("2026-09-24T12:34:56.123456-03:30"), "2026-09-24T16:04:56.123456Z"),
                Arguments.of(new SqlExpression("TO_TIMESTAMP_TZ('2026-09-24 12:34:56.123456 +08:00','YYYY-MM-DD HH24:MI:SS.FF6 TZH:TZM')"),
                        "2026-09-24T04:34:56.123456Z"),
                Arguments.of(new SqlExpression("TIMESTAMP '2026-09-24 12:34:56.123456 -03:30'"), "2026-09-24T16:04:56.123456Z"),
                Arguments.of(OffsetDateTime.parse("2026-09-25T02:04:56.123456+08:00"), "2026-09-24T18:04:56.123456Z"),
                Arguments.of(new SqlExpression("TIMESTAMP '2026-09-24 12:34:56.123456 -05:30'"), "2026-09-24T18:04:56.123456Z"));
    }

    @Test
    void shouldUseZonedTimestampSchema() {
        assertThat(converters.schemaBuilder(column(Types.TIMESTAMP_WITH_TIMEZONE)).build().name()).isEqualTo(ZonedTimestamp.SCHEMA_NAME);
    }

    @Test
    void shouldConvertTimeWithOffsetFromSnapshotAndRedo() {
        assertThat(convert(Types.TIME_WITH_TIMEZONE, OffsetTime.parse("12:34:56.123+08:00"))).isEqualTo("04:34:56.123Z");
        assertThat(convert(Types.TIME_WITH_TIMEZONE, "12:34:56.123 -03:30")).isEqualTo("16:04:56.123Z");
        assertThat(convert(Types.TIME_WITH_TIMEZONE, new SqlExpression("TIME'12:34:56 +08:00'"))).isEqualTo("04:34:56Z");
        assertThat(convert(Types.TIME_WITH_TIMEZONE, OffsetTime.parse("02:04:56+08:00"))).isEqualTo("18:04:56Z");
    }

    @Test
    void shouldConvertNativeTimeLiteral() {
        assertThat(convert(Types.TIME, new SqlExpression("TIME'12:34:56.123456'"))).isEqualTo(45_296_123_456L);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "TO_DATE('2026-01-02','YYYY-DD-MM')",
            "TO_DATE('2026-01-02')",
            "TO_TIMESTAMP('2026-01-02 03:04:05','YYYY-MM-DD HH12:MI:SS')",
            "TO_TIMESTAMP_TZ('2026-01-02 03:04:05 +08:00','YYYY-MM-DD HH24:MI:SS TZR')",
            "TO_DATE('2026-01-02','YYYY-MM-DD','NLS_DATE_LANGUAGE=ENGLISH')",
            "TO_DATE_OTHER('2026-01-02','YYYY-MM-DD')"
    })
    void shouldRejectUnsupportedTemporalFormatsInsteadOfIgnoringThem(String expression) {
        assertThatThrownBy(() -> convert(Types.TIMESTAMP, new SqlExpression(expression))).isInstanceOf(DataException.class);
    }

    @Test
    void shouldRejectTrailingArgumentsToLiteralFunctions() {
        assertThatThrownBy(() -> convert(Types.VARBINARY, new SqlExpression("HEXTORAW('FF', 'ignored')")))
                .isInstanceOf(DataException.class);
    }

    @Test
    void shouldUnescapeQuotedUnicodeLiteral() {
        assertThat(convert(Types.VARCHAR, new SqlExpression("UNISTR('O''Brien \\4F60\\597D')"))).isEqualTo("O'Brien 你好");
    }

    private Object convert(int jdbcType, Object value) {
        final var column = column(jdbcType);
        final var field = new Field(column.name(), 0, converters.schemaBuilder(column).optional().build());
        return converters.converter(column, field).convert(value);
    }

    private Column column(int jdbcType) {
        return Column.editor().name("VALUE").jdbcType(jdbcType).type("VALUE_TYPE").length(43).scale(6).optional(true).create();
    }
}
