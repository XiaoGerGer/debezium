/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.sql.Types;
import java.util.stream.Stream;

import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.debezium.data.VariableScaleDecimal;
import io.debezium.relational.Column;

class DamengDecimalValueConvertersTest {
    @ParameterizedTest
    @MethodSource("variableDecimals")
    void shouldPreserveVariableScaleInSnapshotAndLogMinerValues(Object value, BigDecimal expected) {
        final var converters = converters("precise");
        final var field = field(converters, column(0, 0));
        final var converted = (Struct) converters.converter(column(0, 0), field).convert(value);
        assertThat(field.schema().name()).isEqualTo(VariableScaleDecimal.LOGICAL_NAME);
        assertThat(VariableScaleDecimal.toLogical(converted).getDecimalValue()).contains(expected);
    }

    static Stream<Arguments> variableDecimals() {
        return Stream.of(
                Arguments.of(new BigDecimal("12345678901234567890.123456"), new BigDecimal("12345678901234567890.123456")),
                Arguments.of("-0.0000000000000001", new BigDecimal("-0.0000000000000001")),
                Arguments.of("1e20", new BigDecimal("1e20")));
    }

    @Test
    void shouldPreserveDeclaredScaleWhenLogMinerOmitsTrailingZeros() {
        final var converters = converters("precise");
        final var column = column(18, 6);
        final var field = field(converters, column);
        final var converted = (BigDecimal) converters.converter(column, field).convert("123");
        assertThat(converted).isEqualTo(new BigDecimal("123.000000"));
        assertThat(Decimal.fromLogical(field.schema(), converted)).isNotEmpty();
    }

    @Test
    void shouldRespectNonPreciseModesForUnconstrainedNumber() {
        final var column = column(0, 0);
        final var stringConverters = converters("string");
        final var stringField = field(stringConverters, column);
        assertThat(stringField.schema().type()).isEqualTo(Schema.Type.STRING);
        assertThat(stringConverters.converter(column, stringField).convert("123.456")).isEqualTo("123.456");

        final var doubleConverters = converters("double");
        final var doubleField = field(doubleConverters, column);
        assertThat(doubleField.schema().type()).isEqualTo(Schema.Type.FLOAT64);
        assertThat(doubleConverters.converter(column, doubleField).convert("123.456")).isEqualTo(123.456d);
    }

    private static DamengValueConverters converters(String mode) {
        return new DamengValueConverters(new DamengConnectorConfig(DamengConnectorConfigTest.configuration()
                .with("decimal.handling.mode", mode).build()));
    }

    private static Column column(int precision, int scale) {
        return Column.editor().name("VALUE").jdbcType(Types.DECIMAL).type("NUMBER").length(precision).scale(scale).optional(true).create();
    }

    private static Field field(DamengValueConverters converters, Column column) {
        return new Field(column.name(), 0, converters.schemaBuilder(column).optional().build());
    }
}
