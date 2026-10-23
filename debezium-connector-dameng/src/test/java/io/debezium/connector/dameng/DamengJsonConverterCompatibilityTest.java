/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.json.JsonConverter;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** Diagnostic of the stock converter, separate from connector data-consistency acceptance. */
class DamengJsonConverterCompatibilityTest {
    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void shouldExposeStockJsonDecoderLossOfNegativeZero(boolean float32) throws Exception {
        final var schema = float32 ? Schema.FLOAT32_SCHEMA : Schema.FLOAT64_SCHEMA;
        final Number original;
        if (float32) {
            original = -0.0f;
        }
        else {
            original = -0.0d;
        }
        try (var converter = new JsonConverter()) {
            converter.configure(Map.of("schemas.enable", true), false);
            final var encoded = converter.fromConnectData("dameng.types", schema, original);
            assertThat(new String(encoded, StandardCharsets.UTF_8)).contains("\"payload\":-0.0");
            final var decoded = (Number) converter.toConnectData("dameng.types", encoded).value();
            assertThat(Double.doubleToRawLongBits(decoded.doubleValue())).isEqualTo(Double.doubleToRawLongBits(0.0d));
            assertThat(Double.doubleToRawLongBits(original.doubleValue())).isEqualTo(Double.doubleToRawLongBits(-0.0d));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void shouldExposeStockJsonDecoderLossOfNaN(boolean float32) throws Exception {
        final var schema = float32 ? Schema.FLOAT32_SCHEMA : Schema.FLOAT64_SCHEMA;
        final Number original;
        if (float32) {
            original = Float.NaN;
        }
        else {
            original = Double.NaN;
        }
        try (var converter = new JsonConverter()) {
            converter.configure(Map.of("schemas.enable", true), false);
            final var encoded = converter.fromConnectData("dameng.types", schema, original);
            assertThat(new String(encoded, StandardCharsets.UTF_8)).contains("\"payload\":\"NaN\"");
            final var decoded = (Number) converter.toConnectData("dameng.types", encoded).value();
            assertThat(decoded.doubleValue()).isZero();
        }
    }
}
