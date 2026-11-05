/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class DamengDatatypeAssertionsTest {
    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void shouldRequireStoredFloatingPointBitsWhileAllowingJsonZeroNormalization(boolean singlePrecision) {
        final var fieldSchema = singlePrecision ? Schema.FLOAT32_SCHEMA : Schema.FLOAT64_SCHEMA;
        final var schema = SchemaBuilder.struct().field("VALUE", fieldSchema).build();
        final Object negativeZero;
        final Object positiveZero;
        final Object nonzero;
        if (singlePrecision) {
            negativeZero = -0.0f;
            positiveZero = 0.0f;
            nonzero = 0.0000001f;
        }
        else {
            negativeZero = -0.0d;
            positiveZero = 0.0d;
            nonzero = 0.0000001d;
        }
        final var positiveRow = new Struct(schema).put("VALUE", positiveZero);
        final var negativeRow = new Struct(schema).put("VALUE", negativeZero);

        assertThatCode(() -> DamengDatatypeAssertions.assertRow(negativeRow, Map.of("VALUE", negativeZero), "negative zero"))
                .doesNotThrowAnyException();
        assertThatCode(() -> DamengDatatypeAssertions.assertRow(positiveRow, Map.of("VALUE", positiveZero), "positive zero"))
                .doesNotThrowAnyException();
        assertThatThrownBy(() -> DamengDatatypeAssertions.assertRow(positiveRow, Map.of("VALUE", negativeZero), "lost sign"))
                .isInstanceOf(AssertionError.class);
        assertThatThrownBy(() -> DamengDatatypeAssertions.assertConnectRow(positiveRow, Map.of("VALUE", negativeZero), "lost sign"))
                .isInstanceOf(AssertionError.class);
        assertThatThrownBy(() -> DamengDatatypeAssertions.assertConnectRow(negativeRow, Map.of("VALUE", positiveZero), "changed sign"))
                .isInstanceOf(AssertionError.class);
        assertThatThrownBy(() -> DamengDatatypeAssertions.assertRow(positiveRow, Map.of("VALUE", nonzero), "lost nonzero"))
                .isInstanceOf(AssertionError.class);
    }
}
