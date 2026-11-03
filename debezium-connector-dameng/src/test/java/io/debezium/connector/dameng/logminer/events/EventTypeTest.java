/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng.logminer.events;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

class EventTypeTest {
    @ParameterizedTest
    @CsvSource({
            "1, INSERT, true",
            "2, DELETE, true",
            "3, UPDATE, true",
            "4, BATCH_UPDATE, true",
            "5, DDL, false",
            "6, START, false",
            "7, COMMIT, false",
            "13, LOB_NEW, false",
            "14, LOB_DATA, false",
            "15, PRE_COMMIT, false",
            "36, ROLLBACK, false",
            "37, SEQUENCE_MODIFY, false",
            "38, XA_COMMIT, false",
            "255, UNSUPPORTED, false"
    })
    void shouldResolveDocumentedDamengOperationCodes(int code, EventType expected, boolean dataChange) {
        assertThat(EventType.from(code)).isEqualTo(expected);
        assertThat(expected.getValue()).isEqualTo(code);
        assertThat(expected.isDataChange()).isEqualTo(dataChange);
    }

    @ParameterizedTest
    @ValueSource(ints = { Integer.MIN_VALUE, -1, 0, 8, 9, 10, 11, 27, 29, 34, 68, 70, 71, 91, 92, 93, 254, 256, 999, Integer.MAX_VALUE })
    void shouldResolveUnknownAndOracleSpecificCodesAsUnsupported(int code) {
        assertThat(EventType.from(code)).isEqualTo(EventType.UNSUPPORTED);
        assertThat(EventType.from(code).isDataChange()).isFalse();
    }
}
