/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import io.debezium.relational.TableId;

class DamengInternalTableTypeTest {
    @ParameterizedTest
    @CsvSource({
            "MTAB$_MV, MATERIALIZED_VIEW",
            "mtab$_mv, MATERIALIZED_VIEW",
            "MLOG$_BASE, MATERIALIZED_VIEW_LOG",
            "Mlog$_Base, MATERIALIZED_VIEW_LOG",
            "BMJ$_I, BITMAP_JOIN_INDEX",
            "bmj$_i, BITMAP_JOIN_INDEX"
    })
    void shouldClassifyInternalTablesInAnySchema(String name, DamengInternalTableType expected) {
        for (final var schema : new String[]{ null, "APP", "OTHER" }) {
            assertThat(DamengInternalTableType.fromTableId(new TableId(null, schema, name))).contains(expected);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "ORDERS", "MTAB$ORDERS", "MLOG_ORDERS", "BMJ_ORDERS", "BMJ$ORDERS", "MY_BMJ$_I", "T$ALOG" })
    void shouldNotClassifySimilarNamesOrAdvancedLogTablesAsFilteredInternalTables(String name) {
        assertThat(DamengInternalTableType.fromTableId(new TableId(null, "APP", name))).isEmpty();
    }
}
