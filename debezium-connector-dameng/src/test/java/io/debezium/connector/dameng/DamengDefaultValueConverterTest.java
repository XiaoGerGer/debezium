/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.sql.Types;

import org.junit.jupiter.api.Test;

import io.debezium.relational.Column;
import io.debezium.relational.Tables;

class DamengDefaultValueConverterTest {
    private final DamengDefaultValueConverter defaults = new DamengDefaultValueConverter(
            new DamengValueConverters(new DamengConnectorConfig(DamengConnectorConfigTest.configuration().build())));

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

    @Test
    void shouldConvertNumericConstantsUsingTheColumnMapping() {
        assertThat(defaults.parseDefaultValue(column(Types.INTEGER), " -42 ")).contains(-42);
        assertThat(defaults.parseDefaultValue(column(Types.INTEGER), "+7")).contains(7);
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
