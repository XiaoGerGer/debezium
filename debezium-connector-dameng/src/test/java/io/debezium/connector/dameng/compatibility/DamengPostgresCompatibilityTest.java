/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng.compatibility;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

import io.debezium.relational.TableId;
import io.debezium.relational.Tables;

class DamengPostgresCompatibilityTest {
    private final DamengCompatibilityProfile profile = DamengCompatibilityRegistry.builtIn().resolve(new DamengCompatibilityMode("7"));

    private DamengCompatibilityEnvironment environment(Map<String, String> overrides) {
        final Map<String, String> parameters = new HashMap<>(Map.of("CASE_SENSITIVE", "1", "LENGTH_IN_CHAR", "0",
                "BACKSLASH_ESCAPE", "0", "NESTED_C_STYLE_COMMENT", "0", "FLOAT_MODE", "0", "NUMBER_MODE", "0", "ORA_DATE_FMT", "0"));
        parameters.putAll(overrides);
        return new DamengCompatibilityEnvironment(new DamengCompatibilityMode("7"), parameters);
    }

    @Test
    void shouldSelectExplicitPostgresSemanticsAndTrackTypeParameters() {
        assertThat(profile.id()).isEqualTo("postgres-v1");
        assertThat(profile.parameterNames()).contains("FLOAT_MODE", "NUMBER_MODE", "ORA_DATE_FMT", "IDENTIFIER_MODE", "BACKSLASH_ESCAPE");
        assertThat(environment(Map.of()).signature()).isNotEqualTo(
                new DamengCompatibilityEnvironment(new DamengCompatibilityMode("0"), environment(Map.of()).parameters()).signature());
    }

    @Test
    void shouldKeepDmIdentifierAndPhysicalTypeRulesInHistoricalDdl() {
        final var parser = profile.createDdlParser(environment(Map.of()), StandardCharsets.UTF_8);
        final var tables = new Tables();
        final var id = new TableId(null, "APP", "MiXeD");
        parser.parse("CREATE TABLE app.\"MiXeD\"(\"Id\" INT PRIMARY KEY, v VARCHAR(50) DEFAULT E'汉字\\n''s;--/*', "
                + "d DATE, ts TIMESTAMP(3) WITHOUT TIME ZONE, t TIME WITHOUT TIME ZONE, f FLOAT(24), n NUMERIC(12,2))", tables, "APP");
        final var table = tables.forTable(id);
        assertThat(table.primaryKeyColumnNames()).containsExactly("Id");
        assertThat(table.columnWithName("V").defaultValueExpression()).contains("E'汉字\\n''s;--/*'");
        assertThat(table.columnWithName("D").jdbcType()).isEqualTo(Types.DATE);
        assertThat(table.columnWithName("TS").jdbcType()).isEqualTo(Types.TIMESTAMP);
        assertThat(table.columnWithName("TS").scale()).contains(3);
        assertThat(table.columnWithName("T").jdbcType()).isEqualTo(Types.TIME);
        assertThat(table.columnWithName("T").scale()).contains(0);
        assertThat(table.columnWithName("F").jdbcType()).isEqualTo(Types.REAL);
        assertThat(table.columnWithName("N").jdbcType()).isEqualTo(Types.DECIMAL);
        parser.parse("ALTER TABLE app.\"MiXeD\" ADD extra VARCHAR(50) DEFAULT e'next\\n'; "
                + "ALTER TABLE app.\"MiXeD\" RENAME COLUMN v TO \"NewValue\"", tables, "APP");
        assertThat(tables.forTable(id).columnWithName("NewValue").defaultValueExpression()).contains("E'汉字\\n''s;--/*'");
        assertThat(parser.analyze("ALTER TABLE app.\"MiXeD\" DROP COLUMN extra", "APP").tableId()).isEqualTo(id);
    }

    @ParameterizedTest
    @ValueSource(strings = { "FLOAT_MODE", "NUMBER_MODE", "ORA_DATE_FMT", "BACKSLASH_ESCAPE" })
    void shouldRejectUnverifiedTypeSettingsIncludingMissingParameters(String name) {
        for (var value : new String[]{ "1", "2", "" }) {
            assertThatThrownBy(() -> profile.validate(environment(Map.of(name, value)))).hasMessageContaining(name + "=0");
        }
        final Map<String, String> parameters = new HashMap<>(environment(Map.of()).parameters());
        parameters.remove(name);
        assertThatThrownBy(() -> profile.validate(new DamengCompatibilityEnvironment(new DamengCompatibilityMode("7"), parameters)))
                .hasMessageContaining(name + "=0");
    }

    @ParameterizedTest
    @ValueSource(strings = { "SERIAL", "BYTEA", "JSONB", "UUID" })
    void shouldNotInventPhysicalMappingsForPostgresTypesRejectedByDm(String type) {
        final var tables = new Tables();
        assertThatThrownBy(() -> profile.createDdlParser(environment(Map.of()), StandardCharsets.UTF_8)
                .parse("CREATE TABLE APP.T(ID " + type + ")", tables, "APP"))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("Unsupported DM column type");
        assertThat(tables.size()).isZero();
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = { "2", "true" })
    void shouldRejectUnknownLexicalAndIdentifierSettings(String value) {
        for (var name : new String[]{ "BACKSLASH_ESCAPE", "NESTED_C_STYLE_COMMENT", "CASE_SENSITIVE", "IDENTIFIER_MODE", "LENGTH_IN_CHAR" }) {
            final Map<String, String> parameters = new HashMap<>(environment(Map.of()).parameters());
            if (value == null) {
                parameters.remove(name);
            }
            else {
                parameters.put(name, value);
            }
            if (value != null || !name.equals("IDENTIFIER_MODE")) {
                assertThatThrownBy(() -> profile.createDdlParser(new DamengCompatibilityEnvironment(new DamengCompatibilityMode("7"), parameters),
                        StandardCharsets.UTF_8)).hasMessageContaining(name);
            }
        }
    }
}
