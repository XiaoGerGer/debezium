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
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

import io.debezium.relational.TableId;
import io.debezium.relational.Tables;

class DamengPostgresCompatibilityTest {
    private final DamengCompatibilityProfile profile = DamengCompatibilityRegistry.builtIn().resolve(DamengCompatibilityMode.POSTGRES);

    private DamengCompatibilityEnvironment environment(Map<String, String> overrides) {
        final Map<String, String> parameters = new HashMap<>(Map.of("CASE_SENSITIVE", "1",
                "BACKSLASH_ESCAPE", "0", "NESTED_C_STYLE_COMMENT", "0", "FLOAT_MODE", "0", "NUMBER_MODE", "0"));
        parameters.putAll(overrides);
        return new DamengCompatibilityEnvironment(DamengCompatibilityMode.POSTGRES, parameters);
    }

    @Test
    void shouldSelectExplicitPostgresSemanticsAndTrackTypeParameters() {
        assertThat(profile.id()).isEqualTo("postgres-v1");
        assertThat(profile.parameterNames()).contains("FLOAT_MODE", "NUMBER_MODE", "IDENTIFIER_MODE", "BACKSLASH_ESCAPE").doesNotContain("ORA_DATE_FMT");
        assertThat(environment(Map.of()).signature()).isNotEqualTo(
                new DamengCompatibilityEnvironment(DamengCompatibilityMode.NATIVE, environment(Map.of()).parameters()).signature());
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

    static Stream<Arguments> parameterCombinations() {
        return IntStream.range(0, 16).mapToObj(value -> Arguments.of(Integer.toString(value & 1),
                Integer.toString((value >> 1) & 1), Integer.toString((value >> 2) & 1), Integer.toString((value >> 3) & 1)));
    }

    @ParameterizedTest
    @MethodSource("parameterCombinations")
    void shouldSelectNumericAndLexicalRulesIndependentlyOfDateSemantics(String floatMode, String numberMode, String dateFormat, String backslash) {
        final var environment = environment(Map.of("FLOAT_MODE", floatMode, "NUMBER_MODE", numberMode, "ORA_DATE_FMT", dateFormat, "BACKSLASH_ESCAPE", backslash));
        profile.validate(environment);
        final var parser = profile.createDdlParser(environment, StandardCharsets.UTF_8);
        final var tables = new Tables();
        parser.parse("CREATE TABLE APP.T(D DATE, TS TIMESTAMP(6) WITHOUT TIME ZONE, T TIME(3) WITHOUT TIME ZONE, F FLOAT(24), R REAL, P DOUBLE PRECISION, B DOUBLE, "
                + "V VARCHAR(100) DEFAULT E'a\\n''b', W VARCHAR(100) DEFAULT 'a\\\\b', Q VARCHAR(100) DEFAULT Q'[a\\nb]', "
                + "C VARCHAR(100) COMMENT 'a\\tb')", tables, "APP");
        final var table = tables.forTable(new TableId(null, "APP", "T"));
        assertThat(table.columnWithName("D").jdbcType()).isEqualTo(Types.DATE);
        assertThat(table.columnWithName("TS").jdbcType()).isEqualTo(Types.TIMESTAMP);
        assertThat(table.columnWithName("TS").scale()).contains(6);
        assertThat(table.columnWithName("T").jdbcType()).isEqualTo(Types.TIME);
        assertThat(table.columnWithName("T").scale()).contains(3);
        for (var name : new String[]{ "F", "R", "P" }) {
            final var column = table.columnWithName(name);
            if (numberMode.equals("1")) {
                assertThat(column.jdbcType()).isEqualTo(Types.DECIMAL);
                assertThat(column.scale()).isEmpty();
                assertThat(column.length()).isEqualTo(name.equals("F") ? 24 : name.equals("R") ? 63 : 126);
            }
            else {
                assertThat(column.jdbcType()).isEqualTo(name.equals("P") ? Types.DOUBLE : Types.REAL);
            }
        }
        assertThat(table.columnWithName("B").jdbcType()).isEqualTo(Types.DOUBLE);
        assertThat(table.columnWithName("V").defaultValueExpression()).contains(backslash.equals("1") ? "'a\n''b'" : "E'a\\n''b'");
        assertThat(table.columnWithName("W").defaultValueExpression()).contains("'a\\\\b'");
        assertThat(table.columnWithName("Q").defaultValueExpression()).contains(backslash.equals("1") ? "'a\\\\nb'" : "Q'[a\\nb]'");
        assertThat(table.columnWithName("C").comment()).isEqualTo(backslash.equals("1") ? "a\tb" : "a\\tb");
        parser.parse("ALTER TABLE APP.T ALTER COLUMN V SET DEFAULT 'x\\rb'", tables, "APP");
        assertThat(tables.forTable(table.id()).columnWithName("V").defaultValueExpression()).contains(backslash.equals("1") ? "'x\rb'" : "'x\\rb'");
    }

    @ParameterizedTest
    @CsvSource({ "4,0", "4,1", "7,0", "7,1" })
    void shouldRecompileDefaultsOnlyWhenAddingSeveralColumns(String mode, String backslash) {
        final var selectedMode = DamengCompatibilityMode.parse(mode);
        final var selectedProfile = DamengCompatibilityRegistry.builtIn().resolve(selectedMode);
        final Map<String, String> parameters = new HashMap<>(environment(Map.of("BACKSLASH_ESCAPE", backslash)).parameters());
        parameters.put("MY_STRICT_TABLES", "1");
        final var parser = selectedProfile.createDdlParser(new DamengCompatibilityEnvironment(selectedMode, parameters), StandardCharsets.UTF_8);
        final var tables = new Tables();
        final var id = new TableId(null, "APP", "T");
        parser.parse("CREATE TABLE APP.T(ID INT, Q VARCHAR(100) DEFAULT Q'[a\\nb]', S VARCHAR(100) DEFAULT 'a\\\\b')", tables, "APP");
        final var initial = tables.forTable(id);
        parser.parse("ALTER TABLE APP.T ADD (SINGLE_VALUE INT)", tables, "APP");
        assertThat(tables.forTable(id).columnWithName("Q")).isEqualTo(initial.columnWithName("Q"));
        parser.parse("ALTER TABLE APP.T ADD (A VARCHAR(100) DEFAULT Q'[a\\nb]', B INT, C INT)", tables, "APP");
        final var table = tables.forTable(id);
        assertThat(table.columnWithName("Q").defaultValueExpression()).contains(backslash.equals("1") ? "'a" + "\\".repeat(4) + "nb'" : "Q'[a\\nb]'");
        assertThat(table.columnWithName("A").defaultValueExpression()).isEqualTo(table.columnWithName("Q").defaultValueExpression());
        assertThat(table.columnWithName("S").defaultValueExpression()).contains("'a" + "\\".repeat(backslash.equals("1") ? 4 : 2) + "b'");
        assertThat(table.columnWithName("ID")).isEqualTo(initial.columnWithName("ID"));
    }

    @ParameterizedTest
    @ValueSource(strings = { "FLOAT_MODE", "NUMBER_MODE", "BACKSLASH_ESCAPE" })
    void shouldRejectInvalidTypeSettingsIncludingMissingParameters(String name) {
        for (var value : new String[]{ "-1", "2", "", "true" }) {
            assertThatThrownBy(() -> profile.validate(environment(Map.of(name, value)))).hasMessageContaining(name + "=0 or 1");
        }
        final Map<String, String> parameters = new HashMap<>(environment(Map.of()).parameters());
        parameters.remove(name);
        assertThatThrownBy(() -> profile.validate(new DamengCompatibilityEnvironment(DamengCompatibilityMode.POSTGRES, parameters)))
                .hasMessageContaining(name + "=0 or 1");
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
        for (var name : new String[]{ "BACKSLASH_ESCAPE", "NESTED_C_STYLE_COMMENT", "CASE_SENSITIVE", "IDENTIFIER_MODE" }) {
            final Map<String, String> parameters = new HashMap<>(environment(Map.of()).parameters());
            if (value == null) {
                parameters.remove(name);
            }
            else {
                parameters.put(name, value);
            }
            if (value != null || !name.equals("IDENTIFIER_MODE")) {
                assertThatThrownBy(() -> profile.createDdlParser(new DamengCompatibilityEnvironment(DamengCompatibilityMode.POSTGRES, parameters),
                        StandardCharsets.UTF_8)).hasMessageContaining(name);
            }
        }
    }
}
