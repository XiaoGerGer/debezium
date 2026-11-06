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
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import io.debezium.relational.TableId;
import io.debezium.relational.Tables;

class DamengOracleCompatibilityTest {
    private final DamengCompatibilityProfile profile = DamengCompatibilityRegistry.builtIn().resolve(DamengCompatibilityMode.ORACLE);

    private DamengCompatibilityEnvironment environment(Map<String, String> overrides) {
        final Map<String, String> parameters = new HashMap<>(Map.of("CASE_SENSITIVE", "1", "LENGTH_IN_CHAR", "0",
                "BACKSLASH_ESCAPE", "0", "NESTED_C_STYLE_COMMENT", "0", "FLOAT_MODE", "0", "NUMBER_MODE", "0", "ORA_DATE_FMT", "0"));
        parameters.putAll(overrides);
        return new DamengCompatibilityEnvironment(DamengCompatibilityMode.ORACLE, parameters);
    }

    @Test
    void shouldSelectOracleProfileAndBindTypeParameters() {
        assertThat(profile.id()).isEqualTo("oracle-v1");
        assertThat(profile.parameterNames()).contains("FLOAT_MODE", "NUMBER_MODE", "ORA_DATE_FMT", "BACKSLASH_ESCAPE");
        assertThat(DamengCompatibilityRegistry.builtIn().registeredModes())
                .containsExactly(DamengCompatibilityMode.NATIVE, DamengCompatibilityMode.ORACLE, DamengCompatibilityMode.MYSQL, DamengCompatibilityMode.POSTGRES);
    }

    @ParameterizedTest
    @CsvSource({ "0,0", "0,1", "1,0", "1,1" })
    void shouldMapOracleDateAndNumericAliasesInCreateAndAlter(String floatMode, String dateFormat) {
        final var parser = profile.createDdlParser(environment(Map.of("FLOAT_MODE", floatMode, "ORA_DATE_FMT", dateFormat)), StandardCharsets.UTF_8);
        final var tables = new Tables();
        final var id = new TableId(null, "APP", "MiXeD");
        parser.parse("CREATE TABLE app.\"MiXeD\"(\"Id\" PLS_INTEGER PRIMARY KEY, d DATE DEFAULT DATE'2000-02-29', "
                + "n NUMBER, fixed NUMBER(12,2), f FLOAT(24), wide FLOAT(126), bf BINARY_FLOAT, bd BINARY_DOUBLE, "
                + "v VARCHAR2(30), nv NVARCHAR2(8), raw RAW(8))", tables, "APP");
        final var table = tables.forTable(id);
        assertThat(table.primaryKeyColumnNames()).containsExactly("Id");
        assertThat(table.columnWithName("D").jdbcType()).isEqualTo(Types.TIMESTAMP);
        assertThat(table.columnWithName("D").typeName()).isEqualTo("TIMESTAMP");
        assertThat(table.columnWithName("D").length()).isEqualTo(32);
        assertThat(table.columnWithName("D").scale()).contains(0);
        assertThat(table.columnWithName("N").jdbcType()).isEqualTo(Types.DECIMAL);
        assertThat(table.columnWithName("N").length()).isZero();
        assertThat(table.columnWithName("FIXED").scale()).contains(2);
        assertThat(table.columnWithName("F").jdbcType()).isEqualTo(Types.REAL);
        assertThat(table.columnWithName("WIDE").jdbcType()).isEqualTo(Types.FLOAT);
        assertThat(table.columnWithName("BF").jdbcType()).isEqualTo(Types.REAL);
        assertThat(table.columnWithName("BF").length()).isEqualTo(4);
        assertThat(table.columnWithName("BD").jdbcType()).isEqualTo(Types.DOUBLE);
        assertThat(table.columnWithName("BD").typeName()).isEqualTo("DOUBLE");
        assertThat(table.columnWithName("V").jdbcType()).isEqualTo(Types.VARCHAR);
        assertThat(table.columnWithName("NV").jdbcType()).isEqualTo(Types.VARCHAR);
        assertThat(table.columnWithName("RAW").jdbcType()).isEqualTo(Types.VARBINARY);
        parser.parse("ALTER TABLE app.\"MiXeD\" ADD extra DATE; ALTER TABLE app.\"MiXeD\" MODIFY v DATE; "
                + "ALTER TABLE app.\"MiXeD\" RENAME COLUMN d TO \"NewDate\"", tables, "APP");
        for (var name : new String[]{ "NewDate", "EXTRA", "V" }) {
            assertThat(tables.forTable(id).columnWithName(name).jdbcType()).isEqualTo(Types.TIMESTAMP);
            assertThat(tables.forTable(id).columnWithName(name).scale()).contains(0);
        }
    }

    @ParameterizedTest
    @CsvSource({ "0,0", "0,1", "1,0", "1,1" })
    void shouldRebuildDecimalFloatsWithTheirBinaryPrecision(String floatMode, String dateFormat) {
        final var parser = profile.createDdlParser(environment(Map.of("NUMBER_MODE", "1", "FLOAT_MODE", floatMode,
                "ORA_DATE_FMT", dateFormat)), StandardCharsets.UTF_8);
        final var tables = new Tables();
        final var id = new TableId(null, "APP", "NUMBERS");
        parser.parse("CREATE TABLE app.numbers(id INT, n NUMBER, fixed NUMBER(12,2), f FLOAT, f1 FLOAT(1), f24 FLOAT(24), "
                + "f25 FLOAT(25), f53 FLOAT(53), f126 FLOAT(126), r REAL, dp DOUBLE PRECISION, "
                + "d DOUBLE, bf BINARY_FLOAT, bd BINARY_DOUBLE)", tables, "APP");
        final Map<String, Integer> precisions = Map.of("F", 126, "F1", 1, "F24", 24, "F25", 25,
                "F53", 53, "F126", 126, "R", 63, "DP", 126);
        precisions.forEach((name, precision) -> {
            final var column = tables.forTable(id).columnWithName(name);
            assertThat(column.jdbcType()).as(name).isEqualTo(Types.DECIMAL);
            assertThat(column.typeName()).isEqualTo("FLOAT");
            assertThat(column.length()).isEqualTo(precision);
            assertThat(column.scale()).isEmpty();
        });
        assertThat(tables.forTable(id).columnWithName("N").length()).isZero();
        assertThat(tables.forTable(id).columnWithName("FIXED").scale()).contains(2);
        assertThat(tables.forTable(id).columnWithName("D").jdbcType()).isEqualTo(Types.DOUBLE);
        assertThat(tables.forTable(id).columnWithName("BF").jdbcType()).isEqualTo(Types.REAL);
        assertThat(tables.forTable(id).columnWithName("BD").jdbcType()).isEqualTo(Types.DOUBLE);
        parser.parse("ALTER TABLE app.numbers MODIFY id FLOAT(53); ALTER TABLE app.numbers MODIFY f24 FLOAT(126); "
                + "ALTER TABLE app.numbers ADD extra REAL; ALTER TABLE app.numbers MODIFY f NUMBER(12,2)", tables, "APP");
        assertThat(tables.forTable(id).columnWithName("ID").jdbcType()).isEqualTo(Types.DECIMAL);
        assertThat(tables.forTable(id).columnWithName("ID").scale()).isEmpty();
        assertThat(tables.forTable(id).columnWithName("F24").length()).isEqualTo(126);
        assertThat(tables.forTable(id).columnWithName("EXTRA").typeName()).isEqualTo("FLOAT");
        assertThat(tables.forTable(id).columnWithName("F").scale()).contains(2);
    }

    @ParameterizedTest
    @ValueSource(strings = { "FLOAT_MODE", "ORA_DATE_FMT", "NUMBER_MODE", "BACKSLASH_ESCAPE" })
    void shouldRejectMissingAndUnverifiedSettings(String name) {
        for (var value : new String[]{ "2", "" }) {
            assertThatThrownBy(() -> profile.validate(environment(Map.of(name, value)))).hasMessageContaining(name);
        }
        if (name.equals("BACKSLASH_ESCAPE")) {
            assertThatThrownBy(() -> profile.validate(environment(Map.of(name, "1")))).hasMessageContaining(name + "=0");
        }
        final Map<String, String> parameters = new HashMap<>(environment(Map.of()).parameters());
        parameters.remove(name);
        assertThatThrownBy(() -> profile.validate(new DamengCompatibilityEnvironment(DamengCompatibilityMode.ORACLE, parameters)))
                .hasMessageContaining(name);
    }
}
