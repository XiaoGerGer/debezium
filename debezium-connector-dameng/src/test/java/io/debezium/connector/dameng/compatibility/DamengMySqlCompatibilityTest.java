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
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import io.debezium.connector.dameng.antlr.DamengAntlrSqlLexer;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;

class DamengMySqlCompatibilityTest {
    private final DamengCompatibilityProfile profile = DamengCompatibilityRegistry.builtIn().resolve(DamengCompatibilityMode.MYSQL);

    private DamengCompatibilityEnvironment environment(Map<String, String> overrides) {
        final Map<String, String> parameters = new HashMap<>(Map.of("CASE_SENSITIVE", "1", "LENGTH_IN_CHAR", "0",
                "BACKSLASH_ESCAPE", "0", "NESTED_C_STYLE_COMMENT", "0", "FLOAT_MODE", "0", "NUMBER_MODE", "0", "ORA_DATE_FMT", "0",
                "MY_STRICT_TABLES", "1"));
        parameters.putAll(overrides);
        return new DamengCompatibilityEnvironment(DamengCompatibilityMode.MYSQL, parameters);
    }

    @Test
    void shouldSelectMySqlProfileAndBindItsTypeAndStrictSettings() {
        assertThat(DamengCompatibilityMode.parse("4")).isSameAs(DamengCompatibilityMode.MYSQL);
        assertThat(profile.id()).isEqualTo("mysql-v1");
        assertThat(profile.parameterNames()).contains("FLOAT_MODE", "NUMBER_MODE", "ORA_DATE_FMT", "MY_STRICT_TABLES", "BACKSLASH_ESCAPE");
        profile.validate(environment(Map.of("MY_STRICT_TABLES", "0")));
        assertThat(environment(Map.of()).signature()).isNotEqualTo(environment(Map.of("MY_STRICT_TABLES", "0")).signature());
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
        parser.parse("CREATE TABLE APP.T(D DATE, F FLOAT(24), R REAL, P DOUBLE PRECISION, B DOUBLE, "
                + "V VARCHAR(100) DEFAULT 'a\\n''b', W VARCHAR(100) DEFAULT 'a\\\\b', Q VARCHAR(100) DEFAULT Q'[a\\nb]', "
                + "C VARCHAR(100) COMMENT 'a\\tb')", tables, "APP");
        final var table = tables.forTable(new TableId(null, "APP", "T"));
        assertThat(table.columnWithName("D").jdbcType()).isEqualTo(Types.DATE);
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
        assertThat(table.columnWithName("V").defaultValueExpression()).contains(backslash.equals("1") ? "'a\n''b'" : "'a\\n''b'");
        assertThat(table.columnWithName("W").defaultValueExpression()).contains("'a\\\\b'");
        assertThat(table.columnWithName("Q").defaultValueExpression()).contains(backslash.equals("1") ? "'a\\\\nb'" : "Q'[a\\nb]'");
        assertThat(table.columnWithName("C").comment()).isEqualTo(backslash.equals("1") ? "a\tb" : "a\\tb");
        parser.parse("ALTER TABLE APP.T ALTER COLUMN V SET DEFAULT 'x\\rb'", tables, "APP");
        assertThat(tables.forTable(table.id()).columnWithName("V").defaultValueExpression()).contains(backslash.equals("1") ? "'x\rb'" : "'x\\rb'");
    }

    static Stream<Arguments> escapedLiterals() {
        return Stream.of(
                Arguments.of("'a\\nb'", "a\nb", "a\nb"),
                Arguments.of("'a\\rb'", "a\rb", "a\rb"),
                Arguments.of("'a\\tb'", "a\tb", "a\tb"),
                Arguments.of("'a\\bb'", "a\bb", "a\bb"),
                Arguments.of("'a\\fb'", "a\fb", "a\fb"),
                Arguments.of("'a\\0b'", "a0b", "a0b"),
                Arguments.of("'a\\Zb'", "aZb", "aZb"),
                Arguments.of("'a\\qb'", "aqb", "aqb"),
                Arguments.of("'a\\%b'", "a%b", "a%b"),
                Arguments.of("'a\\_b'", "a_b", "a_b"),
                Arguments.of("'a\\\\b'", "a\\b", "a\\\\b"),
                Arguments.of("Q'[a\\nb]'", "a\\nb", "a\\\\nb"),
                Arguments.of("N'a\\tb'", "a\tb", "a\tb"),
                Arguments.of("E'a\\rb'", "a\rb", "a\rb"),
                Arguments.of("'a\\n''b'", "a\n'b", "a\n'b"));
    }

    @ParameterizedTest
    @MethodSource("escapedLiterals")
    void shouldDistinguishCompiledDefaultsFromOrdinaryStringValues(String literal, String value, String defaultValue) {
        final var lexer = new DamengAntlrSqlLexer();
        final var token = lexer.tokenize(literal).get(0);
        assertThat(token.stringValue(true)).isEqualTo(value);
        assertThat(lexer.tokenize(token.defaultValueExpression()).get(0).stringValue()).isEqualTo(defaultValue);
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void shouldReplayBacktickNamesAutoIncrementTemporalAndLobAliases(boolean insensitive) {
        final var parser = profile.createDdlParser(environment(Map.of("CASE_SENSITIVE", insensitive ? "0" : "1")), StandardCharsets.UTF_8);
        final var tables = new Tables();
        final var id = new TableId(null, "APP", insensitive ? "A`B;--/*" : "a`b;--/*");
        parser.parse("CREATE TABLE app.`a``b;--/*`(`Id` INT AUTO_INCREMENT PRIMARY KEY, `v` VARCHAR(80) DEFAULT '汉字\\n''a;--/*', "
                + "d DATE, dt DATETIME(3), f FLOAT(24), n DECIMAL(38,6), mt MEDIUMTEXT, lt LONGTEXT, mb MEDIUMBLOB, lb LONGBLOB)", tables, "APP");
        final var table = tables.forTable(id);
        assertThat(table.primaryKeyColumnNames()).containsExactly(insensitive ? "ID" : "Id");
        assertThat(table.columnWithName(insensitive ? "ID" : "Id").isAutoIncremented()).isTrue();
        assertThat(table.columnWithName(insensitive ? "V" : "v").defaultValueExpression()).contains("'汉字\\n''a;--/*'");
        assertThat(table.columnWithName("D").jdbcType()).isEqualTo(Types.DATE);
        assertThat(table.columnWithName("DT").jdbcType()).isEqualTo(Types.TIMESTAMP);
        assertThat(table.columnWithName("DT").scale()).contains(3);
        assertThat(table.columnWithName("F").jdbcType()).isEqualTo(Types.REAL);
        assertThat(table.columnWithName("N").jdbcType()).isEqualTo(Types.DECIMAL);
        for (var name : new String[]{ "MT", "LT" }) {
            assertThat(table.columnWithName(name).jdbcType()).isEqualTo(Types.CLOB);
            assertThat(table.columnWithName(name).typeName()).isEqualTo("TEXT");
        }
        for (var name : new String[]{ "MB", "LB" }) {
            assertThat(table.columnWithName(name).jdbcType()).isEqualTo(Types.BLOB);
        }
        parser.parse("ALTER TABLE app.`a``b;--/*` MODIFY `v` VARCHAR(200); "
                + "ALTER TABLE app.`a``b;--/*` RENAME COLUMN `v` TO `new``Value`; "
                + "ALTER TABLE app.`a``b;--/*` ADD extra LONGTEXT", tables, "APP");
        assertThat(tables.forTable(id).columnWithName(insensitive ? "NEW`VALUE" : "new`Value").length()).isEqualTo(200);
        assertThat(tables.forTable(id).columnWithName("EXTRA").jdbcType()).isEqualTo(Types.CLOB);
        assertThat(parser.analyze("DROP TABLE app.`a``b;--/*`", "APP").tableId()).isEqualTo(id);
        final var previous = tables.forTable(id);
        assertThatThrownBy(() -> parser.parse("ALTER TABLE app.`a``b;--/*` ADD `unclosed INT", tables, "APP"))
                .hasMessageContaining("Unclosed SQL quoted identifier");
        assertThat(tables.forTable(id)).isEqualTo(previous);
    }

    @ParameterizedTest
    @ValueSource(strings = { "FLOAT_MODE", "NUMBER_MODE", "ORA_DATE_FMT", "BACKSLASH_ESCAPE", "MY_STRICT_TABLES" })
    void shouldRejectUnverifiedOrMissingParameters(String name) {
        final var invalid = "2";
        assertThatThrownBy(() -> profile.validate(environment(Map.of(name, invalid)))).hasMessageContaining(name);
        final Map<String, String> parameters = new HashMap<>(environment(Map.of()).parameters());
        parameters.remove(name);
        assertThatThrownBy(() -> profile.validate(new DamengCompatibilityEnvironment(DamengCompatibilityMode.MYSQL, parameters)))
                .hasMessageContaining(name);
    }

    @ParameterizedTest
    @ValueSource(strings = { "BOOL", "MEDIUMINT", "YEAR", "JSON", "INT UNSIGNED", "ENUM('a','b')", "TINYINT(1)" })
    void shouldRejectUnsupportedMySqlDeclarationsWithoutChangingHistory(String type) {
        final var tables = new Tables();
        assertThatThrownBy(() -> profile.createDdlParser(environment(Map.of()), StandardCharsets.UTF_8)
                .parse("CREATE TABLE APP.T(ID " + type + ")", tables, "APP"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(tables.size()).isZero();
    }
}
