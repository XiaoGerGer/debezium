/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng.compatibility;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
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

class DamengCompatibilityTest {
    @Test
    void shouldCanonicalizeCaseInsensitiveHistoricalIdentifiersWithoutChangingValues() {
        final var environment = new DamengCompatibilityEnvironment(DamengCompatibilityMode.NATIVE,
                Map.of("BACKSLASH_ESCAPE", "0", "NESTED_C_STYLE_COMMENT", "0", "CASE_SENSITIVE", "0", "NUMBER_MODE", "0"));
        final var parser = new DamengNativeCompatibilityProfile().createDdlParser(environment, StandardCharsets.UTF_8);
        final var tables = new Tables();
        parser.parse("CREATE TABLE \"MiXeD\"(\"Id\" INT, \"Value\" VARCHAR(30) DEFAULT 'AbC', CONSTRAINT \"Pk\" PRIMARY KEY (\"id\"))", tables, "app");
        final var id = new TableId(null, "APP", "MIXED");
        assertThat(parser.analyze("ALTER TABLE mixed ADD \"Extra\" INT", "app").tableId()).isEqualTo(id);
        assertThat(tables.forTable(id).primaryKeyColumnNames()).containsExactly("ID");
        assertThat(tables.forTable(id).columnWithName("VALUE").defaultValueExpression()).contains("'AbC'");
        parser.parse("ALTER TABLE \"app\".\"mixed\" RENAME COLUMN \"value\" TO \"NewValue\"", tables, "app");
        parser.parse("ALTER TABLE aPp.miXed DROP CONSTRAINT \"pk\"", tables, "app");
        assertThat(tables.forTable(id).retrieveColumnNames()).containsExactly("ID", "NEWVALUE");
        assertThat(tables.forTable(id).primaryKeyColumnNames()).isEmpty();
    }

    @ParameterizedTest
    @ValueSource(strings = { "1", "3", "5", "6", "8", "9", "42", " 0042 ", "2147483648", "99999999999999999999999999999999999999" })
    void shouldRejectUnsupportedModesDuringParsing(String value) {
        assertThatThrownBy(() -> DamengCompatibilityMode.parse(value))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("COMPATIBLE_MODE=" + value, "supported values: 0 (native), 2 (Oracle), 4 (MySQL), 7 (PostgreSQL)");
    }

    @Test
    void shouldCanonicalizeIdentifiersBeforeLookingUpSemantics() {
        assertThat(DamengCompatibilityMode.parse(" 000 ")).isEqualTo(DamengCompatibilityMode.NATIVE);
        assertThat(DamengCompatibilityRegistry.builtIn().resolve(DamengCompatibilityMode.parse("000")).id()).isEqualTo("native-v1");
    }

    @Test
    void shouldKeepNamedModesCompatibleWithServerValuesAndPersistedSignatures() {
        final var registry = DamengCompatibilityRegistry.builtIn();
        assertThat(registry.registeredModes()).containsExactly(DamengCompatibilityMode.NATIVE, DamengCompatibilityMode.ORACLE, DamengCompatibilityMode.MYSQL,
                DamengCompatibilityMode.POSTGRES);
        assertThat(DamengCompatibilityMode.values()).containsExactly(DamengCompatibilityMode.NATIVE, DamengCompatibilityMode.ORACLE, DamengCompatibilityMode.MYSQL,
                DamengCompatibilityMode.POSTGRES);
        assertThat(DamengCompatibilityMode.NATIVE.value()).isEqualTo("0");
        assertThat(DamengCompatibilityMode.ORACLE.value()).isEqualTo("2");
        assertThat(DamengCompatibilityMode.MYSQL.value()).isEqualTo("4");
        assertThat(DamengCompatibilityMode.POSTGRES.value()).isEqualTo("7");
        for (var mode : registry.registeredModes()) {
            assertThat(DamengCompatibilityMode.parse(mode.value())).isSameAs(mode);
            assertThat(DamengCompatibilityMode.parse(" 00" + mode.value() + " ")).isSameAs(mode);
            assertThat(mode.toString()).isEqualTo(mode.value());
            assertThat(new DamengCompatibilityEnvironment(mode, Map.of()).signature()).isEqualTo("v1:1:" + mode.value());
        }
        assertThat(registry.resolve(DamengCompatibilityMode.NATIVE).id()).isEqualTo("native-v1");
        assertThat(registry.resolve(DamengCompatibilityMode.ORACLE).id()).isEqualTo("oracle-v1");
        assertThat(registry.resolve(DamengCompatibilityMode.MYSQL).id()).isEqualTo("mysql-v1");
        assertThat(registry.resolve(DamengCompatibilityMode.POSTGRES).id()).isEqualTo("postgres-v1");
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = { " ", "-1", "+1", "oracle", "0.0", "1e2", "1 OR 1=1" })
    void shouldRejectMissingAndMalformedIdentifiers(String value) {
        assertThatThrownBy(() -> DamengCompatibilityMode.parse(value))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("COMPATIBLE_MODE");
    }

    @Test
    void shouldCopyRegistrationsAndKeepRegistriesIndependent() {
        final var custom = profile("custom-v1", DamengCompatibilityMode.ORACLE, DamengCompatibilityMode.MYSQL, DamengCompatibilityMode.POSTGRES);
        final List<DamengCompatibilityProfile> profiles = new ArrayList<>(List.of(new DamengNativeCompatibilityProfile(), custom));
        final var registry = new DamengCompatibilityRegistry(profiles);
        profiles.clear();
        assertThat(registry.resolve(DamengCompatibilityMode.ORACLE)).isSameAs(custom);
        assertThat(registry.resolve(DamengCompatibilityMode.POSTGRES)).isSameAs(custom);
        assertThat(registry.resolve(DamengCompatibilityMode.NATIVE).id()).isEqualTo("native-v1");
        assertThatThrownBy(() -> registry.registeredModes().clear()).isInstanceOf(UnsupportedOperationException.class);
        assertThat(DamengCompatibilityRegistry.builtIn().resolve(DamengCompatibilityMode.ORACLE).id()).isEqualTo("oracle-v1");
    }

    @Test
    void shouldRejectAmbiguousRegistrationIncludingCanonicalModeAliases() {
        assertThatThrownBy(() -> new DamengCompatibilityRegistry(List.of(profile("first", DamengCompatibilityMode.ORACLE),
                profile("second", DamengCompatibilityMode.parse("002")))))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("COMPATIBLE_MODE=2", "first", "second");
    }

    @Test
    void shouldRequireIdentifiableProfilesAndRejectUnregisteredModesEvenWithAnEmptyRegistry() {
        assertThatThrownBy(() -> new DamengCompatibilityRegistry(List.of(profile(null, DamengCompatibilityMode.ORACLE))))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new DamengCompatibilityRegistry(List.of(profile(" ", DamengCompatibilityMode.ORACLE))))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new DamengCompatibilityRegistry(List.of(profile("empty"))))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new DamengCompatibilityRegistry(List.of()).resolve(DamengCompatibilityMode.NATIVE))
                .hasMessageContaining("registered modes: []");
        assertThatThrownBy(() -> new DamengCompatibilityRegistry(List.of(new DamengNativeCompatibilityProfile())).resolve(DamengCompatibilityMode.ORACLE))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("COMPATIBLE_MODE=2", "registered modes: [0]");
        assertThatThrownBy(() -> DamengCompatibilityRegistry.builtIn().resolve(null)).isInstanceOf(NullPointerException.class);
    }

    @Test
    void shouldPreserveObservedParametersWithoutConflatingMissingAndZero() {
        final Map<String, String> values = new HashMap<>(Map.of("NUMBER_MODE", "0"));
        final var environment = new DamengCompatibilityEnvironment(DamengCompatibilityMode.NATIVE, values);
        values.put("NUMBER_MODE", "1");
        assertThat(environment.parameter("NUMBER_MODE")).isEqualTo("0");
        assertThat(environment.parameter("MISSING_PARAMETER")).isNull();
        assertThatThrownBy(() -> environment.parameters().clear()).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> new DamengCompatibilityEnvironment(null, Map.of())).isInstanceOf(NullPointerException.class);
    }

    @Test
    void shouldKeepNativeDateAndResolvedCharacterSemanticsInTheHistoricalSchema() {
        final var profile = DamengCompatibilityRegistry.builtIn().resolve(DamengCompatibilityMode.NATIVE);
        assertThat(profile.parameterNames()).containsExactlyInAnyOrder("NLS_LENGTH_SEMANTICS", "NVARCHAR_LENGTH_IN_CHAR", "BACKSLASH_ESCAPE",
                "NESTED_C_STYLE_COMMENT", "IDENTIFIER_MODE", "NUMBER_MODE");
        for (var defaults : List.of(Map.of("BACKSLASH_ESCAPE", "0", "NESTED_C_STYLE_COMMENT", "0", "CASE_SENSITIVE", "1", "NUMBER_MODE", "0"),
                Map.of("NLS_LENGTH_SEMANTICS", "CHAR", "NVARCHAR_LENGTH_IN_CHAR", "1", "BACKSLASH_ESCAPE", "0", "NESTED_C_STYLE_COMMENT", "0",
                        "CASE_SENSITIVE", "1", "NUMBER_MODE", "0"))) {
            final var tables = new Tables();
            profile.createDdlParser(new DamengCompatibilityEnvironment(DamengCompatibilityMode.NATIVE, defaults), StandardCharsets.UTF_8)
                    .parse("CREATE TABLE APP.T (ID INT PRIMARY KEY, D DATE, C CHAR(8), N NCHAR(8))", tables, "APP");
            final var table = tables.forTable(new TableId(null, "APP", "T"));
            assertThat(table.columnWithName("D").jdbcType()).isEqualTo(Types.DATE);
            assertThat(table.columnWithName("D").scale()).contains(0);
            assertThat(table.columnWithName("C").typeExpression()).isEqualTo("CHAR(8 BYTE)");
            assertThat(table.columnWithName("N").typeExpression()).isEqualTo("NCHAR(8 BYTE)");
        }
    }

    static Stream<Arguments> nativeParameters() {
        return IntStream.range(0, 16).mapToObj(value -> Arguments.of(Integer.toString(value & 1),
                Integer.toString((value >> 1) & 1), Integer.toString((value >> 2) & 1), Integer.toString((value >> 3) & 1)));
    }

    @Test
    void shouldKeepRawNativeBackslashesOnTheLegacyBuild() {
        final var parameters = Map.of("CASE_SENSITIVE", "1", "NESTED_C_STYLE_COMMENT", "0",
                "NUMBER_MODE", "1", "BACKSLASH_ESCAPE", "1", "NATIVE_BACKSLASH_SEMANTICS", "raw", "NATIVE_REAL_SEMANTICS", "binary");
        final var parser = new DamengNativeCompatibilityProfile().createDdlParser(
                new DamengCompatibilityEnvironment(DamengCompatibilityMode.NATIVE, parameters), StandardCharsets.UTF_8);
        final var tables = new Tables();
        final var id = new TableId(null, "APP", "T");
        parser.parse("CREATE TABLE APP.T(ID INT, F FLOAT(24), R REAL, P DOUBLE PRECISION, V VARCHAR(100) DEFAULT 'a\\nb', Q VARCHAR(100) DEFAULT Q'[a\\nb]')", tables,
                "APP");
        assertThat(tables.forTable(id).columnWithName("F").jdbcType()).isEqualTo(Types.DECIMAL);
        assertThat(tables.forTable(id).columnWithName("R").jdbcType()).isEqualTo(Types.REAL);
        assertThat(tables.forTable(id).columnWithName("P").jdbcType()).isEqualTo(Types.DECIMAL);
        parser.parse("ALTER TABLE APP.T ADD (A VARCHAR(100) DEFAULT Q'[a\\nb]', B INT)", tables, "APP");
        assertThat(tables.forTable(id).columnWithName("V").defaultValueExpression()).contains("'a\\nb'");
        for (var name : List.of("Q", "A")) {
            assertThat(tables.forTable(id).columnWithName(name).defaultValueExpression()).contains("Q'[a\\nb]'");
        }
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = { "0", "unknown" })
    void shouldRejectUnobservedNativeBackslashSemantics(String semantics) {
        final Map<String, String> parameters = new HashMap<>(Map.of("CASE_SENSITIVE", "1", "NESTED_C_STYLE_COMMENT", "0",
                "NUMBER_MODE", "0", "BACKSLASH_ESCAPE", "1"));
        if (semantics != null) {
            parameters.put("NATIVE_BACKSLASH_SEMANTICS", semantics);
        }
        assertThatThrownBy(() -> new DamengNativeCompatibilityProfile().validate(new DamengCompatibilityEnvironment(DamengCompatibilityMode.NATIVE, parameters)))
                .hasMessageContaining("observed NATIVE_BACKSLASH_SEMANTICS");
    }

    @ParameterizedTest
    @MethodSource("nativeParameters")
    void shouldRebuildNativeNumericTypesAndDefaultsAcrossParameterCombinations(String floatMode, String numberMode, String dateFormat, String backslash) {
        final Map<String, String> parameters = new HashMap<>(Map.of("CASE_SENSITIVE", "1", "NESTED_C_STYLE_COMMENT", "0",
                "FLOAT_MODE", floatMode, "NUMBER_MODE", numberMode, "ORA_DATE_FMT", dateFormat, "BACKSLASH_ESCAPE", backslash));
        parameters.put("NATIVE_BACKSLASH_SEMANTICS", "escaped");
        parameters.put("NATIVE_REAL_SEMANTICS", "decimal");
        final var profile = new DamengNativeCompatibilityProfile();
        final var parser = profile.createDdlParser(new DamengCompatibilityEnvironment(DamengCompatibilityMode.NATIVE, parameters), StandardCharsets.UTF_8);
        final var tables = new Tables();
        final var id = new TableId(null, "APP", "T");
        parser.parse("CREATE TABLE APP.T(ID INT, D DATE, F FLOAT, SMALL_FLOAT FLOAT(24), R REAL, P DOUBLE PRECISION, "
                + "BF BINARY_FLOAT, BD BINARY_DOUBLE, N NUMBER(12,2), "
                + "V VARCHAR(100) DEFAULT E'a\\n''b', Q VARCHAR(100) DEFAULT Q'[a\\nb]', C VARCHAR(100) COMMENT 'a\\tb')", tables, "APP");
        final var initial = tables.forTable(id);
        assertThat(initial.columnWithName("D").jdbcType()).isEqualTo(Types.DATE);
        assertThat(initial.columnWithName("D").scale()).contains(0);
        for (var name : List.of("F", "SMALL_FLOAT", "R", "P")) {
            final var column = initial.columnWithName(name);
            if (numberMode.equals("1")) {
                assertThat(column.jdbcType()).as(name).isEqualTo(Types.DECIMAL);
                assertThat(column.scale()).isEmpty();
                assertThat(column.length()).isEqualTo(name.equals("SMALL_FLOAT") ? 24 : name.equals("R") ? 63 : 126);
            }
            else {
                assertThat(column.jdbcType()).as(name).isEqualTo(name.equals("F") ? Types.FLOAT : name.equals("P") ? Types.DOUBLE : Types.REAL);
            }
        }
        assertThat(initial.columnWithName("BF").jdbcType()).isEqualTo(Types.REAL);
        assertThat(initial.columnWithName("BD").jdbcType()).isEqualTo(Types.DOUBLE);
        assertThat(initial.columnWithName("N").scale()).contains(2);
        assertThat(initial.columnWithName("V").defaultValueExpression()).contains(backslash.equals("1") ? "'a\n''b'" : "E'a\\n''b'");
        assertThat(initial.columnWithName("C").comment()).isEqualTo(backslash.equals("1") ? "a\tb" : "a\\tb");
        parser.parse("ALTER TABLE APP.T ADD (ONE INT)", tables, "APP");
        assertThat(tables.forTable(id).columnWithName("Q")).isEqualTo(initial.columnWithName("Q"));
        parser.parse("ALTER TABLE APP.T ADD (A VARCHAR(100) DEFAULT Q'[a\\nb]', B INT)", tables, "APP");
        assertThat(tables.forTable(id).columnWithName("Q").defaultValueExpression()).contains(backslash.equals("1") ? "'a" + "\\".repeat(4) + "nb'" : "Q'[a\\nb]'");
        assertThat(tables.forTable(id).columnWithName("A").defaultValueExpression()).isEqualTo(tables.forTable(id).columnWithName("Q").defaultValueExpression());
        parser.parse("ALTER TABLE APP.T MODIFY SMALL_FLOAT FLOAT(53); ALTER TABLE APP.T ALTER COLUMN V SET DEFAULT 'x\\rb'", tables, "APP");
        assertThat(tables.forTable(id).columnWithName("SMALL_FLOAT").jdbcType()).isEqualTo(numberMode.equals("1") ? Types.DECIMAL : Types.FLOAT);
        assertThat(tables.forTable(id).columnWithName("V").defaultValueExpression()).contains(backslash.equals("1") ? "'x\rb'" : "'x\\rb'");
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = { "0", "unknown" })
    void shouldRejectUnobservedNativeRealSemantics(String semantics) {
        final Map<String, String> parameters = new HashMap<>(Map.of("CASE_SENSITIVE", "1", "NESTED_C_STYLE_COMMENT", "0",
                "NUMBER_MODE", "1", "BACKSLASH_ESCAPE", "0"));
        if (semantics != null) {
            parameters.put("NATIVE_REAL_SEMANTICS", semantics);
        }
        assertThatThrownBy(() -> new DamengNativeCompatibilityProfile().validate(new DamengCompatibilityEnvironment(DamengCompatibilityMode.NATIVE, parameters)))
                .hasMessageContaining("observed NATIVE_REAL_SEMANTICS");
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = { "-1", "2", "true" })
    void shouldRejectMissingOrUnknownNativeNumberMode(String value) {
        final Map<String, String> parameters = new HashMap<>(
                Map.of("CASE_SENSITIVE", "1", "BACKSLASH_ESCAPE", "0", "NESTED_C_STYLE_COMMENT", "0"));
        if (value != null) {
            parameters.put("NUMBER_MODE", value);
        }
        assertThatThrownBy(() -> new DamengNativeCompatibilityProfile().validate(new DamengCompatibilityEnvironment(DamengCompatibilityMode.NATIVE, parameters)))
                .hasMessageContaining("NUMBER_MODE=0 or 1");
    }

    private static DamengCompatibilityProfile profile(String id, DamengCompatibilityMode... modes) {
        final var profile = mock(DamengCompatibilityProfile.class);
        when(profile.id()).thenReturn(id);
        when(profile.modes()).thenReturn(Set.of(modes));
        return profile;
    }

    @ParameterizedTest
    @CsvSource({ "0,0", "0,1", "1,0", "1,1" })
    void shouldApplyNativeLexicalSettingsToAnalysisAndHistoricalDdl(String backslash, String nested) {
        final var environment = new DamengCompatibilityEnvironment(DamengCompatibilityMode.NATIVE,
                Map.of("BACKSLASH_ESCAPE", backslash, "NESTED_C_STYLE_COMMENT", nested, "CASE_SENSITIVE", "1", "NUMBER_MODE", "0",
                        "NATIVE_BACKSLASH_SEMANTICS", "escaped"));
        final var parser = new DamengNativeCompatibilityProfile().createDdlParser(environment, StandardCharsets.UTF_8);
        final var comment = "1".equals(nested) ? "/* outer /* ; DROP TABLE T; */ outer */" : "/* outer /* text ends here */";
        final var tables = new Tables();
        final var ddl = "CREATE" + comment + "TABLE APP.T(ID INT, V VARCHAR(100) DEFAULT 'a\\n\\\\b''c')";
        assertThat(parser.analyze(ddl, "APP").tableId()).isEqualTo(new TableId(null, "APP", "T"));
        assertThat(parser.plan(parser.analyze(ddl, "APP"), tables)).hasSize(1);
        assertThat(tables.size()).isZero();
        parser.parse(ddl, tables, "APP");
        assertThat(tables.forTable(new TableId(null, "APP", "T")).columnWithName("V").defaultValueExpression())
                .contains(backslash.equals("1") ? "'a\n\\\\b''c'" : "'a\\n\\\\b''c'");
        assertThat(parser.isNonTableDdl("COMMENT" + comment + "ON TABLE APP.T IS 'a\\n'")).isTrue();
        final var previous = tables.forTable(new TableId(null, "APP", "T"));
        assertThatThrownBy(() -> parser.parse("ALTER TABLE APP.T ADD W INT /* unclosed", tables, "APP"))
                .hasMessageContaining("Unclosed SQL comment");
        assertThat(tables.forTable(previous.id())).isEqualTo(previous);
        final var expressionComment = "1".equals(nested) ? "/* outer /* inner */ -- hidden */" : "/* -- hidden */";
        parser.parse("ALTER TABLE APP.T ADD W VARCHAR(10) DEFAULT 'a' " + expressionComment + " || 'b'", tables, "APP");
        assertThat(tables.forTable(previous.id()).columnWithName("W").defaultValueExpression()).contains("'a' || 'b'");
        parser.parse("ALTER TABLE APP.T ALTER COLUMN W SET DEFAULT 'c' " + expressionComment + " || 'd'", tables, "APP");
        assertThat(tables.forTable(previous.id()).columnWithName("W").defaultValueExpression()).contains("'c' || 'd'");
        final var beforeInvalidDefault = tables.forTable(previous.id());
        final var invalidDdl = "ALTER TABLE APP.T ALTER COLUMN W SET DEFAULT";
        assertThatThrownBy(() -> parser.parse(invalidDdl, tables, "APP"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("character " + invalidDdl.length(), "line 1, column " + (invalidDdl.length() + 1));
        assertThat(tables.forTable(previous.id())).isSameAs(beforeInvalidDefault);
        assertThat(tables.forTable(previous.id()).columnWithName("W").defaultValueExpression()).contains("'c' || 'd'");
        parser.parse("ALTER TABLE APP.T ADD AMOUNT DECIMAL(10,2) DEFAULT -.5", tables, "APP");
        assertThat(tables.forTable(previous.id()).columnWithName("AMOUNT").defaultValueExpression()).contains("-.5");
    }
}
