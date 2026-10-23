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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

import io.debezium.relational.TableId;
import io.debezium.relational.Tables;

class DamengCompatibilityTest {
    @Test
    void shouldCanonicalizeCaseInsensitiveHistoricalIdentifiersWithoutChangingValues() {
        final var environment = new DamengCompatibilityEnvironment(new DamengCompatibilityMode("0"),
                Map.of("LENGTH_IN_CHAR", "0", "BACKSLASH_ESCAPE", "0", "NESTED_C_STYLE_COMMENT", "0", "CASE_SENSITIVE", "0"));
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
    @ValueSource(strings = { "9", "42", "2147483648", "99999999999999999999999999999999999999" })
    void shouldRepresentFutureServerModesWithoutAnEnumeratedUpperBound(String value) {
        final var mode = new DamengCompatibilityMode(value);
        assertThat(mode.value()).isEqualTo(value);
        assertThat(mode.toString()).isEqualTo(value);
        assertThatThrownBy(() -> DamengCompatibilityRegistry.builtIn().resolve(mode))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("COMPATIBLE_MODE=" + value, "registered modes: [0]");
    }

    @Test
    void shouldCanonicalizeIdentifiersBeforeLookingUpSemantics() {
        assertThat(new DamengCompatibilityMode(" 000 ")).isEqualTo(new DamengCompatibilityMode("0"));
        assertThat(DamengCompatibilityRegistry.builtIn().resolve(new DamengCompatibilityMode("000")).id()).isEqualTo("native-v1");
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = { " ", "-1", "+1", "oracle", "0.0", "1e2", "1 OR 1=1" })
    void shouldRejectMissingAndMalformedIdentifiers(String value) {
        assertThatThrownBy(() -> new DamengCompatibilityMode(value))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("COMPATIBLE_MODE");
    }

    @Test
    void shouldAllowAValidatedFutureImplementationToBeRegisteredWithoutChangingTheModeModel() {
        final var future = profile("future-v1", "42", "43");
        final List<DamengCompatibilityProfile> profiles = new ArrayList<>(List.of(new DamengNativeCompatibilityProfile(), future));
        final var registry = new DamengCompatibilityRegistry(profiles);
        profiles.clear();
        assertThat(registry.resolve(new DamengCompatibilityMode("42"))).isSameAs(future);
        assertThat(registry.resolve(new DamengCompatibilityMode("43"))).isSameAs(future);
        assertThat(registry.resolve(new DamengCompatibilityMode("0")).id()).isEqualTo("native-v1");
        assertThatThrownBy(() -> registry.registeredModes().clear()).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> registry.resolve(new DamengCompatibilityMode("44"))).hasMessageContaining("COMPATIBLE_MODE=44");
        // Registration in a test-local registry never enables the built-in production registry.
        assertThatThrownBy(() -> DamengCompatibilityRegistry.builtIn().resolve(new DamengCompatibilityMode("42")))
                .hasMessageContaining("No implemented Dameng compatibility profile");
    }

    @Test
    void shouldRejectAmbiguousRegistrationIncludingCanonicalModeAliases() {
        assertThatThrownBy(() -> new DamengCompatibilityRegistry(List.of(profile("first", "42"), profile("second", "042"))))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("COMPATIBLE_MODE=42", "first", "second");
    }

    @Test
    void shouldRequireIdentifiableProfilesAndRejectUnregisteredModesEvenWithAnEmptyRegistry() {
        assertThatThrownBy(() -> new DamengCompatibilityRegistry(List.of(profile(null, "42"))))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new DamengCompatibilityRegistry(List.of(profile(" ", "42"))))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new DamengCompatibilityRegistry(List.of(profile("empty"))))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new DamengCompatibilityRegistry(List.of()).resolve(new DamengCompatibilityMode("0")))
                .hasMessageContaining("registered modes: []");
        assertThatThrownBy(() -> DamengCompatibilityRegistry.builtIn().resolve(null)).isInstanceOf(NullPointerException.class);
    }

    @Test
    void shouldPreserveObservedParametersWithoutConflatingMissingAndZero() {
        final Map<String, String> values = new HashMap<>(Map.of("LENGTH_IN_CHAR", "0"));
        final var environment = new DamengCompatibilityEnvironment(new DamengCompatibilityMode("0"), values);
        values.put("LENGTH_IN_CHAR", "1");
        assertThat(environment.parameter("LENGTH_IN_CHAR")).isEqualTo("0");
        assertThat(environment.parameter("MISSING_PARAMETER")).isNull();
        assertThatThrownBy(() -> environment.parameters().clear()).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> new DamengCompatibilityEnvironment(null, Map.of())).isInstanceOf(NullPointerException.class);
    }

    @Test
    void shouldKeepNativeDateAndResolvedCharacterSemanticsInTheHistoricalSchema() {
        final var profile = DamengCompatibilityRegistry.builtIn().resolve(new DamengCompatibilityMode("0"));
        assertThat(profile.parameterNames()).containsExactlyInAnyOrder("LENGTH_IN_CHAR", "NLS_LENGTH_SEMANTICS", "NVARCHAR_LENGTH_IN_CHAR", "BACKSLASH_ESCAPE",
                "NESTED_C_STYLE_COMMENT", "IDENTIFIER_MODE");
        for (var defaults : List.of(Map.of("LENGTH_IN_CHAR", "0", "BACKSLASH_ESCAPE", "0", "NESTED_C_STYLE_COMMENT", "0", "CASE_SENSITIVE", "1"),
                Map.of("LENGTH_IN_CHAR", "0", "NLS_LENGTH_SEMANTICS", "CHAR", "NVARCHAR_LENGTH_IN_CHAR", "1", "BACKSLASH_ESCAPE", "0", "NESTED_C_STYLE_COMMENT", "0",
                        "CASE_SENSITIVE", "1"))) {
            final var tables = new Tables();
            profile.createDdlParser(new DamengCompatibilityEnvironment(new DamengCompatibilityMode("0"), defaults), StandardCharsets.UTF_8)
                    .parse("CREATE TABLE APP.T (ID INT PRIMARY KEY, D DATE, C CHAR(8), N NCHAR(8))", tables, "APP");
            final var table = tables.forTable(new TableId(null, "APP", "T"));
            assertThat(table.columnWithName("D").jdbcType()).isEqualTo(Types.DATE);
            assertThat(table.columnWithName("D").scale()).contains(0);
            assertThat(table.columnWithName("C").typeExpression()).isEqualTo("CHAR(8 BYTE)");
            assertThat(table.columnWithName("N").typeExpression()).isEqualTo("NCHAR(8 BYTE)");
        }
        final var missing = new DamengCompatibilityEnvironment(new DamengCompatibilityMode("0"), Map.of("CASE_SENSITIVE", "1"));
        assertThatThrownBy(() -> profile.createDdlParser(missing, StandardCharsets.UTF_8)).hasMessageContaining("LENGTH_IN_CHAR=0");
    }

    private static DamengCompatibilityProfile profile(String id, String... modes) {
        final var profile = mock(DamengCompatibilityProfile.class);
        when(profile.id()).thenReturn(id);
        when(profile.modes()).thenReturn(Set.copyOf(java.util.Arrays.stream(modes).map(DamengCompatibilityMode::new).toList()));
        return profile;
    }

    @ParameterizedTest
    @CsvSource({ "0,0", "0,1", "1,0", "1,1" })
    void shouldApplyNativeLexicalSettingsToAnalysisAndHistoricalDdl(String backslash, String nested) {
        final var environment = new DamengCompatibilityEnvironment(new DamengCompatibilityMode("0"),
                Map.of("LENGTH_IN_CHAR", "0", "BACKSLASH_ESCAPE", backslash, "NESTED_C_STYLE_COMMENT", nested, "CASE_SENSITIVE", "1"));
        final var parser = new DamengNativeCompatibilityProfile().createDdlParser(environment, StandardCharsets.UTF_8);
        final var comment = "1".equals(nested) ? "/* outer /* ; DROP TABLE T; */ outer */" : "/* outer /* text ends here */";
        final var tables = new Tables();
        final var ddl = "CREATE" + comment + "TABLE APP.T(ID INT, V VARCHAR(100) DEFAULT 'a\\n\\\\b''c')";
        assertThat(parser.analyze(ddl, "APP").tableId()).isEqualTo(new TableId(null, "APP", "T"));
        assertThat(parser.plan(parser.analyze(ddl, "APP"), tables)).hasSize(1);
        assertThat(tables.size()).isZero();
        parser.parse(ddl, tables, "APP");
        assertThat(tables.forTable(new TableId(null, "APP", "T")).columnWithName("V").defaultValueExpression())
                .contains("'a\\n\\\\b''c'");
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
        assertThatThrownBy(() -> parser.parse("ALTER TABLE APP.T ALTER COLUMN W SET DEFAULT", tables, "APP"))
                .hasMessageContaining("Empty SQL expression");
        assertThat(tables.forTable(previous.id()).columnWithName("W").defaultValueExpression()).contains("'c' || 'd'");
        parser.parse("ALTER TABLE APP.T ADD AMOUNT DECIMAL(10,2) DEFAULT -.5", tables, "APP");
        assertThat(tables.forTable(previous.id()).columnWithName("AMOUNT").defaultValueExpression()).contains("-.5");
    }
}
