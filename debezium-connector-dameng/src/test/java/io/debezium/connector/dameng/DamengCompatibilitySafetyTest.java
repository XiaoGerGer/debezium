/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

import io.debezium.connector.dameng.compatibility.DamengCompatibilityEnvironment;
import io.debezium.connector.dameng.compatibility.DamengCompatibilityMode;
import io.debezium.connector.dameng.compatibility.DamengCompatibilityRegistry;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.jdbc.JdbcConnection.ResultSetMapper;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;

class DamengCompatibilitySafetyTest {
    private final DamengConnectorConfig config = new DamengConnectorConfig(DamengTestConfiguration.configuration().build());

    private DamengCompatibilityEnvironment environment(String identifier) {
        final Map<String, String> parameters = new HashMap<>(Map.of("LENGTH_IN_CHAR", "0", "CASE_SENSITIVE", "1",
                "BACKSLASH_ESCAPE", "0", "NESTED_C_STYLE_COMMENT", "0"));
        if (identifier != null) {
            parameters.put("IDENTIFIER_MODE", identifier);
        }
        return new DamengCompatibilityEnvironment(DamengCompatibilityMode.NATIVE, parameters);
    }

    @Test
    void shouldPersistSettingsAndRejectChangedUnknownOrLegacyHistory() {
        final var offset = new DamengOffsetContext(config, 100);
        offset.validateCompatibilityEnvironment(environment("0"));
        final var recovered = new DamengOffsetContext.Loader(config).load(offset.getOffset());
        recovered.validateCompatibilityEnvironment(environment("0"));
        final Map<String, String> insensitive = new HashMap<>(environment("0").parameters());
        insensitive.put("CASE_SENSITIVE", "0");
        assertThatThrownBy(() -> recovered.validateCompatibilityEnvironment(
                new DamengCompatibilityEnvironment(DamengCompatibilityMode.NATIVE, insensitive)))
                .hasMessageContaining("differ from the stored offset");
        assertThat(recovered.getOffset()).isEqualTo(offset.getOffset());
        assertThatThrownBy(() -> recovered.validateCompatibilityEnvironment(environment("1"))).hasMessageContaining("differ from the stored offset");
        assertThatThrownBy(() -> recovered.validateCompatibilityEnvironment(environment(null))).hasMessageContaining("differ from the stored offset");
        assertThat(recovered.getOffset()).isEqualTo(offset.getOffset());
        final Map<String, Object> legacy = new HashMap<>(offset.getOffset());
        legacy.remove(DamengOffsetContext.COMPATIBILITY_SIGNATURE_KEY);
        assertThatThrownBy(() -> new DamengOffsetContext.Loader(config).load(legacy).validateCompatibilityEnvironment(environment("0")))
                .hasMessageContaining("no compatibility signature");
        for (var invalid : List.of(42, "", "v2:future")) {
            legacy.put(DamengOffsetContext.COMPATIBILITY_SIGNATURE_KEY, invalid);
            assertThatThrownBy(() -> new DamengOffsetContext.Loader(config).load(legacy)).hasMessageContaining("Invalid or unsupported");
        }
    }

    @ParameterizedTest
    @NullSource
    @ValueSource(strings = { "0", "1" })
    void shouldShareIdentifierNormalizationAcrossScopeAndHistoricalParsing(String mode) {
        final var environment = environment(mode);
        final var parser = DamengCompatibilityRegistry.builtIn().resolve(environment.mode()).createDdlParser(environment, StandardCharsets.UTF_8);
        final var tables = new Tables();
        final var ddl = "CREATE TABLE \"app\".\"MiXeD\"(\"id\" INT PRIMARY KEY, \"v\" VARCHAR(20))";
        final var id = "1".equals(mode) ? new TableId(null, "APP", "MIXED") : new TableId(null, "app", "MiXeD");
        assertThat(parser.analyze(ddl, "SYSDBA").tableId()).isEqualTo(id);
        parser.parse(ddl, tables, "SYSDBA");
        assertThat(tables.forTable(id).retrieveColumnNames()).containsExactly("1".equals(mode) ? "ID" : "id", "1".equals(mode) ? "V" : "v");
        final var original = tables.forTable(id);
        final var invalidDdl = "CREATE TABLE 42(ID INT)";
        final var invalidPosition = invalidDdl.indexOf("42");
        assertThatThrownBy(() -> parser.parse(invalidDdl, tables, "SYSDBA"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("character " + invalidPosition, "line 1, column " + (invalidPosition + 1));
        assertThat(tables.tableIds()).containsExactly(id);
        assertThat(tables.forTable(id)).isSameAs(original);
    }

    @ParameterizedTest
    @ValueSource(strings = { "2", "7" })
    void shouldPersistTypeSettingsAndRejectSwitchingSupportedModes(String mode) {
        final Map<String, String> parameters = new HashMap<>(environment(null).parameters());
        parameters.putAll(Map.of("FLOAT_MODE", "0", "NUMBER_MODE", "0", "ORA_DATE_FMT", "0"));
        final var compatible = new DamengCompatibilityEnvironment(DamengCompatibilityMode.parse(mode), parameters);
        final var offset = new DamengOffsetContext(config, 100);
        offset.validateCompatibilityEnvironment(compatible);
        final var recovered = new DamengOffsetContext.Loader(config).load(offset.getOffset());
        recovered.validateCompatibilityEnvironment(compatible);
        assertThatThrownBy(() -> recovered.validateCompatibilityEnvironment(environment(null))).hasMessageContaining("differ from the stored offset");
        final var otherMode = mode.equals("2") ? "7" : "2";
        assertThatThrownBy(() -> recovered.validateCompatibilityEnvironment(new DamengCompatibilityEnvironment(DamengCompatibilityMode.parse(otherMode), parameters)))
                .hasMessageContaining("differ from the stored offset");
        parameters.put("FLOAT_MODE", "1");
        assertThatThrownBy(() -> recovered.validateCompatibilityEnvironment(new DamengCompatibilityEnvironment(compatible.mode(), parameters)))
                .hasMessageContaining("differ from the stored offset");
        assertThat(recovered.getOffset()).isEqualTo(offset.getOffset());
        final var nativeOffset = new DamengOffsetContext(config, 100);
        nativeOffset.validateCompatibilityEnvironment(environment(null));
        assertThatThrownBy(() -> nativeOffset.validateCompatibilityEnvironment(compatible)).hasMessageContaining("differ from the stored offset");
    }

    @Test
    void shouldRejectUnverifiedIdentifierEnvironments() {
        final var profile = DamengCompatibilityRegistry.builtIn().resolve(DamengCompatibilityMode.NATIVE);
        final Map<String, String> parameters = new HashMap<>(environment("0").parameters());
        for (var invalid : List.of("", "2")) {
            parameters.put("CASE_SENSITIVE", invalid);
            assertThatThrownBy(() -> profile.validate(new DamengCompatibilityEnvironment(DamengCompatibilityMode.NATIVE, parameters)))
                    .hasMessageContaining("CASE_SENSITIVE=0 or 1");
        }
        parameters.put("CASE_SENSITIVE", "1");
        parameters.put("IDENTIFIER_MODE", "2");
        assertThatThrownBy(() -> profile.validate(new DamengCompatibilityEnvironment(DamengCompatibilityMode.NATIVE, parameters)))
                .hasMessageContaining("IDENTIFIER_MODE=0 or 1");
    }

    @ParameterizedTest
    @ValueSource(strings = { "0", "1" })
    void shouldRejectChangingNumberModeWhenRecoveringAnOracleOffset(String numberMode) {
        final Map<String, String> parameters = new HashMap<>(environment(null).parameters());
        parameters.putAll(Map.of("FLOAT_MODE", "0", "NUMBER_MODE", numberMode, "ORA_DATE_FMT", "0"));
        final var initial = new DamengCompatibilityEnvironment(DamengCompatibilityMode.ORACLE, parameters);
        DamengCompatibilityRegistry.builtIn().resolve(initial.mode()).validate(initial);
        final var offset = new DamengOffsetContext(config, 100);
        offset.validateCompatibilityEnvironment(initial);
        final var recovered = new DamengOffsetContext.Loader(config).load(offset.getOffset());
        recovered.validateCompatibilityEnvironment(initial);
        parameters.put("NUMBER_MODE", numberMode.equals("0") ? "1" : "0");
        final var changed = new DamengCompatibilityEnvironment(initial.mode(), parameters);
        DamengCompatibilityRegistry.builtIn().resolve(changed.mode()).validate(changed);
        assertThatThrownBy(() -> recovered.validateCompatibilityEnvironment(changed)).hasMessageContaining("differ from the stored offset");
        assertThat(recovered.getOffset()).isEqualTo(offset.getOffset());
    }

    @ParameterizedTest
    @ValueSource(strings = { "0", "2", "7" })
    void shouldCheckLiveParametersWithoutRebindingAChangedSnapshotOrOffset(String mode) throws Exception {
        final var connection = spy(new DamengConnection(JdbcConfiguration.create().build()));
        final Map<String, String> parameters = new HashMap<>(environment("0").parameters());
        if (!mode.equals("0")) {
            parameters.putAll(Map.of("FLOAT_MODE", "0", "NUMBER_MODE", "0", "ORA_DATE_FMT", "0"));
        }
        final var environment = new DamengCompatibilityEnvironment(DamengCompatibilityMode.parse(mode), parameters);
        doReturn(environment).when(connection).getCompatibilityEnvironment();
        assertThatThrownBy(connection::validateUnchangedCompatibility).hasMessageContaining("not been bound");
        final var offset = new DamengOffsetContext(config, 100);
        connection.validateCompatibilityEnvironment(offset);
        final List<Map.Entry<String, String>> entries = new java.util.ArrayList<>(environment.parameters().entrySet());
        entries.removeIf(e -> e.getKey().equals("CASE_SENSITIVE"));
        entries.add(Map.entry("COMPATIBLE_MODE", mode));
        final var index = new AtomicInteger(-1);
        final var rows = mock(ResultSet.class);
        when(rows.next()).thenAnswer(invocation -> index.incrementAndGet() < entries.size());
        when(rows.getString(1)).thenAnswer(invocation -> entries.get(index.get()).getKey());
        when(rows.getString(2)).thenAnswer(invocation -> entries.get(index.get()).getValue());
        when(rows.getString(3)).thenReturn("1");
        doAnswer(invocation -> {
            if (!mode.equals("0")) {
                assertThat((String) invocation.getArgument(0)).contains("'FLOAT_MODE'", "'NUMBER_MODE'", "'ORA_DATE_FMT'");
            }
            index.set(-1);
            return ((ResultSetMapper<?>) invocation.getArgument(1)).apply(rows);
        }).when(connection).queryAndMap(anyString(), any());
        connection.validateUnchangedCompatibility();
        final var changedParameter = mode.equals("2") ? "NUMBER_MODE" : mode.equals("7") ? "FLOAT_MODE" : "NESTED_C_STYLE_COMMENT";
        entries.removeIf(e -> e.getKey().equals(changedParameter));
        entries.add(Map.entry(changedParameter, "1"));
        assertThatThrownBy(connection::validateUnchangedCompatibility).hasMessageContaining("changed while the task was running");
        doReturn(environment("1")).when(connection).getCompatibilityEnvironment();
        assertThatThrownBy(() -> connection.validateCompatibilityEnvironment(new DamengOffsetContext(config, 200)))
                .hasMessageContaining("changed during snapshot");
    }
}
