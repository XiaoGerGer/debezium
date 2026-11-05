/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import io.debezium.connector.dameng.antlr.DamengDdlParser;
import io.debezium.connector.dameng.antlr.DamengDdlStatementAnalyzer;
import io.debezium.connector.dameng.antlr.DamengDdlStatementAnalyzer.Impact;
import io.debezium.relational.Attribute;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;

class DamengNonTableDdlTest {
    private static final TableId CAPTURED = new TableId(null, "APP", "A");

    @ParameterizedTest
    @MethodSource("io.debezium.connector.dameng.DamengDdlTestCases#safeStatements")
    void shouldRecognizeCompleteMaintenanceDdlWithoutChangingHistoricalTables(String sql) {
        final var parser = new DamengDdlParser();
        final var tables = historicalTables(parser);
        final var original = tables.forTable(CAPTURED);

        final var analysis = parser.analyze(sql, "APP");

        assertThat(analysis.impact()).isEqualTo(Impact.NO_TABLE_CHANGE);
        assertThat(analysis.tableId()).isNull();
        assertThat(analysis.renamedTableId()).isNull();
        assertThat(parser.isNonTableDdl(sql)).isTrue();
        assertThat(parser.plan(analysis, tables)).isEmpty();
        assertThat(parser.parse(sql, tables, "APP")).isEmpty();
        assertThat(tables.tableIds()).containsExactly(CAPTURED);
        assertThat(tables.forTable(CAPTURED)).isSameAs(original);
        assertThat(original.retrieveColumnNames()).containsExactly("ID", "V");
        assertThat(original.primaryKeyColumnNames()).containsExactly("ID");
        assertThat(original.columnWithName("V").defaultValueExpression()).contains("'before'");
        assertThat(original.attributeWithName("dameng.object.id").asLong()).isEqualTo(7L);
    }

    @ParameterizedTest
    @MethodSource("io.debezium.connector.dameng.DamengDdlTestCases#unsafeStatements")
    void shouldRejectCompoundOrIncompleteRecordsBeforeChangingHistoricalTables(String sql) {
        final var parser = new DamengDdlParser();
        final var tables = historicalTables(parser);
        final var original = tables.forTable(CAPTURED);

        assertThatThrownBy(() -> parser.analyze(sql, "APP")).isInstanceOf(IllegalArgumentException.class);
        assertThat(tables.forTable(CAPTURED)).isSameAs(original);
        assertThat(original.retrieveColumnNames()).containsExactly("ID", "V");
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "DROP USER CDC_USER CASCADE",
            "DROP SCHEMA CDC_SCHEMA CASCADE",
            "DROP DOMAIN APP.CDC_DOMAIN CASCADE",
            "CREATE SCHEMA CDC_SCHEMA AUTHORIZATION SYSDBA CREATE TABLE A(ID INT)"
    })
    void shouldRejectUnsafeMaintenanceBeforeApplyingEarlierHistoryScriptChanges(String sql) {
        final var parser = new DamengDdlParser();
        final var tables = historicalTables(parser);
        final var original = tables.forTable(CAPTURED);

        assertThatThrownBy(() -> parser.parse("ALTER TABLE APP.A ADD BAD INT;" + sql, tables, "APP"))
                .isInstanceOf(IllegalArgumentException.class);

        assertThat(tables.forTable(CAPTURED)).isSameAs(original);
        assertThat(original.retrieveColumnNames()).containsExactly("ID", "V");
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "CREATE USER CDC_USER IDENTIFIED BY Test_123456",
            "ALTER USER CDC_USER DROP PROFILE",
            "CREATE SCHEMA CDC_SCHEMA AUTHORIZATION SYSDBA",
            "DROP SCHEMA CDC_SCHEMA RESTRICT",
            "CREATE DIRECTORY CDC_DIRECTORY AS '/tmp/cdc'",
            "DROP DIRECTORY CDC_DIRECTORY",
            "CREATE DOMAIN APP.CDC_DOMAIN INT",
            "DROP DOMAIN APP.CDC_DOMAIN RESTRICT",
            "CREATE CONTEXT CDC_CONTEXT USING APP.CDC_PACKAGE",
            "DROP CONTEXT CDC_CONTEXT",
            "CREATE PROFILE CDC_PROFILE LIMIT SESSION_PER_USER 10",
            "ALTER PROFILE CDC_PROFILE LIMIT SESSION_PER_USER 10",
            "DROP PROFILE CDC_PROFILE CASCADE",
            "CREATE LIBRARY APP.CDC_LIBRARY AS '/tmp/libcdc.so'",
            "ALTER LIBRARY APP.CDC_LIBRARY COMPILE",
            "DROP LIBRARY APP.CDC_LIBRARY",
            "CREATE TYPE APP.CDC_TYPE AS OBJECT(ID INT)",
            "DROP TYPE APP.CDC_TYPE",
            "DROP TYPE BODY APP.CDC_TYPE",
            "DROP CLASS APP.CDC_CLASS",
            "DROP CLASS BODY APP.CDC_CLASS",
            "ALTER TYPE APP.CDC_TYPE COMPILE DEBUG",
            "ALTER CLASS APP.CDC_CLASS COMPILE DEBUG",
            "ALTER PACKAGE APP.CDC_PACKAGE COMPILE CASCADE DEBUG",
            "DROP PACKAGE BODY APP.CDC_PACKAGE",
            "STAT ON APP.A"
    })
    void shouldKeepTableDdlVisibleAfterAnUnterminatedMaintenanceStatement(String sql) {
        final var analyzer = new DamengDdlStatementAnalyzer();

        for (final var delimiter : new String[]{ " ", "; " }) {
            final var analysis = analyzer.analyze(sql + delimiter + "ALTER TABLE APP.A ADD BAD INT", "APP");
            assertThat(analysis.impact()).isEqualTo(Impact.UNSUPPORTED);
            assertThatThrownBy(analysis::requireSupported).isInstanceOf(IllegalArgumentException.class);
        }
    }

    private static Tables historicalTables(DamengDdlParser parser) {
        final var tables = new Tables();
        parser.parse("CREATE TABLE APP.A(ID INT PRIMARY KEY,V VARCHAR(20) DEFAULT 'before')", tables, "APP");
        final var table = tables.forTable(CAPTURED);
        tables.overwriteTable(table.edit().addAttributes(table.attributes())
                .addAttribute(Attribute.editor().name("dameng.object.id").value("7").create()).create());
        return tables;
    }

    @Test
    void shouldKeepObjectKeywordsAvailableAsColumnAndExpressionNames() {
        final var parser = new DamengDdlParser();
        final var tables = new Tables();
        final var names = new String[]{ "ALLOW_IP", "AUTHORIZATION", "BODY", "EXTERNALLY", "IDENTIFIED", "NOT_ALLOW_IP", "PROFILE", "SIZE", "THROUGH",
                "ABSTRACT", "LINK", "LOGFILE", "OPERATOR", "SYSTEM" };
        final var columns = String.join(" INT,", names) + " INT";
        parser.parse("CREATE TABLE APP.KEYWORDS(" + columns + ")", tables, "APP");
        assertThat(tables.forTable(null, "APP", "KEYWORDS").retrieveColumnNames()).containsExactly(names);
        final var analysis = parser.analyze("CREATE TABLE APP.COPIED AS SELECT " + String.join(",", names) + " FROM APP.KEYWORDS", "APP");
        assertThat(analysis.isCtas()).isTrue();
        assertThat(analysis.tableId()).isEqualTo(new TableId(null, "APP", "COPIED"));
    }
}
