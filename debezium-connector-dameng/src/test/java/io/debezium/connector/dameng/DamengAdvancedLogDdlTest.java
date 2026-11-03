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
import org.junit.jupiter.params.provider.ValueSource;

import io.debezium.connector.dameng.antlr.DamengDdlParser;
import io.debezium.connector.dameng.antlr.DamengDdlStatementAnalyzer;
import io.debezium.connector.dameng.antlr.DamengDdlStatementAnalyzer.Analysis;
import io.debezium.connector.dameng.antlr.DamengDdlStatementAnalyzer.Impact;
import io.debezium.connector.dameng.antlr.DamengParseTree;
import io.debezium.relational.Attribute;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;

class DamengAdvancedLogDdlTest {
    private static final TableId BASE = new TableId(null, "APP", "T");
    private static final TableId AUXILIARY = new TableId(null, "APP", "T$ALOG");

    private final DamengDdlParser parser = new DamengDdlParser();

    @ParameterizedTest
    @ValueSource(strings = {
            "CREATE TABLE APP.T(ID INT PRIMARY KEY, V VARCHAR(80)) WITH ADVANCED LOG",
            "ALTER TABLE APP.T WITH ADVANCED LOG",
            "ALTER TABLE APP.T WITHOUT ADVANCED LOG",
            "ALTER TABLE APP.T TRUNCATE ADVANCED LOG"
    })
    void shouldRecognizeTheSyntaxAndRejectItsImplicitAuxiliaryTableScope(String sql) {
        final var parsed = DamengParseTree.parseScope(sql, false);
        assertThat(parsed.tree().statement(0).tableStatement()).isNotNull();

        final var analysis = new DamengDdlStatementAnalyzer().analyze(sql, "OTHER");

        assertThat(analysis.impact()).isEqualTo(Impact.UNSUPPORTED);
        assertThatThrownBy(analysis::requireSupported).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("ADVANCED LOG", "implicit auxiliary table APP.T$ALOG", "character", "line");
        assertThat(parser.isNonTableDdl(sql)).isFalse();
    }

    @Test
    void shouldResolveTheImplicitTableInTheStatementSchemaBeforeFiltering() {
        final var analysis = new DamengDdlStatementAnalyzer()
                .analyze("ALTER TABLE \"Case.Schema\".\"Case.Table\" WITHOUT ADVANCED LOG", "OTHER");

        assertThat(analysis.impact()).isEqualTo(Impact.UNSUPPORTED);
        assertThatThrownBy(analysis::requireSupported)
                .hasMessageContaining("implicit auxiliary table Case.Schema.Case.Table$ALOG");
        assertThatThrownBy(() -> parser.analyze("ALTER TABLE T TRUNCATE ADVANCED LOG", "APP"))
                .hasMessageContaining("implicit auxiliary table APP.T$ALOG");
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "CREATE TABLE APP.T(ID INT) WITHOUT ADVANCED LOG",
            "CREATE TABLE APP.T(ID INT) TRUNCATE ADVANCED LOG",
            "ALTER TABLE APP.T ADVANCED LOG",
            "ALTER TABLE APP.T WITH ADVANCED",
            "ALTER TABLE APP.T TRUNCATE ADVANCED LOG EXTRA"
    })
    void shouldRejectIncompleteOrMisplacedAdvancedLogClauses(String sql) {
        assertThatThrownBy(() -> DamengParseTree.parseScope(sql, false))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("Unsupported table DDL");
    }

    @ParameterizedTest
    @ValueSource(strings = { "WITH", "WITHOUT", "TRUNCATE" })
    void shouldKeepTheBaseAndAlreadyCapturedAuxiliaryHistoryUnchanged(String operation) {
        final var tables = historicalTables();
        final var base = tables.forTable(BASE);
        final var auxiliary = tables.forTable(AUXILIARY);

        assertThatThrownBy(() -> parser.parse("ALTER TABLE APP.T " + operation + " ADVANCED LOG", tables, "APP"))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("implicit auxiliary table APP.T$ALOG");

        assertThat(tables.tableIds()).containsExactlyInAnyOrder(BASE, AUXILIARY);
        assertThat(tables.forTable(BASE)).isSameAs(base);
        assertThat(tables.forTable(AUXILIARY)).isSameAs(auxiliary);
        assertThat(base.columnWithName("V").defaultValueExpression()).contains("'before'");
        assertThat(base.primaryKeyColumnNames()).containsExactly("ID");
        assertThat(base.attributeWithName("dameng.object.id").asLong()).isEqualTo(101L);
        assertThat(auxiliary.attributeWithName("dameng.object.id").asLong()).isEqualTo(202L);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "CREATE TABLE APP.NEW_TABLE(ID INT PRIMARY KEY) WITH ADVANCED LOG",
            "ALTER TABLE APP.T WITH ADVANCED LOG",
            "ALTER TABLE APP.T WITHOUT ADVANCED LOG",
            "ALTER TABLE APP.T TRUNCATE ADVANCED LOG"
    })
    void shouldAlsoRejectAuxiliaryChangesWhenPlanningAnAlreadyParsedTree(String sql) {
        final var tables = historicalTables();
        final var base = tables.forTable(BASE);
        final var auxiliary = tables.forTable(AUXILIARY);
        final var parsed = DamengParseTree.parseScope(sql, false);
        final var target = sql.startsWith("CREATE") ? new TableId(null, "APP", "NEW_TABLE") : BASE;
        final var analysis = new Analysis(sql, parsed.tokens(), Impact.TABLE, target, null, null,
                parsed.tree().statement(0).tableStatement(), "APP");

        assertThatThrownBy(() -> parser.plan(analysis, tables)).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("ADVANCED LOG", "implicit auxiliary table");

        assertThat(tables.tableIds()).containsExactlyInAnyOrder(BASE, AUXILIARY);
        assertThat(tables.forTable(BASE)).isSameAs(base);
        assertThat(tables.forTable(AUXILIARY)).isSameAs(auxiliary);
    }

    @Test
    void shouldRejectTheWholeHistoryScriptBeforeApplyingAnEarlierTableChange() {
        final var tables = historicalTables();
        final var base = tables.forTable(BASE);
        final var auxiliary = tables.forTable(AUXILIARY);

        assertThatThrownBy(() -> parser.parse("ALTER TABLE APP.T ADD NEW_COLUMN INT;"
                + " ALTER TABLE APP.T TRUNCATE ADVANCED LOG; DROP TABLE APP.T", tables, "APP"))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("implicit auxiliary table APP.T$ALOG");

        assertThat(tables.tableIds()).containsExactlyInAnyOrder(BASE, AUXILIARY);
        assertThat(tables.forTable(BASE)).isSameAs(base);
        assertThat(tables.forTable(AUXILIARY)).isSameAs(auxiliary);
        assertThat(base.columnWithName("NEW_COLUMN")).isNull();
    }

    @Test
    void shouldKeepAdvancedAvailableAsAContextualObjectName() {
        final var tables = new Tables();

        parser.parse("CREATE TABLE APP.ADVANCED(ID INT PRIMARY KEY, ADVANCED VARCHAR(20)) TABLESPACE ADVANCED;"
                + " ALTER TABLE APP.ADVANCED ADD LOGIC LOG", tables, "APP");

        final var table = tables.forTable(null, "APP", "ADVANCED");
        assertThat(table.retrieveColumnNames()).containsExactly("ID", "ADVANCED");
        assertThat(table.primaryKeyColumnNames()).containsExactly("ID");
    }

    private Tables historicalTables() {
        final var tables = new Tables();
        parser.parse("CREATE TABLE APP.T(ID INT PRIMARY KEY, V VARCHAR(80) DEFAULT 'before');"
                + " CREATE TABLE APP.T$ALOG(ORG_ROWID BIGINT NOT NULL, OP_TYPE SMALLINT NOT NULL,"
                + " COLMAP VARBINARY(2048), COL_0 INT, PRIMARY KEY(ORG_ROWID, OP_TYPE))", tables, "APP");
        final var base = tables.forTable(BASE);
        tables.overwriteTable(base.edit().addAttributes(base.attributes())
                .addAttribute(Attribute.editor().name("dameng.object.id").value("101").create()).create());
        final var auxiliary = tables.forTable(AUXILIARY);
        tables.overwriteTable(auxiliary.edit().addAttributes(auxiliary.attributes())
                .addAttribute(Attribute.editor().name("dameng.object.id").value("202").create()).create());
        return tables;
    }
}
