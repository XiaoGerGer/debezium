/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import io.debezium.connector.dameng.antlr.DamengDdlParser;
import io.debezium.connector.dameng.antlr.DamengDdlStatementAnalyzer;
import io.debezium.connector.dameng.antlr.DamengDdlStatementAnalyzer.Impact;
import io.debezium.connector.dameng.antlr.DamengParseTree;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;

class DamengBitmapJoinIndexScopeTest {
    private final DamengDdlStatementAnalyzer analyzer = new DamengDdlStatementAnalyzer();

    @ParameterizedTest
    @ValueSource(strings = {
            "CREATE BITMAP JOIN INDEX APP.BI ON APP.T(ID)",
            "CREATE BITMAP INDEX APP.BI ON APP.T(ID) FROM APP.T, APP.U WHERE T.ID=U.ID",
            "CREATE OR REPLACE BITMAP INDEX IF NOT EXISTS APP.BI ON APP.T(U.NAME) FROM APP.T, APP.U WHERE T.ID=U.ID"
    })
    void shouldSkipBitmapJoinIndexAfterRecognizingTheStatement(String sql) {
        assertThat(DamengParseTree.parseScope(sql, false).tree().statement(0).nonTableStatement().createIndex()).isNotNull();

        final var analysis = analyzer.analyze(sql, "OTHER").requireSupported();

        assertThat(analysis.impact()).isEqualTo(Impact.NO_TABLE_CHANGE);
        assertThat(analysis.tableId()).isNull();
        assertThat(new DamengDdlParser().isNonTableDdl(sql)).isTrue();
    }

    @Test
    void shouldSkipQualifiedAndUnqualifiedIndexesWithoutChangingDefaultSchema() {
        final var quoted = analyzer.analyze("CREATE BITMAP INDEX \"Case.Schema\".\"Case.Index\""
                + " ON APP.T(U.NAME) FROM APP.T, APP.U WHERE T.ID=U.ID", "OTHER");
        assertThat(quoted.requireSupported().impact()).isEqualTo(Impact.NO_TABLE_CHANGE);
        assertThat(quoted.defaultSchema()).isEqualTo("OTHER");

        final var unqualified = analyzer.analyze("CREATE BITMAP INDEX BI ON T(ID) FROM T,U WHERE T.ID=U.ID", "APP");
        assertThat(unqualified.requireSupported().impact()).isEqualTo(Impact.NO_TABLE_CHANGE);
        assertThat(unqualified.defaultSchema()).isEqualTo("APP");
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "CREATE INDEX APP.I ON APP.T(EXTRACT(DAY FROM D))",
            "CREATE BITMAP INDEX APP.I ON APP.T(EXTRACT(DAY FROM D))",
            "CREATE BITMAP INDEX APP.I ON APP.T(F('FROM',q'[FROM APP.U; DROP TABLE APP.T]'))",
            "CREATE BITMAP INDEX APP.I ON APP.T(\"FROM\")",
            "CREATE BITMAP INDEX APP.I ON APP.T(ID) /* FROM APP.U */"
    })
    void shouldSkipOrdinaryIndexesAndNestedFromExpressions(String sql) {
        final var analysis = analyzer.analyze(sql, "APP").requireSupported();

        assertThat(analysis.impact()).isEqualTo(Impact.NO_TABLE_CHANGE);
        assertThat(analysis.tableId()).isNull();
    }

    @Test
    void shouldApplyTableChangesAroundIndexStatementsWithoutCreatingAuxiliaryModels() {
        final var parser = new DamengDdlParser();
        final var tables = new Tables();
        final var id = new TableId(null, "APP", "T");
        parser.parse("CREATE TABLE APP.T(ID INT PRIMARY KEY)", tables, "APP");
        final var changes = parser.parse("ALTER TABLE APP.T ADD V INT;"
                + " CREATE BITMAP INDEX APP.BI ON APP.T(ID) FROM APP.T, APP.U WHERE T.ID=U.ID;"
                + " DROP INDEX APP.BI; ALTER TABLE APP.T ADD W INT", tables, "APP");

        assertThat(changes).hasSize(2).allSatisfy(change -> assertThat(change.type()).isEqualTo(DamengDdlParser.Type.ALTER));
        assertThat(tables.tableIds()).containsExactly(id);
        assertThat(tables.forTable(id).retrieveColumnNames()).containsExactly("ID", "V", "W");
    }
}
