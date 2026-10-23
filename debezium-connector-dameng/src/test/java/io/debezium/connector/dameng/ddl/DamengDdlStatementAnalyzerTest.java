/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng.ddl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import io.debezium.connector.dameng.DamengDdlParser;
import io.debezium.connector.dameng.ddl.DamengDdlStatementAnalyzer.Impact;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;

class DamengDdlStatementAnalyzerTest {
    private final DamengDdlStatementAnalyzer analyzer = new DamengDdlStatementAnalyzer();

    @ParameterizedTest
    @ValueSource(strings = {
            "COMMENT ON TABLE APP.T IS q'[it's; DROP TABLE T]'",
            "COMMENT ON COLUMN APP.T.ID IS NULL;",
            "COMMENT ON COLUMN T.\"DROP\" IS n'字段'",
            "GRANT CREATE TABLE, CREATE VIEW TO U,V WITH ADMIN OPTION; REVOKE SELECT ON T FROM U",
            "CREATE INDEX I ON T(\"DROP\"); DROP INDEX I;",
            "CREATE OR REPLACE PROCEDURE APP.P AS BEGIN NULL; BEGIN NULL; END; END P;",
            "CREATE FUNCTION F RETURN INT AS BEGIN RETURN CASE WHEN 1=1 THEN 1 ELSE 0 END; END;",
            "CREATE TRIGGER TR BEFORE INSERT ON T BEGIN IF 1=1 THEN NULL; END IF; FOR I IN 1..2 LOOP NULL; END LOOP; END;"
    })
    void shouldProveTheCompleteInputHasNoRowSchemaChange(String sql) {
        final var result = analyzer.analyze(sql, "APP").requireSupported();
        assertThat(result.impact()).isEqualTo(Impact.NO_TABLE_CHANGE);
        assertThat(result.tableId()).isNull();
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "CREATE INDEX I ON T(ID); ALTER TABLE APP.T ADD X INT",
            "CREATE VIEW V AS SELECT ID FROM T; DROP TABLE T",
            "CREATE PROCEDURE P AS BEGIN NULL; END; ALTER TABLE T ADD X INT",
            "CREATE FUNCTION F RETURN INT AS BEGIN RETURN 1; END; UNKNOWN STATEMENT",
            "CREATE INDEX I ON T(ID) ALTER TABLE T ADD X INT",
            "CREATE TABLE T(ID INT) CREATE TABLE U(ID INT)",
            "GRANT SELECT TO U; UNKNOWN STATEMENT",
            "GRANT SELECT TO U ALTER TABLE T ADD X INT",
            "GRANT TO U", "GRANT SELECT", "GRANT SELECT TO", "GRANT SELECT TO U WITH WRONG OPTION",
            "REVOKE SELECT TO U", "COMMENT ON TABLE T IS 42", "COMMENT ON TABLE A.B.C IS 'x'",
            "COMMENT ON COLUMN A IS 'x'", "COMMENT ON INDEX I IS 'x'", "COMMENT ON TABLE T IS 'x' UNKNOWN",
            "CREATE SCHEMA S AUTHORIZATION SYSDBA CREATE TABLE A(ID INT) CREATE TABLE B(ID INT)",
            "DROP SCHEMA S CASCADE", "DROP USER U CASCADE", "ALTER SYSTEM SWITCH LOGFILE",
            "\"CREATE\" TABLE T(ID INT)", "CREATE \"INDEX\" I", "CREATE OR WRONG VIEW V AS SELECT 1",
            "CREATE PROCEDURE P AS BEGIN NULL;", "CREATE PROCEDURE P AS X; BEGIN NULL; END;",
            "/* outer /* inner */ residual */ ALTER TABLE T ADD X INT",
            "ALTER TABLE", "RENAME TABLE T TO", "RENAME TABLE T TO U UNKNOWN", "DROP TABLE 'T'", "CREATE", "", ";"
    })
    void shouldFailClosedForUnknownOrCompoundScope(String sql) {
        final var result = analyzer.analyze(sql, "APP");
        assertThat(result.impact()).isEqualTo(Impact.UNSUPPORTED);
        assertThatThrownBy(result::requireSupported).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("character", "line");
    }

    @Test
    void shouldResolveQuotedTargetsAndRenameWithoutTheHistoricalModel() {
        final var result = analyzer.analyze("ALTER/*comment*/TABLE \"a\"\"b\".$t RENAME TO \"DROP\";", "OTHER").requireSupported();
        assertThat(result.tableId()).isEqualTo(new TableId(null, "a\"b", "$T"));
        assertThat(result.renamedTableId()).isEqualTo(new TableId(null, "a\"b", "DROP"));
        assertThat(analyzer.analyze("RENAME T TO OTHER.U", "APP").requireSupported().renamedTableId()).isEqualTo(new TableId(null, "OTHER", "U"));
    }

    @Test
    void shouldPlanOnACopyAndRetainOriginalDefaultExpressions() {
        final var parser = new DamengDdlParser();
        final var tables = new Tables();
        parser.parse("CREATE TABLE APP.$T (ID INT PRIMARY KEY, \"NOT\" VARCHAR(30))", tables, "APP");
        final var id = new TableId(null, "APP", "$T");
        final var original = tables.forTable(id);
        final var sql = "ALTER TABLE APP.$T ADD AMOUNT DECIMAL(10,2) DEFAULT -1.25";
        final var plan = parser.plan(parser.analyze(sql, "APP"), tables);
        assertThat(tables.forTable(id)).isSameAs(original);
        assertThat(plan).hasSize(1);
        assertThat(plan.get(0).table().columnWithName("AMOUNT").defaultValueExpression()).contains("-1.25");
        assertThat(parser.plan(parser.analyze("GRANT SELECT ON APP.$T TO U", "APP"), tables)).isEmpty();
        assertThatThrownBy(() -> parser.plan(parser.analyze("ALTER TABLE APP.$T MOVE", "APP"), tables)).hasMessageContaining("Unsupported ALTER TABLE");
        assertThat(tables.forTable(id)).isSameAs(original);
    }
}
