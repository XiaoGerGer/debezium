/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import io.debezium.relational.TableId;
import io.debezium.relational.Tables;

class DamengDdlExtensionsTest {
    private final DamengDdlParser parser = new DamengDdlParser();
    private final Tables tables = new Tables();

    @ParameterizedTest
    @ValueSource(strings = { "TABLESPACE MAIN", "STORAGE(INITIAL 1,NEXT 2,MINEXTENTS 1,ON MAIN,FILLFACTOR 80,USING LONG ROW)",
            "STORAGE(BRANCH(2,3),WITHOUT COUNTER)", "STORAGE(BRANCH 2,WITH COUNTER)", "STORAGE(NOBRANCH,CLUSTERBTR,DISABLE USING LONG ROW)",
            "DISKSPACE LIMIT 100", "DISKSPACE UNLIMITED", "PARALLEL", "PARALLEL 2", "NOPARALLEL",
            "SEGMENT CREATION IMMEDIATE", "SEGMENT CREATION DEFERRED", "COMPRESS", "COMPRESS(ID)", "COMPRESS EXCEPT(ID)",
            "PARTITION BY HASH(ID) PARTITIONS 2 STORE IN(MAIN)",
            "PARTITION BY HASH(ID) (PARTITION P0 TABLESPACE MAIN, PARTITION P1 STORAGE(ON MAIN))",
            "PARTITION BY RANGE(ID) (PARTITION P0 VALUES EQU OR LESS THAN(10),PARTITION P1 VALUES LESS THAN(MAXVALUE))",
            "PARTITION BY LIST(ID) (PARTITION P0 VALUES(1,2),PARTITION P1 VALUES(DEFAULT))" })
    void shouldPreserveRowSchemaAcrossPhysicalTableOptions(String options) {
        final var sql = "CREATE TABLE APP.T(ID INT PRIMARY KEY,V VARCHAR(30)) " + options;
        assertThat(parser.analyze(sql, "APP").tableId()).isEqualTo(new TableId(null, "APP", "T"));
        parser.parse(sql, tables, "APP");
        final var table = tables.forTable(null, "APP", "T");
        assertThat(table.retrieveColumnNames()).containsExactly("ID", "V");
        assertThat(table.primaryKeyColumnNames()).containsExactly("ID");
        assertThat(table.columnWithName("V").length()).isEqualTo(30);
    }

    @ParameterizedTest
    @ValueSource(strings = { "LIKE APP.S", "AS SELECT * FROM APP.S", "AS SELECT ID,V FROM APP.S WHERE ID>1",
            "AS SELECT A.ID,A.V FROM APP.S AS A WHERE A.ID IN (1,2)", "AS SELECT A.ID,A.V FROM APP.S A" })
    void shouldDeriveCopiedTableFromHistoricalColumnsWithoutCopyingKeysOrDefaults(String query) {
        parser.parse("CREATE TABLE APP.S(ID INT PRIMARY KEY,V VARCHAR(30) DEFAULT 'old' COMMENT 'source')", tables, "APP");
        parser.parse("CREATE TABLE APP.T " + query, tables, "APP");
        final var table = tables.forTable(null, "APP", "T");
        assertThat(table.primaryKeyColumnNames()).isEmpty();
        assertThat(table.attributes()).isEmpty();
        assertThat(table.columnWithName("ID").isOptional()).isFalse();
        assertThat(table.columnWithName("V").length()).isEqualTo(30);
        assertThat(table.columnWithName("V").defaultValueExpression()).isEmpty();
        assertThat(table.columnWithName("V").comment()).isNull();
        parser.parse("ALTER TABLE APP.S MODIFY V VARCHAR(90)", tables, "APP");
        assertThat(table.columnWithName("V").length()).isEqualTo(30);
    }

    @Test
    void shouldPreserveExplicitProjectionNamesCastsAndInlineComments() {
        parser.parse("CREATE TABLE APP.S(ID INT,V VARCHAR(30))", tables, "APP");
        parser.parse("CREATE TABLE APP.T AS SELECT ID AS KEY_ID, CAST(V AS VARCHAR(50)) AS TEXT_VALUE FROM APP.S", tables, "APP");
        assertThat(tables.forTable(null, "APP", "T").retrieveColumnNames()).containsExactly("KEY_ID", "TEXT_VALUE");
        assertThat(tables.forTable(null, "APP", "T").columnWithName("TEXT_VALUE").length()).isEqualTo(50);
        parser.parse("ALTER TABLE APP.T ADD C VARCHAR(30) DEFAULT ON NULL 'fallback' COMMENT q'[column;--/*]'", tables, "APP");
        final var column = tables.forTable(null, "APP", "T").columnWithName("C");
        assertThat(column.isOptional()).isFalse();
        assertThat(column.defaultValueExpression()).contains("'fallback'");
        assertThat(column.comment()).isEqualTo("column;--/*");
    }

    @ParameterizedTest
    @ValueSource(strings = { "CREATE TABLE APP.T AS SELECT * FROM APP.MISSING", "CREATE TABLE APP.T AS SELECT UNKNOWN FROM APP.S",
            "CREATE TABLE APP.T AS SELECT X.ID FROM APP.S A", "CREATE TABLE APP.T AS SELECT ID+1 FROM APP.S",
            "CREATE TABLE APP.T AS SELECT COUNT(*) FROM APP.S", "CREATE TABLE APP.T AS SELECT ID,ID FROM APP.S",
            "CREATE TABLE APP.T AS SELECT ID,* FROM APP.S", "CREATE TABLE APP.T AS SELECT *,* FROM APP.S",
            "CREATE TABLE APP.T(\"id\",\"ID\") AS SELECT ID,V FROM APP.S", "CREATE TABLE APP.T(\"id\" INT,\"ID\" INT)",
            "CREATE TABLE APP.T AS SELECT ID FROM APP.S JOIN APP.S B ON 1=1", "CREATE TABLE APP.T AS SELECT ID FROM APP.S WHERE ID IN(SELECT ID FROM APP.S)",
            "CREATE TABLE APP.T(A) AS SELECT ID,V FROM APP.S", "CREATE TABLE APP.T(A,A) AS SELECT ID,V FROM APP.S",
            "CREATE TABLE APP.T(A,B) AS SELECT ID,V FROM APP.S", "CREATE TABLE APP.T(A INT,B VARCHAR(30)) AS SELECT ID,V FROM APP.S",
            "CREATE TABLE APP.T AS SELECT ID FROM APP.S WHERE", "CREATE TABLE APP.T(ID INT) PARTITION BY HASH(ID) PARTITIONS 0",
            "CREATE TABLE APP.T(ID INT) PARTITION BY HASH(UNKNOWN) PARTITIONS 2", "CREATE TABLE APP.T(ID INT) UNKNOWN OPTION",
            "CREATE TABLE APP.T(ID INT) STORAGE(USING UNKNOWN ROW)", "CREATE TABLE APP.T(ID INT COMMENT)",
            "CREATE TABLE APP.T(ID INT COMMENT 42)", "CREATE TABLE APP.T(ID INT,V INT GENERATED ALWAYS AS(ID+1) VIRTUAL)",
            "CREATE TABLE APP.T(ID INT,V AS(ID+1))", "CREATE GLOBAL TEMPORARY TABLE APP.T(ID INT) ON COMMIT PRESERVE ROWS",
            "CREATE EXTERNAL TABLE APP.T(ID INT)", "CREATE HUGE TABLE APP.T(ID INT)", "CREATE TABLE APP.\"#T\"(ID INT)" })
    void shouldRejectUnverifiableOrIncompleteDefinitionsWithoutChangingHistory(String ddl) {
        parser.parse("CREATE TABLE APP.S(ID INT,V VARCHAR(30))", tables, "APP");
        final var before = tables.forTable(null, "APP", "S");
        assertThatThrownBy(() -> parser.parse(ddl, tables, "APP")).isInstanceOf(IllegalArgumentException.class);
        assertThat(tables.tableIds()).containsExactly(new TableId(null, "APP", "S"));
        assertThat(tables.forTable(null, "APP", "S")).isEqualTo(before);
    }

    @Test
    void shouldResolveExcludedSpecialTableScopeButRejectCompoundOrDisguisedStatements() {
        for (var prefix : List.of("GLOBAL TEMPORARY", "TEMPORARY", "HUGE", "EXTERNAL")) {
            assertThat(parser.resolveTableId("CREATE " + prefix + " TABLE OTHER.T(ID INT)", "APP"))
                    .isEqualTo(new TableId(null, "OTHER", "T"));
        }
        assertThatThrownBy(() -> parser.analyze("CREATE TABLE APP.T(ID INT) COMMENT ON TABLE APP.S IS 'x'", "APP"))
                .hasMessageContaining("additional statement");
    }
}
