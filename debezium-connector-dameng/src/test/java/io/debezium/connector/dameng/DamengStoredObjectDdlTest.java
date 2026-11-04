/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import io.debezium.connector.dameng.antlr.DamengDdlParser;
import io.debezium.connector.dameng.antlr.DamengDdlStatementAnalyzer;
import io.debezium.connector.dameng.antlr.DamengDdlStatementAnalyzer.Impact;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;

/**
 * Stored-object boundaries from the DM SQL Programming Manual. These tests
 * validate historical table isolation, not execution of the stored code.
 */
class DamengStoredObjectDdlTest {
    private final DamengDdlParser parser = new DamengDdlParser();

    @ParameterizedTest
    @MethodSource("storedDefinitions")
    void shouldKeepStoredDefinitionsOutOfHistoricalTableChanges(String sql) {
        final var tables = new Tables();
        parser.parse("CREATE TABLE APP.T(ID INT PRIMARY KEY, V VARCHAR(20))", tables, "APP");
        final var id = new TableId(null, "APP", "T");
        final var original = tables.forTable(id);

        final var analysis = parser.analyze(sql, "APP");

        assertThat(analysis.impact()).isEqualTo(Impact.NO_TABLE_CHANGE);
        assertThat(analysis.tableId()).isNull();
        assertThat(parser.plan(analysis, tables)).isEmpty();
        assertThat(parser.parse(sql, tables, "APP")).isEmpty();
        assertThat(tables.tableIds()).containsExactly(id);
        assertThat(tables.forTable(id)).isSameAs(original);
    }

    static Stream<String> storedDefinitions() {
        return Stream.of(
                // pm/dm8_sql-definition-deletion.html, sections 3.1, 3.2, 3.4.
                "CREATE PROCEDURE APP.P(A VARCHAR(10) DEFAULT 'abc', B INT:=123) AS BEGIN PRINT A; PRINT B; END;",
                "CREATE PROCEDURE APP.P(A OUT IN INT) AS BEGIN A:=1; END;",
                "CREATE FUNCTION APP.F(A OUT IN INT) RETURN INT AS BEGIN RETURN A; END;",
                "CREATE PROCEDURE APP.P AS CURSOR C FOR SELECT ID FROM APP.T; BEGIN OPEN C; CLOSE C; END P;",
                "CREATE PROCEDURE APP.P AS DECLARE CURSOR C FOR SELECT ID FROM APP.T; BEGIN OPEN C; END;",
                "CREATE FUNCTION APP.F(C OUT CURSOR) RETURN INT AS CNT INT:=0; BEGIN OPEN C; LOOP FETCH C;"
                        + " EXIT WHEN C%NOTFOUND; CNT:=CNT+1; END LOOP; RETURN CNT; END;",
                // pm/dm8_sql-data-types-operators.html, sections 2.4, 2.5, 2.7, 2.8.
                "CREATE FUNCTION APP.F RETURN INT IS V APP.T.ID%TYPE; BEGIN RETURN V; END;",
                "CREATE FUNCTION APP.F RETURN INT IS V T.ID%TYPE; BEGIN RETURN V; END F;",
                "CREATE PROCEDURE APP.P AS R APP.T%ROWTYPE; C SYS_REFCURSOR; BEGIN NULL; END;",
                "CREATE FUNCTION APP.F RETURN BIGINT AS TYPE NAME_ARR_T IS TABLE OF VARCHAR; A NAME_ARR_T;"
                        + " BEGIN RETURN 0; END;",
                "CREATE PROCEDURE APP.P AS TYPE REC IS RECORD(ID APP.T.ID%TYPE,V VARCHAR(20) DEFAULT 'x');"
                        + " TYPE ARR IS TABLE OF REC INDEX BY INT; R ARR; BEGIN NULL; END;",
                "CREATE PROCEDURE APP.P AS TYPE ARR IS ARRAY VARCHAR[2,4]; A ARR; BEGIN NULL; END;",
                "CREATE PROCEDURE APP.P AS TYPE ARR IS ARRAY VARCHAR[,]; A ARR; BEGIN NULL; END;",
                "CREATE PROCEDURE APP.P AS TYPE ARR IS VARRAY(10) OF INT NOT NULL; A ARR; BEGIN NULL; END;",
                "CREATE PROCEDURE APP.P AS SUBTYPE COUNT_TYPE IS NUMBER(5); N COUNT_TYPE; BEGIN NULL; END;",
                "CREATE PROCEDURE APP.P AS CURSOR C; BEGIN NULL; END;",
                // pm/Custom-functions.html, section 21.1.2.
                "CREATE FUNCTION APP.F(INPUT NUMBER) RETURN NUMBER AGGREGATE USING APP.MY_MAX_IMPL",
                "CREATE FUNCTION APP.F(INPUT NUMBER) RETURN NUMBER PARALLEL_ENABLE DETERMINISTIC AGGREGATE USING APP.MY_MAX_IMPL;",
                // pm/package.html, sections 11.1.1 and 11.1.2.
                "CREATE PACKAGE APP.P WITH ENCRYPTION AUTHID CURRENT_USER AS V INT DEFAULT 0;"
                        + " E EXCEPTION FOR -20001, 'missing'; CURSOR C FOR SELECT ID FROM APP.T;"
                        + " PROCEDURE RUN(A INT); FUNCTION GET_VALUE RETURN INT RESULT_CACHE DETERMINISTIC; END P;",
                "CREATE OR REPLACE PACKAGE APP.P AS TYPE ARR IS TABLE OF INT; PROCEDURE RUN; END",
                "CREATE PACKAGE BODY APP.P AS PROCEDURE RUN AS BEGIN BEGIN NULL; END; END RUN;"
                        + " FUNCTION GET_VALUE RETURN INT AS BEGIN RETURN CASE WHEN 1=1 THEN 1 ELSE 0 END; END; END P;",
                "CREATE PACKAGE BODY APP.P AS N INT:=0; PROCEDURE RUN AS BEGIN N:=N+1; END;"
                        + " BEGIN N:=CASE WHEN 1=1 THEN 1 ELSE 0 END; BEGIN NULL; END; END P;",
                "CREATE PACKAGE BODY APP.P AS FUNCTION F RETURN INT AS FUNCTION G RETURN INT AS"
                        + " BEGIN RETURN 1; END; BEGIN RETURN G(); END; END;",
                "CREATE PACKAGE BODY APP.P AS PROCEDURE RUN AS BEGIN NULL; END;"
                        + " DECLARE N INT:=0; BEGIN N:=1; END P;",
                // pm/class-type.html, SQL class declarations and implementations.
                "CREATE CLASS APP.C UNDER APP.BASE NOT FINAL NOT INSTANTIABLE AUTHID DEFINER AS"
                        + " TYPE REC IS RECORD(ID INT); R REC; MEMBER FUNCTION F(A INT) RETURN INT; END C;",
                "CREATE CLASS APP.C AS ID INT; STATIC PROCEDURE RUN; FUNCTION C(ID INT) RETURN APP.C; END;",
                "CREATE CLASS BODY APP.C AS MEMBER FUNCTION F(A INT) RETURN INT AS BEGIN RETURN A; END F;"
                        + " FUNCTION C(ID INT) RETURN APP.C AS BEGIN THIS.ID:=ID; RETURN THIS; END; END C;",
                "CREATE CLASS BODY APP.C AS N INT; BEGIN N:=1; END;",
                "CREATE CLASS BODY APP.C AS DECLARE N INT:=0; BEGIN N:=1; END C;",
                // pm/custom-type.html, section 13.2.
                "CREATE TYPE BODY APP.COMPLEX AS FUNCTION PLUS(X APP.COMPLEX) RETURN APP.COMPLEX IS"
                        + " BEGIN RETURN APP.COMPLEX(RPART+X.RPART,IPART+X.IPART); END; END;",
                "CREATE OR REPLACE TYPE BODY APP.OBJ WITH ENCRYPTION AS CONSTRUCTOR FUNCTION OBJ(ID INT)"
                        + " RETURN SELF AS RESULT AS BEGIN SELF.ID:=ID; RETURN; END; END OBJ;",
                // pm/external-function.html, sections 10.1.2 and 10.2.2.
                "CREATE FUNCTION APP.F(A VARCHAR,B VARCHAR) RETURN VARCHAR EXTERNAL '/tmp/libtest.so' \"C_CONCAT\" USING C;",
                "CREATE FUNCTION APP.F(A VARCHAR) RETURN VARCHAR EXTERNAL '/tmp/libtest.so' USING CS;",
                "CREATE FUNCTION APP.F(A VARCHAR) RETURN VARCHAR AS LANGUAGE CS LIBRARY APP.CODE NAME \"C_CONCAT\";",
                "CREATE FUNCTION APP.F(A INT) RETURN INT EXTERNAL '/tmp/test.jar' AND 'com.example.Test.run' (INT) USING JAVA;",
                // pm/trigger.html, section 14.1.3; this header is preserved in 8.1.3.162 SQL_REDO.
                "CREATE OR REPLACE TRIGGER APP.TR AFTER TIMER ON DATABASE FOR EACH 1 DAY FOR EACH 1 MINUTE"
                        + " BEGIN INSERT INTO APP.T VALUES(2,'timer'); END;",
                "CREATE TRIGGER APP.TR AFTER TIMER ON DATABASE FOR EACH 1 DAY AT TIME '12:00:00'"
                        + " DECLARE V INT:=1; BEGIN PRINT V; END;",
                "CREATE TRIGGER APP.TR AFTER TIMER ON DATABASE FOR ONCE AT DATETIME '2030-01-01 12:00:00'"
                        + " EXECUTE AT 0 BEGIN PRINT 'END; DROP TABLE APP.T'; END;",
                "CREATE TRIGGER APP.TR AFTER TIMER ON DATABASE FOR EACH 1 MONTH DAY 2 OF WEEK LAST AT TIME '12:00:00'"
                        + " BEGIN NULL; END;",
                "CREATE TRIGGER APP.TR AFTER TIMER ON DATABASE FOR EACH 1 WEEK 1,3,5"
                        + " FROM TIME '09:00:00' TO TIME '18:00:00' FOR EACH 30 SECOND"
                        + " FROM DATETIME '2030-01-01' TO DATETIME '2030-12-31' EXECUTE AT 0 WHEN 1=1 BEGIN NULL; END;",
                "CREATE TRIGGER APP.TR AFTER TIMER ON DATABASE EXECUTE AT RAFT1"
                        + " FOR ONCE AT DATETIME (TO_DATE('2030-01-01','YYYY-MM-DD')) BEGIN NULL; END;",
                "CREATE FUNCTION APP.F RETURN INT AS BEGIN EXECUTE IMMEDIATE q'[ALTER TABLE APP.T ADD X INT]';"
                        + " PRINT 'END; CREATE TABLE APP.HIDDEN(ID INT)'; RETURN 1; END F;");
    }

    @ParameterizedTest
    @MethodSource("definitionsWithTableTails")
    void shouldRejectTableChangesAfterTheStoredDefinition(String definition, String suffix) {
        final var sql = definition + suffix;

        assertThatThrownBy(() -> parser.analyze(sql, "APP"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("character");
    }

    static Stream<Arguments> definitionsWithTableTails() {
        final var definitions = List.of(
                "CREATE PACKAGE APP.P AS PROCEDURE RUN; END",
                "CREATE PACKAGE APP.P AS PROCEDURE RUN; END P",
                "CREATE PACKAGE BODY APP.P AS PROCEDURE RUN AS BEGIN NULL; END; END",
                "CREATE CLASS APP.C AS ID INT; END C",
                "CREATE CLASS BODY APP.C AS BEGIN NULL; END",
                "CREATE TYPE BODY APP.O AS FUNCTION F RETURN INT AS BEGIN RETURN 1; END; END O",
                "CREATE PROCEDURE APP.P(A OUT IN INT) AS BEGIN A:=1; END",
                "CREATE TRIGGER APP.TR AFTER TIMER ON DATABASE FOR EACH 1 DAY FOR EACH 1 MINUTE BEGIN NULL; END",
                "CREATE FUNCTION APP.F RETURN INT EXTERNAL '/tmp/lib.so' USING C");
        final var suffixes = List.of(
                " ALTER TABLE APP.T ADD X INT", "; ALTER TABLE APP.T ADD X INT",
                "; DROP TABLE APP.T", "; CREATE TABLE APP.U(ID INT)", "; TRUNCATE TABLE APP.T");
        // Every definition must reject a table tail. Tail kinds share the statement boundary,
        // so use the package body with internal semicolons for the remaining delimiter/DDL forms.
        return Stream.concat(
                definitions.stream().map(definition -> Arguments.of(definition, suffixes.get(0))),
                suffixes.subList(1, suffixes.size()).stream().map(suffix -> Arguments.of(definitions.get(2), suffix)));
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "CREATE PACKAGE APP.P AS PROCEDURE RUN;",
            "CREATE PACKAGE APP.P AS PROCEDURE RUN; END; END;",
            "CREATE PACKAGE BODY APP.P AS PROCEDURE RUN AS BEGIN NULL; END;",
            "CREATE PACKAGE BODY APP.P AS PROCEDURE RUN AS BEGIN NULL; END P;",
            "CREATE PACKAGE BODY APP.P AS BEGIN PRINT 'END';",
            "CREATE PACKAGE BODY APP.P AS DECLARE N INT; END P;",
            "CREATE PACKAGE BODY APP.P AS DECLARE N INT; BEGIN N:=1;",
            "CREATE CLASS APP.C AS ID INT;",
            "CREATE CLASS BODY APP.C AS FUNCTION F RETURN INT AS BEGIN RETURN 1; END;",
            "CREATE CLASS BODY APP.C AS DECLARE N INT; END C;",
            "CREATE CLASS BODY APP.C AS DECLARE N INT; BEGIN N:=1;",
            "CREATE TYPE BODY APP.O AS FUNCTION F RETURN INT AS BEGIN RETURN 1; END;",
            "CREATE TYPE BODY APP.O AS END;",
            "CREATE PROCEDURE APP.P AS TYPE ARR IS TABLE OF; BEGIN NULL; END;",
            "CREATE FUNCTION APP.F RETURN INT AS V APP.T.ID%UNKNOWN; BEGIN RETURN 1; END;",
            "CREATE FUNCTION APP.F RETURN INT AGGREGATE USING;",
            "CREATE FUNCTION APP.F RETURN INT EXTERNAL '/tmp/lib.so' USING;",
            "CREATE FUNCTION APP.F RETURN INT AS LANGUAGE CS LIBRARY APP.CODE NAME 'F' UNKNOWN;",
            "CREATE PROCEDURE APP.P(A OUT IN INT) AS BEGIN A:=1;",
            "CREATE FUNCTION APP.F(A OUT IN INT) RETURN INT AS BEGIN RETURN A;",
            "CREATE TRIGGER APP.TR AFTER TIMER ON DATABASE FOR EACH 1 DAY FOR EACH 1 MINUTE BEGIN PRINT 'END';",
            "CREATE TRIGGER APP.TR AFTER TIMER ON DATABASE FOR EACH 1 DAY ALTER TABLE APP.T ADD V INT BEGIN NULL; END;",
            "CREATE TRIGGER APP.TR AFTER TIMER ON DATABASE FOR EACH 1 DAY; BEGIN NULL; END;",
            "CREATE TRIGGER APP.TR AFTER TIMER ON DATABASE FOR ONCE AT DATETIME (TO_DATE('2030-01-01','YYYY-MM-DD') BEGIN NULL; END;",
            "CREATE TRIGGER APP.TR AFTER TIMER ON DATABASE FOR ONCE AT DATETIME '2030-01-01') BEGIN NULL; END;",
            "DROP TYPE APP.O CASCADE", "DROP CLASS APP.C CASCADE"
    })
    void shouldKeepIncompleteOrUnverifiedDefinitionsUnsupported(String sql) {
        final var analysis = new DamengDdlStatementAnalyzer().analyze(sql, "APP");

        assertThat(analysis.impact()).isEqualTo(Impact.UNSUPPORTED);
        assertThatThrownBy(analysis::requireSupported).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void shouldKeepProgramKeywordsAvailableAsExistingExpressionNames() {
        final var tables = new Tables();

        parser.parse("CREATE TABLE APP.T(C INT, NAME INT, SELF INT)", tables, "APP");
        final var analysis = new DamengDdlStatementAnalyzer().analyze("CREATE TABLE APP.U AS SELECT C,NAME,SELF FROM APP.T", "APP").requireSupported();
        assertThat(analysis.isCtas()).isTrue();
        assertThat(analysis.tableId()).isEqualTo(new io.debezium.relational.TableId(null, "APP", "U"));
    }
}
