/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng.antlr;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.Trees;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import io.debezium.ddl.parser.dameng.generated.DamengSqlLexer;
import io.debezium.ddl.parser.dameng.generated.DamengSqlParser;

class DamengAntlrGrammarTest {
    // https://eco.dameng.com/document/dm/zh-cn/pm/definition-statement.html
    @ParameterizedTest
    @ValueSource(strings = {
            "CREATE TABLE APP.T(ID INT PRIMARY KEY, NAME VARCHAR(50) DEFAULT 'unknown' NOT NULL)",
            "CREATE TABLE IF NOT EXISTS APP.T(ID INT, CONSTRAINT PK_T PRIMARY KEY(ID))",
            "CREATE TABLE APP.T(D DOUBLE(24), P DOUBLE PRECISION(24))",
            "CREATE TABLE APP.T(Y INTERVAL YEAR(4) TO MONTH, D INTERVAL DAY(4) TO SECOND(6))",
            "CREATE TABLE APP.COPY LIKE APP.T",
            "CREATE TABLE APP.COPY AS SELECT ID, NAME FROM APP.T",
            "CREATE GLOBAL TEMPORARY TABLE APP.T(ID INT) ON COMMIT DELETE ROWS",
            "CREATE HUGE TABLE APP.T(ID INT)",
            "CREATE EXTERNAL TABLE APP.T(ID INT) FROM DEFAULT DIRECTORY D LOCATION ('data.ctl')",
            "ALTER TABLE APP.T ADD V VARCHAR(30)",
            "ALTER TABLE APP.T MODIFY V VARCHAR(50)",
            "ALTER TABLE APP.T DROP COLUMN V",
            "ALTER TABLE APP.T ALTER COLUMN V SET DEFAULT 'new'",
            "ALTER TABLE APP.T RENAME COLUMN V TO NAME",
            "ALTER TABLE APP.T RENAME TO APP.U",
            "ALTER TABLE APP.T ADD LOGIC LOG",
            "ALTER TABLE APP.T DROP LOGIC LOG",
            "DROP TABLE IF EXISTS APP.T CASCADE CONSTRAINTS PURGE",
            "TRUNCATE TABLE APP.T REUSE STORAGE"
    })
    void shouldParseTableDefinitionsAndChanges(String sql) {
        parse(sql);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "CREATE HUGE TABLE APP.T(ID INT) STORAGE(SECTION(64), FILESIZE(1024), STAT SYNCHRONOUS ON(ID), WITH DELTA, ON MAIN)",
            "CREATE HUGE TABLE APP.T(ID INT) STORAGE(INITIAL 2, STAT NONE, WITHOUT DELTA, ON MAIN)",
            "CREATE HUGE TABLE APP.T(ID INT, NAME VARCHAR(20)) STORAGE(STAT ASYNCHRONOUS EXCEPT(NAME), ON MAIN)",
            "CREATE EXTERNAL TABLE APP.T(ID INT) FROM DATAFILE DEFAULT DIRECTORY D LOCATION ('data.csv') PARMS(FIELDS DELIMITED BY ',', RECORDS DELIMITED BY '\\n', ERRORS 10, SKIP 1)"
    })
    void shouldParseDistinctHugeAndExternalStorageClauses(String sql) {
        parse(sql);
    }

    // Examples and options from sections 3.6 and 3.7 of definition-statement.html.
    @ParameterizedTest
    @ValueSource(strings = {
            "CREATE INDEX S1 ON PURCHASING.VENDOR(VENDORID)",
            "CREATE UNIQUE INDEX S2 ON PURCHASING.VENDOR(ACCOUNTNO, NAME)",
            "CREATE INDEX INDEX_FBI ON SALES.SALESPERSON(SALESTHISYEAR-SALESLASTYEAR)",
            "CREATE INDEX APP.I ON APP.T(ID DESC) TABLESPACE MAIN STORAGE(ON MAIN, CLUSTERBTR) NOSORT ONLINE REVERSE UNUSABLE PARALLEL 2",
            "ALTER INDEX PURCHASING.S1 RENAME TO PURCHASING.S2",
            "ALTER INDEX APP.I REBUILD SHARE ASYNCHRONOUS 3",
            "ALTER INDEX APP.I REBUILD SHARE ASYNCHRONOUS",
            "ALTER INDEX APP.I INVISIBLE",
            "DROP INDEX IF EXISTS APP.I",
            "CREATE SEQUENCE SEQ_QUANTITY INCREMENT BY 10",
            "CREATE SEQUENCE SEQ1 INCREMENT BY 1000 START WITH 5 MAXVALUE 1000000 MINVALUE 1 CACHE 10",
            "CREATE SEQUENCE IF NOT EXISTS APP.S INCREMENT BY -1 NOMAXVALUE NOMINVALUE NOCACHE NOCYCLE NOORDER GLOBAL",
            "ALTER SEQUENCE SEQ1 INCREMENT BY 1",
            "ALTER SEQUENCE SEQ4 CURRENT VALUE 300",
            "ALTER SEQUENCE SEQ5 RENAME TO SEQ6",
            "DROP SEQUENCE IF EXISTS SEQ_QUANTITY"
    })
    void shouldParseIndexAndSequenceClauses(String sql) {
        parse(sql);
    }

    // https://eco.dameng.com/document/dm/zh-cn/pm/view.html
    @ParameterizedTest
    @ValueSource(strings = {
            "CREATE VIEW PURCHASING.VENDOR_EXCELLENT AS SELECT VENDORID, ACCOUNTNO, NAME, ACTIVEFLAG, CREDIT FROM PURCHASING.VENDOR WHERE CREDIT = 1",
            "CREATE VIEW PRODUCTION.VENDOR_STATIS(VENDORID, PRODUCT_COUNT) AS SELECT VENDORID, COUNT(PRODUCTID) FROM PRODUCTION.PRODUCT_VENDOR GROUP BY VENDORID ORDER BY VENDORID",
            "CREATE OR REPLACE FORCE VIEW IF NOT EXISTS APP.V(ID) AS SELECT ID FROM APP.T WITH LOCAL CHECK OPTION",
            "CREATE VIEW APP.V AS SELECT ID FROM APP.T WITH READ ONLY",
            "CREATE VIEW APP.V AS SELECT REPLACE(NAME, 'a', 'b'), LEFT(NAME, 2), RIGHT(NAME, 2) FROM APP.T",
            "ALTER VIEW APP.V COMPILE",
            "DROP VIEW IF EXISTS APP.V CASCADE"
    })
    void shouldParseViewDefinitionQueryAndOptions(String sql) {
        parse(sql);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "CREATE VIEW APP.V AS SELECT ROW_NUMBER() OVER(PARTITION BY REGION ORDER BY ID) AS N FROM APP.T",
            "CREATE VIEW APP.V AS SELECT SUM(N) OVER(ORDER BY ID ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS TOTAL FROM APP.T",
            "CREATE VIEW APP.V AS SELECT JSON_VALUE(DOC,'$.x' RETURNING VARCHAR NULL ON ERROR) AS V FROM APP.T",
            "CREATE VIEW APP.V AS SELECT XMLSERIALIZE(CONTENT XMLTYPE(DOC) AS CLOB INDENT SIZE=2) AS V FROM APP.T",
            "CREATE VIEW APP.V AS SELECT ID FROM APP.T OFFSET 2 ROWS FETCH NEXT 3 ROWS ONLY",
            "GRANT CREATE OPERATOR TO U",
            "REVOKE CREATE OPERATOR FROM U"
    })
    void shouldRecognizeNonTableDdlWithoutValidatingItsExpressionsAndOptions(String sql) {
        parse(sql);
    }

    // https://eco.dameng.com/document/dm/zh-cn/pm/advanced-log.html, section 19.3.4.
    @Test
    void shouldKeepAHexadecimalLiteralInsideOneExpressionInsteadOfAnAlias() {
        final var tree = parse("CREATE TABLE APP.T(ID INT DEFAULT 0xF1)");
        final var literals = Trees.findAllRuleNodes(tree, DamengSqlParser.RULE_primaryExpression);
        assertThat(literals).singleElement().satisfies(literal -> assertThat(literal.getText()).isEqualTo("0xF1"));
    }

    // https://eco.dameng.com/document/dm/zh-cn/pm/function.html
    @ParameterizedTest
    @ValueSource(strings = {
            "CREATE TABLE APP.T(PAYLOAD VARBINARY(4) DEFAULT 0x00F1)",
            "CREATE TABLE APP.T(PAYLOAD VARBINARY(4) DEFAULT 0XABCD CHECK(PAYLOAD <> 0x0000))",
            "CREATE TABLE APP.T(N DOUBLE DEFAULT LOG(10,100), C VARCHAR(4) DEFAULT CHAR(65), F INT DEFAULT FIELD(2,1,2,3))",
            "ALTER TABLE APP.T ALTER COLUMN N SET DEFAULT LOG(2,8)",
            "CREATE TABLE APP.JSON(STRICT INT, LAX INT, KEYS INT)"
    })
    void shouldParseHexadecimalDefaultsAndFunctionsWithContextualKeywords(String sql) {
        parse(sql);
    }

    // https://eco.dameng.com/document/dm/zh-cn/pm/json.html, section 18.5.
    @ParameterizedTest
    @ValueSource(strings = {
            "CREATE TABLE APP.T(DOC CLOB CHECK(DOC IS JSON))",
            "CREATE TABLE APP.T(DOC CLOB CONSTRAINT JSON_CK CHECK(DOC IS JSON STRICT WITH UNIQUE KEYS))",
            "CREATE TABLE APP.T(DOC CLOB CHECK(DOC IS JSON (STRICT)))",
            "CREATE TABLE APP.T(DOC CLOB CHECK(DOC IS JSON (LAX) WITHOUT UNIQUE KEYS))",
            "CREATE TABLE APP.T(DOC VARCHAR(100) CHECK(DOC IS NOT JSON LAX WITHOUT UNIQUE KEYS))",
            "ALTER TABLE APP.T ADD CONSTRAINT JSON_CK CHECK(DOC IS JSON WITH UNIQUE KEYS)",
            "CREATE VIEW APP.V AS SELECT DOC FROM APP.T WHERE DOC IS JSON AND DOC IS NOT NULL"
    })
    void shouldParseJsonPredicatesInsideDefinitions(String sql) {
        parse(sql);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "CREATE TABLE APP.T(PAYLOAD VARBINARY(4) DEFAULT 0x)",
            "CREATE TABLE APP.T(PAYLOAD VARBINARY(4) DEFAULT 0xG1)",
            "CREATE TABLE APP.T(PAYLOAD VARBINARY(4) DEFAULT 0xF1G)",
            "CREATE TABLE APP.T(N INT DEFAULT DATEDIFF(MONTH,DATE '2023-01-01',DATE '2024-01-01')) DROP TABLE APP.T",
            "CREATE TABLE APP.T(N INT DEFAULT MONTH)",
            "CREATE TABLE APP.T(DOC CLOB CHECK(DOC IS JSON STRICT LAX))",
            "CREATE TABLE APP.T(DOC CLOB CHECK(DOC IS JSON (STRICT LAX)))",
            "CREATE TABLE APP.T(DOC CLOB CHECK(DOC IS JSON ()))",
            "CREATE TABLE APP.T(DOC CLOB CHECK(DOC IS JSON WITH UNIQUE))",
            "CREATE TABLE APP.T(DOC CLOB CHECK(DOC IS JSON WITHOUT KEYS))",
            "CREATE TABLE APP.T(DOC CLOB CHECK(DOC IS JSON UNIQUE KEYS))"
    })
    void shouldRejectMalformedLiteralsJsonClausesAndTrailingStatements(String sql) {
        assertThatThrownBy(() -> parse(sql)).isInstanceOf(IllegalArgumentException.class);
    }

    // https://eco.dameng.com/document/dm/zh-cn/pm/synonym.html
    @ParameterizedTest
    @ValueSource(strings = {
            "CREATE SYNONYM A.S1 FOR A.T1",
            "CREATE OR REPLACE PUBLIC SYNONYM IF NOT EXISTS S1 FOR A.T1",
            "DROP PUBLIC SYNONYM IF EXISTS S1",
            "COMMENT ON TABLE APP.T IS 'description'",
            "COMMENT ON COLUMN APP.T.ID IS NULL",
            "GRANT SELECT, UPDATE(ID) ON APP.T TO U, V WITH GRANT OPTION",
            "GRANT CREATE TABLE, CREATE VIEW TO U WITH ADMIN OPTION",
            "REVOKE SELECT ON APP.T FROM U",
            "CREATE VIEW APP.V AS SELECT ID FROM APP.T; COMMENT ON TABLE APP.T IS 'description';"
    })
    void shouldParseOtherDefinitionsAndNonTableScripts(String sql) {
        parse(sql);
    }

    // https://eco.dameng.com/document/dm/zh-cn/pm/dm8_sql-definition-deletion.html
    @ParameterizedTest
    @ValueSource(strings = {
            "CREATE PROCEDURE APP.P(P_ID IN INT DEFAULT 1) AS V INT := 1; V_TEXT VARCHAR(20) DEFAULT 'x'; BEGIN NULL; END P;",
            "CREATE PROCEDURE APP.P(A OUT IN INT, B IN OUT INT, C OUT INT, D INOUT INT, E IN INT) AS BEGIN A:=E; END;",
            "CREATE FUNCTION APP.F(P_ID IN INT) RETURN INT AS V INT; BEGIN RETURN P_ID; END F;",
            "CREATE PROCEDURE APP.P AS CURSOR C(P_ID IN INT) IS SELECT ID FROM APP.T WHERE ID = P_ID; V INT; BEGIN OPEN C(1); END P;",
            "CREATE PROCEDURE APP.P AS PROCEDURE LOCAL_P AS V INT; BEGIN NULL; END LOCAL_P; FUNCTION LOCAL_F RETURN INT AS BEGIN RETURN 1; END LOCAL_F; BEGIN LOCAL_P; BEGIN NULL; END; END P;",
            "CREATE TRIGGER APP.TR BEFORE UPDATE OF ID ON APP.T REFERENCING OLD ROW AS OLD_ROW NEW ROW AS NEW_ROW FOR EACH ROW DECLARE V INT := 1; BEGIN NULL; END;",
            "CREATE OR REPLACE FUNCTION APP.F WITH ENCRYPTION(P_ID IN INT) RETURN INT AUTHID DEFINER DETERMINISTIC AS DECLARE V INT DEFAULT 1; BEGIN RETURN V; END F;"
    })
    void shouldParseProgramHeadersDeclarationsAndNestedDefinitions(String sql) {
        parse(sql);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "CREATE PROCEDURE APP.P BEGIN NULL; END;",
            "CREATE FUNCTION APP.F AS BEGIN RETURN 1; END;",
            "CREATE FUNCTION APP.F RETURN AS BEGIN RETURN 1; END;",
            "CREATE PROCEDURE APP.P AS V INT BEGIN NULL; END;",
            "CREATE PROCEDURE APP.P AS CURSOR C IS; BEGIN NULL; END;",
            "CREATE PROCEDURE APP.P AS CURSOR C IS SELECT ID FROM APP.T BEGIN NULL; END;",
            "CREATE PROCEDURE APP.P AS BEGIN BEGIN NULL; END;",
            "CREATE FUNCTION APP.F RETURN INT AS BEGIN RETURN CASE WHEN 1=1 THEN 1; END;",
            "CREATE TRIGGER APP.TR BEFORE INSERT BEGIN NULL; END;",
            "CREATE PROCEDURE APP.P AS BEGIN NULL; END DROP TABLE APP.T"
    })
    void shouldRejectIncompleteProgramDefinitionsAndTrailingDdl(String sql) {
        assertThatThrownBy(() -> parse(sql)).isInstanceOf(IllegalArgumentException.class);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "CREATE PROCEDURE APP.P AS BEGIN NULL; END; CREATE TABLE APP.T(ID INT)",
            "CREATE FUNCTION APP.F RETURN INT AS BEGIN RETURN 1; END; ALTER TABLE APP.T ADD V INT"
    })
    void shouldRejectCompoundProgramRecordsAfterParsingTheirStatementBoundaries(String sql) {
        parse(sql);
        assertThat(new DamengDdlStatementAnalyzer().analyze(sql, "APP").impact())
                .isEqualTo(DamengDdlStatementAnalyzer.Impact.UNSUPPORTED);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "ALTER TABLE APP.T RENAME TO",
            "ALTER TABLE RENAME TO APP.U",
            "DROP TABLE",
            "TRUNCATE TABLE",
            "CREATE EXTERNAL TABLE APP.T(ID INT)",
            "CREATE EXTERNAL TABLE APP.T(ID INT) FROM UNKNOWN DIRECTORY D LOCATION ('data.ctl')",
            "CREATE EXTERNAL TABLE APP.T(ID INT) FROM DEFAULT DIRECTORY D LOCATION ('data.ctl') PARMS(ERRORS 10)",
            "CREATE EXTERNAL TABLE APP.T(ID INT) FROM DATAFILE DEFAULT DIRECTORY D LOCATION ('data.csv') PARMS(FILESIZE(1024))",
            "CREATE HUGE TABLE APP.T(ID INT) STORAGE(SECTION(64), FILESIZE(1024))",
            "CREATE HUGE TABLE APP.T(ID INT) STORAGE(ON MAIN, WITH DELTA)",
            "CREATE HUGE TABLE APP.T(ID INT) STORAGE(FILESIZE 1024, ON MAIN)",
            "CREATE HUGE TABLE APP.T(ID INT) STORAGE(SECTION('many'), ON MAIN)",
            "CREATE HUGE TABLE APP.T(ID INT) STORAGE(STAT UNKNOWN, ON MAIN)",
            "CREATE HUGE TABLE APP.T(ID INT) STORAGE(WITH COUNTER, ON MAIN)"
    })
    void shouldRejectIncompleteTargetsAndMixedPhysicalStorageSyntax(String sql) {
        assertThatThrownBy(() -> parse(sql)).isInstanceOf(IllegalArgumentException.class);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "CREATE GLOBAL SEQUENCE APP.S",
            "CREATE OR REPLACE UNIQUE HUGE TABLE APP.T(ID INT)",
            "CREATE TEMPORARY UNIQUE INDEX APP.I ON APP.T(ID)",
            "CREATE FORCE INDEX APP.I ON APP.T(ID)",
            "DROP VIEW DROP TABLE APP.T",
            "ALTER INDEX ALTER TABLE APP.T ADD V INT",
            "CREATE VIEW CREATE TABLE APP.T(ID INT)",
            "COMMENT ON TABLE DROP TABLE APP.T",
            "COMMENT ON INDEX APP.I IS 'x'",
            "TRUNCATE ROLE R",
            "ALTER SYNONYM A.S1 FOR A.T1",
            "CREATE TABLE APP.T(ID INT) CREATE TABLE APP.U(ID INT)",
            "CREATE VIEW APP.V AS SELECT ID FROM APP.T CREATE TABLE APP.U(ID INT)",
            "CREATE INDEX APP.I ON APP.T(ID) ALTER TABLE APP.T ADD V INT",
            "CREATE SEQUENCE APP.S INCREMENT BY 1 DROP TABLE APP.T"
    })
    void shouldRejectInvalidObjectModifiersClausesAndJoinedStatements(String sql) {
        assertThatThrownBy(() -> parse(sql)).isInstanceOf(IllegalArgumentException.class);
    }

    private ParseTree parse(String sql) {
        final var errors = new BaseErrorListener() {
            @Override
            public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol, int line, int charPositionInLine, String message,
                                    RecognitionException exception) {
                throw new IllegalArgumentException("line " + line + ", column " + (charPositionInLine + 1) + ": " + message, exception);
            }
        };
        final var lexer = new DamengSqlLexer(CharStreams.fromString(sql));
        lexer.removeErrorListeners();
        lexer.addErrorListener(errors);
        final var parser = new DamengSqlParser(new CommonTokenStream(lexer));
        parser.removeErrorListeners();
        parser.addErrorListener(errors);

        final var tree = parser.ddlScope();

        assertThat(parser.getNumberOfSyntaxErrors()).isZero();
        assertThat(parser.getCurrentToken().getType()).isEqualTo(Token.EOF);
        return tree;
    }
}
