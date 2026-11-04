/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.stream.Stream;

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

    // DM SQL Language Manual, sections 3.2, 3.3, 3.8, 3.11-3.13 and 3.18-3.21:
    // https://eco.dameng.com/document/dm/zh-cn/pm/definition-statement.html
    // https://eco.dameng.com/document/dm/zh-cn/pm/custom-type.html
    // https://eco.dameng.com/document/dm/zh-cn/pm/package.html
    // https://eco.dameng.com/document/dm/zh-cn/pm/class-type.html
    static Stream<String> safeStatements() {
        return Stream.of(
                "CREATE BITMAP INDEX BI ON APP.A(U.V) FROM APP.A A, APP.U U WHERE A.ID=U.ID",
                "DROP INDEX APP.BI",
                "CREATE USER CDC_USER IDENTIFIED BY Test_123456",
                "CREATE USER IF NOT EXISTS CDC_USER IDENTIFIED BY Test_123456 LIMIT CONNECT_TIME 30",
                "CREATE USER CDC_USER IDENTIFIED EXTERNALLY",
                "CREATE USER CDC_USER IDENTIFIED BY \"DROP TABLE APP.A\" PASSWORD_POLICY 0 ACCOUNT LOCK",
                "CREATE USER CDC_USER IDENTIFIED BY Test_123456 DROP PROFILE",
                "CREATE USER CDC_USER IDENTIFIED BY Test_123456 HASH WITH SHA512 SALT",
                "ALTER USER CDC_USER IDENTIFIED BY Test_654321 HASH WITH SHA512 NO SALT",
                "CREATE USER CDC_USER IDENTIFIED BY Test_123456 DEFAULT TABLESPACE MAIN DEFAULT INDEX TABLESPACE MAIN",
                "ALTER USER CDC_USER IDENTIFIED BY Test_654321",
                "ALTER USER CDC_USER ACCOUNT UNLOCK",
                "ALTER USER CDC_USER PASSWORD EXPIRE",
                "ALTER USER CDC_USER PROFILE CDC_PROFILE",
                "ALTER USER CDC_USER DROP PROFILE",
                "ALTER USER CDC_USER ALLOW_IP ADD \"192.168.0.1\"",
                "ALTER USER CDC_USER ALLOW_IP DROP \"192.168.0.1\"",
                "ALTER USER CDC_USER ALLOW_IP DROP '192.168.0.1'",
                "ALTER USER CDC_USER NOT_ALLOW_IP DROP \"192.168.0.1\", \"192.168.0.2\"",
                "ALTER USER CDC_USER NOT_ALLOW_IP DROP '192.168.0.1', '192.168.0.2'",
                "ALTER USER CDC_USER GRANT CONNECT THROUGH PROXY_USER",
                "ALTER USER CDC_USER REVOKE CONNECT THROUGH PROXY_USER",
                "ALTER USER CDC_USER ON SCHEMA APP",
                "DROP USER CDC_USER",
                "DROP USER IF EXISTS CDC_USER RESTRICT",
                "CREATE SCHEMA CDC_SCHEMA",
                "CREATE SCHEMA CDC_SCHEMA AUTHORIZATION SYSDBA",
                "CREATE SCHEMA AUTHORIZATION CDC_USER",
                "DROP SCHEMA CDC_SCHEMA",
                "DROP SCHEMA IF EXISTS CDC_SCHEMA RESTRICT",
                "CREATE DIRECTORY CDC_DIRECTORY AS '/tmp/cdc'",
                "CREATE OR REPLACE DIRECTORY CDC_DIRECTORY AS '/tmp/cdc'",
                "CREATE OR REPLACE DIRECTORY IF NOT EXISTS CDC_DIRECTORY AS '/tmp/cdc'",
                "CREATE DIRECTORY \"DROP TABLE APP.A\" AS '/tmp/; ALTER TABLE APP.A ADD BAD INT'",
                "DROP DIRECTORY CDC_DIRECTORY",
                "DROP DIRECTORY IF EXISTS \"DROP TABLE APP.A\"",
                "CREATE DOMAIN APP.CDC_DOMAIN INT CHECK(VALUE < 100)",
                "CREATE DOMAIN IF NOT EXISTS APP.CDC_DOMAIN AS VARCHAR(20) DEFAULT 'DROP TABLE APP.A'",
                "DROP DOMAIN APP.CDC_DOMAIN",
                "DROP DOMAIN IF EXISTS APP.CDC_DOMAIN RESTRICT",
                "CREATE CONTEXT CDC_CONTEXT USING APP.CDC_PACKAGE",
                "CREATE OR REPLACE CONTEXT IF NOT EXISTS CDC_CONTEXT USING APP.CDC_PACKAGE",
                "DROP CONTEXT CDC_CONTEXT",
                "DROP CONTEXT IF EXISTS SYS.CDC_CONTEXT",
                "CREATE CONTEXT INDEX IF NOT EXISTS CDC_FULLTEXT ON APP.A(V) LEXER CHINESE_LEXER SYNC TRANSACTION",
                "ALTER CONTEXT INDEX CDC_FULLTEXT ON APP.A REBUILD ONLINE",
                "DROP CONTEXT INDEX IF EXISTS CDC_FULLTEXT ON APP.A",
                "CREATE PROFILE CDC_PROFILE LIMIT SESSION_PER_USER 100 PASSWORD_REUSE_TIME 10",
                "CREATE PROFILE IF NOT EXISTS CDC_PROFILE",
                "ALTER PROFILE CDC_PROFILE LIMIT SESSION_PER_USER 10",
                "DROP PROFILE CDC_PROFILE",
                "DROP PROFILE IF EXISTS CDC_PROFILE RESTRICT",
                "DROP PROFILE CDC_PROFILE CASCADE",
                "CREATE LIBRARY APP.CDC_LIBRARY AS '/tmp/libcdc.so'",
                "CREATE OR REPLACE LIBRARY IF NOT EXISTS APP.CDC_LIBRARY AS '/tmp/; DROP TABLE APP.A'",
                "CREATE LIBRARY APP.CDC_LIBRARY AS",
                "ALTER LIBRARY APP.CDC_LIBRARY COMPILE",
                "ALTER LIBRARY APP.CDC_LIBRARY",
                "DROP LIBRARY APP.CDC_LIBRARY",
                "DROP LIBRARY IF EXISTS APP.CDC_LIBRARY",
                "CREATE TYPE APP.CDC_TYPE AS OBJECT(ID INT, V VARCHAR(20))",
                "CREATE OR REPLACE TYPE APP.CDC_TYPE IS RECORD(ID INT, V VARCHAR(20))",
                "CREATE TYPE APP.CDC_TYPE WITH ENCRYPTION AUTHID CURRENT_USER AS OBJECT(ID INT)",
                "CREATE TYPE APP.CDC_TYPE AS VARRAY(10) OF INT NOT NULL",
                "CREATE TYPE APP.CDC_TYPE IS TABLE OF VARCHAR(20) INDEX BY INT",
                "DROP TYPE APP.CDC_TYPE",
                "DROP TYPE APP.CDC_TYPE RESTRICT",
                "DROP TYPE BODY IF EXISTS APP.CDC_TYPE",
                "DROP TYPE BODY IF EXISTS APP.CDC_TYPE RESTRICT",
                "DROP TYPE BODY APP.CDC_TYPE CASCADE",
                "DROP CLASS APP.CDC_CLASS",
                "DROP CLASS APP.CDC_CLASS RESTRICT",
                "DROP CLASS BODY IF EXISTS APP.CDC_CLASS",
                "DROP CLASS BODY IF EXISTS APP.CDC_CLASS RESTRICT",
                "ALTER TYPE APP.CDC_TYPE COMPILE",
                "ALTER TYPE APP.CDC_TYPE COMPILE DEBUG",
                "ALTER CLASS APP.CDC_CLASS COMPILE DEBUG",
                "ALTER PACKAGE APP.CDC_PACKAGE COMPILE",
                "ALTER PACKAGE APP.CDC_PACKAGE COMPILE CASCADE DEBUG",
                "DROP PACKAGE IF EXISTS APP.CDC_PACKAGE",
                "DROP PACKAGE BODY IF EXISTS APP.CDC_PACKAGE",
                "STAT ON APP.A",
                "STAT ON APP.A GLOBAL",
                "STAT 30 ON APP.A(ID)",
                "STAT 30.123456 SIZE 20 ON APP.A(ID, V)",
                "STAT 50 ON INDEX APP.CDC_INDEX",
                "STAT 50 SIZE 10 ON INDEX APP.CDC_INDEX GLOBAL",
                "/*+parallel(12)*/ STAT ON APP.A",
                "STAT /*+parallel(12)*/ 30 ON APP.A(ID)",
                "STAT 30 ON APP.A(ID) /*+parallel(12)*/",
                "CREATE MATERIALIZED VIEW APP.MV AS SELECT ID,V FROM APP.A",
                "CREATE MATERIALIZED VIEW APP.MV REFRESH COMPLETE ON DEMAND USING TRUNCATE AS SELECT ID,V FROM APP.A",
                "CREATE MATERIALIZED VIEW APP.MV REFRESH FAST WITH ROWID START WITH SYSDATE NEXT SYSDATE+1 AS SELECT ID,V,ROWID AS X FROM APP.A",
                "CREATE MATERIALIZED VIEW \"Case.Schema\".\"MV; DROP TABLE APP.A\" AS SELECT q'[ON PREBUILT TABLE; ALTER TABLE APP.A]' AS V FROM APP.A",
                "CREATE MATERIALIZED VIEW APP.MV AS SELECT PREBUILT,REFRESH,TRUNCATE(1.25,1) FROM APP.A",
                "CREATE MATERIALIZED VIEW LOG ON APP.A WITH ROWID(V) PURGE IMMEDIATE SYNCHRONOUS",
                "CREATE MATERIALIZED VIEW LOG ON APP.A WITH PRIMARY KEY PURGE START WITH SYSDATE+5 REPEAT INTERVAL '1' DAY",
                "ALTER MATERIALIZED VIEW APP.MV REFRESH COMPLETE USING TRUNCATE",
                "ALTER MATERIALIZED VIEW APP.MV ENABLE QUERY REWRITE",
                "ALTER MATERIALIZED VIEW APP.MV COMPILE",
                "REFRESH MATERIALIZED VIEW APP.MV",
                "REFRESH MATERIALIZED VIEW APP.MV FAST",
                "REFRESH MATERIALIZED VIEW APP.MV COMPLETE USING DELETE",
                "REFRESH MATERIALIZED VIEW APP.MV FORCE USING TRUNCATE",
                "DROP MATERIALIZED VIEW APP.MV",
                "DROP MATERIALIZED VIEW IF EXISTS APP.MV",
                "DROP MATERIALIZED VIEW LOG ON APP.A",
                "DROP MATERIALIZED VIEW LOG IF EXISTS ON APP.A",
                "create user \"Case.User\" identified by \"Case.Password\";",
                "drop schema \"Case.Schema\" restrict;",
                "// database maintenance\nSTAT ON \"Case.Schema\".\"Case.Table\";");
    }

    @ParameterizedTest
    @MethodSource("safeStatements")
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

    static Stream<String> unsafeStatements() {
        return Stream.of(
                "DROP USER CDC_USER CASCADE",
                "DROP SCHEMA CDC_SCHEMA CASCADE",
                "DROP DOMAIN APP.CDC_DOMAIN CASCADE",
                "DROP TYPE APP.CDC_TYPE CASCADE",
                "DROP CLASS APP.CDC_CLASS CASCADE",
                "DROP CLASS BODY APP.CDC_CLASS CASCADE",
                "ALTER TYPE APP.CDC_TYPE COMPILE CASCADE",
                "ALTER CLASS APP.CDC_CLASS COMPILE CASCADE",
                "ALTER PACKAGE APP.CDC_PACKAGE COMPILE DEBUG CASCADE",
                "DROP PACKAGE APP.CDC_PACKAGE CASCADE",
                "CREATE SCHEMA CDC_SCHEMA AUTHORIZATION SYSDBA CREATE TABLE A(ID INT)",
                "CREATE SCHEMA AUTHORIZATION SYSDBA CREATE TABLE A(ID INT) CREATE TABLE B(ID INT)",
                "CREATE SCHEMA CDC_SCHEMA GRANT SELECT ON APP.A TO CDC_USER",
                "CREATE USER",
                "ALTER USER",
                "DROP USER",
                "CREATE SCHEMA",
                "DROP SCHEMA",
                "CREATE DIRECTORY",
                "DROP DIRECTORY",
                "CREATE DOMAIN",
                "DROP DOMAIN",
                "CREATE CONTEXT",
                "DROP CONTEXT",
                "CREATE PROFILE",
                "ALTER PROFILE",
                "DROP PROFILE",
                "DROP LIBRARY",
                "CREATE TYPE APP.CDC_TYPE AS",
                "DROP TYPE IF EXISTS",
                "ALTER TYPE APP.CDC_TYPE",
                "ALTER CLASS APP.CDC_CLASS",
                "ALTER PACKAGE APP.CDC_PACKAGE",
                "DROP PACKAGE BODY IF EXISTS",
                "STAT",
                "STAT ON",
                "STAT 30 ON INDEX",
                "CREATE DIRECTORY CDC_DIRECTORY AS '/tmp/cdc",
                "CREATE USER CDC_USER IDENTIFIED BY Test_123456; ALTER TABLE APP.A ADD BAD INT",
                "ALTER USER CDC_USER DROP PROFILE DROP TABLE APP.A",
                "DROP USER CDC_USER DROP TABLE APP.A",
                "CREATE DIRECTORY CDC_DIRECTORY AS '/tmp/cdc' ALTER TABLE APP.A ADD BAD INT",
                "DROP DIRECTORY CDC_DIRECTORY; TRUNCATE TABLE APP.A",
                "CREATE SCHEMA CDC_SCHEMA; DROP TABLE APP.A",
                "CREATE TYPE APP.CDC_TYPE AS OBJECT(ID INT) DROP TABLE APP.A",
                "DROP TYPE APP.CDC_TYPE RESTRICT; ALTER TABLE APP.A ADD BAD INT",
                "ALTER PACKAGE APP.CDC_PACKAGE COMPILE CASCADE DEBUG TRUNCATE TABLE APP.A",
                "STAT 30 ON APP.A(ID) ALTER TABLE APP.A ADD BAD INT",
                "CREATE MATERIALIZED VIEW",
                "DROP MATERIALIZED VIEW LOG ON",
                "REFRESH MATERIALIZED VIEW",
                "CREATE MATERIALIZED VIEW APP.MV FOR APP.A ON PREBUILT TABLE AS SELECT ID,V FROM APP.A",
                "CREATE MATERIALIZED VIEW APP.MV FOR APP.A ON /* comment */ PREBUILT TABLE WITH REDUCED PRECISION AS SELECT ID,V FROM APP.A",
                "CREATE MATERIALIZED VIEW APP.MV AS SELECT ID FROM APP.A; ALTER TABLE APP.A ADD BAD INT",
                "CREATE MATERIALIZED VIEW APP.MV AS SELECT ID FROM APP.A DROP TABLE APP.A",
                "REFRESH MATERIALIZED VIEW APP.MV COMPLETE USING TRUNCATE TRUNCATE TABLE APP.A",
                "REFRESH MATERIALIZED VIEW APP.MV COMPLETE USING TRUNCATE; TRUNCATE TABLE APP.A",
                "DROP MATERIALIZED VIEW LOG ON APP.A; DROP TABLE APP.A");
    }

    @ParameterizedTest
    @MethodSource("unsafeStatements")
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
