/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;

/** Records server syntax, column metadata and dictionary-free redo before enabling DDL extensions. */
class DamengDdlExtensionsProbeIT {
    @Test
    void shouldExposeCtasJoinAsOriginalDdlAndMaterializedTargetInserts() throws Exception {
        assumeTrue(System.getProperty("dameng.hostname") == null, "Probe owns its database");
        DamengTestDatabase.start();
        final var evidence = new StringBuilder();
        try (var connection = new DamengConnection(Configuration.create().with("hostname", DamengTestDatabase.hostname())
                .with("port", DamengTestDatabase.port()).with("user", DamengTestDatabase.user())
                .with("password", DamengTestDatabase.password()).build());
                var jdbc = DriverManager.getConnection(DamengTestDatabase.jdbcUrl(), DamengTestDatabase.user(), DamengTestDatabase.password())) {
            jdbc.setAutoCommit(true);
            evidence.append("VERSION: ").append(jdbc.getMetaData().getDatabaseProductVersion()).append('\n');
            for (var sql : List.of("CREATE TABLE JT_SOURCE(ID INT PRIMARY KEY, CATEGORY_ID INT NOT NULL)",
                    "CREATE TABLE JT_LOOKUP(ID INT PRIMARY KEY, LABEL VARCHAR(30) NOT NULL)",
                    "INSERT INTO JT_SOURCE VALUES(1,10)", "INSERT INTO JT_SOURCE VALUES(2,20)", "INSERT INTO JT_SOURCE VALUES(3,30)",
                    "INSERT INTO JT_LOOKUP VALUES(10,'ten')", "INSERT INTO JT_LOOKUP VALUES(20,'twenty')")) {
                try (var statement = jdbc.createStatement()) {
                    statement.execute(sql);
                    evidence.append("SETUP: ").append(sql).append('\n');
                }
            }
            final var inner = "CREATE TABLE JT_INNER AS SELECT A.ID AS ORDER_ID,B.ID AS CATEGORY_ID,B.LABEL "
                    + "FROM JT_SOURCE A INNER JOIN JT_LOOKUP B ON A.CATEGORY_ID=B.ID";
            final var left = "CREATE TABLE JT_LEFT AS SELECT A.ID AS ORDER_ID,B.ID AS CATEGORY_ID,B.LABEL "
                    + "FROM JT_SOURCE A LEFT JOIN JT_LOOKUP B ON A.CATEGORY_ID=B.ID";
            final long start = connection.currentLsn();
            for (var sql : List.of(inner, left)) {
                try (var statement = jdbc.createStatement()) {
                    statement.execute(sql);
                    evidence.append("EXECUTED: ").append(sql).append('\n');
                }
            }
            for (var table : List.of("JT_INNER", "JT_LEFT")) {
                final List<String> actual = new ArrayList<>();
                try (var statement = jdbc.createStatement(); var rows = statement.executeQuery("SELECT * FROM " + table + " ORDER BY ORDER_ID")) {
                    while (rows.next()) {
                        actual.add(rows.getString(1) + "," + rows.getString(2) + "," + rows.getString(3));
                    }
                }
                catch (SQLException failure) {
                    // Preserve the old server's LEFT JOIN result-read failure while
                    // still collecting the raw log. This probe is not CDC acceptance.
                    evidence.append("SELECT_FAILURE: ").append(table).append(" code=").append(failure.getErrorCode())
                            .append(" message=").append(failure.getMessage()).append('\n');
                    assertThat(table).isEqualTo("JT_LEFT");
                    continue;
                }
                final List<String> expected = new ArrayList<>(List.of("1,10,ten", "2,20,twenty"));
                if (table.equals("JT_LEFT")) {
                    expected.add("3,null,null");
                }
                assertThat(actual).containsExactlyElementsOf(expected);
                evidence.append("ROWS: ").append(table).append('=').append(actual).append('\n');
            }
            try (var columns = jdbc.getMetaData().getColumns(null, "SYSDBA", "JT_%", null)) {
                while (columns.next()) {
                    evidence.append("COLUMN: ").append(columns.getString("TABLE_NAME")).append('.').append(columns.getString("COLUMN_NAME"))
                            .append(" type=").append(columns.getString("TYPE_NAME")).append(" nullable=").append(columns.getString("NULLABLE"))
                            .append('\n');
                }
            }
            final long end = connection.currentLsn();
            Awaitility.await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(100))
                    .until(() -> connection.latestArchivedLsn() >= end);
            connection.startLogMining(start, end, false);
            try {
                record MinedRow(long lsn, int operation, long objectId, String sql) {
                }
                final List<MinedRow> mined = connection.queryAndMap("SELECT SCN,OPERATION_CODE,DATA_OBJ#,SQL_REDO,CSF FROM V$LOGMNR_CONTENTS "
                        + "WHERE SCN > " + start + " AND SCN <= " + end + " ORDER BY SCN", rows -> {
                            final List<MinedRow> result = new ArrayList<>();
                            while (rows.next()) {
                                assertThat(rows.getInt(5)).as("short probe statements must not be fragmented").isZero();
                                result.add(new MinedRow(rows.getLong(1), rows.getInt(2), rows.getLong(3), rows.getString(4)));
                            }
                            return result;
                        });
                mined.forEach(row -> evidence.append("REDO: ").append(row).append('\n'));
                for (var entry : Map.of("JT_INNER", inner, "JT_LEFT", left).entrySet()) {
                    final long objectId = connection.getTableObjectId(new io.debezium.relational.TableId(null, "SYSDBA", entry.getKey()));
                    evidence.append("OBJECT: ").append(entry.getKey()).append('=').append(objectId).append('\n');
                    final var target = mined.stream().filter(row -> row.objectId() == objectId).toList();
                    assertThat(target.stream().filter(row -> row.operation() == 5).map(MinedRow::sql))
                            .containsExactly(entry.getValue());
                    final var inserts = target.stream().filter(row -> row.operation() == 1).map(MinedRow::sql).toList();
                    assertThat(inserts).hasSize(entry.getKey().equals("JT_INNER") ? 2 : 3);
                    assertThat(inserts).anySatisfy(sql -> assertThat(sql).contains("VALUES(1, 10, 'ten')"));
                    assertThat(inserts).anySatisfy(sql -> assertThat(sql).contains("VALUES(2, 20, 'twenty')"));
                    if (entry.getKey().equals("JT_LEFT")) {
                        assertThat(inserts).anySatisfy(sql -> assertThat(sql).contains("VALUES(3, NULL, NULL)"));
                    }
                }
                for (var table : List.of("JT_SOURCE", "JT_LOOKUP")) {
                    final long objectId = connection.getTableObjectId(new io.debezium.relational.TableId(null, "SYSDBA", table));
                    assertThat(mined).noneMatch(row -> row.objectId() == objectId && row.operation() >= 1 && row.operation() <= 3);
                }
            }
            finally {
                connection.endLogMining();
            }
        }
        finally {
            final var path = Path.of("target", "dameng-p1-evidence", "ddl-join-probe.txt");
            Files.createDirectories(path.getParent());
            Files.writeString(path, evidence);
            DamengTestDatabase.stop();
        }
    }

    @Test
    void shouldRecordDdlAndPhysicalColumnSemantics() throws Exception {
        assumeTrue(System.getProperty("dameng.hostname") == null, "Probe owns its database");
        DamengTestDatabase.start();
        final var evidence = new StringBuilder();
        try (var connection = new DamengConnection(Configuration.create().with("hostname", DamengTestDatabase.hostname())
                .with("port", DamengTestDatabase.port()).with("user", DamengTestDatabase.user())
                .with("password", DamengTestDatabase.password()).build());
                var jdbc = DriverManager.getConnection(DamengTestDatabase.jdbcUrl(), DamengTestDatabase.user(), DamengTestDatabase.password())) {
            jdbc.setAutoCommit(true);
            evidence.append("VERSION: ").append(jdbc.getMetaData().getDatabaseProductVersion()).append('\n');
            final long start = connection.currentLsn();
            final List<String> statements = List.of(
                    "CREATE TABLE P1_SOURCE (ID INT PRIMARY KEY, V VARCHAR(30) DEFAULT 'text')",
                    "INSERT INTO P1_SOURCE VALUES (1,'first')",
                    "CREATE TABLE P1_CTAS AS SELECT * FROM P1_SOURCE",
                    "INSERT INTO P1_CTAS VALUES (2,'second')",
                    "CREATE TABLE P1_CTAS_EXPR AS SELECT ID+1 AS NEW_ID, CAST(V AS VARCHAR(50)) AS NEW_V FROM P1_SOURCE",
                    "CREATE TABLE P1_CTAS_TYPED(ID INT, V VARCHAR(30)) AS SELECT * FROM P1_SOURCE",
                    "CREATE TABLE P1_LIKE LIKE P1_SOURCE",
                    "CREATE TABLE P1_CTAS_RENAME(A,B) AS SELECT ID,V FROM P1_SOURCE WHERE ID=1",
                    "CREATE TABLE P1_PHYSICAL (ID INT, V VARCHAR(30)) STORAGE(INITIAL 1,NEXT 1,MINEXTENTS 1,ON MAIN,FILLFACTOR 80,USING LONG ROW)",
                    "CREATE TABLE P1_TABLESPACE (ID INT, V VARCHAR(30)) TABLESPACE MAIN",
                    "CREATE TABLE P1_DEFAULT (ID INT, V VARCHAR(30) DEFAULT ON NULL 'fallback')",
                    "INSERT INTO P1_DEFAULT(ID,V) VALUES(1,NULL)",
                    "CREATE TABLE P1_COMMENT (ID INT COMMENT 'identifier', V VARCHAR(30) DEFAULT 'text' COMMENT 'value')",
                    "CREATE TABLE P1_VIRTUAL (ID INT, V INT GENERATED ALWAYS AS(ID+1) VIRTUAL, T VARCHAR(30))",
                    "INSERT INTO P1_VIRTUAL(ID,T) VALUES (1,'first')",
                    "UPDATE P1_VIRTUAL SET ID=2,T='second'",
                    "CREATE TABLE P1_PARTITION (ID INT, V VARCHAR(30)) PARTITION BY RANGE(ID) (PARTITION P0 VALUES LESS THAN(10), PARTITION P1 VALUES LESS THAN(MAXVALUE))",
                    "INSERT INTO P1_PARTITION VALUES(1,'low')",
                    "INSERT INTO P1_PARTITION VALUES(20,'high')",
                    "UPDATE P1_PARTITION SET ID=21 WHERE ID=1",
                    "CREATE GLOBAL TEMPORARY TABLE P1_TEMP (ID INT, V VARCHAR(30)) ON COMMIT PRESERVE ROWS",
                    "INSERT INTO P1_TEMP VALUES(1,'temporary')",
                    "CREATE HUGE TABLE P1_HUGE (ID INT, V VARCHAR(30))",
                    "INSERT INTO P1_HUGE VALUES(1,'huge')",
                    "UPDATE P1_HUGE SET V='updated' WHERE ID=1",
                    "DELETE FROM P1_HUGE WHERE ID=1",
                    "CREATE TABLE P1_HASH(ID INT, V VARCHAR(30)) PARTITION BY HASH(ID) PARTITIONS 2",
                    "INSERT INTO P1_HASH VALUES(1,'hash')",
                    "CREATE TABLE P1_LIST(ID INT, V VARCHAR(30)) PARTITION BY LIST(ID) (PARTITION P0 VALUES(1,2), PARTITION P1 VALUES(DEFAULT))",
                    "INSERT INTO P1_LIST VALUES(1,'list')",
                    "ALTER TABLE P1_PARTITION ADD PARTITION P2 VALUES LESS THAN(100)",
                    "CREATE SCHEMA P1_COMPOUND AUTHORIZATION SYSDBA CREATE TABLE P1_A(ID INT) CREATE TABLE P1_B(ID INT)");
            for (var sql : statements) {
                evidence.append("SQL: ").append(sql).append('\n');
                try (var statement = jdbc.createStatement()) {
                    statement.execute(sql);
                    evidence.append("ACCEPTED\n");
                }
                catch (SQLException rejected) {
                    evidence.append("REJECTED: ").append(rejected.getErrorCode()).append(' ').append(rejected.getMessage()).append('\n');
                }
            }
            try (var statement = jdbc.createStatement();
                    var rows = statement.executeQuery(
                            "SELECT PARA_NAME,PARA_VALUE,SESS_VALUE,FILE_VALUE FROM V$DM_INI WHERE PARA_NAME IN ('IDENTIFIER_MODE','CASE_SENSITIVE','NESTED_C_STYLE_COMMENT')")) {
                while (rows.next()) {
                    evidence.append("PARAM: ").append(rows.getString(1)).append('=').append(rows.getString(2)).append('/')
                            .append(rows.getString(3)).append('/').append(rows.getString(4)).append('\n');
                }
            }
            try (var statement = jdbc.createStatement(); var rows = statement.executeQuery("SELECT SF_GET_CASE_SENSITIVE_FLAG() FROM DUAL")) {
                assertThat(rows.next()).isTrue();
                evidence.append("CASE_SENSITIVE: ").append(rows.getString(1)).append('\n');
            }
            try (var rows = jdbc.getMetaData().getColumns(null, "SYSDBA", "P1_%", null)) {
                while (rows.next()) {
                    evidence.append("COLUMN: ").append(rows.getString("TABLE_NAME")).append('.').append(rows.getString("COLUMN_NAME"))
                            .append(" type=").append(rows.getString("TYPE_NAME")).append(" position=").append(rows.getString("ORDINAL_POSITION"))
                            .append(" size=").append(rows.getString("COLUMN_SIZE")).append(" scale=").append(rows.getString("DECIMAL_DIGITS"))
                            .append(" nullable=").append(rows.getString("NULLABLE")).append(" default=").append(rows.getString("COLUMN_DEF"))
                            .append(" generated=").append(rows.getString("IS_GENERATEDCOLUMN")).append('\n');
                }
            }
            for (var query : List.of("SELECT * FROM ALL_TABLES WHERE OWNER='SYSDBA' AND TABLE_NAME IN ('P1_HUGE','P1_TEMP','P1_PARTITION','P1_VIRTUAL')",
                    "SELECT * FROM SYS.SYSOBJECTS WHERE NAME IN ('P1_HUGE','P1_TEMP','P1_PARTITION','P1_VIRTUAL')")) {
                try (var statement = jdbc.createStatement(); var rows = statement.executeQuery(query)) {
                    while (rows.next()) {
                        evidence.append("OBJECT: ");
                        for (int i = 1; i <= rows.getMetaData().getColumnCount(); i++) {
                            evidence.append(rows.getMetaData().getColumnName(i)).append('=').append(rows.getString(i)).append(';');
                        }
                        evidence.append('\n');
                    }
                }
            }
            final long end = connection.currentLsn();
            Awaitility.await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(100))
                    .until(() -> connection.latestArchivedLsn() >= end);
            connection.startLogMining(start, end, false);
            try {
                final List<String> redo = connection.queryAndMap("SELECT SCN,OPERATION_CODE,DATA_OBJ#,SEG_OWNER,TABLE_NAME,SQL_REDO FROM V$LOGMNR_CONTENTS "
                        + "WHERE SCN >= " + start + " AND SCN <= " + end + " ORDER BY SCN", rows -> {
                            final List<String> result = new ArrayList<>();
                            while (rows.next()) {
                                result.add(rows.getString(1) + " op=" + rows.getString(2) + " obj=" + rows.getString(3)
                                        + " owner=" + rows.getString(4) + " table=" + rows.getString(5) + " sql=" + rows.getString(6));
                            }
                            return result;
                        });
                evidence.append("REDO:\n").append(String.join("\n", redo)).append('\n');
                assertThat(redo).isNotEmpty();
            }
            finally {
                connection.endLogMining();
            }
        }
        finally {
            final var path = Path.of("target", "dameng-p1-evidence", System.getProperty("dameng.probe.name", "ddl-probe") + ".txt");
            Files.createDirectories(path.getParent());
            Files.writeString(path, evidence);
            DamengTestDatabase.stop();
        }
    }
}
