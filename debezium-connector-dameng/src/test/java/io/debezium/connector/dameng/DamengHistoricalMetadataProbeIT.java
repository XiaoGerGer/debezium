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
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.dameng.logminer.LogMinerSql;
import io.debezium.relational.TableId;

/** Diagnostic only: an accepted API call does not establish historical schema recovery. */
class DamengHistoricalMetadataProbeIT {
    private final StringBuilder evidence = new StringBuilder();

    @Test
    void shouldProbeHistoricalMetadataAfterRenameAlterDropAndRecreate() throws Exception {
        assumeTrue(System.getProperty("dameng.hostname") == null, "Probe owns its database");
        DamengTestDatabase.start();
        try (var miner = new DamengConnection(configuration()); var jdbc = openJdbc()) {
            evidence.append("VERSION: ").append(jdbc.getMetaData().getDatabaseProductVersion()).append('\n');
            execute(jdbc, "ALTER SYSTEM SET 'ENABLE_FLASHBACK'=1");
            execute(jdbc, "ALTER SYSTEM SET 'UNDO_RETENTION'=3600");
            for (var name : List.of("DBMS_METADATA", "DBMS_FLASHBACK")) {
                try {
                    execute(jdbc, "SP_CREATE_SYSTEM_PACKAGES(1,'" + name + "')");
                }
                catch (SQLException failure) {
                    rejected("INSTALL_PACKAGE " + name, failure);
                }
            }
            query(jdbc, "SELECT PARA_NAME,PARA_VALUE,SESS_VALUE FROM V$DM_INI WHERE PARA_NAME IN ('ENABLE_FLASHBACK','UNDO_RETENTION')");
            final long start = miner.currentLsn();
            for (var sql : List.of(
                    "CREATE TABLE HM_CONTROL(ID INT PRIMARY KEY, VALUE VARCHAR(30))",
                    "CREATE TABLE HM_RENAME(ID INT PRIMARY KEY, OLD_NAME VARCHAR(30))",
                    "CREATE TABLE HM_EVOLVE(ID INT PRIMARY KEY, OLD_MIDDLE VARCHAR(30), TAIL_VALUE VARCHAR(30))",
                    "CREATE TABLE HM_GONE(ID INT PRIMARY KEY, LOST_VALUE VARCHAR(30))",
                    "CREATE TABLE HM_REUSE(ID INT PRIMARY KEY, OLD_VALUE VARCHAR(30))",
                    "INSERT INTO HM_CONTROL VALUES(1,'control-before')",
                    "INSERT INTO HM_RENAME VALUES(1,'rename-before')",
                    "INSERT INTO HM_EVOLVE VALUES(1,'middle-before','tail-before')",
                    "INSERT INTO HM_GONE VALUES(1,'gone-before')",
                    "INSERT INTO HM_REUSE VALUES(1,'reuse-before')",
                    "CREATE TABLE HM_CTAS AS SELECT A.ID,B.OLD_NAME FROM HM_CONTROL A JOIN HM_RENAME B ON A.ID=B.ID")) {
                execute(jdbc, sql);
            }
            final long s0 = miner.currentLsn();
            evidence.append("S0_LSN: ").append(s0).append('\n');
            final Map<String, Long> originalIds = new LinkedHashMap<>();
            for (var table : List.of("HM_RENAME", "HM_EVOLVE", "HM_GONE", "HM_REUSE", "HM_CTAS")) {
                final long id = miner.getTableObjectId(new TableId(null, "SYSDBA", table));
                originalIds.put(table, id);
                query(jdbc, "SELECT * FROM SYS.SYSCOLUMNS WHERE ID=" + id + " ORDER BY COLID");
                query(jdbc, "SELECT * FROM SYS.SYSCOLUMNS AS OF SCN " + s0 + " WHERE ID=" + id + " ORDER BY COLID");
            }
            final Map<Long, Map<Long, List<String>>> expectedColumns = new LinkedHashMap<>();
            expectedColumns.put(s0, captureColumns(jdbc, originalIds));
            metadata(jdbc, "S0");
            assertThat(query(jdbc, "SELECT DBMS_METADATA.GET_DDL('TABLE','HM_RENAME','SYSDBA') FROM DUAL").toString())
                    .contains("OLD_NAME");
            for (var sql : List.of(
                    "UPDATE HM_CONTROL SET VALUE='control-after' WHERE ID=1",
                    "ALTER TABLE HM_RENAME RENAME COLUMN OLD_NAME TO NEW_NAME",
                    "INSERT INTO HM_RENAME VALUES(2,'rename-after')",
                    "ALTER TABLE HM_EVOLVE RENAME COLUMN OLD_MIDDLE TO RENAMED_MIDDLE",
                    "INSERT INTO HM_EVOLVE VALUES(2,'middle-renamed','tail-renamed')",
                    "ALTER TABLE HM_CTAS RENAME COLUMN OLD_NAME TO JOIN_VALUE")) {
                execute(jdbc, sql);
            }
            final long s1 = miner.currentLsn();
            evidence.append("S1_LSN: ").append(s1).append('\n');
            expectedColumns.put(s1, captureColumns(jdbc, originalIds));
            metadata(jdbc, "S1");
            for (var sql : List.of(
                    "ALTER TABLE HM_EVOLVE DROP COLUMN RENAMED_MIDDLE",
                    "ALTER TABLE HM_EVOLVE ADD NEW_NUMBER DECIMAL(18,4) DEFAULT 0 NOT NULL",
                    "INSERT INTO HM_EVOLVE VALUES(3,'tail-final',1234.5678)",
                    "DROP TABLE HM_GONE",
                    "DROP TABLE HM_CTAS",
                    "DROP TABLE HM_REUSE",
                    "CREATE TABLE HM_REUSE(ID INT PRIMARY KEY, NEW_NUMBER DECIMAL(18,4))",
                    "INSERT INTO HM_REUSE VALUES(2,9876.5432)")) {
                execute(jdbc, sql);
            }
            final long end = miner.currentLsn();
            evidence.append("S2_LSN: ").append(end).append('\n');
            metadata(jdbc, "S2");
            assertThat(query(jdbc, "SELECT VALUE FROM HM_CONTROL AS OF SCN " + s0).toString())
                    .as("flashback must actually recover an old row in the unchanged control table")
                    .contains("control-before").doesNotContain("control-after");
            for (long lsn : List.of(s0, s1)) {
                query(jdbc, "SELECT * FROM HM_RENAME AS OF SCN " + lsn);
                query(jdbc, "SELECT * FROM HM_EVOLVE AS OF SCN " + lsn);
                query(jdbc, "SELECT * FROM HM_GONE AS OF SCN " + lsn);
                query(jdbc, "SELECT * FROM HM_REUSE AS OF SCN " + lsn);
                query(jdbc, "SELECT ID,NAME,INFO4 FROM SYS.SYSOBJECTS AS OF SCN " + lsn + " WHERE NAME LIKE 'HM_%' AND TYPE$='SCHOBJ'");
                query(jdbc, "SELECT * FROM SYS.SYSCOLUMNS AS OF SCN " + lsn
                        + " WHERE ID IN (SELECT ID FROM SYS.SYSOBJECTS WHERE NAME IN ('HM_RENAME','HM_EVOLVE'))");
                for (var entry : originalIds.entrySet()) {
                    evidence.append("HISTORICAL_COLUMNS: ").append(entry.getKey()).append(" object=").append(entry.getValue()).append('\n');
                    final var from = " FROM SYS.SYSCOLUMNS AS OF SCN " + lsn + " WHERE ID=" + entry.getValue();
                    query(jdbc, "SELECT NAME,COLID,TYPE$,LENGTH$,SCALE,NULLABLE$,DEFVAL" + from + " ORDER BY COLID");
                    query(jdbc, "SELECT /*+ FULL(SYSCOLUMNS) */ NAME,COLID,TYPE$,LENGTH$,SCALE,NULLABLE$,DEFVAL" + from + " ORDER BY COLID");
                    for (int col = 0; col < 4; col++) {
                        query(jdbc, "SELECT ROWID,NAME,COLID,TYPE$,LENGTH$,SCALE,NULLABLE$,DEFVAL" + from + " AND COLID=" + col);
                    }
                }
                try (var historical = openJdbc()) {
                    boolean enabled = false;
                    try {
                        execute(historical, "BEGIN DBMS_FLASHBACK.ENABLE_AT_SYSTEM_CHANGE_NUMBER(" + lsn + "); END;");
                        enabled = true;
                        evidence.append("FLASHBACK_SESSION_LSN: ").append(lsn).append('\n');
                        assertThat(query(historical, "SELECT VALUE FROM HM_CONTROL").toString())
                                .contains(lsn == s0 ? "control-before" : "control-after");
                        metadata(historical, "FLASHBACK_SESSION");
                        query(historical, "SELECT * FROM HM_RENAME");
                        query(historical, "SELECT * FROM HM_EVOLVE");
                    }
                    catch (SQLException failure) {
                        rejected("FLASHBACK_SESSION LSN=" + lsn, failure);
                    }
                    finally {
                        if (enabled) {
                            execute(historical, "BEGIN DBMS_FLASHBACK.DISABLE(); END;");
                        }
                    }
                }
            }
            final var disql = "SELECT NAME,COLID,TYPE$ FROM SYS.SYSCOLUMNS WHERE ID=" + originalIds.get("HM_EVOLVE") + " ORDER BY COLID;\n"
                    + "SELECT NAME,COLID,TYPE$ FROM SYS.SYSCOLUMNS AS OF SCN " + s0 + " WHERE ID=" + originalIds.get("HM_EVOLVE") + " ORDER BY COLID;\n"
                    + "SELECT NAME,COLID,TYPE$ FROM SYS.SYSCOLUMNS AS OF SCN " + s0 + " WHERE ID=" + originalIds.get("HM_EVOLVE") + " AND COLID=1;\n";
            evidence.append("DISQL_COLUMNS:\n").append(DamengTestDatabase.executeDisql(disql)).append('\n');
            // Compare exact pre-change definitions, including the now-dropped CTAS and old same-name table.
            // A fresh connection prevents a client-side metadata cache from satisfying these assertions.
            try (var historical = openJdbc()) {
                for (var snapshot : expectedColumns.entrySet()) {
                    for (var table : snapshot.getValue().entrySet()) {
                        assertThat(columnsAt(historical, table.getKey(), " AS OF SCN " + snapshot.getKey()))
                                .as("historical columns for object %s at LSN %s", table.getKey(), snapshot.getKey())
                                .containsExactlyElementsOf(table.getValue());
                    }
                }
                evidence.append("EXACT_HISTORICAL_COLUMN_COMPARISONS: 10 passed in a new JDBC session\n");
            }
            Awaitility.await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(100))
                    .until(() -> miner.latestArchivedLsn() >= end);
            final var baseline = mine(start, end, 2048, "", "dictionary-free");
            assertThat(baseline.toString()).contains("rename-before", "rename-after", "middle-before", "middle-renamed",
                    "tail-final", "gone-before", "reuse-before", "9876.5432");
            for (var row : baseline) {
                if (row.contains("OPERATION_CODE=5;") && row.contains("SQL_REDO=ALTER TABLE HM_RENAME RENAME COLUMN")) {
                    final long ddlLsn = Long.parseLong(field(row, "SCN"));
                    final long commitLsn = Long.parseLong(field(row, "COMMIT_SCN"));
                    evidence.append("DDL_BOUNDARY: record=").append(ddlLsn).append(" commit=").append(commitLsn).append('\n');
                    for (long lsn : List.of(ddlLsn - 1, ddlLsn, commitLsn - 1, commitLsn, commitLsn + 1)) {
                        columnsAt(jdbc, originalIds.get("HM_RENAME"), " AS OF SCN " + lsn);
                    }
                }
            }
            mine(start, end, 2064, "", "online");
            mine(start, end, 2068, "", "online-plus-bit4");
            mine(start, end, 2056, "", "dictionary-free-plus-bit8");
            mine(start, end, 2048, "/tmp/dbz-no-such-historical-dictionary.ora", "missing-dictionary-file");
            // Each mine call uses a fresh connection, including this replay after other modes.
            assertThat(mine(start, end, 2048, "", "dictionary-free-new-session")).isEqualTo(baseline);
            query(jdbc, "SELECT OWNER,OBJECT_NAME,OBJECT_TYPE FROM ALL_OBJECTS WHERE OBJECT_NAME LIKE 'DBMS_LOGMNR%'");
            try {
                execute(jdbc, "BEGIN DBMS_LOGMNR_D.BUILD('dbz-historical-dictionary.ora','/tmp'); END;");
            }
            catch (SQLException failure) {
                rejected("DBMS_LOGMNR_D.BUILD", failure);
            }
        }
        finally {
            try {
                final var path = Path.of("target", "dameng-historical-metadata-evidence", "probe.txt");
                Files.createDirectories(path.getParent());
                Files.writeString(path, evidence);
            }
            finally {
                DamengTestDatabase.stop();
            }
        }
    }

    private Map<Long, List<String>> captureColumns(Connection jdbc, Map<String, Long> ids) {
        final Map<Long, List<String>> columns = new LinkedHashMap<>();
        for (long id : ids.values()) {
            final var definition = columnsAt(jdbc, id, "");
            assertThat(definition).isNotEmpty();
            columns.put(id, definition);
        }
        return columns;
    }

    private List<String> columnsAt(Connection jdbc, long objectId, String at) {
        return query(jdbc, "SELECT NAME,COLID,TYPE$,LENGTH$,SCALE,NULLABLE$,DEFVAL FROM SYS.SYSCOLUMNS" + at
                + " WHERE ID=" + objectId + " ORDER BY COLID");
    }

    private String field(String row, String key) {
        final int start = row.indexOf(key + "=") + key.length() + 1;
        return row.substring(start, row.indexOf(';', start));
    }

    private List<String> mine(long start, long end, int options, String dictionary, String label) throws Exception {
        evidence.append("MINING: ").append(label).append(" options=").append(options).append('\n');
        try (var connection = new DamengConnection(configuration())) {
            for (var file : connection.archiveLogsForRange(start, end)) {
                connection.execute(LogMinerSql.addLogFile(file.path()));
            }
            boolean started = false;
            try {
                connection.execute("BEGIN DBMS_LOGMNR.START_LOGMNR(STARTSCN=>" + start + ",ENDSCN=>" + end
                        + ",OPTIONS=>" + options + ",DICTFILENAME=>'" + dictionary + "'); END;");
                started = true;
                return query(connection.connection(), "SELECT * FROM V$LOGMNR_CONTENTS WHERE SCN>" + start + " AND SCN<=" + end + " ORDER BY SCN");
            }
            catch (SQLException failure) {
                rejected(label, failure);
                return List.of();
            }
            finally {
                if (started) {
                    connection.execute(LogMinerSql.end());
                }
            }
        }
    }

    private void metadata(Connection jdbc, String phase) throws SQLException {
        evidence.append("METADATA_PHASE: ").append(phase).append('\n');
        try (var columns = jdbc.getMetaData().getColumns(null, "SYSDBA", "HM_%", null)) {
            while (columns.next()) {
                evidence.append("JDBC_COLUMN: ").append(columns.getString("TABLE_NAME")).append('.')
                        .append(columns.getString("COLUMN_NAME")).append(" type=").append(columns.getString("TYPE_NAME"))
                        .append(" position=").append(columns.getString("ORDINAL_POSITION"))
                        .append(" size=").append(columns.getString("COLUMN_SIZE"))
                        .append(" scale=").append(columns.getString("DECIMAL_DIGITS"))
                        .append(" nullable=").append(columns.getString("NULLABLE")).append('\n');
            }
        }
        query(jdbc, "SELECT ID,NAME,INFO4 FROM SYS.SYSOBJECTS WHERE NAME LIKE 'HM_%' AND TYPE$='SCHOBJ'");
        for (var table : List.of("HM_RENAME", "HM_EVOLVE", "HM_GONE", "HM_REUSE")) {
            query(jdbc, "SELECT DBMS_METADATA.GET_DDL('TABLE','" + table + "','SYSDBA') FROM DUAL");
        }
    }

    private List<String> query(Connection jdbc, String sql) {
        evidence.append("QUERY: ").append(sql).append('\n');
        final List<String> result = new ArrayList<>();
        try (var statement = jdbc.createStatement(); var rows = statement.executeQuery(sql)) {
            final var metadata = rows.getMetaData();
            while (rows.next()) {
                final var row = new StringBuilder();
                for (int i = 1; i <= metadata.getColumnCount(); i++) {
                    // Native handles are connection-specific; exclude them from replay equality.
                    if (metadata.getColumnLabel(i).equals("REDO_VALUE") || metadata.getColumnLabel(i).equals("UNDO_VALUE")) {
                        continue;
                    }
                    row.append(metadata.getColumnLabel(i)).append('=').append(rows.getString(i)).append(';');
                }
                result.add(row.toString());
                evidence.append("ROW: ").append(row).append('\n');
            }
            evidence.append("COUNT: ").append(result.size()).append('\n');
        }
        catch (SQLException failure) {
            rejected(sql, failure);
        }
        return result;
    }

    private void execute(Connection jdbc, String sql) throws SQLException {
        evidence.append("EXECUTE: ").append(sql).append('\n');
        try (var statement = jdbc.createStatement()) {
            statement.execute(sql);
        }
    }

    private void rejected(String operation, SQLException failure) {
        evidence.append("REJECTED: ").append(operation).append(" code=").append(failure.getErrorCode())
                .append(" state=").append(failure.getSQLState()).append(" message=").append(failure.getMessage()).append('\n');
    }

    private Connection openJdbc() throws SQLException {
        final var connection = DriverManager.getConnection(DamengTestDatabase.jdbcUrl(), DamengTestDatabase.user(), DamengTestDatabase.password());
        connection.setAutoCommit(true);
        return connection;
    }

    private Configuration configuration() {
        return Configuration.create().with("hostname", DamengTestDatabase.hostname()).with("port", DamengTestDatabase.port())
                .with("user", DamengTestDatabase.user()).with("password", DamengTestDatabase.password()).build();
    }
}
