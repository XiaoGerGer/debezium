/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.dameng.logminer.LogMinerRow;
import io.debezium.connector.dameng.logminer.LogMinerSql;
import io.debezium.connector.dameng.logminer.LogMinerTransactionBuffer;

/** Direct database capability probe, not acceptance of standby support in the Connector. */
class DamengStandbyLogMinerProbeIT {
    private final StringBuilder evidence = new StringBuilder();
    private final Path output = Path.of("target", "dameng-standby-logminer-evidence", "probe.txt");

    @Test
    void shouldCharacterizeStandbyMiningAcrossReconnectRestartAndSwitchover() throws Exception {
        try (var environment = new DamengPrimaryStandbyEnvironment()) {
            phase("START physical backup/restore Data Watch pair; fixture=" + environment.logDirectory());
            environment.start();
            phase("PAIR_READY");
            final String originalPrimary = environment.primaryJdbcUrl();
            final String originalStandby = environment.standbyJdbcUrl();
            try (var writer = open(originalPrimary); var standby = open(originalStandby)) {
                writer.setAutoCommit(true);
                recordEnvironment(writer, "primary");
                recordEnvironment(standby, "standby");
                assertRole(writer, "PRIMARY");
                assertRole(standby, "STANDBY");
                execute(writer, "CREATE TABLE SM_PROBE(ID INT PRIMARY KEY, LABEL VARCHAR(80), AMOUNT DECIMAL(18,4))");
                execute(writer, "CREATE TABLE SM_BULK(ID INT PRIMARY KEY, VALUE INT)");
                awaitCount(originalStandby, "SM_PROBE", 0);
                final long start = scalar(writer, "SELECT CUR_LSN FROM V$RLOG");
                execute(writer, "INSERT INTO SM_PROBE VALUES(1,'before',1234.5678)");
                execute(writer, "UPDATE SM_PROBE SET LABEL='after',AMOUNT=-0.0001 WHERE ID=1");
                execute(writer, "INSERT INTO SM_PROBE VALUES(2,'delete-me',22.2200)");
                execute(writer, "DELETE FROM SM_PROBE WHERE ID=2");
                writer.setAutoCommit(false);
                execute(writer, "INSERT INTO SM_PROBE VALUES(901,'full-rollback',1)");
                writer.rollback();
                execute(writer, "INSERT INTO SM_PROBE VALUES(3,'savepoint-kept',3)");
                final var savepoint = writer.setSavepoint("SM_SAVEPOINT");
                execute(writer, "INSERT INTO SM_PROBE VALUES(902,'savepoint-rollback',2)");
                writer.rollback(savepoint);
                execute(writer, "INSERT INTO SM_PROBE VALUES(4,'after-savepoint',4)");
                writer.commit();
                try (var insert = writer.prepareStatement("INSERT INTO SM_BULK VALUES(?,?)")) {
                    for (int i = 1; i <= 100; i++) {
                        insert.setInt(1, i);
                        insert.setInt(2, i);
                        insert.addBatch();
                    }
                    assertThat(insert.executeBatch()).hasSize(100);
                }
                writer.commit();
                writer.setAutoCommit(true);
                assertThat(update(writer, "UPDATE SM_BULK SET VALUE=VALUE+1000")).isEqualTo(100);
                execute(writer, "ALTER TABLE SM_PROBE RENAME COLUMN LABEL TO DESCRIPTION");
                execute(writer, "ALTER TABLE SM_PROBE ADD EXTRA VARCHAR(40)");
                execute(writer, "INSERT INTO SM_PROBE VALUES(5,'after-ddl',5.1234,'extra-value')");
                awaitCount(originalStandby, "SM_PROBE", 4);
                assertThat(query(standby, "SELECT * FROM SM_PROBE ORDER BY ID"))
                        .containsExactlyElementsOf(query(writer, "SELECT * FROM SM_PROBE ORDER BY ID"));
                assertThat(query(standby, "SELECT * FROM SM_BULK ORDER BY ID"))
                        .containsExactlyElementsOf(query(writer, "SELECT * FROM SM_BULK ORDER BY ID"));
                evidence.append("REPLICATED_TABLE_COMPARISON: SM_PROBE=4 rows, SM_BULK=100 rows passed\n");
                final long end = scalar(writer, "SELECT CUR_LSN FROM V$RLOG");
                phase("CRUD_DDL start=" + start + " end=" + end);
                final var primaryRows = mine(originalPrimary, start, end, false, "primary-crud");
                final var committed = committed(primaryRows);
                assertThat(committed).hasSize(209);
                assertThat(committed.stream().filter(row -> row.operation() == 1)).hasSize(105);
                assertThat(committed.stream().filter(row -> row.operation() == 3)).hasSize(101);
                assertThat(committed.stream().filter(row -> row.operation() == 2)).hasSize(1);
                assertThat(committed.stream().map(LogMinerRow::redo)).noneMatch(sql -> sql.contains("full-rollback") || sql.contains("savepoint-rollback"));
                assertThat(committed.stream().map(LogMinerRow::redo)).anyMatch(sql -> sql.contains("-0.0001"));
                assertThat(committed.stream().map(LogMinerRow::redo)).anyMatch(sql -> sql.contains("after-ddl") && sql.contains("extra-value"));
                assertThat(committed.stream().filter(row -> row.operation() == 5)).hasSize(2);
                evidence.append("PRIMARY_COMMITTED_RECORDS: 209 total, INSERT=105, UPDATE=101, DELETE=1, DDL=2; rollback filtering passed\n");
                final List<LogMinerRow> standbyRows;
                try {
                    standbyRows = mine(originalStandby, start, end, false, "standby-crud");
                }
                catch (SQLException rejected) {
                    recordFailure("STANDBY_MINING", rejected);
                    assertThat(rejected.getErrorCode()).as("record the observed standby restriction, not arbitrary failures").isEqualTo(-710);
                    characterizeStandbyRestriction(environment, start, end, primaryRows);
                    phase("NEGATIVE_CAPABILITY_CONFIRMED: standby DBMS_LOGMNR rejects archive registration; full standby CDC checks not reached");
                    return;
                }
                assertLogicalEquality(standbyRows, primaryRows);
                assertLogicalEquality(mine(originalStandby, start, end, false, "standby-reconnect"), standbyRows);
                assertLogicalEquality(mine(originalStandby, start, end, true, "standby-committed-only"),
                        mine(originalPrimary, start, end, true, "primary-committed-only"));
                probeCurrentConnectorAndReadOnlyOperations(originalStandby);

                // A transaction spans a mining window while another transaction commits.
                final long openStart = scalar(writer, "SELECT CUR_LSN FROM V$RLOG");
                writer.setAutoCommit(false);
                execute(writer, "INSERT INTO SM_PROBE VALUES(200,'long-open',200,NULL)");
                try (var shortWriter = open(originalPrimary)) {
                    shortWriter.setAutoCommit(true);
                    execute(shortWriter, "INSERT INTO SM_PROBE VALUES(201,'short-committed',201,NULL)");
                    final long openEnd = scalar(shortWriter, "SELECT CUR_LSN FROM V$RLOG");
                    phase("OPEN_TRANSACTION start=" + openStart + " end=" + openEnd);
                    final var beforeCommit = mine(originalStandby, openStart, openEnd, false, "standby-open-transaction");
                    assertLogicalEquality(beforeCommit, mine(originalPrimary, openStart, openEnd, false, "primary-open-transaction"));
                    final var analysis = analyze(beforeCommit, openEnd);
                    assertThat(analysis.openTransactions()).isGreaterThanOrEqualTo(1);
                    assertThat(analysis.restartLsn()).isLessThan(openEnd);
                    assertThat(committed(beforeCommit).stream().map(LogMinerRow::redo)).noneMatch(sql -> sql.contains("long-open"));
                    assertThat(committed(beforeCommit).stream().map(LogMinerRow::redo)).anyMatch(sql -> sql.contains("short-committed"));
                    writer.commit();
                    final long commitEnd = scalar(shortWriter, "SELECT CUR_LSN FROM V$RLOG");
                    final var afterCommit = mine(originalStandby, openStart, commitEnd, false, "standby-after-long-commit");
                    assertLogicalEquality(afterCommit, mine(originalPrimary, openStart, commitEnd, false, "primary-after-long-commit"));
                    assertThat(committed(afterCommit)).hasSize(2);
                    assertThat(committed(afterCommit).stream().map(LogMinerRow::redo)).anyMatch(sql -> sql.contains("long-open"));
                }
                writer.setAutoCommit(true);
                recordEnvironment(standby, "standby-after-workload");
                phase("RESTART_STANDBY");
                environment.restartStandby();
                assertLogicalEquality(mine(originalStandby, start, end, false, "standby-after-database-restart"), standbyRows);
                phase("MANUAL_SWITCHOVER");
                environment.switchover();
                try (var newWriter = environment.primaryConnection(); var newStandby = open(environment.standbyJdbcUrl())) {
                    newWriter.setAutoCommit(true);
                    assertRole(newWriter, "PRIMARY");
                    assertRole(newStandby, "STANDBY");
                    recordEnvironment(newWriter, "new-primary");
                    recordEnvironment(newStandby, "new-standby");
                    execute(newWriter, "INSERT INTO SM_PROBE VALUES(301,'after-switchover',301,NULL)");
                    awaitCount(environment.standbyJdbcUrl(), "SM_PROBE", 7);
                    final long switchEnd = scalar(newWriter, "SELECT CUR_LSN FROM V$RLOG");
                    final var switchedPrimary = mine(environment.primaryJdbcUrl(), start, switchEnd, false, "new-primary-all-history");
                    final var switchedStandby = mine(environment.standbyJdbcUrl(), start, switchEnd, false, "new-standby-all-history");
                    assertLogicalEquality(switchedStandby, switchedPrimary);
                    assertThat(committed(switchedStandby).stream().map(LogMinerRow::redo)).anyMatch(sql -> sql.contains("after-switchover"));
                    probeCurrentConnectorAndReadOnlyOperations(environment.standbyJdbcUrl());
                }
            }
            phase("PASS: primary/standby raw logical records, transaction filtering, reconnect, database restart and manual switchover");
        }
        finally {
            flush();
        }
    }

    private void characterizeStandbyRestriction(DamengPrimaryStandbyEnvironment environment, long start, long end,
                                                List<LogMinerRow> primaryRows)
            throws Exception {
        final var standbyUrl = environment.standbyJdbcUrl();
        try (var connection = open(standbyUrl)) {
            recordEnvironment(connection, "standby-rejection");
            for (var sql : List.of("SELECT OWNER,NAME,TYPE,LINE,TEXT FROM ALL_SOURCE WHERE NAME='DBMS_LOGMNR' ORDER BY TYPE,LINE",
                    "SELECT OWNER,OBJECT_NAME,OBJECT_TYPE FROM ALL_OBJECTS WHERE OBJECT_NAME LIKE '%LOGMNR%' ORDER BY OBJECT_NAME",
                    "SELECT * FROM V$LOGMNR_LOGS", "SELECT * FROM V$LOGMNR_CONTENTS WHERE ROWNUM<2")) {
                try {
                    query(connection, sql);
                }
                catch (SQLException rejected) {
                    recordFailure(sql, rejected);
                }
            }
        }
        probeCurrentConnectorAndReadOnlyOperations(standbyUrl);
        probeRegistrationVariants(standbyUrl, start, end);
        phase("NEGATIVE_CAPABILITY_RESTART_STANDBY");
        environment.restartStandby();
        probeRegistrationVariants(environment.standbyJdbcUrl(), start, end);
        phase("NEGATIVE_CAPABILITY_MANUAL_SWITCHOVER");
        environment.switchover();
        try (var connection = environment.primaryConnection(); var standby = open(environment.standbyJdbcUrl())) {
            assertRole(connection, "PRIMARY");
            assertRole(standby, "STANDBY");
            recordEnvironment(connection, "promoted-former-standby");
            recordEnvironment(standby, "demoted-former-primary");
        }
        // The exact same node and old files are now tested after promotion.
        assertLogicalEquality(mine(environment.primaryJdbcUrl(), start, end, false, "promoted-former-standby-old-history"), primaryRows);
        probeRegistrationVariants(environment.standbyJdbcUrl(), start, end);
    }

    private void probeRegistrationVariants(String url, long start, long end) throws Exception {
        for (boolean autoCommit : List.of(true, false)) {
            for (int options : List.of(1, 3)) {
                try (var connection = open(url)) {
                    assertRole(connection, "STANDBY");
                    connection.setAutoCommit(autoCommit);
                    final var paths = query(connection, "SELECT PATH FROM V$ARCH_FILE WHERE ARCH_LSN<=" + end + " AND CLSN>=" + start + " ORDER BY ARCH_LSN");
                    assertThat(paths).isNotEmpty();
                    final var encoded = paths.get(0);
                    final var path = encoded.substring("PATH=".length(), encoded.length() - 1);
                    final var label = "REGISTER autoCommit=" + autoCommit + " options=" + options;
                    assertStandbyWriteRejected(connection, "CALL DBMS_LOGMNR.ADD_LOGFILE('" + LogMinerSql.quoteLiteral(path) + "'," + options + ")", label);
                    try {
                        assertStandbyWriteRejected(connection, LogMinerSql.start(start, end, false), "START_WITHOUT_REGISTERED_FILE");
                    }
                    finally {
                        assertStandbyWriteRejected(connection, LogMinerSql.end(), "END_LOGMNR");
                        if (!autoCommit) {
                            connection.rollback();
                        }
                    }
                }
            }
        }
        flush();
    }

    private void assertStandbyWriteRejected(Connection connection, String sql, String label) {
        final var rejected = assertThrows(SQLException.class, () -> execute(connection, sql), label);
        recordFailure(label, rejected);
        assertThat(rejected.getErrorCode()).as(label).isEqualTo(-710);
    }

    private void probeCurrentConnectorAndReadOnlyOperations(String url) throws Exception {
        final var endpoint = URI.create(url.substring("jdbc:".length()));
        final var config = Configuration.create().with("hostname", endpoint.getHost()).with("port", endpoint.getPort())
                .with("user", DamengPrimaryStandbyEnvironment.USER)
                .with("password", DamengPrimaryStandbyEnvironment.PASSWORD).build();
        try (var connection = new DamengConnection(config)) {
            try {
                connection.validateServerRole();
                evidence.append("CONNECTOR_ROLE: accepted\n");
            }
            catch (SQLException rejected) {
                recordFailure("CONNECTOR_ROLE", rejected);
            }
            try {
                connection.validateLogMiningConfiguration();
                evidence.append("CONNECTOR_LOG_CONFIGURATION: accepted\n");
            }
            catch (Exception rejected) {
                evidence.append("CONNECTOR_LOG_CONFIGURATION: ").append(rejected).append('\n');
            }
            try {
                final long end = connection.latestArchivedLsn();
                connection.archiveLogsForRange(end - 1, end);
                evidence.append("CONNECTOR_ARCHIVE_VALIDATION: accepted\n");
            }
            catch (Exception rejected) {
                evidence.append("CONNECTOR_ARCHIVE_VALIDATION: ").append(rejected).append('\n');
            }
        }
        for (var sql : List.of("LOCK TABLE SM_PROBE IN SHARE MODE", "CREATE TABLE SM_SIGNAL(ID VARCHAR(64),TYPE VARCHAR(64),DATA VARCHAR(2000))",
                "INSERT INTO SM_PROBE VALUES(999,'standby-write',999,NULL)")) {
            try (var connection = open(url)) {
                connection.setAutoCommit(false);
                try {
                    execute(connection, sql);
                    evidence.append("STANDBY_OPERATION_ACCEPTED: ").append(sql).append('\n');
                }
                catch (SQLException rejected) {
                    recordFailure(sql, rejected);
                }
                finally {
                    connection.rollback();
                }
            }
        }
        flush();
    }

    private List<LogMinerRow> mine(String url, long start, long end, boolean committedOnly, String label) throws Exception {
        phase("MINING " + label + " start=" + start + " end=" + end + " committedOnly=" + committedOnly);
        try (var connection = open(url)) {
            Awaitility.await("standby/primary archives cover the test window").atMost(Duration.ofSeconds(60))
                    .pollInterval(Duration.ofMillis(200)).until(() -> scalar(connection, "SELECT MAX(CLSN) FROM V$ARCH_FILE") >= end);
            final var files = query(connection, "SELECT PATH FROM V$ARCH_FILE WHERE ARCH_LSN<=" + end + " AND CLSN>=" + start + " ORDER BY ARCH_LSN");
            assertThat(files).isNotEmpty();
            for (var path : files) {
                // The query contains one column; preserve the exact server-side filename.
                execute(connection, LogMinerSql.addLogFile(path.substring("PATH=".length(), path.length() - 1)));
            }
            execute(connection, LogMinerSql.start(start, end, committedOnly));
            try {
                final List<LogMinerRow> result = new ArrayList<>();
                try (var statement = connection.prepareStatement(LogMinerSql.contentsQuery(null, committedOnly))) {
                    statement.setQueryTimeout(30);
                    statement.setLong(1, start);
                    statement.setLong(2, end);
                    try (var rows = statement.executeQuery()) {
                        while (rows.next()) {
                            final var xid = rows.getBytes("XID");
                            final var timestamp = rows.getTimestamp("TIMESTAMP");
                            final var row = new LogMinerRow(rows.getLong("SCN"), rows.getLong("START_SCN"), rows.getLong("COMMIT_SCN"),
                                    xid == null ? null : HexFormat.of().withUpperCase().formatHex(xid), rows.getInt("OPERATION_CODE"), rows.getInt("ROLL_BACK"),
                                    rows.getString("SEG_OWNER"), rows.getString("TABLE_NAME"), rows.getString("USERNAME"), rows.getString("SQL_REDO"),
                                    rows.getString("RS_ID"), rows.getInt("SSN"), rows.getInt("CSF"), timestamp == null ? null : timestamp.toInstant(),
                                    rows.getLong("DATA_OBJ#"));
                            result.add(row);
                            evidence.append("REDO: ").append(row).append('\n');
                        }
                    }
                }
                evidence.append("MINED_COUNT: ").append(result.size()).append('\n');
                return result;
            }
            finally {
                execute(connection, LogMinerSql.end());
                flush();
            }
        }
    }

    private void assertLogicalEquality(List<LogMinerRow> standby, List<LogMinerRow> primary) {
        assertThat(standby).containsExactlyElementsOf(primary);
        evidence.append("EXACT_LOGICAL_COMPARISON: ").append(standby.size()).append(" rows passed\n");
    }

    private record LogMinerAnalysis(long openTransactions, long restartLsn, List<LogMinerRow> committedRows) {
    }

    private LogMinerAnalysis analyze(List<LogMinerRow> rows, long endLsn) {
        try (var buffer = new LogMinerTransactionBuffer(64 * 1024 * 1024, 4 * 1024 * 1024, 10_000,
                Path.of(System.getProperty("java.io.tmpdir")), 256 * 1024 * 1024)) {
            rows.forEach(buffer::add);
            final long openTransactions = buffer.openTransactions();
            final long restartLsn = buffer.completeWindow(endLsn);
            return new LogMinerAnalysis(openTransactions, restartLsn, committedRows(buffer));
        }
    }

    private List<LogMinerRow> committed(List<LogMinerRow> rows) {
        final long endLsn = rows.stream().mapToLong(LogMinerRow::lsn).max().orElse(0);
        return analyze(rows, endLsn).committedRows();
    }

    private List<LogMinerRow> committedRows(LogMinerTransactionBuffer buffer) {
        final List<LogMinerRow> committed = new ArrayList<>();
        for (var transaction : buffer.committedTransactions()) {
            try (var cursor = transaction.open()) {
                LogMinerRow row;
                while ((row = cursor.next()) != null) {
                    committed.add(row);
                }
            }
        }
        return List.copyOf(committed);
    }

    private void recordEnvironment(Connection connection, String label) throws SQLException {
        evidence.append("ENVIRONMENT: ").append(label).append(" database=").append(connection.getMetaData().getDatabaseProductVersion())
                .append(" driver=").append(connection.getMetaData().getDriverVersion()).append('\n');
        for (var sql : List.of("SELECT * FROM V$VERSION", "SELECT INSTANCE_NAME,MODE$,STATUS$,DSC_ROLE,DSC_SEQNO FROM V$INSTANCE",
                "SELECT * FROM V$RLOG", "SELECT * FROM V$RAPPLY_SYS", "SELECT * FROM V$RAPPLY_LOG_TASK", "SELECT * FROM V$KEEP_RLOG_PKG",
                "SELECT DB_MAGIC,SRC_DB_MAGIC,PMNT_MAGIC,DSC_SEQNO,ARCH_LSN,CLSN,PREV_LSN,PATH FROM V$ARCH_FILE ORDER BY ARCH_LSN",
                "SELECT RGUID,OPEN_TIME,CUR_DB_MAGIC,APPLY_LSN_0,N_EP,PRIMARY_DB_MAGIC FROM SYS.SYSOPENHISTORY WHERE OPEN_TIME IS NOT NULL ORDER BY OPEN_TIME",
                "SELECT PARA_NAME,PARA_VALUE FROM V$DM_INI WHERE PARA_NAME IN ('RLOG_APPEND_LOGIC','RLOG_IGNORE_TABLE_SET','RLOG_LLOG_COMPRESS','COMPATIBLE_MODE')")) {
            try {
                query(connection, sql);
            }
            catch (SQLException unavailable) {
                recordFailure(sql, unavailable);
            }
        }
    }

    private void assertRole(Connection connection, String role) throws SQLException {
        assertThat(query(connection, "SELECT MODE$,STATUS$ FROM V$INSTANCE")).containsExactly("MODE$=" + role + ";STATUS$=OPEN;");
    }

    private void awaitCount(String url, String table, long expected) {
        Awaitility.await("standby query sees committed primary rows").atMost(Duration.ofSeconds(60)).pollInterval(Duration.ofMillis(200))
                .ignoreExceptionsInstanceOf(SQLException.class).until(() -> {
                    try (var connection = open(url)) {
                        return scalar(connection, "SELECT COUNT(*) FROM " + table) == expected;
                    }
                });
    }

    private Connection open(String url) throws SQLException {
        final var connection = DriverManager.getConnection(url, DamengPrimaryStandbyEnvironment.USER, DamengPrimaryStandbyEnvironment.PASSWORD);
        connection.setAutoCommit(true);
        return connection;
    }

    private long scalar(Connection connection, String sql) throws SQLException {
        try (var statement = connection.createStatement()) {
            statement.setQueryTimeout(15);
            try (var rows = statement.executeQuery(sql)) {
                assertThat(rows.next()).isTrue();
                return rows.getLong(1);
            }
        }
    }

    private List<String> query(Connection connection, String sql) throws SQLException {
        evidence.append("QUERY: ").append(sql).append('\n');
        final List<String> result = new ArrayList<>();
        try (var statement = connection.createStatement()) {
            statement.setQueryTimeout(15);
            try (var rows = statement.executeQuery(sql)) {
                while (rows.next()) {
                    final var row = new StringBuilder();
                    for (int i = 1; i <= rows.getMetaData().getColumnCount(); i++) {
                        row.append(rows.getMetaData().getColumnLabel(i)).append('=').append(rows.getString(i)).append(';');
                    }
                    result.add(row.toString());
                    evidence.append("ROW: ").append(row).append('\n');
                }
            }
        }
        return result;
    }

    private void execute(Connection connection, String sql) throws SQLException {
        evidence.append("EXECUTE: ").append(sql).append('\n');
        try (var statement = connection.createStatement()) {
            statement.setQueryTimeout(15);
            statement.execute(sql);
        }
    }

    private int update(Connection connection, String sql) throws SQLException {
        evidence.append("UPDATE: ").append(sql).append('\n');
        try (var statement = connection.createStatement()) {
            statement.setQueryTimeout(15);
            return statement.executeUpdate(sql);
        }
    }

    private void recordFailure(String operation, SQLException failure) {
        evidence.append("REJECTED: ").append(operation).append(" code=").append(failure.getErrorCode())
                .append(" state=").append(failure.getSQLState()).append(" message=").append(failure.getMessage()).append('\n');
    }

    private void phase(String message) throws Exception {
        evidence.append("PHASE: ").append(message).append('\n');
        flush();
    }

    private void flush() throws java.io.IOException {
        Files.createDirectories(output.getParent());
        Files.writeString(output, evidence);
    }
}
