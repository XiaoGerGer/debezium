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
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.debezium.config.Configuration;
import io.debezium.connector.dameng.logminer.LogMinerSql;
import io.debezium.relational.TableId;

/** Compares identical materialized-storage redo with and without the current online dictionary. */
class DamengMaterializedViewDictionaryProbeIT {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final Path EVIDENCE = Path.of("target/materialized-view-dictionary-it.json");
    private static final String BASE = "DBZ_MVD_BASE";
    private static final String COMPLETE = "DBZ_MVD_COMPLETE";
    private static final String FAST = "DBZ_MVD_FAST";
    private static final String MARKER = "DBZ_MVD_MARKER";

    @BeforeAll
    static void startDatabase() throws Exception {
        assumeTrue(System.getProperty("dameng.hostname") == null, "Dictionary replay requires a test-owned database");
        assumeTrue(DamengTestDatabase.requestedCompatibilityMode() == 0, "Dictionary replay uses COMPATIBLE_MODE=0");
        Files.deleteIfExists(EVIDENCE);
        DamengTestDatabase.start();
    }

    @AfterAll
    static void stopDatabase() {
        DamengTestDatabase.stop();
    }

    @Test
    void comparesRawNamesForTheSameRedoAcrossDictionaryOptions() throws Exception {
        final var report = JSON.createObjectNode();
        report.put("validationPassed", false);
        report.put("dictionaryOption", "16 enables DICT_FROM_ONLINE_CATALOG; each option runs in a fresh JDBC session");
        report.put("scope", "Objects stay present throughout mining; names in SQL_REDO and TABLE_NAME are observed without assuming resolution");
        report.put("xidEncoding", "Lowercase hex of original JDBC getBytes(XID)");
        report.set("steps", JSON.createArrayNode());
        report.set("modes", JSON.createArrayNode());
        try (var jdbc = DriverManager.getConnection(DamengTestDatabase.jdbcUrl(), DamengTestDatabase.user(), DamengTestDatabase.password());
                var miner = connect()) {
            jdbc.setAutoCommit(true);
            report.put("databaseVersion", jdbc.getMetaData().getDatabaseProductVersion());
            report.put("driverVersion", jdbc.getMetaData().getDriverVersion());
            for (final String sql : List.of("CREATE TABLE SYSDBA." + BASE + "(ID INT PRIMARY KEY,V VARCHAR(32))",
                    "INSERT INTO SYSDBA." + BASE + " VALUES(1,'one')", "INSERT INTO SYSDBA." + BASE + " VALUES(2,'two')",
                    "CREATE TABLE SYSDBA." + MARKER + "(ID INT PRIMARY KEY)")) {
                execute(jdbc, sql);
            }
            final long markerId = miner.getTableObjectId(new TableId(null, "SYSDBA", MARKER));
            final long start = miner.currentLsn();
            report.put("startLsn", start);
            for (final String sql : List.of("CREATE MATERIALIZED VIEW SYSDBA." + COMPLETE + " REFRESH COMPLETE ON DEMAND AS SELECT ID,V FROM SYSDBA." + BASE,
                    "CREATE MATERIALIZED VIEW LOG ON SYSDBA." + BASE + " WITH ROWID(V) PURGE IMMEDIATE SYNCHRONOUS",
                    "CREATE MATERIALIZED VIEW SYSDBA." + FAST + " REFRESH FAST ON DEMAND WITH ROWID AS SELECT ID,V,ROWID AS X FROM SYSDBA." + BASE,
                    "UPDATE SYSDBA." + BASE + " SET V='updated' WHERE ID=1", "REFRESH MATERIALIZED VIEW SYSDBA." + FAST + " FAST",
                    "REFRESH MATERIALIZED VIEW SYSDBA." + COMPLETE + " COMPLETE USING DELETE")) {
                final var step = JSON.createObjectNode().put("sql", sql).put("startLsn", miner.currentLsn());
                ((ArrayNode) report.required("steps")).add(step);
                execute(jdbc, sql);
                step.put("endLsn", miner.currentLsn());
            }
            final var objects = query(jdbc, "SELECT T.ID,T.NAME,T.TYPE$,T.SUBTYPE$,T.PID FROM SYS.SYSOBJECTS T JOIN SYS.SYSOBJECTS S ON T.SCHID=S.ID "
                    + "WHERE S.NAME='SYSDBA' AND (T.NAME LIKE 'DBZ_MVD_%' OR T.NAME LIKE 'MTAB$_DBZ_MVD_%' OR T.NAME LIKE 'MLOG$_DBZ_MVD_%') ORDER BY T.ID");
            report.set("objects", objects);
            final long completeId = objectId(objects, "MTAB$_" + COMPLETE);
            final long fastId = objectId(objects, "MTAB$_" + FAST);
            report.put("completeStorageObjectId", completeId);
            report.put("fastStorageObjectId", fastId);
            report.put("markerObjectId", markerId);
            final var metadata = JSON.createObjectNode();
            report.set("columnMetadata", metadata);
            for (final String table : List.of(BASE, COMPLETE, FAST, "MTAB$_" + COMPLETE, "MTAB$_" + FAST, "MLOG$_" + BASE)) {
                metadata.set(table, columns(jdbc, table));
            }
            report.set("baseRows", query(jdbc, "SELECT ID,V FROM SYSDBA." + BASE + " ORDER BY ID"));
            report.set("completeRows", query(jdbc, "SELECT ID,V FROM SYSDBA." + COMPLETE + " ORDER BY ID"));
            report.set("fastRows", query(jdbc, "SELECT ID,V FROM SYSDBA." + FAST + " ORDER BY ID"));
            final long beforeMarker = miner.currentLsn();
            execute(jdbc, "INSERT INTO SYSDBA." + MARKER + " VALUES(1)");
            final long end = miner.currentLsn();
            report.put("beforeMarkerLsn", beforeMarker);
            report.put("endLsn", end);
            Awaitility.await("dictionary comparison redo archived").atMost(Duration.ofSeconds(90)).pollInterval(Duration.ofMillis(200))
                    .until(() -> miner.latestArchivedLsn() >= end);
            final var archives = JSON.createArrayNode();
            for (final var archive : miner.archiveLogsForRange(start, end)) {
                archives.add(JSON.createObjectNode().put("path", archive.path()).put("firstLsn", archive.firstLsn()).put("lastLsn", archive.lastLsn()));
            }
            report.set("archiveFiles", archives);
            for (final int options : List.of(2048, 2064, 2128, 16)) {
                final var mode = JSON.createObjectNode().put("options", options).put("success", false);
                ((ArrayNode) report.required("modes")).add(mode);
                try (var session = connect()) {
                    try {
                        for (final var archive : session.archiveLogsForRange(start, end)) {
                            session.execute(LogMinerSql.addLogFile(archive.path()));
                        }
                        session.execute("BEGIN DBMS_LOGMNR.START_LOGMNR(STARTSCN=>" + start + ",ENDSCN=>" + end + ",OPTIONS=>" + options + "); END;");
                        final var rows = readRedo(session, start, end);
                        mode.set("redo", rows);
                        mode.put("markerInserts", count(rows, markerId, beforeMarker));
                        mode.put("completeStorageInserts", count(rows, completeId, start));
                        mode.put("fastStorageInserts", count(rows, fastId, start));
                        mode.put("success", true);
                    }
                    catch (SQLException error) {
                        mode.put("error", error.toString()).put("sqlState", error.getSQLState()).put("vendorCode", error.getErrorCode());
                    }
                    finally {
                        try {
                            session.execute(LogMinerSql.end());
                        }
                        catch (SQLException error) {
                            mode.put("endError", error.toString());
                        }
                        writeReport(report);
                    }
                }
            }
            report.set("sameRedoComparisons", comparisons(report.required("modes")));
            writeReport(report);
            assertThat(completeId).isPositive();
            assertThat(fastId).isPositive();
            assertThat(metadata.required(COMPLETE)).hasSize(2);
            assertThat(metadata.required(FAST)).hasSize(3);
            assertThat(report.required("baseRows")).isEqualTo(report.required("completeRows")).isEqualTo(report.required("fastRows"));
            for (final JsonNode mode : report.required("modes")) {
                assertThat(mode.path("success").asBoolean()).as("OPTIONS=%s", mode.path("options")).isTrue();
                assertThat(mode.path("markerInserts").asInt()).as("positive control OPTIONS=%s", mode.path("options")).isPositive();
                assertThat(mode.path("completeStorageInserts").asInt()).isPositive();
                assertThat(mode.path("fastStorageInserts").asInt()).isPositive();
            }
            report.put("validationPassed", true);
        }
        finally {
            writeReport(report);
        }
    }

    private static ArrayNode comparisons(JsonNode modes) {
        final Map<String, ObjectNode> same = new LinkedHashMap<>();
        for (final JsonNode mode : modes) {
            for (final JsonNode row : mode.path("redo")) {
                final String key = List.of("SCN", "SSN", "RBASQN", "RBABLK", "RBABYTE", "RS_ID", "CSF").stream()
                        .map(column -> row.path(column).asText("null")).collect(java.util.stream.Collectors.joining(":"));
                final var comparison = same.computeIfAbsent(key, ignored -> JSON.createObjectNode().put("physicalRecordKey", key));
                comparison.set("options" + mode.required("options").asInt(), row.deepCopy());
            }
        }
        final var result = JSON.createArrayNode();
        for (final var comparison : same.values()) {
            final var baseline = comparison.path("options2048");
            for (final int option : List.of(2064, 2128, 16)) {
                final var other = comparison.path("options" + option);
                comparison.put("matched" + option, !baseline.isMissingNode() && !other.isMissingNode());
                if (!baseline.isMissingNode() && !other.isMissingNode()) {
                    comparison.put("sameSqlRedo" + option, baseline.path("SQL_REDO").equals(other.path("SQL_REDO")));
                    comparison.put("sameTableName" + option, baseline.path("TABLE_NAME").equals(other.path("TABLE_NAME")));
                }
            }
            result.add(comparison);
        }
        return result;
    }

    private static ArrayNode readRedo(DamengConnection miner, long start, long end) throws SQLException {
        return miner.queryAndMap("SELECT SCN,START_SCN,COMMIT_SCN,XID,OPERATION_CODE,ROLL_BACK,SEG_OWNER,TABLE_NAME,SQL_REDO,SQL_UNDO,"
                + "DATA_OBJ#,DATA_OBJV#,SSN,RS_ID,CSF,RBASQN,RBABLK,RBABYTE FROM V$LOGMNR_CONTENTS WHERE SCN>" + start + " AND SCN<=" + end
                + " ORDER BY SCN,RBASQN,RBABLK,RBABYTE,SSN", rows -> {
                    final var result = JSON.createArrayNode();
                    while (rows.next()) {
                        final var row = JSON.createObjectNode();
                        for (int column = 1; column <= rows.getMetaData().getColumnCount(); column++) {
                            final String name = rows.getMetaData().getColumnLabel(column);
                            if (name.equals("XID")) {
                                final byte[] value = rows.getBytes(column);
                                row.put(name, value == null ? null : HexFormat.of().formatHex(value));
                            }
                            else {
                                row.put(name, rows.getString(column));
                            }
                        }
                        result.add(row);
                    }
                    return result;
                });
    }

    private static int count(JsonNode rows, long objectId, long after) {
        int count = 0;
        for (final JsonNode row : rows) {
            if (row.path("DATA_OBJ#").asLong() == objectId && row.path("OPERATION_CODE").asInt() == 1 && row.path("SCN").asLong() > after) {
                count++;
            }
        }
        return count;
    }

    private static long objectId(JsonNode objects, String name) {
        for (final JsonNode object : objects) {
            if (object.path("NAME").asText().equals(name)) {
                return object.required("ID").asLong();
            }
        }
        return 0;
    }

    private static DamengConnection connect() {
        return new DamengConnection(Configuration.create().with("hostname", DamengTestDatabase.hostname()).with("port", DamengTestDatabase.port())
                .with("user", DamengTestDatabase.user()).with("password", DamengTestDatabase.password()).build());
    }

    private static ArrayNode columns(Connection jdbc, String table) throws SQLException {
        final var result = JSON.createArrayNode();
        try (var statement = jdbc.createStatement(); var rows = statement.executeQuery("SELECT * FROM SYSDBA.\"" + table + "\" WHERE 1=0")) {
            for (int column = 1; column <= rows.getMetaData().getColumnCount(); column++) {
                result.add(rows.getMetaData().getColumnLabel(column));
            }
        }
        return result;
    }

    private static ArrayNode query(Connection jdbc, String sql) throws SQLException {
        final var result = JSON.createArrayNode();
        try (var statement = jdbc.createStatement(); var rows = statement.executeQuery(sql)) {
            while (rows.next()) {
                final var row = JSON.createObjectNode();
                for (int column = 1; column <= rows.getMetaData().getColumnCount(); column++) {
                    row.put(rows.getMetaData().getColumnLabel(column), rows.getString(column));
                }
                result.add(row);
            }
        }
        return result;
    }

    private static void execute(Connection jdbc, String sql) throws SQLException {
        try (var statement = jdbc.createStatement()) {
            statement.execute(sql);
        }
    }

    private static void writeReport(ObjectNode report) throws Exception {
        Files.createDirectories(EVIDENCE.getParent());
        JSON.writerWithDefaultPrettyPrinter().writeValue(EVIDENCE.toFile(), report);
    }
}
