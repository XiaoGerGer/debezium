/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.time.Duration;
import java.util.HexFormat;
import java.util.List;

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
import io.debezium.connector.dameng.logminer.parser.LogMinerDmlParser;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;

/** Separates persisted generation baselines from diagnostic historical-catalog recovery. */
class DamengMaterializedViewSchemaRecoveryIT {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final Path EVIDENCE = Path.of("target/materialized-view-schema-recovery-it.json");
    private static final String MARKER = "DBZ_MVS_MARKER";
    private static final String COLUMN_FIELDS = "NAME,COLID,TYPE$,LENGTH$,SCALE,NULLABLE$,DEFVAL";

    @BeforeAll
    static void startDatabase() throws Exception {
        assumeTrue(System.getProperty("dameng.hostname") == null, "Schema recovery requires a test-owned database");
        assumeTrue(DamengTestDatabase.requestedCompatibilityMode() == 0, "Schema recovery currently uses COMPATIBLE_MODE=0");
        Files.deleteIfExists(EVIDENCE);
        DamengTestDatabase.start();
    }

    @AfterAll
    static void stopDatabase() {
        DamengTestDatabase.stop();
    }

    @Test
    void distinguishesSavedGenerationsFromCurrentAndHistoricalMetadata() throws Exception {
        final var report = JSON.createObjectNode();
        report.put("options", 2048);
        report.put("baselineReplayPassed", false);
        report.put("historicalRecoveryPassed", false);
        report.put("scope", "Persisted test baselines and exact catalog flashback comparisons; production schema recovery and connector events are not implemented");
        report.set("generations", JSON.createArrayNode());
        report.set("steps", JSON.createArrayNode());
        try (var jdbc = open(); var miner = miner()) {
            report.put("databaseVersion", jdbc.getMetaData().getDatabaseProductVersion());
            report.put("driverVersion", jdbc.getMetaData().getDriverVersion());
            report.set("parametersBefore", parameters(jdbc));
            execute(jdbc, "ALTER SYSTEM SET 'ENABLE_FLASHBACK'=1");
            execute(jdbc, "ALTER SYSTEM SET 'UNDO_RETENTION'=3600");
            report.set("parametersAfter", parameters(jdbc));
            execute(jdbc, "CREATE TABLE SYSDBA." + MARKER + "(ID INT PRIMARY KEY,V VARCHAR(32))");
            execute(jdbc, "INSERT INTO SYSDBA." + MARKER + " VALUES(0,'control-old')");
            final long markerId = miner.getTableObjectId(new TableId(null, "SYSDBA", MARKER));
            final long start = miner.currentLsn();
            final var flashback = JSON.createObjectNode().put("snapshotLsn", start);
            flashback.set("savedRows", query(jdbc, "SELECT ID,V FROM SYSDBA." + MARKER + " WHERE ID=0"));
            report.set("userTableFlashbackControl", flashback);
            report.put("startLsn", start);
            report.put("markerObjectId", markerId);
            for (final String mode : List.of("COMPLETE", "FAST")) {
                fixture(jdbc, miner, report, mode);
            }
            final long beforeMarker = miner.currentLsn();
            execute(jdbc, "UPDATE SYSDBA." + MARKER + " SET V='control-new' WHERE ID=0");
            execute(jdbc, "INSERT INTO SYSDBA." + MARKER + " VALUES(1,'marker')");
            final long end = miner.currentLsn();
            report.put("endLsn", end);
            for (final JsonNode generation : report.required("generations")) {
                if (!generation.has("endLsn")) {
                    ((ObjectNode) generation).put("endLsn", end);
                }
            }
            write(report);
            // Reload independently after every old object has been dropped and its name reused.
            final JsonNode saved = JSON.readTree(EVIDENCE.toFile());
            report.put("baselineReloadedFromDisk", true);
            try (var fresh = open()) {
                flashback.set("currentRows", query(fresh, "SELECT ID,V FROM SYSDBA." + MARKER + " WHERE ID=0"));
                flashback.set("historical", attempt(fresh, "SELECT ID,V FROM SYSDBA." + MARKER + " AS OF SCN " + start + " WHERE ID=0"));
                flashback.put("passed", flashback.path("historical").path("success").asBoolean()
                        && flashback.path("historical").path("rows").equals(flashback.required("savedRows"))
                        && "control-new".equals(flashback.path("currentRows").path(0).path("V").asText()));
                final var current = JSON.createArrayNode();
                report.set("currentMetadataAfterAllRebuilds", current);
                for (final JsonNode generation : saved.required("generations")) {
                    final var state = JSON.createObjectNode().put("generation", generation.required("key").asText());
                    current.add(state);
                    for (final String role : List.of("logical", "physical")) {
                        final long id = generation.required(role).required("id").asLong();
                        final var object = JSON.createObjectNode();
                        object.set("objectsByOriginalId", attempt(fresh, objectSql(id, "")));
                        object.set("columnsByOriginalId", attempt(fresh, columnSql(id, "")));
                        object.set("jdbcByReusedName", jdbcMetadata(fresh, generation.required(role).required("name").asText()));
                        object.put("currentCatalogExactlyMatchesSavedGeneration", object.path("objectsByOriginalId").path("rows")
                                .equals(generation.required(role).required("objects"))
                                && object.path("columnsByOriginalId").path("rows")
                                        .equals(generation.required(role).required("columns")));
                        object.put("objectIdentityMatchesSavedGeneration", identities(object.path("objectsByOriginalId").path("rows"))
                                .equals(identities(generation.required(role).required("objects"))));
                        object.put("columnSchemaMatchesSavedGeneration", object.path("columnsByOriginalId").path("rows")
                                .equals(generation.required(role).required("columns")));
                        object.put("jdbcColumnsMatchSavedGeneration", object.path("jdbcByReusedName").path("resultColumns")
                                .equals(generation.required(role).required("jdbc").required("resultColumns")));
                        state.set(role, object);
                    }
                }
            }
            Awaitility.await("schema-generation redo archived").atMost(Duration.ofSeconds(90)).pollInterval(Duration.ofMillis(200))
                    .until(() -> miner.latestArchivedLsn() >= end);
            final ArrayNode redo = mine(start, end);
            report.set("allRedo", redo);
            final var marker = JSON.createArrayNode();
            for (final JsonNode row : redo) {
                if (row.path("DATA_OBJ#").asLong() == markerId && row.path("OPERATION_CODE").asInt() == 1 && row.path("SCN").asLong() > beforeMarker) {
                    marker.add(row.deepCopy());
                }
            }
            report.set("positiveControl", marker);
            final var replays = replay(saved.required("generations"), redo);
            report.set("baselineReplays", replays);
            try (var historical = open()) {
                historical(report, saved.required("generations"), redo, historical);
            }
            write(report);
            assertThat(marker).as("positive control with actual object identity").isNotEmpty();
            assertThat(flashback.path("passed").asBoolean()).as("ordinary user-table flashback returns the old committed row").isTrue();
            for (final JsonNode current : report.required("currentMetadataAfterAllRebuilds")) {
                final boolean latest = current.required("generation").asText().endsWith("-v2");
                for (final String role : List.of("logical", "physical")) {
                    assertThat(current.path(role).path("columnSchemaMatchesSavedGeneration").asBoolean())
                            .as("current catalog schema for %s %s", current.path("generation"), role).isEqualTo(latest);
                    assertThat(current.path(role).path("jdbcColumnsMatchSavedGeneration").asBoolean())
                            .as("current JDBC schema for %s %s", current.path("generation"), role).isEqualTo(latest);
                    if (latest) {
                        assertThat(current.path(role).path("objectIdentityMatchesSavedGeneration").asBoolean())
                                .as("current object identity for %s %s", current.path("generation"), role).isTrue();
                    }
                }
            }
            assertThat(replays).hasSize(4);
            for (final JsonNode replay : replays) {
                assertThat(replay.path("insertRows")).as("MTAB INSERTs for %s", replay.path("generation")).isNotEmpty();
                assertThat(replay.path("allOrdinalsAndTypesMatch").asBoolean()).as("generation schema and redo mapping %s", replay.path("generation")).isTrue();
                assertThat(replay.path("refreshedValueFound").asBoolean()).as("actual refreshed values for %s", replay.path("generation")).isTrue();
            }
            report.put("baselineReplayPassed", true);
            report.put("diagnosticRunPassed", true);
        }
        finally {
            write(report);
        }
    }

    private static void fixture(Connection jdbc, DamengConnection miner, ObjectNode report, String mode) throws Exception {
        final String base = "DBZ_MVS_" + mode + "_BASE";
        final String view = "DBZ_MVS_" + mode;
        step(jdbc, miner, report, "CREATE TABLE SYSDBA." + base
                + "(ID INT PRIMARY KEY,OLD_TEXT VARCHAR(32),NEW_AMOUNT DECIMAL(12,2),NEW_NOTE VARCHAR(40))");
        step(jdbc, miner, report, "INSERT INTO SYSDBA." + base + " VALUES(1,'old-initial',10.25,'new-initial')");
        step(jdbc, miner, report, "INSERT INTO SYSDBA." + base + " VALUES(2,'old-second',20.50,'new-second')");
        if (mode.equals("FAST")) {
            step(jdbc, miner, report, "CREATE MATERIALIZED VIEW LOG ON SYSDBA." + base
                    + " WITH ROWID(OLD_TEXT,NEW_AMOUNT,NEW_NOTE) PURGE IMMEDIATE SYNCHRONOUS");
        }
        ObjectNode previous = null;
        for (final int version : List.of(1, 2)) {
            if (previous != null) {
                previous.put("endLsn", miner.currentLsn());
                step(jdbc, miner, report, "DROP MATERIALIZED VIEW SYSDBA." + view);
            }
            final var generation = JSON.createObjectNode().put("key", mode + "-v" + version).put("version", version).put("mode", mode);
            ((ArrayNode) report.required("generations")).add(generation);
            generation.put("startLsn", miner.currentLsn());
            final String sql = "CREATE MATERIALIZED VIEW SYSDBA." + view + " REFRESH " + mode + " ON DEMAND "
                    + (mode.equals("FAST") ? "WITH ROWID " : "") + "AS SELECT " + (version == 1 ? "ID,OLD_TEXT" : "ID,NEW_AMOUNT,NEW_NOTE")
                    + (mode.equals("FAST") ? ",ROWID AS X" : "") + " FROM SYSDBA." + base;
            generation.put("createSql", sql);
            step(jdbc, miner, report, sql);
            generation.put("createEndLsn", miner.currentLsn());
            generation.set("logical", definition(jdbc, view));
            generation.set("physical", definition(jdbc, "MTAB$_" + view));
            generation.put("snapshotLsn", miner.currentLsn());
            write(report);
            final String expected = version == 1 ? "v1-old-refreshed" : "9876.54";
            generation.put("expectedRefreshLiteral", expected);
            step(jdbc, miner, report,
                    "UPDATE SYSDBA." + base + (version == 1 ? " SET OLD_TEXT='" + expected + "'" : " SET NEW_AMOUNT=" + expected + ",NEW_NOTE='v2-new-refreshed'")
                            + " WHERE ID=1");
            step(jdbc, miner, report, "REFRESH MATERIALIZED VIEW SYSDBA." + view + (mode.equals("FAST") ? " FAST" : " COMPLETE USING DELETE"));
            generation.set("rowsAfterRefresh", query(jdbc, "SELECT * FROM SYSDBA." + view + " ORDER BY ID"));
            generation.put("refreshEndLsn", miner.currentLsn());
            previous = generation;
            write(report);
        }
    }

    private static ObjectNode definition(Connection jdbc, String name) throws SQLException {
        final var objects = query(jdbc, "SELECT * FROM SYS.SYSOBJECTS WHERE NAME='" + name
                + "' AND SCHID=(SELECT ID FROM SYS.SYSOBJECTS WHERE NAME='SYSDBA' AND TYPE$='SCH')");
        assertThat(objects).as("unambiguous object %s", name).hasSize(1);
        final long id = objects.get(0).required("ID").asLong();
        final var definition = JSON.createObjectNode().put("name", name).put("id", id);
        definition.set("objects", query(jdbc, objectSql(id, "")));
        definition.set("columns", query(jdbc, columnSql(id, "")));
        definition.set("jdbc", jdbcMetadata(jdbc, name));
        return definition;
    }

    private static void historical(ObjectNode report, JsonNode generations, ArrayNode redo, Connection jdbc) throws SQLException {
        final var candidates = JSON.createArrayNode();
        report.set("historicalCandidates", candidates);
        boolean allPassed = true;
        for (final JsonNode generation : generations) {
            long createCommit = 0;
            long actualCreateCommit = 0;
            for (final JsonNode row : redo) {
                if (row.path("OPERATION_CODE").asInt() == 5 && row.path("SCN").asLong() > generation.required("startLsn").asLong()
                        && row.path("SCN").asLong() <= generation.required("createEndLsn").asLong()
                        && row.path("SQL_REDO").asText().contains("CREATE MATERIALIZED VIEW")
                        && row.path("SQL_REDO").asText().contains(generation.required("logical").required("name").asText())) {
                    createCommit = Math.max(createCommit, row.path("COMMIT_SCN").asLong());
                    if (row.hasNonNull("XID")) {
                        for (final JsonNode transactionRow : redo) {
                            if (transactionRow.path("OPERATION_CODE").asInt() == 7 && transactionRow.path("XID").equals(row.path("XID"))) {
                                actualCreateCommit = Math.max(actualCreateCommit, transactionRow.path("SCN").asLong());
                                createCommit = Math.max(createCommit, transactionRow.path("SCN").asLong());
                            }
                        }
                    }
                }
            }
            for (final String boundary : List.of("snapshotLsn", "createDdlCommitScn")) {
                final long lsn = boundary.equals("snapshotLsn") ? generation.required("snapshotLsn").asLong() : createCommit;
                final var candidate = JSON.createObjectNode().put("generation", generation.required("key").asText()).put("boundary", boundary).put("lsn", lsn);
                candidate.put("actualCreateCommitScn", actualCreateCommit);
                candidates.add(candidate);
                boolean passed = lsn > 0 && actualCreateCommit > 0;
                for (final String role : List.of("logical", "physical")) {
                    final var saved = generation.required(role);
                    final long id = saved.required("id").asLong();
                    final var recovered = JSON.createObjectNode();
                    candidate.set(role, recovered);
                    recovered.set("objects", attempt(jdbc, objectSql(id, " AS OF SCN " + lsn)));
                    recovered.set("columns", attempt(jdbc, columnSql(id, " AS OF SCN " + lsn)));
                    final boolean exact = recovered.path("objects").path("success").asBoolean() && recovered.path("columns").path("success").asBoolean()
                            && recovered.path("objects").path("rows").equals(saved.required("objects"))
                            && recovered.path("columns").path("rows").equals(saved.required("columns"));
                    recovered.put("exactSavedCatalogMatch", exact);
                    final boolean identityMatches = recovered.path("objects").path("success").asBoolean()
                            && identities(recovered.path("objects").path("rows")).equals(identities(saved.required("objects")));
                    final boolean columnsMatch = recovered.path("columns").path("success").asBoolean()
                            && recovered.path("columns").path("rows").equals(saved.required("columns"));
                    recovered.put("objectIdentityMatchesSavedGeneration", identityMatches);
                    recovered.put("columnSchemaMatchesSavedGeneration", columnsMatch);
                    passed &= identityMatches && columnsMatch;
                }
                candidate.put("historicalRecoveryPassed", passed);
                allPassed &= passed;
            }
        }
        report.put("historicalRecoveryPassed", allPassed && !candidates.isEmpty());
    }

    private static ArrayNode replay(JsonNode generations, ArrayNode redo) {
        final var replays = JSON.createArrayNode();
        for (final JsonNode generation : generations) {
            final var replay = JSON.createObjectNode().put("generation", generation.required("key").asText());
            final var inserts = JSON.createArrayNode();
            replay.set("insertRows", inserts);
            replays.add(replay);
            final var columns = generation.required("physical").required("jdbc").required("resultColumns");
            final var table = Table.editor().tableId(new TableId(null, null, "OBJ# " + generation.required("physical").required("id").asLong()));
            for (final JsonNode column : columns) {
                table.addColumn(Column.editor().name("COL " + column.required("ordinal").asInt()).position(column.required("ordinal").asInt())
                        .jdbcType(column.required("jdbcType").asInt()).type(column.required("typeName").asText()).create());
            }
            final int version = generation.required("version").asInt();
            boolean valid = columns.size() == (version == 1 ? 2 : 3) + (generation.required("mode").asText().equals("FAST") ? 1 : 0);
            boolean refreshed = false;
            for (final JsonNode row : redo) {
                final long lsn = row.path("SCN").asLong();
                if (row.path("DATA_OBJ#").asLong() != generation.required("physical").required("id").asLong()
                        || lsn <= generation.required("startLsn").asLong() || lsn > generation.required("endLsn").asLong()
                        || row.path("OPERATION_CODE").asInt() != 1) {
                    continue;
                }
                final var decoded = JSON.createObjectNode();
                decoded.set("originalRedo", row.deepCopy());
                decoded.put("generation", generation.required("key").asText());
                final var bindings = JSON.createArrayNode();
                decoded.set("bindings", bindings);
                inserts.add(decoded);
                final LogMinerDmlParser.DmlEntry entry;
                try {
                    entry = new LogMinerDmlParser().parse(row.path("SQL_REDO").asText(), table.create());
                }
                catch (RuntimeException error) {
                    decoded.put("parserError", error.toString());
                    decoded.put("mappingPassed", false);
                    valid = false;
                    continue;
                }
                decoded.put("parserOperation", entry.operation().name());
                boolean rowValid = entry.newValues().length == columns.size() && entry.newPresent().cardinality() == columns.size();
                rowValid &= "ID".equals(columns.get(0).path("name").asText()) && columns.get(0).path("jdbcType").asInt() == Types.INTEGER;
                if (version == 2) {
                    rowValid &= "NEW_NOTE".equals(columns.get(2).path("name").asText()) && columns.get(2).path("jdbcType").asInt() == Types.VARCHAR;
                }
                if (generation.required("mode").asText().equals("FAST")) {
                    final var rowId = columns.get(columns.size() - 1);
                    rowValid &= "X".equals(rowId.path("name").asText()) && rowId.path("jdbcType").asInt() == Types.ROWID;
                }
                for (int position = 0; position < columns.size(); position++) {
                    final var column = columns.get(position);
                    final var binding = column.deepCopy();
                    final Object value = entry.newValues()[position];
                    ((ObjectNode) binding).set("parsedValue", JSON.valueToTree(value));
                    bindings.add(binding);
                    if (position == 1) {
                        final int type = column.required("jdbcType").asInt();
                        rowValid &= column.required("name").asText().equals(version == 1 ? "OLD_TEXT" : "NEW_AMOUNT")
                                && (version == 1 ? type == Types.VARCHAR : type == Types.DECIMAL || type == Types.NUMERIC);
                        if (version == 1) {
                            refreshed |= "1".equals(entry.newValues()[0].toString()) && generation.required("expectedRefreshLiteral").asText().equals(value);
                        }
                        else {
                            rowValid &= column.required("precision").asInt() == 12 && column.required("scale").asInt() == 2;
                            refreshed |= "1".equals(entry.newValues()[0].toString())
                                    && new BigDecimal(value.toString()).compareTo(new BigDecimal(generation.required("expectedRefreshLiteral").asText())) == 0
                                    && "v2-new-refreshed".equals(entry.newValues()[2]);
                        }
                    }
                }
                decoded.put("mappingPassed", rowValid);
                valid &= rowValid;
            }
            replay.put("allOrdinalsAndTypesMatch", valid);
            replay.put("refreshedValueFound", refreshed);
        }
        return replays;
    }

    private static ArrayNode mine(long start, long end) throws SQLException {
        try (var connection = miner()) {
            for (final var archive : connection.archiveLogsForRange(start, end)) {
                connection.execute(LogMinerSql.addLogFile(archive.path()));
            }
            try {
                connection.execute("BEGIN DBMS_LOGMNR.START_LOGMNR(STARTSCN=>" + start + ",ENDSCN=>" + end + ",OPTIONS=>2048); END;");
                return connection.queryAndMap("SELECT SCN,START_SCN,COMMIT_SCN,XID,OPERATION_CODE,ROLL_BACK,SEG_OWNER,TABLE_NAME,SQL_REDO,SQL_UNDO,"
                        + "DATA_OBJ#,DATA_OBJV#,SSN,RS_ID,CSF,RBASQN,RBABLK,RBABYTE FROM V$LOGMNR_CONTENTS WHERE SCN>" + start + " AND SCN<=" + end
                        + " ORDER BY SCN,RBASQN,RBABLK,RBABYTE,SSN", rows -> records(rows));
            }
            finally {
                connection.execute(LogMinerSql.end());
            }
        }
    }

    private static ObjectNode jdbcMetadata(Connection jdbc, String name) throws SQLException {
        final var result = JSON.createObjectNode();
        try (var rows = jdbc.getMetaData().getColumns(null, "SYSDBA", name, null)) {
            result.set("databaseColumns", records(rows));
        }
        final var columns = JSON.createArrayNode();
        result.set("resultColumns", columns);
        try (var statement = jdbc.createStatement(); var rows = statement.executeQuery("SELECT * FROM SYSDBA.\"" + name + "\" WHERE 1=0")) {
            final var metadata = rows.getMetaData();
            for (int ordinal = 1; ordinal <= metadata.getColumnCount(); ordinal++) {
                columns.add(JSON.createObjectNode().put("ordinal", ordinal).put("name", metadata.getColumnName(ordinal)).put("label", metadata.getColumnLabel(ordinal))
                        .put("jdbcType", metadata.getColumnType(ordinal)).put("typeName", metadata.getColumnTypeName(ordinal))
                        .put("precision", metadata.getPrecision(ordinal))
                        .put("scale", metadata.getScale(ordinal)).put("nullable", metadata.isNullable(ordinal)).put("className", metadata.getColumnClassName(ordinal))
                        .put("table", metadata.getTableName(ordinal)).put("schema", metadata.getSchemaName(ordinal)).put("catalog", metadata.getCatalogName(ordinal)));
            }
        }
        return result;
    }

    private static String objectSql(long id, String at) {
        return "SELECT * FROM SYS.SYSOBJECTS" + at + " WHERE ID=" + id;
    }

    private static ArrayNode identities(JsonNode rows) {
        final var identities = JSON.createArrayNode();
        for (final JsonNode row : rows) {
            final var identity = JSON.createObjectNode();
            for (final String field : List.of("ID", "NAME", "SCHID", "TYPE$", "SUBTYPE$")) {
                identity.set(field, row.required(field));
            }
            identities.add(identity);
        }
        return identities;
    }

    private static String columnSql(long id, String at) {
        return "SELECT " + COLUMN_FIELDS + " FROM SYS.SYSCOLUMNS" + at + " WHERE ID=" + id + " ORDER BY COLID";
    }

    private static ArrayNode parameters(Connection jdbc) throws SQLException {
        return query(jdbc, "SELECT PARA_NAME,PARA_VALUE,SESS_VALUE,FILE_VALUE FROM V$DM_INI WHERE PARA_NAME IN ('ENABLE_FLASHBACK','UNDO_RETENTION') ORDER BY PARA_NAME");
    }

    private static ObjectNode attempt(Connection jdbc, String sql) {
        final var attempt = JSON.createObjectNode().put("sql", sql).put("success", false);
        try {
            attempt.set("rows", query(jdbc, sql));
            attempt.put("success", true);
        }
        catch (SQLException error) {
            attempt.put("error", error.toString()).put("sqlState", error.getSQLState()).put("vendorCode", error.getErrorCode());
        }
        return attempt;
    }

    private static void step(Connection jdbc, DamengConnection miner, ObjectNode report, String sql) throws Exception {
        final var step = JSON.createObjectNode().put("sql", sql).put("startLsn", miner.currentLsn());
        ((ArrayNode) report.required("steps")).add(step);
        execute(jdbc, sql);
        step.put("endLsn", miner.currentLsn());
        write(report);
    }

    private static ArrayNode records(ResultSet rows) throws SQLException {
        final var records = JSON.createArrayNode();
        while (rows.next()) {
            final var record = JSON.createObjectNode();
            for (int column = 1; column <= rows.getMetaData().getColumnCount(); column++) {
                final String label = rows.getMetaData().getColumnLabel(column);
                if (label.equals("XID")) {
                    final byte[] value = rows.getBytes(column);
                    record.put(label, value == null ? null : HexFormat.of().formatHex(value));
                }
                else {
                    record.put(label, rows.getString(column));
                }
            }
            records.add(record);
        }
        return records;
    }

    private static ArrayNode query(Connection jdbc, String sql) throws SQLException {
        try (var statement = jdbc.createStatement(); var rows = statement.executeQuery(sql)) {
            return records(rows);
        }
    }

    private static Connection open() throws SQLException {
        final var connection = DriverManager.getConnection(DamengTestDatabase.jdbcUrl(), DamengTestDatabase.user(), DamengTestDatabase.password());
        connection.setAutoCommit(true);
        return connection;
    }

    private static DamengConnection miner() {
        return new DamengConnection(Configuration.create().with("hostname", DamengTestDatabase.hostname()).with("port", DamengTestDatabase.port())
                .with("user", DamengTestDatabase.user()).with("password", DamengTestDatabase.password()).build());
    }

    private static void execute(Connection jdbc, String sql) throws SQLException {
        try (var statement = jdbc.createStatement()) {
            statement.execute(sql);
        }
    }

    private static void write(ObjectNode report) throws Exception {
        Files.createDirectories(EVIDENCE.getParent());
        JSON.writerWithDefaultPrettyPrinter().writeValue(EVIDENCE.toFile(), report);
    }
}
