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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.awaitility.Awaitility;
import org.awaitility.core.ConditionTimeoutException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.debezium.config.Configuration;
import io.debezium.relational.TableId;

/** Row-state and raw-redo evidence for asynchronous and explicitly requested FAST refreshes. */
class DamengMaterializedViewRedoProbeIT {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final Path EVIDENCE = Path.of("target/materialized-view-redo-it.json");
    private static final String MANUAL = "https://eco.dameng.com/document/dm/zh-cn/pm/materialized-view.html";
    private static final String MARKER = "DBZ_MVR_MARKER";
    private static final Duration REFRESH_TIMEOUT = Duration.ofSeconds(30);
    private static final Duration ROLLBACK_OBSERVATION = Duration.ofSeconds(2);
    private static final Duration POLL_INTERVAL = Duration.ofMillis(100);

    private record Fixture(String mode, String base, String view) {
        boolean automatic() {
            return mode.equals("COMMIT");
        }
    }

    @BeforeAll
    static void startDatabase() throws Exception {
        assumeTrue(System.getProperty("dameng.hostname") == null, "Materialized view replay requires a test-owned database");
        assumeTrue(DamengTestDatabase.requestedCompatibilityMode() == 0, "Materialized view replay currently uses COMPATIBLE_MODE=0");
        Files.deleteIfExists(EVIDENCE);
        DamengTestDatabase.start();
    }

    @AfterAll
    static void stopDatabase() {
        DamengTestDatabase.stop();
    }

    @Test
    void recordsCommittedAndRolledBackChangesAcrossFastRefreshBoundaries() throws Exception {
        final var report = JSON.createObjectNode();
        report.put("source", MANUAL);
        report.put("onCommitContract", "The documented ON COMMIT worker is asynchronous; commit return does not guarantee refreshed rows");
        report.put("scope", "Direct JDBC row-state and unfiltered LogMiner evidence; emitted connector CDC records are not tested");
        report.put("xidEncoding", "Lowercase hex of JDBC getBytes(XID), preserving the original bytes");
        report.put("rollbackInterpretation", "OPERATION_CODE=36 alone is not evidence that a user transaction rolled back; XID, ROLL_BACK and row state are retained");
        report.put("refreshTimeoutMillis", REFRESH_TIMEOUT.toMillis());
        report.put("rollbackObservationMillis", ROLLBACK_OBSERVATION.toMillis());
        report.put("runComplete", false);
        report.put("validationPassed", false);
        report.set("steps", JSON.createArrayNode());
        report.set("checks", JSON.createArrayNode());
        report.set("allRedo", JSON.createArrayNode());
        report.set("positiveControl", JSON.createArrayNode());
        try (var observer = connect();
                var writer = connect();
                var miner = new DamengConnection(Configuration.create().with("hostname", DamengTestDatabase.hostname())
                        .with("port", DamengTestDatabase.port()).with("user", DamengTestDatabase.user())
                        .with("password", DamengTestDatabase.password()).build())) {
            observer.setAutoCommit(true);
            writer.setAutoCommit(false);
            final var audit = new Audit(observer, writer, miner, report);
            Throwable failure = null;
            try {
                audit.prepare();
                for (final Fixture fixture : audit.fixtures) {
                    audit.exercise(fixture);
                }
                report.put("runComplete", true);
            }
            catch (Exception | AssertionError error) {
                failure = error;
                report.put("exerciseFailure", error.toString());
            }
            try {
                writer.rollback();
                audit.harvest();
            }
            catch (Exception | AssertionError error) {
                report.put("harvestFailure", error.toString());
                if (failure == null) {
                    failure = error;
                }
                else {
                    failure.addSuppressed(error);
                }
            }
            final List<String> failedChecks = new ArrayList<>();
            for (final JsonNode check : report.required("checks")) {
                if (!check.path("passed").asBoolean()) {
                    failedChecks.add(check.path("step").asText() + ": " + check.path("name").asText());
                }
            }
            report.set("failedChecks", JSON.valueToTree(failedChecks));
            report.put("validationPassed", failure == null && failedChecks.isEmpty());
            writeReport(report);
            if (failure instanceof AssertionError error) {
                throw error;
            }
            if (failure instanceof Exception error) {
                throw error;
            }
            assertThat(failedChecks).as("row-state and redo checks; see %s", EVIDENCE).isEmpty();
        }
        finally {
            writeReport(report);
        }
    }

    private static final class Audit {
        private final Connection observer;
        private final Connection writer;
        private final DamengConnection miner;
        private final ObjectNode report;
        private final List<Fixture> fixtures = List.of(new Fixture("COMMIT", "DBZ_MVR_COMMIT_BASE", "DBZ_MVR_COMMIT"),
                new Fixture("DEMAND", "DBZ_MVR_DEMAND_BASE", "DBZ_MVR_DEMAND"));
        private final Map<Long, String> objectNames = new LinkedHashMap<>();
        private final Map<String, Long> baseIds = new LinkedHashMap<>();
        private long markerId;
        private long startLsn;
        private int ordinal;

        private Audit(Connection observer, Connection writer, DamengConnection miner, ObjectNode report) {
            this.observer = observer;
            this.writer = writer;
            this.miner = miner;
            this.report = report;
        }

        private void prepare() throws Exception {
            report.put("databaseVersion", observer.getMetaData().getDatabaseProductVersion());
            report.put("driverVersion", observer.getMetaData().getDriverVersion());
            report.put("executionUser", observer.getMetaData().getUserName());
            final var fixtureSql = JSON.createArrayNode();
            report.set("fixture", fixtureSql);
            execute(observer, "CREATE TABLE SYSDBA." + MARKER + "(ID INT PRIMARY KEY)");
            markerId = miner.getTableObjectId(new TableId(null, "SYSDBA", MARKER));
            objectNames.put(markerId, MARKER);
            for (final Fixture fixture : fixtures) {
                for (final String sql : List.of("CREATE TABLE SYSDBA." + fixture.base() + "(ID INT PRIMARY KEY,V VARCHAR(32))",
                        "INSERT INTO SYSDBA." + fixture.base() + " VALUES(1,'one')",
                        "INSERT INTO SYSDBA." + fixture.base() + " VALUES(2,'two')")) {
                    execute(observer, sql);
                    fixtureSql.add(sql);
                }
                final long id = miner.getTableObjectId(new TableId(null, "SYSDBA", fixture.base()));
                baseIds.put(fixture.base(), id);
                objectNames.put(id, fixture.base());
            }
            startLsn = miner.currentLsn();
            report.put("startLsn", startLsn);
            report.put("markerObjectId", markerId);
            report.set("baseObjectIds", JSON.valueToTree(baseIds));
            writeReport(report);
        }

        private void exercise(Fixture fixture) throws Exception {
            final var log = ddl(fixture, "create-log", "CREATE MATERIALIZED VIEW LOG ON SYSDBA." + fixture.base()
                    + " WITH ROWID(V) PURGE IMMEDIATE SYNCHRONOUS");
            check(log, "materialized view log is accepted", log.path("jdbc").asText().equals("accepted"));
            final var create = ddl(fixture, "create-view", "CREATE MATERIALIZED VIEW SYSDBA." + fixture.view()
                    + " REFRESH FAST ON " + fixture.mode() + " WITH ROWID AS SELECT ID,V,ROWID AS X FROM SYSDBA." + fixture.base());
            final boolean accepted = create.path("jdbc").asText().equals("accepted");
            report.put(fixture.automatic() ? "onCommitAccepted" : "onDemandAccepted", accepted);
            if (!accepted) {
                create.put("coverage", "Database rejected the requested mode; refresh and rollback behavior for this mode remain untested");
                check(create, "rejected creation leaves no view", !create.path("after").path("viewExists").asBoolean());
                check(create, "rejected creation leaves no MTAB storage",
                        !create.path("after").path("internalTables").path("MTAB$_" + fixture.view()).path("exists").asBoolean());
                if (!fixture.automatic()) {
                    check(create, "independent ON DEMAND FAST fixture is available", false);
                }
                return;
            }
            create.put("coverage", "Accepted mode exercised with independent base/MV row observations and complete redo windows");
            checkRows(create, "initial base rows", create.path("after").path("baseRows"), rows("1", "one", "2", "two"));
            checkRows(create, "initial materialized view rows", create.path("after").path("viewRows"), rows("1", "one", "2", "two"));
            ArrayNode committed = rows("1", "one", "2", "two");
            final ArrayNode inserted = rows("1", "one", "2", "two", "3", "three");
            commitChange(fixture, "net-insert", "INSERT INTO SYSDBA." + fixture.base() + " VALUES(3,'three')", committed, inserted);
            committed = inserted;
            final ArrayNode updated = rows("1", "updated", "2", "two", "3", "three");
            commitChange(fixture, "net-update", "UPDATE SYSDBA." + fixture.base() + " SET V='updated' WHERE ID=1", committed, updated);
            committed = updated;
            final ArrayNode deleted = rows("1", "updated", "3", "three");
            commitChange(fixture, "net-delete", "DELETE FROM SYSDBA." + fixture.base() + " WHERE ID=2", committed, deleted);
            committed = deleted;
            rollbackChanges(fixture, committed);
            commitChange(fixture, "post-rollback-catchup", "UPDATE SYSDBA." + fixture.base() + " SET V='barrier' WHERE ID=1",
                    committed, rows("1", "barrier", "3", "three"));
            if (!fixture.automatic()) {
                refresh(fixture, "no-change-fast-refresh", null, rows("1", "barrier", "3", "three"), false);
            }
            writeReport(report);
        }

        private ObjectNode ddl(Fixture fixture, String action, String sql) throws Exception {
            final var step = begin(fixture, action);
            step.put("sql", sql);
            try {
                execute(observer, sql);
                step.put("jdbc", "accepted");
            }
            catch (SQLException error) {
                rejection(step, error);
            }
            finish(fixture, step);
            return step;
        }

        private void commitChange(Fixture fixture, String action, String sql, ArrayNode previous, ArrayNode expected) throws Exception {
            final var step = begin(fixture, action);
            step.put("transactionKind", "committed-base-dml");
            step.put("sql", sql);
            step.put("transactionBoundary", "JDBC.commit()");
            step.set("expectedRows", expected);
            final long pendingStart = miner.currentLsn();
            execute(writer, sql);
            step.set("writerPendingBaseRows", query(writer, "SELECT ID,V FROM SYSDBA." + fixture.base() + " ORDER BY ID"));
            final var beforeCommit = snapshot(fixture);
            step.set("beforeCommit", beforeCommit);
            window(step, "uncommitted-base-dml", pendingStart, miner.currentLsn());
            checkRows(step, "writer sees pending rows", step.required("writerPendingBaseRows"), expected);
            checkRows(step, "observer base remains committed before commit", beforeCommit.required("baseRows"), previous);
            checkRows(step, "view remains committed before commit", beforeCommit.required("viewRows"), previous);
            final long commitStart = miner.currentLsn();
            writer.commit();
            final long returnedAt = System.nanoTime();
            final var immediate = rowObservation(fixture);
            immediate.put("millisAfterCommitReturn", Duration.ofNanos(System.nanoTime() - returnedAt).toMillis());
            step.set("immediatelyAfterCommitReturn", immediate);
            final long commitEnd = miner.currentLsn();
            step.put("commitStartLsn", commitStart);
            step.put("commitReturnLsn", commitEnd);
            window(step, "commit-and-immediate-row-observation", commitStart, commitEnd);
            checkRows(step, "observer sees committed base after commit return", immediate.required("baseRows"), expected);
            if (fixture.automatic()) {
                step.put("instantViewAlreadyCurrent", immediate.required("viewRows").equals(expected));
                awaitRows(fixture, step, expected, returnedAt);
            }
            else {
                checkRows(step, "ON DEMAND view stays stale after base commit", immediate.required("viewRows"), previous);
            }
            finish(fixture, step);
            if (!fixture.automatic()) {
                refresh(fixture, action + "-fast-refresh", step.required("id").asText(), expected, true);
            }
        }

        private void awaitRows(Fixture fixture, ObjectNode step, ArrayNode expected, long returnedAt) throws Exception {
            final long start = miner.currentLsn();
            final var polls = JSON.createArrayNode();
            step.set("asyncRefreshObservations", polls);
            boolean settled = true;
            try {
                Awaitility.await("ON COMMIT FAST refresh " + step.required("id").asText()).atMost(REFRESH_TIMEOUT)
                        .pollInterval(POLL_INTERVAL).until(() -> {
                            final var observation = rowObservation(fixture);
                            observation.put("millisAfterCommitReturn", Duration.ofNanos(System.nanoTime() - returnedAt).toMillis());
                            observation.put("lsn", miner.currentLsn());
                            polls.add(observation);
                            return observation.required("viewRows").equals(expected);
                        });
            }
            catch (ConditionTimeoutException timeout) {
                settled = false;
                step.put("refreshWaitFailure", timeout.toString());
            }
            final var after = snapshot(fixture);
            step.set("afterAsyncWait", after);
            window(step, "asynchronous-refresh-observation", start, miner.currentLsn());
            check(step, "ON COMMIT worker reaches expected rows within bounded wait", settled);
            checkRows(step, "base rows after asynchronous refresh", after.required("baseRows"), expected);
            checkRows(step, "view rows after asynchronous refresh", after.required("viewRows"), expected);
        }

        private void refresh(Fixture fixture, String action, String baseStepId, ArrayNode expected, boolean changed) throws Exception {
            final var step = begin(fixture, action);
            step.put("transactionKind", "explicit-fast-refresh");
            step.put("expectedRowChange", changed);
            step.put("baseTransactionStepId", baseStepId);
            step.set("expectedRows", expected);
            final String sql = "REFRESH MATERIALIZED VIEW SYSDBA." + fixture.view() + " FAST";
            step.put("sql", sql);
            final long start = miner.currentLsn();
            try {
                execute(observer, sql);
                step.put("jdbc", "accepted");
            }
            catch (SQLException error) {
                rejection(step, error);
            }
            step.set("immediatelyAfterRefreshReturn", rowObservation(fixture));
            window(step, "explicit-fast-refresh", start, miner.currentLsn());
            finish(fixture, step);
            check(step, "explicit FAST refresh is accepted", step.path("jdbc").asText().equals("accepted"));
            checkRows(step, "base rows after explicit refresh", step.path("after").path("baseRows"), expected);
            checkRows(step, "view rows after explicit refresh", step.path("after").path("viewRows"), expected);
        }

        private void rollbackChanges(Fixture fixture, ArrayNode committed) throws Exception {
            final var step = begin(fixture, "mixed-dml-rollback");
            step.put("transactionKind", "rolled-back-base-dml");
            step.put("transactionBoundary", "JDBC.rollback()");
            step.set("expectedCommittedRows", committed);
            final var statements = JSON.createArrayNode();
            step.set("statements", statements);
            for (final String sql : List.of("UPDATE SYSDBA." + fixture.base() + " SET V='rolled' WHERE ID=1",
                    "DELETE FROM SYSDBA." + fixture.base() + " WHERE ID=3", "INSERT INTO SYSDBA." + fixture.base() + " VALUES(4,'rolled')")) {
                final var statement = JSON.createObjectNode();
                statement.put("sql", sql);
                statement.put("startLsn", miner.currentLsn());
                statements.add(statement);
                execute(writer, sql);
                statement.put("endLsn", miner.currentLsn());
            }
            step.set("writerPendingBaseRows", query(writer, "SELECT ID,V FROM SYSDBA." + fixture.base() + " ORDER BY ID"));
            step.set("beforeRollback", snapshot(fixture));
            checkRows(step, "rollback writer has actual mixed pending changes", step.required("writerPendingBaseRows"), rows("1", "rolled", "4", "rolled"));
            checkRows(step, "rollback pending base is invisible to observer", step.path("beforeRollback").path("baseRows"), committed);
            checkRows(step, "rollback pending changes are invisible in view", step.path("beforeRollback").path("viewRows"), committed);
            final long rollbackStart = miner.currentLsn();
            writer.rollback();
            step.set("immediatelyAfterRollbackReturn", rowObservation(fixture));
            window(step, "rollback-call", rollbackStart, miner.currentLsn());
            final var polls = JSON.createArrayNode();
            step.set("rollbackObservations", polls);
            final long observeStart = miner.currentLsn();
            final long started = System.nanoTime();
            boolean unchanged = true;
            do {
                final var observation = rowObservation(fixture);
                observation.put("elapsedMillis", Duration.ofNanos(System.nanoTime() - started).toMillis());
                observation.put("lsn", miner.currentLsn());
                unchanged &= observation.required("baseRows").equals(committed) && observation.required("viewRows").equals(committed);
                polls.add(observation);
                Thread.sleep(POLL_INTERVAL.toMillis());
            } while (System.nanoTime() - started < ROLLBACK_OBSERVATION.toNanos());
            window(step, "bounded-post-rollback-observation", observeStart, miner.currentLsn());
            check(step, "base and view ignore rolled-back mixed changes throughout observation window", unchanged);
            finish(fixture, step);
            checkRows(step, "base rows after rollback observation", step.path("after").path("baseRows"), committed);
            checkRows(step, "view rows after rollback observation", step.path("after").path("viewRows"), committed);
        }

        private ObjectNode begin(Fixture fixture, String action) throws Exception {
            final var step = JSON.createObjectNode();
            step.put("id", fixture.mode().toLowerCase(java.util.Locale.ROOT) + "-" + (++ordinal) + "-" + action);
            step.put("mode", fixture.mode());
            step.put("action", action);
            step.put("base", fixture.base());
            step.put("view", fixture.view());
            step.put("startLsn", miner.currentLsn());
            step.set("windows", JSON.createArrayNode());
            ((ArrayNode) report.required("steps")).add(step);
            step.set("before", snapshot(fixture));
            writeReport(report);
            return step;
        }

        private void finish(Fixture fixture, ObjectNode step) throws Exception {
            step.set("after", snapshot(fixture));
            step.put("endLsn", miner.currentLsn());
            writeReport(report);
        }

        private ObjectNode rowObservation(Fixture fixture) throws SQLException {
            final var state = JSON.createObjectNode();
            state.set("baseRows", query(observer, "SELECT ID,V FROM SYSDBA." + fixture.base() + " ORDER BY ID"));
            state.set("viewRows", query(observer, "SELECT ID,V FROM SYSDBA." + fixture.view() + " ORDER BY ID"));
            return state;
        }

        private ObjectNode snapshot(Fixture fixture) throws SQLException {
            final var state = JSON.createObjectNode();
            state.set("baseRows", query(observer, "SELECT ID,V FROM SYSDBA." + fixture.base() + " ORDER BY ID"));
            final var objects = objects();
            state.set("objects", objects);
            final boolean exists = hasObject(objects, fixture.view());
            state.put("viewExists", exists);
            state.set("viewRows", exists ? query(observer, "SELECT ID,V FROM SYSDBA." + fixture.view() + " ORDER BY ID") : JSON.createArrayNode());
            final var internals = JSON.createObjectNode();
            state.set("internalTables", internals);
            for (final String name : List.of("MLOG$_" + fixture.base(), "MTAB$_" + fixture.view())) {
                final var internal = JSON.createObjectNode();
                internal.put("exists", hasObject(objects, name));
                if (internal.required("exists").asBoolean()) {
                    internal.set("contents", tableContents(observer, "SELECT * FROM SYSDBA.\"" + name + "\""));
                }
                internals.set(name, internal);
            }
            try {
                state.set("userMviews", tableContents(observer, "SELECT * FROM SYS.USER_MVIEWS WHERE MVIEW_NAME='" + fixture.view() + "'"));
            }
            catch (SQLException error) {
                final var unavailable = JSON.createObjectNode();
                rejection(unavailable, error);
                state.set("userMviews", unavailable);
            }
            return state;
        }

        private ArrayNode objects() throws SQLException {
            final var objects = query(observer, "SELECT T.ID,T.NAME,T.TYPE$,T.SUBTYPE$,T.PID,T.INFO1,T.INFO2,T.INFO3 "
                    + "FROM SYS.SYSOBJECTS T JOIN SYS.SYSOBJECTS S ON T.SCHID=S.ID WHERE S.NAME='SYSDBA' "
                    + "AND (T.NAME LIKE 'DBZ_MVR_%' OR T.NAME LIKE 'MLOG$_DBZ_MVR_%' OR T.NAME LIKE 'MTAB$_DBZ_MVR_%' "
                    + "OR T.NAME LIKE 'MTRG$_DBZ_MVR_%' OR T.NAME LIKE 'MTRG_REFRESH_MVIEW_%' OR T.NAME LIKE 'MTRG_PURGE_MVLOG_%') ORDER BY T.ID");
            for (final JsonNode object : objects) {
                objectNames.put(object.required("ID").asLong(), object.required("NAME").asText());
            }
            return objects;
        }

        private void harvest() throws Exception {
            if (startLsn == 0 || markerId == 0) {
                report.put("redoCoverage", "Fixture was incomplete before the mining window could be established");
                return;
            }
            final long beforeMarker = miner.currentLsn();
            execute(observer, "INSERT INTO SYSDBA." + MARKER + " VALUES(1)");
            final long markerLsn = miner.currentLsn();
            report.put("endLsn", beforeMarker);
            report.put("markerLsn", markerLsn);
            report.set("finalObjects", objects());
            report.set("objectNames", JSON.valueToTree(objectNames));
            Awaitility.await("materialized-view transaction redo archived").atMost(Duration.ofSeconds(90))
                    .pollInterval(Duration.ofMillis(200)).until(() -> miner.latestArchivedLsn() >= markerLsn);
            miner.startLogMining(startLsn, markerLsn, false);
            try {
                final var redo = readRedo(miner, startLsn, markerLsn);
                report.set("allRedo", redo);
                final var marker = (ArrayNode) report.required("positiveControl");
                for (final JsonNode item : redo) {
                    final var row = (ObjectNode) item;
                    final String name = objectNames.get(row.path("DATA_OBJ#").asLong());
                    row.put("resolvedObjectName", name);
                    row.put("objectRole", role(name));
                    if (row.path("SCN").asLong() > beforeMarker && row.path("DATA_OBJ#").asLong() == markerId
                            && row.path("OPERATION_CODE").asInt() == 1) {
                        marker.add(row.deepCopy());
                    }
                }
                report.put("positiveControlVerified", !marker.isEmpty());
                check(null, "marker INSERT is visible with its actual DATA_OBJ#", !marker.isEmpty());
                for (final JsonNode item : report.required("steps")) {
                    final var step = (ObjectNode) item;
                    if (!step.has("endLsn")) {
                        step.put("endLsn", beforeMarker);
                        step.put("incomplete", true);
                    }
                    annotate(step, redo);
                    for (final JsonNode window : step.required("windows")) {
                        annotate((ObjectNode) window, redo);
                    }
                    correlate(step);
                }
                report.set("redoSummary", summarize(redo));
            }
            finally {
                miner.endLogMining();
                writeReport(report);
            }
        }

        private void annotate(ObjectNode window, ArrayNode redo) {
            final var matched = JSON.createArrayNode();
            for (final JsonNode row : redo) {
                final long lsn = row.path("SCN").asLong();
                if (lsn > window.required("startLsn").asLong() && lsn <= window.required("endLsn").asLong()) {
                    matched.add(row.deepCopy());
                }
            }
            window.set("redo", matched);
            window.set("redoSummary", summarize(matched));
        }

        private void correlate(ObjectNode step) {
            final long baseId = baseIds.get(step.required("base").asText());
            final long backingId = objectId("MTAB$_" + step.required("view").asText());
            step.put("baseObjectId", baseId);
            step.put("backingObjectId", backingId);
            final Set<String> baseXids = xids(step.required("redo"), baseId);
            final Set<String> backingXids = xids(step.required("redo"), backingId);
            step.set("baseDmlXids", JSON.valueToTree(baseXids));
            step.set("backingDmlXids", JSON.valueToTree(backingXids));
            step.set("baseTransactionRecords", transactionRecords(baseXids));
            step.set("backingTransactionRecords", transactionRecords(backingXids));
            if (step.path("transactionKind").asText().equals("committed-base-dml")) {
                check(step, "net base change has object-matched raw DML", !baseXids.isEmpty());
                if (step.path("mode").asText().equals("COMMIT")) {
                    check(step, "changed ON COMMIT view has object-matched backing-table DML", !backingXids.isEmpty());
                }
                else {
                    check(step, "ON DEMAND base commit does not write backing-table DML", backingXids.isEmpty());
                }
            }
            if (step.path("transactionKind").asText().equals("explicit-fast-refresh")
                    && step.path("jdbc").asText().equals("accepted") && step.path("expectedRowChange").asBoolean()) {
                check(step, "changed FAST refresh has object-matched backing-table DML", backingId > 0 && !backingXids.isEmpty());
                final var base = findStep(step.path("baseTransactionStepId").asText());
                final Set<String> parentBaseXids = xids(base.path("redo"), baseId);
                final Set<String> common = new LinkedHashSet<>(parentBaseXids);
                common.retainAll(backingXids);
                step.set("precedingBaseCommitXids", JSON.valueToTree(parentBaseXids));
                step.set("baseAndRefreshSharedXids", JSON.valueToTree(common));
                check(step, "separate ON DEMAND base commit and refresh use distinct XIDs", !parentBaseXids.isEmpty() && !backingXids.isEmpty() && common.isEmpty());
            }
        }

        private ObjectNode findStep(String id) {
            for (final JsonNode step : report.required("steps")) {
                if (step.path("id").asText().equals(id)) {
                    return (ObjectNode) step;
                }
            }
            return JSON.createObjectNode();
        }

        private ArrayNode transactionRecords(Set<String> xids) {
            final var records = JSON.createArrayNode();
            for (final JsonNode row : report.required("allRedo")) {
                if (xids.contains(row.path("XID").asText())) {
                    records.add(row.deepCopy());
                }
            }
            return records;
        }

        private long objectId(String name) {
            for (final var entry : objectNames.entrySet()) {
                if (entry.getValue().equals(name)) {
                    return entry.getKey();
                }
            }
            return 0;
        }

        private void checkRows(ObjectNode step, String name, JsonNode actual, JsonNode expected) {
            final var check = check(step, name, actual.equals(expected));
            check.set("expected", expected.deepCopy());
            check.set("actual", actual.deepCopy());
        }

        private ObjectNode check(ObjectNode step, String name, boolean passed) {
            final var check = JSON.createObjectNode();
            check.put("step", step == null ? "report" : step.path("id").asText());
            check.put("name", name);
            check.put("passed", passed);
            ((ArrayNode) report.required("checks")).add(check);
            return check;
        }
    }

    private static void window(ObjectNode step, String name, long start, long end) {
        final var window = JSON.createObjectNode();
        window.put("name", name);
        window.put("startLsn", start);
        window.put("endLsn", end);
        ((ArrayNode) step.required("windows")).add(window);
    }

    private static Set<String> xids(JsonNode redo, long objectId) {
        final Set<String> result = new LinkedHashSet<>();
        if (objectId > 0) {
            for (final JsonNode row : redo) {
                final int operation = row.path("OPERATION_CODE").asInt();
                if (row.path("DATA_OBJ#").asLong() == objectId && operation >= 1 && operation <= 4 && row.hasNonNull("XID")) {
                    result.add(row.required("XID").asText());
                }
            }
        }
        return result;
    }

    private static String role(String name) {
        if (name == null) {
            return "unresolved";
        }
        if (name.equals(MARKER)) {
            return "marker";
        }
        if (name.startsWith("MTAB$_")) {
            return "materializedStorage";
        }
        if (name.startsWith("MLOG$_")) {
            return "materializedLog";
        }
        return name.endsWith("_BASE") ? "base" : "other";
    }

    private static ObjectNode summarize(JsonNode rows) {
        final var result = JSON.createObjectNode();
        for (final JsonNode row : rows) {
            final var counts = result.withObjectProperty(row.path("objectRole").asText("unresolved"));
            final String operation = row.path("OPERATION_CODE").asText();
            counts.put(operation, counts.path(operation).asInt() + 1);
        }
        return result;
    }

    private static ArrayNode readRedo(DamengConnection miner, long start, long end) throws SQLException {
        final List<String> numeric = List.of("SCN", "START_SCN", "COMMIT_SCN", "OPERATION_CODE", "ROLL_BACK", "SSN", "CSF",
                "DATA_OBJ#", "DATA_OBJV#", "RBASQN", "RBABLK", "RBABYTE");
        final List<String> strings = List.of("SEG_OWNER", "TABLE_NAME", "USERNAME", "SQL_REDO", "SQL_UNDO", "RS_ID", "TIMESTAMP");
        return miner.queryAndMap("SELECT SCN,START_SCN,COMMIT_SCN,XID,OPERATION_CODE,ROLL_BACK,SEG_OWNER,TABLE_NAME,USERNAME,SQL_REDO,SQL_UNDO,"
                + "RS_ID,SSN,CSF,DATA_OBJ#,DATA_OBJV#,RBASQN,RBABLK,RBABYTE,TIMESTAMP FROM V$LOGMNR_CONTENTS WHERE SCN > " + start
                + " AND SCN <= " + end + " ORDER BY SCN,RBASQN,RBABLK,RBABYTE,SSN", rows -> {
                    final var result = JSON.createArrayNode();
                    while (rows.next()) {
                        final var row = JSON.createObjectNode();
                        for (final String column : numeric) {
                            final long value = rows.getLong(column);
                            if (rows.wasNull()) {
                                row.putNull(column);
                            }
                            else {
                                row.put(column, value);
                            }
                        }
                        for (final String column : strings) {
                            row.put(column, rows.getString(column));
                        }
                        final byte[] xid = rows.getBytes("XID");
                        row.put("XID", xid == null ? null : HexFormat.of().formatHex(xid));
                        result.add(row);
                    }
                    return result;
                });
    }

    private static boolean hasObject(JsonNode objects, String name) {
        for (final JsonNode object : objects) {
            if (object.path("NAME").asText().equals(name)) {
                return true;
            }
        }
        return false;
    }

    private static Connection connect() throws SQLException {
        return DriverManager.getConnection(DamengTestDatabase.jdbcUrl(), DamengTestDatabase.user(), DamengTestDatabase.password());
    }

    private static ArrayNode query(Connection jdbc, String sql) throws SQLException {
        return (ArrayNode) tableContents(jdbc, sql).required("rows");
    }

    private static ObjectNode tableContents(Connection jdbc, String sql) throws SQLException {
        final var result = JSON.createObjectNode();
        final var columns = JSON.createArrayNode();
        final var records = JSON.createArrayNode();
        result.put("sql", sql);
        result.set("columns", columns);
        result.set("rows", records);
        try (var statement = jdbc.createStatement()) {
            statement.setQueryTimeout(15);
            try (var rows = statement.executeQuery(sql)) {
                final var metadata = rows.getMetaData();
                for (int column = 1; column <= metadata.getColumnCount(); column++) {
                    columns.add(JSON.createObjectNode().put("label", metadata.getColumnLabel(column)).put("jdbcType", metadata.getColumnType(column))
                            .put("typeName", metadata.getColumnTypeName(column)));
                }
                while (rows.next()) {
                    records.add(record(rows));
                }
            }
        }
        return result;
    }

    private static ObjectNode record(ResultSet rows) throws SQLException {
        final var result = JSON.createObjectNode();
        final var metadata = rows.getMetaData();
        for (int column = 1; column <= metadata.getColumnCount(); column++) {
            result.put(metadata.getColumnLabel(column), rows.getString(column));
        }
        return result;
    }

    private static void execute(Connection jdbc, String sql) throws SQLException {
        try (var statement = jdbc.createStatement()) {
            statement.setQueryTimeout(15);
            statement.execute(sql);
        }
    }

    private static void rejection(ObjectNode attempt, SQLException error) {
        attempt.put("jdbc", "rejected");
        attempt.put("jdbcError", error.toString());
        attempt.put("sqlState", error.getSQLState());
        attempt.put("vendorCode", error.getErrorCode());
    }

    private static ArrayNode rows(String... values) {
        final var result = JSON.createArrayNode();
        for (int index = 0; index < values.length; index += 2) {
            result.add(JSON.createObjectNode().put("ID", values[index]).put("V", values[index + 1]));
        }
        return result;
    }

    private static void writeReport(ObjectNode report) throws Exception {
        Files.createDirectories(EVIDENCE.getParent());
        JSON.writerWithDefaultPrettyPrinter().writeValue(EVIDENCE.toFile(), report);
    }
}
