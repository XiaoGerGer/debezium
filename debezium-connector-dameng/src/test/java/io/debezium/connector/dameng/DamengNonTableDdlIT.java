/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.debezium.config.Configuration;
import io.debezium.connector.dameng.antlr.DamengDdlStatementAnalyzer.Impact;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.storage.file.history.FileSchemaHistory;

/** Verifies non-table DDL using real archived redo, live CDC and persisted offset/history recovery. */
class DamengNonTableDdlIT extends AbstractAsyncEngineConnectorTest {
    private static final ObjectMapper JSON = new ObjectMapper();

    private enum DropDependency {
        TYPE_COLUMN,
        CLASS_COLUMN,
        NESTED_TYPE_MEMBER,
        SUBTYPE
    }

    @TempDir
    Path directory;

    private final AtomicReference<Throwable> failure = new AtomicReference<>();
    private final List<String> cleanup = new ArrayList<>();
    private final ObjectNode evidence = JSON.createObjectNode();
    private final ArrayNode attempts = evidence.putArray("statements");
    private Connection jdbc;
    private Configuration config;
    private String table;
    private String index;
    private String schema;
    private String evidenceProperty = "dameng.non.table.ddl.it.evidence";
    private String evidenceDefault = "target/non-table-ddl-it.json";
    private Schema rowSchema;
    private int markerId;

    @BeforeAll
    static void startDatabase() throws Exception {
        assumeTrue(System.getProperty("dameng.hostname") == null, "Object lifecycle tests require a test-owned database");
        DamengTestDatabase.start();
    }

    @AfterAll
    static void stopDatabase() {
        DamengTestDatabase.stop();
    }

    @AfterEach
    void cleanup() throws Exception {
        stopConnector();
        if (jdbc != null) {
            try {
                // Every name belongs to this test. No CASCADE is needed or allowed.
                for (int position = cleanup.size() - 1; position >= 0; position--) {
                    try {
                        execute(cleanup.get(position));
                    }
                    catch (SQLException ignored) {
                        // Objects successfully dropped by the test are already absent.
                    }
                }
            }
            finally {
                jdbc.close();
            }
        }
        writeEvidence();
    }

    @Test
    void shouldIgnoreUnrelatedObjectDdlWithoutChangingCapturedSchemaAcrossRestarts() throws Exception {
        final var prefix = "DBZ_NT_" + UUID.randomUUID().toString().replace("-", "").substring(0, 10).toUpperCase();
        schema = DamengTestDatabase.schema();
        table = schema + "." + prefix + "_DATA";
        index = schema + "." + prefix + "_INDEX";
        jdbc = DriverManager.getConnection(DamengTestDatabase.jdbcUrl(), DamengTestDatabase.user(), DamengTestDatabase.password());
        jdbc.setAutoCommit(true);
        evidence.put("databaseVersion", jdbc.getMetaData().getDatabaseProductVersion());
        evidence.put("compatibleMode", DamengTestDatabase.requestedCompatibilityMode());
        evidence.put("validationPassed", false);
        evidence.put("scope", "isolated object lifecycle representatives; not exhaustive SQL grammar coverage");
        cleanup.add("DROP TABLE " + table);
        execute("CREATE TABLE " + table + "(ID INT PRIMARY KEY,LABEL VARCHAR(80))");
        execute("CREATE INDEX " + index + " ON " + table + "(LABEL)");
        execute("INSERT INTO " + table + " VALUES(0,'snapshot')");
        config = Configuration.create()
                .with("topic.prefix", "dameng")
                .with("database.hostname", DamengTestDatabase.hostname()).with("database.port", DamengTestDatabase.port())
                .with("database.user", DamengTestDatabase.user()).with("database.password", DamengTestDatabase.password())
                .with("database.dbname", System.getProperty("dameng.database", "DAMENG"))
                .with("table.include.list", Pattern.quote(table))
                .with("schema.history.internal", FileSchemaHistory.class.getName())
                .with("schema.history.internal.file.filename", directory.resolve("history.dat").toString())
                .with("snapshot.mode", "initial").with("poll.interval.ms", 100)
                .with("include.schema.changes", false).with("tombstones.on.delete", false).build();
        startEngine();
        rowSchema = after(next("r", 0)).schema();
        waitForStreamingRunning("dameng", "dameng");

        try (var miner = new DamengConnection(Configuration.create()
                .with("hostname", DamengTestDatabase.hostname()).with("port", DamengTestDatabase.port())
                .with("user", DamengTestDatabase.user()).with("password", DamengTestDatabase.password()).build())) {
            final long objectId = miner.getTableObjectId(new TableId(null, schema, prefix + "_DATA"));
            final long startLsn = miner.currentLsn();
            final var online = executeLifecycle(prefix + "_LIVE", miner, true);
            checkpoint(online);

            // The same kinds of DDL must pass when no running task can inspect their original objects.
            // In particular, user/schema deletion is RESTRICT, never an unverified CASCADE.
            executeLifecycle(prefix + "_STOP", miner, false);
            final int lastOfflineMarker = markerId;
            startEngine();
            SourceRecord replayed = null;
            for (final JsonNode attempt : attempts) {
                if (!attempt.path("phase").asText().equals("offline")) {
                    continue;
                }
                replayed = next("c", attempt.required("markerId").asInt());
                assertMarker(replayed, attempt.required("markerId").asInt());
                ((ObjectNode) attempt).put("cdcMarkerVerified", true);
            }
            assertThat(replayed).isNotNull();
            checkpoint(replayed);

            // This start recovers the acknowledged history/offset without replaying the object definitions.
            startEngine();
            waitForStreamingRunning("dameng", "dameng");
            execute("UPDATE " + table + " SET LABEL='after-recovery' WHERE ID=" + lastOfflineMarker);
            final var update = next("u", lastOfflineMarker);
            assertThat(after(update).schema()).isEqualTo(rowSchema);
            assertThat(after(update).getString("LABEL")).isEqualTo("after-recovery");
            assertThat(((Struct) update.value()).getStruct("before").getString("LABEL")).isEqualTo("marker-" + lastOfflineMarker);
            checkpoint(update);
            evidence.put("historyRecoveryVerified", true);

            final long endLsn = miner.currentLsn();
            verifyRedo(miner, startLsn, endLsn, objectId);
            assertThat(failure.get()).as("connector failure after unrelated DDL and two recoveries").isNull();
            assertThat(attempts).isNotEmpty();
            for (final JsonNode attempt : attempts) {
                assertThat(attempt.required("jdbc").asText()).as("database accepted %s", attempt.required("sql").asText()).isEqualTo("accepted");
                assertThat(attempt.required("cdcMarkerVerified").asBoolean()).as("CDC marker for %s", attempt.required("sql").asText()).isTrue();
            }
            evidence.put("validationPassed", true);
            writeEvidence();
        }
    }

    private SourceRecord executeLifecycle(String prefix, DamengConnection miner, boolean online) throws Exception {
        final var user = prefix + "_USER";
        final var otherSchema = prefix + "_SCHEMA";
        final var directoryName = prefix + "_DIR";
        final var type = schema + "." + prefix + "_TYPE";
        final var domain = schema + "." + prefix + "_DOMAIN";
        final var context = prefix + "_CONTEXT";
        final var packageName = schema + "." + prefix + "_PACKAGE";
        final var className = schema + "." + prefix + "_CLASS";
        final var profile = prefix + "_PROFILE";
        final var operatorFunction = schema + "." + prefix + "_OP_FUNC";
        final var contextIndex = prefix + "_FULLTEXT";
        final var timer = prefix + "_TIMER";
        final var procedure = schema + "." + prefix + "_PROC";
        final var function = schema + "." + prefix + "_FUNC";
        cleanup.add("DROP USER " + user);
        cleanup.add("DROP SCHEMA " + otherSchema + " RESTRICT");
        cleanup.add("DROP DIRECTORY " + directoryName);
        cleanup.add("DROP TYPE " + type + " RESTRICT");
        cleanup.add("DROP DOMAIN " + domain + " RESTRICT");
        cleanup.add("DROP PACKAGE " + packageName);
        cleanup.add("DROP CONTEXT " + context);
        cleanup.add("DROP CLASS " + className + " RESTRICT");
        cleanup.add("DROP PROFILE " + profile + " RESTRICT");
        cleanup.add("DROP FUNCTION " + operatorFunction);
        cleanup.add("DROP OPERATOR IF EXISTS @^(INT,INT)");
        cleanup.add("DROP CONTEXT INDEX IF EXISTS " + contextIndex + " ON " + table);
        cleanup.add("DROP TRIGGER " + timer);
        cleanup.add("DROP PROCEDURE " + procedure);
        cleanup.add("DROP FUNCTION " + function);
        final List<String> sql = List.of(
                "CREATE USER " + user + " IDENTIFIED BY \"Dbz_nt_12345\"",
                "ALTER USER " + user + " LIMIT CONNECT_TIME 30",
                "DROP USER " + user,
                "CREATE OR REPLACE DIRECTORY " + directoryName + " AS '/opt/dmdbms/data/DAMENG'",
                "CREATE OR REPLACE DIRECTORY " + directoryName + " AS '/tmp'",
                "DROP DIRECTORY " + directoryName,
                "CREATE SCHEMA " + otherSchema + " AUTHORIZATION " + DamengTestDatabase.user(),
                "DROP SCHEMA " + otherSchema + " RESTRICT",
                "STAT 30 ON " + table + "(ID)",
                "STAT 50 ON INDEX " + index,
                "/*+parallel(12)*/ STAT ON " + table,
                "CREATE OR REPLACE TYPE " + type + " AS OBJECT(ID NUMBER,DAYTIME DATE,MEMBER FUNCTION F RETURN INT)",
                "CREATE OR REPLACE TYPE BODY " + type + " AS MEMBER FUNCTION F RETURN INT AS BEGIN RETURN 1; END; END;",
                "ALTER TYPE " + type + " COMPILE",
                "DROP TYPE BODY " + type,
                "DROP TYPE " + type,
                "CREATE DOMAIN " + domain + " INT CHECK(VALUE < 100)",
                "DROP DOMAIN " + domain + " RESTRICT",
                "CREATE OR REPLACE PACKAGE " + packageName + " AS FUNCTION F RETURN INT; END;",
                "CREATE OR REPLACE PACKAGE BODY " + packageName + " AS FUNCTION F RETURN INT AS BEGIN RETURN 1; END; END;",
                "ALTER PACKAGE " + packageName + " COMPILE",
                "CREATE OR REPLACE CONTEXT " + context + " USING " + packageName,
                "DROP CONTEXT " + context,
                "DROP PACKAGE BODY " + packageName,
                "DROP PACKAGE " + packageName,
                "CREATE OR REPLACE CLASS " + className + " AS ID INT; FUNCTION F RETURN INT; END;",
                "CREATE OR REPLACE CLASS BODY " + className + " AS FUNCTION F RETURN INT AS BEGIN RETURN ID; END; END;",
                "ALTER CLASS " + className + " COMPILE",
                "DROP CLASS BODY " + className,
                "DROP CLASS " + className,
                "CREATE FUNCTION " + operatorFunction + "(A INT,B INT) RETURN INT AS BEGIN RETURN (A+B)*2; END;",
                "CREATE OPERATOR @^ (FUNCTION " + operatorFunction + ", LEFTARG INT, RIGHTARG INT)",
                "DROP OPERATOR IF EXISTS @^(INT,INT)",
                "DROP FUNCTION " + operatorFunction,
                "CREATE PROFILE " + profile + " LIMIT SESSION_PER_USER 100 PASSWORD_REUSE_TIME 10",
                "ALTER PROFILE " + profile + " LIMIT CONNECT_TIME 30",
                "DROP PROFILE " + profile + " RESTRICT",
                "CREATE CONTEXT INDEX " + contextIndex + " ON " + table + "(LABEL)",
                "ALTER CONTEXT INDEX " + contextIndex + " ON " + table + " REBUILD",
                "ALTER CONTEXT INDEX " + contextIndex + " ON " + table + " OPTIMIZE",
                "DROP CONTEXT INDEX " + contextIndex + " ON " + table,
                "CREATE CONTEXT INDEX IF NOT EXISTS " + contextIndex + " ON " + table + "(LABEL)",
                "DROP CONTEXT INDEX IF EXISTS " + contextIndex + " ON " + table,
                // The timer body is inert so a scheduled execution cannot add CDC records between markers.
                "CREATE OR REPLACE TRIGGER " + timer + " AFTER TIMER ON DATABASE FOR EACH 1 DAY FOR EACH 1 MINUTE BEGIN NULL; END;",
                "DROP TRIGGER " + timer,
                "CREATE PROCEDURE " + procedure + "(A OUT IN INT) AS BEGIN A:=1; END;",
                "DROP PROCEDURE " + procedure,
                "CREATE FUNCTION " + function + "(A OUT IN INT) RETURN INT AS BEGIN RETURN A; END;",
                "DROP FUNCTION " + function);
        SourceRecord last = null;
        for (final var statement : sql) {
            final var attempt = attempts.addObject();
            attempt.put("phase", online ? "online" : "offline");
            attempt.put("sql", statement);
            attempt.put("startLsn", miner.currentLsn());
            attempt.put("cdcMarkerVerified", false);
            attempt.set("redo", JSON.createArrayNode());
            try {
                execute(statement);
                attempt.put("jdbc", "accepted");
            }
            catch (SQLException error) {
                attempt.put("jdbc", "rejected");
                attempt.put("jdbcError", error.toString());
                attempt.put("sqlState", error.getSQLState());
                attempt.put("vendorCode", error.getErrorCode());
                attempt.put("conclusion", "database-rejected; connector support not demonstrated");
                writeEvidence();
                throw error;
            }
            attempt.put("endLsn", miner.currentLsn());
            final int id = ++markerId;
            attempt.put("markerId", id);
            execute("INSERT INTO " + table + " VALUES(" + id + ",'marker-" + id + "')");
            if (online) {
                last = next("c", id);
                assertMarker(last, id);
                attempt.put("cdcMarkerVerified", true);
            }
            writeEvidence();
        }
        return last;
    }

    @Test
    void shouldRecordLibraryDefinitionAvailability() throws Exception {
        evidenceProperty = "dameng.non.table.ddl.library.probe.evidence";
        evidenceDefault = "target/non-table-ddl-library-probe.json";
        final var name = "DBZ_NTL_" + UUID.randomUUID().toString().replace("-", "").substring(0, 10).toUpperCase();
        jdbc = DriverManager.getConnection(DamengTestDatabase.jdbcUrl(), DamengTestDatabase.user(), DamengTestDatabase.password());
        jdbc.setAutoCommit(true);
        evidence.put("databaseVersion", jdbc.getMetaData().getDatabaseProductVersion());
        evidence.put("compatibleMode", DamengTestDatabase.requestedCompatibilityMode());
        evidence.put("scope", "database-only documented LIBRARY availability probe; no connector support conclusion");
        evidence.put("connectorSupportVerified", false);
        evidence.put("validationPassed", false);
        final List<String> libraries = List.of(name + "_SIMPLE", DamengTestDatabase.schema() + "." + name + "_REPLACED");
        int accepted = 0;
        for (int position = 0; position < libraries.size(); position++) {
            final var library = libraries.get(position);
            final var attempt = attempts.addObject();
            final var create = attempt.putObject("create");
            create.put("sql", (position == 0 ? "CREATE LIBRARY " : "CREATE OR REPLACE LIBRARY ")
                    + library + " AS '/opt/dmdbms/bin/libdodbc.so'");
            try {
                execute(create.required("sql").asText());
                create.put("jdbc", "accepted");
                cleanup.add("DROP LIBRARY " + library);
            }
            catch (SQLException error) {
                create.put("jdbc", "rejected");
                create.put("jdbcError", error.toString());
                create.put("sqlState", error.getSQLState());
                create.put("vendorCode", error.getErrorCode());
                create.put("conclusion", "database rejected this LIBRARY form; connector support not demonstrated");
                writeEvidence();
                assertThat(error.getErrorCode()).as("only the observed syntax rejection is an expected LIBRARY availability result").isEqualTo(-2007);
                continue;
            }
            final var drop = attempt.putObject("drop");
            drop.put("sql", "DROP LIBRARY " + library);
            try {
                execute(drop.required("sql").asText());
                drop.put("jdbc", "accepted");
                accepted++;
            }
            catch (SQLException error) {
                drop.put("jdbc", "rejected");
                drop.put("jdbcError", error.toString());
                drop.put("sqlState", error.getSQLState());
                drop.put("vendorCode", error.getErrorCode());
                writeEvidence();
                throw error;
            }
            attempt.put("conclusion", "database accepted this LIBRARY lifecycle; archived redo and CDC remain unverified");
        }
        assertThat(attempts).hasSize(2);
        evidence.put("acceptedLifecycles", accepted);
        evidence.put("conclusion", "database availability recorded separately for minimal and qualified OR REPLACE syntax; connector support not demonstrated");
        evidence.put("validationPassed", true);
        writeEvidence();
    }

    @ParameterizedTest
    @EnumSource(DropDependency.class)
    void shouldRejectDefaultTypeOrClassDropWhileReferencedByATable(DropDependency dependency) throws Exception {
        evidenceProperty = "dameng.non.table.ddl.type.drop.probe.evidence";
        evidenceDefault = "target/non-table-ddl-type-drop-" + dependency.name().toLowerCase(Locale.ROOT) + ".json";
        final var prefix = "DBZ_NTD_" + UUID.randomUUID().toString().replace("-", "").substring(0, 10).toUpperCase();
        final var owner = DamengTestDatabase.schema();
        final var type = owner + "." + prefix + "_TYPE";
        final var dependent = owner + "." + prefix + "_DEPENDENT";
        final var referenced = owner + "." + prefix + "_REF";
        final var kind = dependency == DropDependency.CLASS_COLUMN ? "CLASS" : "TYPE";
        final boolean indirect = dependency == DropDependency.NESTED_TYPE_MEMBER || dependency == DropDependency.SUBTYPE;
        jdbc = DriverManager.getConnection(DamengTestDatabase.jdbcUrl(), DamengTestDatabase.user(), DamengTestDatabase.password());
        jdbc.setAutoCommit(true);
        evidence.put("databaseVersion", jdbc.getMetaData().getDatabaseProductVersion());
        evidence.put("compatibleMode", DamengTestDatabase.requestedCompatibilityMode());
        evidence.put("dependency", dependency.name());
        evidence.put("scope", "database dependency constraint; default DROP must preserve referencing tables and their data");
        evidence.put("connectorSupportVerified", false);
        evidence.put("validationPassed", false);
        cleanup.add("DROP " + kind + " " + type + " RESTRICT");
        execute(switch (dependency) {
            case CLASS_COLUMN -> "CREATE CLASS " + type + " AS ID INT; END;";
            case SUBTYPE -> "CREATE TYPE " + type + " AS OBJECT(ID INT) NOT FINAL";
            default -> "CREATE TYPE " + type + " AS OBJECT(ID INT)";
        });
        if (indirect) {
            cleanup.add("DROP TYPE " + dependent + " RESTRICT");
            execute(dependency == DropDependency.NESTED_TYPE_MEMBER
                    ? "CREATE TYPE " + dependent + " AS OBJECT(V " + type + ")"
                    : "CREATE TYPE " + dependent + " UNDER " + type + "(V INT)");
        }
        cleanup.add("DROP TABLE " + referenced);
        execute("CREATE TABLE " + referenced + "(ID INT PRIMARY KEY,V " + (indirect ? dependent : type) + ")");
        execute("INSERT INTO " + referenced + "(ID) VALUES(1)");
        final var drop = evidence.putObject("drop");
        drop.put("sql", "DROP " + kind + " " + type);
        assertThatThrownBy(() -> {
            execute(drop.required("sql").asText());
            drop.put("jdbc", "accepted");
        }).as("default DROP must reject the %s dependency", dependency)
                .isInstanceOfSatisfying(SQLException.class, error -> {
                    drop.put("jdbc", "rejected");
                    drop.put("jdbcError", error.toString());
                    drop.put("sqlState", error.getSQLState());
                    drop.put("vendorCode", error.getErrorCode());
                });
        final var remaining = evidence.putObject("referencingTableAfterDrop");
        try (var statement = jdbc.createStatement(); var rows = statement.executeQuery("SELECT ID,V FROM " + referenced)) {
            assertThat(rows.getMetaData().getColumnCount()).isEqualTo(2);
            assertThat(rows.getMetaData().getColumnName(2)).isEqualTo("V");
            assertThat(rows.next()).isTrue();
            assertThat(rows.getInt(1)).isEqualTo(1);
            assertThat(rows.next()).isFalse();
            remaining.put("readable", true);
            remaining.put("dataAndColumnPreserved", true);
        }
        execute("DROP TABLE " + referenced);
        if (indirect) {
            execute("DROP TYPE " + dependent);
        }
        // The identical DROP must succeed without dependencies, excluding syntax/privilege errors as false positives.
        execute(drop.required("sql").asText());
        evidence.put("defaultDropAcceptedAfterRemovingDependencies", true);
        evidence.put("validationPassed", true);
        writeEvidence();
    }

    @Test
    void shouldRejectDroppingATablespaceThatStillContainsATable() throws Exception {
        evidenceProperty = "dameng.non.table.ddl.tablespace.drop.probe.evidence";
        evidenceDefault = "target/non-table-ddl-tablespace-drop-probe.json";
        final var tablespace = "DBZ_NTS_" + UUID.randomUUID().toString().replace("-", "").substring(0, 10).toUpperCase();
        final var referenced = DamengTestDatabase.schema() + "." + tablespace + "_DATA";
        final var datafile = "/opt/dmdbms/data/DAMENG/" + tablespace + ".DBF";
        jdbc = DriverManager.getConnection(DamengTestDatabase.jdbcUrl(), DamengTestDatabase.user(), DamengTestDatabase.password());
        jdbc.setAutoCommit(true);
        evidence.put("databaseVersion", jdbc.getMetaData().getDatabaseProductVersion());
        evidence.put("compatibleMode", DamengTestDatabase.requestedCompatibilityMode());
        evidence.put("scope", "Database-only nonempty TABLESPACE deletion constraint on the recorded DM version; not a claim for other versions or CDC");
        evidence.put("connectorSupportVerified", false);
        evidence.put("validationPassed", false);
        evidence.put("tablespace", tablespace);
        evidence.put("datafile", datafile);
        cleanup.add("DROP TABLESPACE " + tablespace);
        cleanup.add("DROP TABLE " + referenced);
        execute("CREATE TABLESPACE " + tablespace + " DATAFILE '" + datafile + "' SIZE 50");
        execute("CREATE TABLE " + referenced + "(ID INT PRIMARY KEY) STORAGE(ON " + tablespace + ")");
        execute("INSERT INTO " + referenced + " VALUES(1)");

        final var drop = evidence.putObject("dropWhileUsed");
        drop.put("sql", "DROP TABLESPACE " + tablespace);
        assertThatThrownBy(() -> execute(drop.required("sql").asText()))
                .as("a tablespace containing the private table cannot be dropped")
                .isInstanceOfSatisfying(SQLException.class, error -> {
                    drop.put("jdbc", "rejected");
                    drop.put("jdbcError", error.toString());
                    drop.put("sqlState", error.getSQLState());
                    drop.put("vendorCode", error.getErrorCode());
                });
        try (var statement = jdbc.createStatement(); var rows = statement.executeQuery("SELECT ID FROM " + referenced)) {
            assertThat(rows.next()).isTrue();
            assertThat(rows.getInt(1)).isEqualTo(1);
            assertThat(rows.next()).isFalse();
        }
        evidence.put("tableDataPreservedAfterRejectedDrop", true);
        execute("DROP TABLE " + referenced);
        execute("DROP TABLESPACE " + tablespace);
        evidence.put("emptyTablespaceDropAccepted", true);
        evidence.put("validationPassed", true);
        writeEvidence();
    }

    private void verifyRedo(DamengConnection miner, long startLsn, long endLsn, long objectId) throws Exception {
        Awaitility.await("object DDL and CDC markers archived").atMost(Duration.ofSeconds(90))
                .pollInterval(Duration.ofMillis(200)).until(() -> miner.latestArchivedLsn() >= endLsn);
        miner.startLogMining(startLsn, endLsn, false);
        try {
            final ArrayNode redo = miner.queryAndMap("SELECT SCN,OPERATION_CODE,SEG_OWNER,TABLE_NAME,SQL_REDO,DATA_OBJ# "
                    + "FROM V$LOGMNR_CONTENTS WHERE SCN > " + startLsn + " AND SCN <= " + endLsn
                    + " AND OPERATION_CODE IN(1,5) ORDER BY SCN,SSN", rows -> {
                        final ArrayNode events = JSON.createArrayNode();
                        while (rows.next()) {
                            final var event = events.addObject();
                            event.put("lsn", rows.getLong(1));
                            event.put("operation", rows.getInt(2));
                            event.put("owner", rows.getString(3));
                            event.put("table", rows.getString(4));
                            event.put("sqlRedo", rows.getString(5));
                            event.put("objectId", rows.getLong(6));
                        }
                        return events;
                    });
            evidence.put("startLsn", startLsn);
            evidence.put("endLsn", endLsn);
            evidence.put("markerObjectId", objectId);
            final var positiveControl = evidence.putArray("positiveControl");
            for (final JsonNode event : redo) {
                if (event.required("operation").asInt() == 1 && event.required("objectId").asLong() == objectId) {
                    positiveControl.add(event);
                }
            }
            assertThat(positiveControl).as("LogMiner actually read the captured marker table").isNotEmpty();
            final var parser = miner.createDdlParser();
            for (final JsonNode statement : attempts) {
                final var attempt = (ObjectNode) statement;
                final var matched = (ArrayNode) attempt.required("redo");
                for (final JsonNode event : redo) {
                    if (event.required("operation").asInt() != 5 || event.required("lsn").asLong() <= attempt.required("startLsn").asLong()
                            || event.required("lsn").asLong() > attempt.required("endLsn").asLong()) {
                        continue;
                    }
                    final var observed = (ObjectNode) event.deepCopy();
                    matched.add(observed);
                    try {
                        final var owner = event.hasNonNull("owner") && !event.required("owner").asText().isBlank()
                                ? event.required("owner").asText()
                                : schema;
                        final var analysis = parser.analyze(event.required("sqlRedo").asText(), owner);
                        observed.put("impact", analysis.impact().name());
                        assertThat(analysis.impact()).as("actual non-table DDL %s", event.required("sqlRedo").asText()).isEqualTo(Impact.NO_TABLE_CHANGE);
                        assertThat(parser.plan(analysis, new Tables())).isEmpty();
                    }
                    catch (RuntimeException | AssertionError error) {
                        observed.put("parserError", error.toString());
                        writeEvidence();
                        throw error;
                    }
                }
                assertThat(matched).as("actual DDL redo for %s", attempt.required("sql").asText()).isNotEmpty();
                attempt.put("conclusion", "database accepted; actual DDL ignored; live or replayed CDC marker verified");
            }
        }
        finally {
            miner.endLogMining();
            writeEvidence();
        }
    }

    private void checkpoint(SourceRecord record) throws Exception {
        stopConnector();
        assertThat(failure.get()).as("connector failure before saving offset").isNull();
        storeOffsets(config, Map.of(record.sourcePartition(), record.sourceOffset()));
        consumedLines.clear();
    }

    private void assertMarker(SourceRecord record, int id) {
        assertThat(after(record).schema()).isEqualTo(rowSchema);
        assertThat(after(record).getString("LABEL")).isEqualTo("marker-" + id);
        assertThat(((Struct) record.key()).getInt32("ID")).isEqualTo(id);
    }

    private void startEngine() {
        failure.set(null);
        start(DamengConnector.class, config, (success, message, error) -> {
            if (!success) {
                failure.set(error == null ? new IllegalStateException(message) : error);
            }
        }, null);
    }

    private SourceRecord next(String operation, int id) throws Exception {
        final long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
        while (System.nanoTime() < deadline) {
            assertThat(failure.get()).as("connector failure while awaiting %s for ID=%s", operation, id).isNull();
            if (waitForAvailableRecords(1, TimeUnit.SECONDS)) {
                final var record = consumeRecord();
                if (record != null && record.topic().equals("dameng." + table) && record.value() instanceof Struct envelope) {
                    envelope.validate();
                    assertThat(envelope.getString("op")).isEqualTo(operation);
                    assertThat(after(record).getInt32("ID")).isEqualTo(id);
                    return record;
                }
            }
        }
        throw new AssertionError("No " + operation + " event for " + table + " ID=" + id + " in 60 seconds");
    }

    private static Struct after(SourceRecord record) {
        return ((Struct) record.value()).getStruct("after");
    }

    private void execute(String sql) throws SQLException {
        try (var statement = jdbc.createStatement()) {
            statement.execute(sql);
        }
    }

    private void writeEvidence() throws Exception {
        final var output = Path.of(System.getProperty(evidenceProperty, evidenceDefault));
        java.nio.file.Files.createDirectories(output.toAbsolutePath().getParent());
        JSON.writerWithDefaultPrettyPrinter().writeValue(output.toFile(), evidence);
    }
}
