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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.connector.dameng.antlr.DamengDdlStatementAnalyzer.CascadeDropException;
import io.debezium.connector.dameng.antlr.DamengDdlStatementAnalyzer.Impact;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;
import io.debezium.pipeline.spi.Offsets;
import io.debezium.relational.CustomConverterRegistry;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.storage.file.history.FileSchemaHistory;

/** Verifies non-table DDL using real archived redo, live CDC and persisted offset/history recovery. */
class DamengNonTableDdlIT extends AbstractAsyncEngineConnectorTest {
    private static final ObjectMapper JSON = new ObjectMapper();

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
        // RECYCLEBIN is static: set it before the fixture configures and restarts its own database.
        final var parameter = "dameng.test.ini.RECYCLEBIN";
        final var previous = System.setProperty(parameter, "1");
        try {
            DamengTestDatabase.start();
        }
        finally {
            if (previous == null) {
                System.clearProperty(parameter);
            }
            else {
                System.setProperty(parameter, previous);
            }
        }
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
        return executeNonTableStatements(sql, miner, online);
    }

    private SourceRecord executeNonTableStatements(List<String> sql, DamengConnection miner, boolean online) throws Exception {
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
    void shouldIgnoreRecycleBinPurgesWithoutChangingCapturedTableAcrossRestarts() throws Exception {
        evidenceProperty = "dameng.recycle.bin.it.evidence";
        evidenceDefault = "target/recycle-bin-it.json";
        final var prefix = "DBZ_RB_" + UUID.randomUUID().toString().replace("-", "").substring(0, 10).toUpperCase(Locale.ROOT);
        final var tableName = prefix + "_DATA";
        final var indexName = prefix + "_OLD_INDEX";
        schema = DamengTestDatabase.schema();
        table = schema + "." + tableName;
        jdbc = DriverManager.getConnection(DamengTestDatabase.jdbcUrl(), DamengTestDatabase.user(), DamengTestDatabase.password());
        jdbc.setAutoCommit(true);
        evidence.put("databaseVersion", jdbc.getMetaData().getDatabaseProductVersion());
        evidence.put("compatibleMode", DamengTestDatabase.requestedCompatibilityMode());
        evidence.put("validationPassed", false);
        evidence.put("scope", "six recycle-bin purge forms; live CDC, offline replay and same-name table recreation");

        try (var miner = new DamengConnection(Configuration.create()
                .with("hostname", DamengTestDatabase.hostname()).with("port", DamengTestDatabase.port())
                .with("user", DamengTestDatabase.user()).with("password", DamengTestDatabase.password()).build())) {
            // Snapshot the new incarnation while the old table and its index remain in the bin.
            final long oldObjectId = prepareRecycledTable(tableName, indexName, miner);
            execute("CREATE TABLE " + table + "(ID INT PRIMARY KEY,LABEL VARCHAR(80))");
            execute("INSERT INTO " + table + " VALUES(0,'snapshot')");
            final var tableId = new TableId(null, schema, tableName);
            final long objectId = miner.getTableObjectId(tableId);
            assertThat(objectId).isNotEqualTo(oldObjectId);
            evidence.put("recycledObjectId", oldObjectId);
            evidence.put("liveObjectId", objectId);
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
            final var history = java.nio.file.Files.readAllLines(directory.resolve("history.dat"));
            assertThat(history).isNotEmpty();
            final long startLsn = miner.currentLsn();
            checkpoint(executePurgeLifecycle(prefix + "_LIVE", tableName, indexName, miner, objectId, true));
            assertThat(java.nio.file.Files.readAllLines(directory.resolve("history.dat"))).isEqualTo(history);

            final var offlineTable = prefix + "_OFFLINE";
            final var offlineIndex = prefix + "_OFFLINE_INDEX";
            prepareRecycledTable(offlineTable, offlineIndex, miner);
            executePurgeLifecycle(prefix + "_STOP", offlineTable, offlineIndex, miner, objectId, false);
            startEngine();
            SourceRecord replayed = null;
            for (final JsonNode attempt : attempts) {
                if (attempt.required("phase").asText().equals("offline")) {
                    final int id = attempt.required("markerId").asInt();
                    replayed = next("c", id);
                    assertMarker(replayed, id);
                    ((ObjectNode) attempt).put("cdcMarkerVerified", true);
                }
            }
            assertThat(replayed).isNotNull();
            checkpoint(replayed);
            assertThat(java.nio.file.Files.readAllLines(directory.resolve("history.dat"))).isEqualTo(history);

            startEngine();
            waitForStreamingRunning("dameng", "dameng");
            execute("UPDATE " + table + " SET LABEL='after-recovery' WHERE ID=" + markerId);
            final var update = next("u", markerId);
            assertThat(after(update).schema()).isEqualTo(rowSchema);
            assertThat(after(update).getString("LABEL")).isEqualTo("after-recovery");
            assertThat(((Struct) update.value()).getStruct("before").getString("LABEL")).isEqualTo("marker-" + markerId);
            checkpoint(update);
            assertThat(java.nio.file.Files.readAllLines(directory.resolve("history.dat"))).isEqualTo(history);
            assertThat(miner.getTableObjectId(tableId)).isEqualTo(objectId);
            assertThat(miner.readTableSchema(tableId).columnWithName("LABEL").length()).isEqualTo(80);
            try (var statement = jdbc.createStatement(); var rows = statement.executeQuery("SELECT COUNT(*) FROM " + table)) {
                assertThat(rows.next()).isTrue();
                assertThat(rows.getInt(1)).isEqualTo(13);
            }
            verifyRedo(miner, startLsn, miner.currentLsn(), objectId);
            assertThat(attempts).hasSize(12);
            for (final JsonNode attempt : attempts) {
                assertThat(attempt.required("cdcMarkerVerified").asBoolean()).as("CDC after %s", attempt.required("sql").asText()).isTrue();
            }
            assertThat(failure.get()).isNull();
            evidence.put("historyRecoveryVerified", true);
            evidence.put("validationPassed", true);
            writeEvidence();
        }
    }

    @Test
    void shouldIgnoreCascadingSchemaDropsWithStaleHistoryAcrossRestarts() throws Exception {
        evidenceProperty = "dameng.cascade.drop.it.evidence";
        evidenceDefault = "target/cascade-drop-it.json";
        final var prefix = "DBZ_CD_" + UUID.randomUUID().toString().replace("-", "").substring(0, 10).toUpperCase(Locale.ROOT);
        schema = DamengTestDatabase.schema();
        table = schema + "." + prefix + "_MARKER";
        final var liveSchema = prefix + "_LIVE";
        final var offlineSchema = prefix + "_OFFLINE";
        final var victims = List.of(new TableId(null, liveSchema, "VICTIM"), new TableId(null, offlineSchema, "VICTIM"));
        jdbc = DriverManager.getConnection(DamengTestDatabase.jdbcUrl(), DamengTestDatabase.user(), DamengTestDatabase.password());
        jdbc.setAutoCommit(true);
        cleanup.add("DROP TABLE " + table);
        execute("CREATE TABLE " + table + "(ID INT PRIMARY KEY,LABEL VARCHAR(80))");
        execute("INSERT INTO " + table + " VALUES(0,'snapshot')");
        for (final var victim : victims) {
            cleanup.add("DROP SCHEMA " + victim.schema() + " RESTRICT");
            cleanup.add("DROP TABLE " + victim);
            execute("CREATE SCHEMA " + victim.schema() + " AUTHORIZATION " + schema);
            execute("CREATE TABLE " + victim + "(ID INT PRIMARY KEY,LABEL VARCHAR(80))");
        }
        evidence.put("databaseVersion", jdbc.getMetaData().getDatabaseProductVersion());
        evidence.put("compatibleMode", DamengTestDatabase.requestedCompatibilityMode());
        evidence.put("validationPassed", false);
        evidence.put("scope", "captured dependent tables removed by live/offline DROP SCHEMA CASCADE; unchanged persisted history and restart with opt-in disabled");
        config = Configuration.create()
                .with("topic.prefix", "dameng")
                .with("database.hostname", DamengTestDatabase.hostname()).with("database.port", DamengTestDatabase.port())
                .with("database.user", DamengTestDatabase.user()).with("database.password", DamengTestDatabase.password())
                .with("database.dbname", System.getProperty("dameng.database", "DAMENG"))
                .with("table.include.list", Pattern.quote(table) + "," + Pattern.quote(victims.get(0).toString()) + "," + Pattern.quote(victims.get(1).toString()))
                .with("schema.history.internal", FileSchemaHistory.class.getName())
                .with("schema.history.internal.file.filename", directory.resolve("history.dat").toString())
                .with(DamengConnectorConfig.CASCADE_DROP_IGNORE, true)
                .with("snapshot.mode", "initial").with("poll.interval.ms", 100)
                .with("include.schema.changes", false).with("tombstones.on.delete", false).build();
        startEngine();
        final var snapshot = next("r", 0);
        rowSchema = after(snapshot).schema();
        waitForStreamingRunning("dameng", "dameng");
        execute("INSERT INTO " + table + " VALUES(" + (++markerId) + ",'marker-" + markerId + "')");
        final var baseline = next("c", markerId);
        checkpoint(baseline);
        // Recover only while the engine is stopped so its schema-history metrics are unregistered.
        final var before = recoverTables(baseline);
        for (final var victim : victims) {
            assertThat(before.forTable(victim)).as("empty dependent table was captured in the baseline").isNotNull();
        }
        final var history = java.nio.file.Files.readAllLines(directory.resolve("history.dat"));
        try (var miner = new DamengConnection(Configuration.create()
                .with("hostname", DamengTestDatabase.hostname()).with("port", DamengTestDatabase.port())
                .with("user", DamengTestDatabase.user()).with("password", DamengTestDatabase.password()).build())) {
            startEngine();
            waitForStreamingRunning("dameng", "dameng");
            final long start = miner.currentLsn();
            checkpoint(executeNonTableStatements(List.of("DROP SCHEMA " + liveSchema + " CASCADE"), miner, true));
            assertThat(java.nio.file.Files.readAllLines(directory.resolve("history.dat"))).isEqualTo(history);
            executeNonTableStatements(List.of("DROP SCHEMA " + offlineSchema + " CASCADE"), miner, false);
            startEngine();
            final var replayed = next("c", markerId);
            assertMarker(replayed, markerId);
            ((ObjectNode) attempts.get(1)).put("cdcMarkerVerified", true);
            checkpoint(replayed);
            final var recovered = recoverTables(replayed);
            for (final var victim : victims) {
                assertThat(miner.getAllTableIds(null)).doesNotContain(victim);
                assertThat(recovered.forTable(victim)).as("ignored cascade deliberately retains the stale historical definition")
                        .isEqualTo(before.forTable(victim));
            }
            assertThat(java.nio.file.Files.readAllLines(directory.resolve("history.dat"))).isEqualTo(history);

            // Acknowledged cascades do not replay or repair themselves when the opt-in is disabled.
            config = config.edit().with(DamengConnectorConfig.CASCADE_DROP_IGNORE, false).build();
            startEngine();
            waitForStreamingRunning("dameng", "dameng");
            execute("UPDATE " + table + " SET LABEL='after-cascade-recovery' WHERE ID=" + markerId);
            final var update = next("u", markerId);
            assertThat(after(update).schema()).isEqualTo(rowSchema);
            assertThat(after(update).getString("LABEL")).isEqualTo("after-cascade-recovery");
            assertThat(((Struct) update.value()).getStruct("before").getString("LABEL")).isEqualTo("marker-" + markerId);
            checkpoint(update);
            assertThat(java.nio.file.Files.readAllLines(directory.resolve("history.dat"))).isEqualTo(history);

            final long end = miner.currentLsn();
            Awaitility.await("cascade DDL archived").atMost(Duration.ofSeconds(90)).pollInterval(Duration.ofMillis(200))
                    .until(() -> miner.latestArchivedLsn() >= end);
            miner.startLogMining(start, end, false);
            try {
                final List<String> observed = miner.queryAndMap("SELECT SQL_REDO FROM V$LOGMNR_CONTENTS WHERE SCN > " + start
                        + " AND SCN <= " + end + " AND OPERATION_CODE=5 ORDER BY SCN", rows -> {
                            final List<String> result = new ArrayList<>();
                            while (rows.next()) {
                                final var sql = rows.getString(1);
                                assertThatThrownBy(() -> miner.createDdlParser().analyze(sql, schema)).isInstanceOf(CascadeDropException.class);
                                result.add(sql);
                            }
                            return result;
                        });
                assertThat(observed).containsExactly("DROP SCHEMA " + liveSchema + " CASCADE", "DROP SCHEMA " + offlineSchema + " CASCADE");
                final var redo = evidence.putArray("ddlRedo");
                observed.forEach(redo::add);
            }
            finally {
                miner.endLogMining();
            }
        }
        assertThat(failure.get()).isNull();
        evidence.put("staleHistoryVerified", true);
        evidence.put("restartWithOptInDisabledVerified", true);
        evidence.put("validationPassed", true);
        writeEvidence();
    }

    private Tables recoverTables(SourceRecord record) throws Exception {
        final var connectorConfig = new DamengConnectorConfig(config);
        try (var recovered = new DamengDatabaseSchema(connectorConfig, new DamengValueConverters(connectorConfig), connectorConfig.schemaNameAdjuster(),
                connectorConfig.getTopicNamingStrategy(CommonConnectorConfig.TOPIC_NAMING_STRATEGY), new CustomConverterRegistry(List.of()),
                new DamengTaskContext(config, connectorConfig))) {
            recovered.recover(Offsets.of(new DamengPartition(connectorConfig.getLogicalName()),
                    new DamengOffsetContext.Loader(connectorConfig).load(record.sourceOffset())));
            return recovered.getTables().clone();
        }
    }

    private long prepareRecycledTable(String name, String indexName, DamengConnection miner) throws Exception {
        final var qualified = schema + "." + name;
        cleanup.add("DROP TABLE " + qualified);
        execute("CREATE TABLE " + qualified + "(ID INT,LABEL VARCHAR(30)) TABLESPACE MAIN");
        final long objectId = miner.getTableObjectId(new TableId(null, schema, name));
        if (indexName != null) {
            execute("CREATE INDEX " + schema + "." + indexName + " ON " + qualified + "(LABEL)");
        }
        execute("DROP TABLE " + qualified);
        assertThat(recycledObjects(name, "TABLE")).isEqualTo(1);
        if (indexName != null) {
            assertThat(recycledObjects(indexName, "INDEX")).isEqualTo(1);
        }
        return objectId;
    }

    private SourceRecord executePurgeLifecycle(String prefix, String recycledTable, String recycledIndex, DamengConnection miner,
                                               long objectId, boolean online)
            throws Exception {
        final List<String> commands = List.of(
                "PURGE INDEX " + schema + "." + recycledIndex,
                "PURGE TABLE " + schema + "." + recycledTable,
                "PURGE RECYCLEBIN", "PURGE DBA_RECYCLEBIN",
                "PURGE TABLESPACE MAIN", "PURGE TABLESPACE MAIN USER " + DamengTestDatabase.user());
        final var tableId = TableId.parse(table, false);
        SourceRecord last = null;
        for (int position = 0; position < commands.size(); position++) {
            final var purgedName = position == 0 ? recycledIndex : position == 1 ? recycledTable : prefix + "_BIN_" + position;
            if (position >= 2) {
                prepareRecycledTable(purgedName, null, miner);
            }
            last = executeNonTableStatements(List.of(commands.get(position)), miner, online);
            assertThat(recycledObjects(purgedName, position == 0 ? "INDEX" : "TABLE")).isZero();
            assertThat(miner.getTableObjectId(tableId)).isEqualTo(objectId);
        }
        return last;
    }

    private long recycledObjects(String name, String type) throws SQLException {
        try (var statement = jdbc.prepareStatement("SELECT COUNT(*) FROM USER_RECYCLEBIN WHERE ORIGINAL_NAME=? AND TYPE=?")) {
            statement.setString(1, name);
            statement.setString(2, type);
            try (var rows = statement.executeQuery()) {
                assertThat(rows.next()).isTrue();
                return rows.getLong(1);
            }
        }
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
