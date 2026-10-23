/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import io.debezium.config.Configuration;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;
import io.debezium.embedded.util.MetricsHelper;
import io.debezium.junit.logging.LogInterceptor;
import io.debezium.storage.file.history.FileSchemaHistory;

/**
 * Runs a complete Debezium engine against a disposable DM instance or an explicitly configured
 * test database. Each test owns only its randomly named tables and leaves external instances running.
 */
class DamengConnectorIT extends AbstractAsyncEngineConnectorTest {
    private static final String USER = System.getProperty("dameng.user", "SYSDBA");
    private static final String PASSWORD = System.getProperty("dameng.password", "SYSDBA001");
    private static final String SCHEMA = System.getProperty("dameng.schema", "SYSDBA");
    private static String jdbcUrl;
    private static String hostname;
    private static int port;

    @TempDir
    Path temporaryDirectory;

    private Connection connection;
    private String table;
    private Configuration config;
    private final AtomicReference<Throwable> connectorFailure = new AtomicReference<>();

    @BeforeAll
    static void startDatabase() throws Exception {
        DamengTestDatabase.start();
        jdbcUrl = DamengTestDatabase.jdbcUrl();
        hostname = DamengTestDatabase.hostname();
        port = DamengTestDatabase.port();
    }

    @AfterAll
    static void stopDatabase() {
        DamengTestDatabase.stop();
    }

    @BeforeEach
    void before() throws Exception {
        connection = DriverManager.getConnection(jdbcUrl, USER, PASSWORD);
        connection.setAutoCommit(false);
        table = "DBZ_IT_" + UUID.randomUUID().toString().replace("-", "").substring(0, 12).toUpperCase();
        execute("CREATE TABLE " + tableName() + " (ID INT PRIMARY KEY, NAME VARCHAR(200), AMOUNT DECIMAL(20,6))");
        execute("INSERT INTO " + tableName() + " VALUES(1,'snapshot',1.250000)");
        connection.commit();
        config = Configuration.create()
                .with("topic.prefix", "dameng")
                .with("database.hostname", hostname)
                .with("database.port", port)
                .with("database.user", USER)
                .with("database.password", PASSWORD)
                .with("database.dbname", System.getProperty("dameng.database", "DAMENG"))
                .with("table.include.list", SCHEMA + "\\." + table)
                .with("schema.history.internal", FileSchemaHistory.class.getName())
                .with("schema.history.internal.file.filename", temporaryDirectory.resolve("schema-history.dat").toString())
                .with("snapshot.mode", "initial")
                .with("poll.interval.ms", 100)
                .with("include.schema.changes", false)
                .with("tombstones.on.delete", false)
                .build();
    }

    @AfterEach
    void after() throws Exception {
        if (connection != null) {
            connection.rollback();
        }
        stopConnector();
        if (connection != null) {
            try {
                execute("DROP TABLE " + tableName());
                connection.commit();
            }
            finally {
                connection.close();
            }
        }
    }

    @Test
    void shouldLogJdbcDatabaseVersionOnStartup() throws Exception {
        final var version = DamengDatabaseVersion.read(connection.getMetaData());
        final var logs = LogInterceptor.forClass(DamengConnectorTask.class);
        try {
            startAndVerifySnapshot();
            assertThat(logs.containsMessage("Connected to " + version.productName() + " database version " + version.productVersion())).isTrue();
            assertThat(logs.containsMessage("matches tested JDBC version: " + version.isTested())).isTrue();
            assertThat(logs.containsWarnMessage("has not been tested")).isEqualTo(!version.isTested());
            assertThat(logs.containsMessage("Dameng COMPATIBLE_MODE=0 selected compatibility profile native-v1")).isTrue();
        }
        finally {
            logs.stop();
        }
    }

    @Test
    void shouldSnapshotThenStreamCommittedCrud() throws Exception {
        startAndVerifySnapshot();
        execute("INSERT INTO " + tableName() + " VALUES(2,'before',2.500000)");
        connection.commit();
        final var inserted = nextDataRecord();
        assertThat(envelope(inserted).getString("op")).isEqualTo("c");
        assertThat(after(inserted).get("ID")).isEqualTo(2);
        assertThat(after(inserted).getString("NAME")).isEqualTo("before");

        execute("UPDATE " + tableName() + " SET NAME='after' WHERE ID=2");
        connection.commit();
        final var updated = nextDataRecord();
        assertThat(envelope(updated).getString("op")).isEqualTo("u");
        assertThat(before(updated).getString("NAME")).isEqualTo("before");
        assertThat(after(updated).getString("NAME")).isEqualTo("after");
        assertThat(after(updated).get("AMOUNT")).isEqualTo(before(updated).get("AMOUNT"));

        execute("DELETE FROM " + tableName() + " WHERE ID=2");
        connection.commit();
        final var deleted = nextDataRecord();
        assertThat(envelope(deleted).getString("op")).isEqualTo("d");
        assertThat(before(deleted).getString("NAME")).isEqualTo("after");
        assertThat(envelope(deleted).get("after")).isNull();
    }

    @Test
    void shouldPublishInitialSnapshotSchemaChangeWithConfiguredDatabaseName() throws Exception {
        config = config.edit()
                .with("database.dbname", "DM_TEST")
                .with("include.schema.changes", true)
                .build();

        startEngine();
        assertThat(waitForAvailableRecords(60, TimeUnit.SECONDS)).isTrue();
        assertConnectorHealthy();
        final var schemaChange = consumeRecord();
        assertThat(schemaChange.topic()).isEqualTo("dameng");
        assertThat(((Struct) schemaChange.key()).getString("databaseName")).isEqualTo("DM_TEST");
        final var value = (Struct) schemaChange.value();
        assertThat(value.getString("databaseName")).isEqualTo("DM_TEST");
        assertThat(value.getString("schemaName")).isEqualTo(SCHEMA);
        assertThat(value.getStruct("source").getString("db")).isEqualTo("DM_TEST");

        final var snapshot = nextDataRecord();
        assertThat(envelope(snapshot).getString("op")).isEqualTo("r");
        assertThat(after(snapshot).getInt32("ID")).isEqualTo(1);
    }

    @Test
    void shouldStopAfterAnInitialOnlySnapshot() throws Exception {
        config = config.edit().with("snapshot.mode", "initial_only").build();
        startAndVerifySnapshot();
        waitForSnapshotToBeCompleted("dameng", "dameng");

        execute("INSERT INTO " + tableName() + " VALUES(2,'after snapshot',2)");
        connection.commit();
        assertNoDataRecords(Duration.ofSeconds(3));
    }

    @Test
    void shouldSnapshotAnEmptyTableAndCaptureItsFirstInsert() throws Exception {
        execute("DELETE FROM " + tableName());
        connection.commit();
        startEngine();
        waitForStreamingRunning("dameng", "dameng");
        assertNoDataRecords(Duration.ofSeconds(1));

        execute("INSERT INTO " + tableName() + " VALUES(2,'first row',2)");
        connection.commit();
        final var record = nextDataRecord();
        assertThat(envelope(record).getString("op")).isEqualTo("c");
        assertThat(after(record).get("ID")).isEqualTo(2);
        assertThat(((Struct) record.key()).get("ID")).isEqualTo(2);
    }

    @Test
    void shouldWaitForAnExistingWriterBeforeSnapshotAndReleaseSharedLocks() throws Exception {
        execute("INSERT INTO " + tableName() + " VALUES(2,'committed before snapshot position',2)");
        startEngine();
        Awaitility.await("snapshot starts while the existing writer is uncommitted")
                .atMost(Duration.ofSeconds(30)).ignoreExceptions()
                .until(() -> MetricsHelper.<Boolean> getSnapshotMetric("dameng", "dameng", "SnapshotRunning"));
        assertNoDataRecords(Duration.ofSeconds(1));
        connection.commit();

        final Set<Integer> snapshotted = new HashSet<>();
        for (int index = 0; index < 2; index++) {
            final var record = nextDataRecord();
            assertThat(envelope(record).getString("op")).isEqualTo("r");
            snapshotted.add(after(record).getInt32("ID"));
        }
        assertThat(snapshotted).containsExactlyInAnyOrder(1, 2);
        waitForStreamingRunning("dameng", "dameng");

        try (var statement = connection.createStatement()) {
            statement.setQueryTimeout(5);
            statement.executeUpdate("UPDATE " + tableName() + " SET NAME='after shared lock release' WHERE ID=2");
        }
        connection.commit();
        final var changed = nextDataRecord();
        assertThat(envelope(changed).getString("op")).isEqualTo("u");
        assertThat(before(changed).getString("NAME")).isEqualTo("committed before snapshot position");
        assertThat(after(changed).getString("NAME")).isEqualTo("after shared lock release");
        assertNoDataRecords(Duration.ofSeconds(1));
    }

    @Test
    void shouldReleaseSharedLocksWhenSnapshotQueryFails() throws Exception {
        config = config.edit()
                .with("snapshot.select.statement.overrides", SCHEMA + "." + table)
                .with("snapshot.select.statement.overrides." + SCHEMA + "." + table,
                        "SELECT MISSING_SNAPSHOT_COLUMN FROM " + tableName())
                .build();
        start(DamengConnector.class, config, (success, message, error) -> {
            if (!success) {
                connectorFailure.set(error != null ? error : new IllegalStateException(message));
            }
        }, null);
        Awaitility.await("invalid snapshot query fails the connector")
                .atMost(Duration.ofSeconds(30)).until(() -> connectorFailure.get() != null);

        try (var statement = connection.createStatement()) {
            statement.setQueryTimeout(5);
            statement.executeUpdate("UPDATE " + tableName() + " SET NAME='write after snapshot failure' WHERE ID=1");
        }
        connection.commit();
        try (var statement = connection.createStatement();
                var rows = statement.executeQuery("SELECT NAME FROM " + tableName() + " WHERE ID=1")) {
            assertThat(rows.next()).isTrue();
            assertThat(rows.getString(1)).isEqualTo("write after snapshot failure");
        }
    }

    @Test
    void shouldIncludeATableCreatedWhileSnapshotWaitsForAnExistingWriter() throws Exception {
        config = config.edit().with("table.include.list", SCHEMA + "\\." + table + "(_RACE)?").build();
        final String newTable = "\"" + SCHEMA + "\".\"" + table + "_RACE\"";
        execute("INSERT INTO " + tableName() + " VALUES(2,'writer before new table',2)");
        startEngine();
        Awaitility.await("snapshot waits for a writer before choosing its position")
                .atMost(Duration.ofSeconds(30)).ignoreExceptions()
                .until(() -> MetricsHelper.<Boolean> getSnapshotMetric("dameng", "dameng", "SnapshotRunning"));
        assertNoDataRecords(Duration.ofSeconds(1));
        try (Connection second = DriverManager.getConnection(jdbcUrl, USER, PASSWORD);
                var statement = second.createStatement()) {
            statement.execute("CREATE TABLE " + newTable + " (ID INT PRIMARY KEY, LABEL VARCHAR(100))");
            try {
                statement.execute("INSERT INTO " + newTable + " VALUES(3,'created during preparation')");
                connection.commit();
                final Set<Integer> ids = new HashSet<>();
                for (int index = 0; index < 3; index++) {
                    final var record = nextDataRecord();
                    assertThat(envelope(record).getString("op")).isEqualTo("r");
                    final int id = after(record).getInt32("ID");
                    ids.add(id);
                    if (id == 3) {
                        assertThat(record.topic()).endsWith("." + table + "_RACE");
                        assertThat(after(record).getString("LABEL")).isEqualTo("created during preparation");
                    }
                }
                assertThat(ids).containsExactlyInAnyOrder(1, 2, 3);
                waitForStreamingRunning("dameng", "dameng");
                statement.execute("INSERT INTO " + newTable + " VALUES(4,'after snapshot')");
                final var inserted = nextDataRecord();
                assertThat(envelope(inserted).getString("op")).isEqualTo("c");
                assertThat(after(inserted).get("ID")).isEqualTo(4);
                assertNoDataRecords(Duration.ofSeconds(1));
            }
            finally {
                connection.rollback();
                stopConnector();
                statement.execute("DROP TABLE " + newTable);
            }
        }
    }

    @Test
    void shouldPreserveSnapshotKeySchemaAndColumnDefaults() throws Exception {
        execute("ALTER TABLE " + tableName() + " MODIFY NAME VARCHAR(200) DEFAULT 'default name' NOT NULL");
        execute("ALTER TABLE " + tableName() + " MODIFY AMOUNT DECIMAL(20,6) DEFAULT 1.250000 NOT NULL");
        execute("DELETE FROM " + tableName());
        execute("INSERT INTO " + tableName() + " (ID) VALUES(1)");
        connection.commit();
        startEngine();
        final var snapshot = nextDataRecord();
        assertThat(envelope(snapshot).getString("op")).isEqualTo("r");
        final var key = (Struct) snapshot.key();
        assertThat(key.schema().fields()).extracting(org.apache.kafka.connect.data.Field::name).containsExactly("ID");
        assertThat(key.schema().field("ID").schema().type()).isEqualTo(Schema.Type.INT32);
        assertThat(key.schema().field("ID").schema().isOptional()).isFalse();
        assertThat(key.getInt32("ID")).isEqualTo(1);
        final var value = after(snapshot);
        assertThat(value.getString("NAME")).isEqualTo("default name");
        assertThat(value.schema().field("NAME").schema().defaultValue()).isEqualTo("default name");
        assertThat(value.get("AMOUNT")).isEqualTo(new java.math.BigDecimal("1.250000"));
        final var amountSchema = value.schema().field("AMOUNT").schema();
        assertThat(amountSchema.name()).isEqualTo(Decimal.LOGICAL_NAME);
        assertThat(amountSchema.parameters()).containsEntry(Decimal.SCALE_FIELD, "6");
        assertThat(amountSchema.defaultValue()).isEqualTo(new java.math.BigDecimal("1.250000"));

        execute("INSERT INTO " + tableName() + " (ID) VALUES(2)");
        connection.commit();
        final var streamed = after(nextDataRecord());
        assertThat(streamed.getString("NAME")).isEqualTo("default name");
        assertThat(streamed.get("AMOUNT")).isEqualTo(new java.math.BigDecimal("1.250000"));
        assertThat(streamed.schema()).isEqualTo(value.schema());
    }

    @Test
    void shouldFilterSnapshotRowsUsingSelectOverrideAndStillStreamAllChanges() throws Exception {
        execute("INSERT INTO " + tableName() + " VALUES(2,'selected by snapshot query',2)");
        connection.commit();
        config = config.edit()
                .with("snapshot.select.statement.overrides", SCHEMA + "." + table)
                .with("snapshot.select.statement.overrides." + SCHEMA + "." + table,
                        "SELECT ID,NAME,AMOUNT FROM " + tableName() + " WHERE ID=2")
                .build();
        startEngine();
        final var snapshot = nextDataRecord();
        assertThat(envelope(snapshot).getString("op")).isEqualTo("r");
        assertThat(after(snapshot).get("ID")).isEqualTo(2);
        waitForStreamingRunning("dameng", "dameng");
        assertNoDataRecords(Duration.ofSeconds(1));

        execute("UPDATE " + tableName() + " SET NAME='streamed outside snapshot predicate' WHERE ID=1");
        connection.commit();
        final var updated = nextDataRecord();
        assertThat(envelope(updated).getString("op")).isEqualTo("u");
        assertThat(after(updated).get("ID")).isEqualTo(1);
        assertThat(before(updated).getString("NAME")).isEqualTo("snapshot");
        assertThat(after(updated).getString("NAME")).isEqualTo("streamed outside snapshot predicate");
    }

    @ParameterizedTest
    @ValueSource(strings = { "", "no_data", "never" })
    void shouldStreamWithoutPublishingExistingRows(String snapshotMode) throws Exception {
        config = snapshotMode.isEmpty() ? config.edit().without("snapshot.mode").build() : config.edit().with("snapshot.mode", snapshotMode).build();
        startEngine();
        waitForStreamingRunning("dameng", "dameng");
        assertNoDataRecords(Duration.ofSeconds(1));

        execute("INSERT INTO " + tableName() + " VALUES(2,'stream only',2)");
        connection.commit();
        final var record = nextDataRecord();
        assertThat(envelope(record).getString("op")).isEqualTo("c");
        assertThat(after(record).get("ID")).isEqualTo(2);
        assertThat(after(record).getString("NAME")).isEqualTo("stream only");
        assertNoDataRecords(Duration.ofSeconds(1));
    }

    @Test
    void shouldNotPublishUncommittedOrRolledBackChanges() throws Exception {
        startAndVerifySnapshot();
        execute("INSERT INTO " + tableName() + " VALUES(2,'uncommitted',2)");
        assertNoDataRecords(Duration.ofSeconds(3));
        connection.rollback();
        execute("INSERT INTO " + tableName() + " VALUES(3,'committed',3)");
        connection.commit();

        final var committed = nextDataRecord();
        assertThat(after(committed).get("ID")).isEqualTo(3);
        assertNoDataRecords(Duration.ofSeconds(2));
    }

    @ParameterizedTest
    @ValueSource(strings = { "unused", "partial", "statement-failure" })
    void shouldPreserveOnlyEffectiveChangesWithSavepointsAndStatementFailure(String scenario) throws Exception {
        startAndVerifySnapshot();
        execute("INSERT INTO " + tableName() + " VALUES(2,'keep',2)");
        if (scenario.equals("statement-failure")) {
            assertThatThrownBy(() -> execute("INSERT INTO " + tableName()
                    + " SELECT 3,'failed',3 FROM DUAL UNION ALL SELECT 1,'duplicate',1 FROM DUAL"))
                    .isInstanceOf(java.sql.SQLException.class);
        }
        else {
            final var savepoint = connection.setSavepoint("DBZ_SAVEPOINT");
            execute("INSERT INTO " + tableName() + " VALUES(3,'discard',3)");
            if (scenario.equals("partial")) {
                connection.rollback(savepoint);
            }
        }
        connection.commit();
        assertThat(after(nextDataRecord()).getInt32("ID")).isEqualTo(2);
        if (scenario.equals("unused")) {
            assertThat(after(nextDataRecord()).getInt32("ID")).isEqualTo(3);
        }
        assertNoDataRecords(Duration.ofSeconds(1));
    }

    @Test
    void shouldRollBackToAnEarlierSavepointAndContinueAnEmptyTransactionAcrossWindows() throws Exception {
        config = config.edit().with("log.mining.batch.size", 20).with("log.mining.buffer.memory.bytes", 1024)
                .with("log.mining.window.strategy", "fixed")
                .with("log.mining.buffer.spill.path", temporaryDirectory.resolve("spill").toString()).build();
        startAndVerifySnapshot();
        final var first = connection.setSavepoint("FIRST_POINT");
        execute("INSERT INTO " + tableName() + " VALUES(2,'discard before second point',2)");
        connection.setSavepoint("SECOND_POINT");
        execute("INSERT INTO " + tableName() + " VALUES(3,'discard after second point',3)");
        try (var other = DriverManager.getConnection(jdbcUrl, USER, PASSWORD); var statement = other.createStatement()) {
            statement.execute("INSERT INTO " + tableName() + " VALUES(10,'checkpoint before rollback',0)");
            assertThat(after(nextDataRecord()).getInt32("ID")).isEqualTo(10);
            connection.rollback(first);
            // A committed checkpoint forces scanning beyond the rollback while
            // the original, now empty, transaction is still open.
            statement.execute("INSERT INTO " + tableName() + " VALUES(11,'checkpoint after rollback',0)");
            assertThat(after(nextDataRecord()).getInt32("ID")).isEqualTo(11);
        }
        execute("INSERT INTO " + tableName() + " VALUES(4,'after rollback',4)");
        connection.commit();
        assertThat(after(nextDataRecord()).getInt32("ID")).isEqualTo(4);
        assertNoDataRecords(Duration.ofSeconds(1));
    }

    @Test
    void shouldKeepAnOpenTransactionAcrossPollingWindows() throws Exception {
        startAndVerifySnapshot();
        execute("INSERT INTO " + tableName() + " VALUES(2,'long transaction',2)");
        assertNoDataRecords(Duration.ofSeconds(3));
        try (Connection second = DriverManager.getConnection(jdbcUrl, USER, PASSWORD)) {
            second.setAutoCommit(false);
            try (var statement = second.createStatement()) {
                statement.execute("INSERT INTO " + tableName() + " VALUES(3,'other transaction',3)");
            }
            second.commit();
        }
        assertThat(after(nextDataRecord()).get("ID")).isEqualTo(3);
        connection.commit();
        assertThat(after(nextDataRecord()).get("ID")).isEqualTo(2);
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void shouldDiscardSavepointChangesAlreadyReadInEarlierWindows(boolean restart) throws Exception {
        config = config.edit().with("log.mining.batch.size", 20).with("log.mining.buffer.memory.bytes", 1024)
                .with("log.mining.window.strategy", "fixed")
                .with("log.mining.buffer.spill.path", temporaryDirectory.resolve("spill").toString()).build();
        startAndVerifySnapshot();
        execute("INSERT INTO " + tableName() + " VALUES(2,'keep',2)");
        final var savepoint = connection.setSavepoint("CROSS_WINDOW");
        execute("INSERT INTO " + tableName() + " VALUES(3,'discard',3)");
        execute("UPDATE " + tableName() + " SET NAME='discard' WHERE ID=2");
        // Independent committed rows prove that scanning has passed the provisional
        // images before rollback; sleeps alone would not establish that boundary.
        try (var other = DriverManager.getConnection(jdbcUrl, USER, PASSWORD); var statement = other.createStatement()) {
            for (int id = 10; id <= 11; id++) {
                statement.execute("INSERT INTO " + tableName() + " VALUES(" + id + ",'checkpoint',0)");
                final var checkpoint = nextDataRecord();
                assertThat(after(checkpoint).getInt32("ID")).isEqualTo(id);
                if (restart && id == 11) {
                    stopConnector();
                    storeOffsets(config, Map.of(checkpoint.sourcePartition(), checkpoint.sourceOffset()));
                    consumedLines.clear();
                    startEngine();
                }
            }
        }
        connection.rollback(savepoint);
        execute("INSERT INTO " + tableName() + " VALUES(4,'after rollback',4)");
        connection.commit();

        final var kept = nextDataRecord();
        assertThat(after(kept).getInt32("ID")).isEqualTo(2);
        assertThat(after(kept).getString("NAME")).isEqualTo("keep");
        final var added = nextDataRecord();
        assertThat(after(added).getInt32("ID")).isEqualTo(4);
        assertThat(after(added).getString("NAME")).isEqualTo("after rollback");
        assertNoDataRecords(Duration.ofSeconds(1));
    }

    @Test
    void shouldResumeFromPersistedOffsetWithoutRepeatingSnapshot() throws Exception {
        startAndVerifySnapshot();
        execute("INSERT INTO " + tableName() + " VALUES(2,'before restart',2)");
        connection.commit();
        assertThat(after(nextDataRecord()).get("ID")).isEqualTo(2);
        stopConnector();

        execute("INSERT INTO " + tableName() + " VALUES(3,'while stopped',3)");
        connection.commit();
        startEngine();
        final var restarted = nextDataRecord();
        assertThat(envelope(restarted).getString("op")).isEqualTo("c");
        assertThat(after(restarted).get("ID")).isEqualTo(3);
        assertNoDataRecords(Duration.ofSeconds(2));
    }

    @Test
    void shouldRecoverOpenTransactionAcrossRestartAfterAnotherCommit() throws Exception {
        startAndVerifySnapshot();
        execute("INSERT INTO " + tableName() + " VALUES(2,'open transaction',2)");
        try (Connection second = DriverManager.getConnection(jdbcUrl, USER, PASSWORD)) {
            second.setAutoCommit(false);
            try (var statement = second.createStatement()) {
                statement.execute("INSERT INTO " + tableName() + " VALUES(3,'committed later',3)");
            }
            second.commit();
        }
        assertThat(after(nextDataRecord()).get("ID")).isEqualTo(3);
        stopConnector();
        startEngine();
        connection.commit();

        final var recovered = nextDataRecord();
        assertThat(envelope(recovered).getString("op")).isEqualTo("c");
        assertThat(after(recovered).get("ID")).isEqualTo(2);
        assertNoDataRecords(Duration.ofSeconds(2));
    }

    @Test
    void shouldDiscardOpenTransactionRolledBackWhileStopped() throws Exception {
        startAndVerifySnapshot();
        execute("INSERT INTO " + tableName() + " VALUES(2,'rollback while stopped',2)");
        assertNoDataRecords(Duration.ofSeconds(2));
        stopConnector();
        connection.rollback();
        execute("INSERT INTO " + tableName() + " VALUES(3,'commit while stopped',3)");
        connection.commit();

        startEngine();
        final var record = nextDataRecord();
        assertThat(envelope(record).getString("op")).isEqualTo("c");
        assertThat(after(record).get("ID")).isEqualTo(3);
        assertNoDataRecords(Duration.ofSeconds(2));
    }

    @Test
    void shouldPreserveTheOrderOfChangesWithinOneTransaction() throws Exception {
        startAndVerifySnapshot();
        execute("INSERT INTO " + tableName() + " VALUES(2,'first',2)");
        execute("UPDATE " + tableName() + " SET NAME='second' WHERE ID=2");
        execute("DELETE FROM " + tableName() + " WHERE ID=2");
        connection.commit();

        final var inserted = nextDataRecord();
        final var updated = nextDataRecord();
        final var deleted = nextDataRecord();
        assertThat(envelope(inserted).getString("op")).isEqualTo("c");
        assertThat(after(inserted).getString("NAME")).isEqualTo("first");
        assertThat(envelope(updated).getString("op")).isEqualTo("u");
        assertThat(before(updated).getString("NAME")).isEqualTo("first");
        assertThat(after(updated).getString("NAME")).isEqualTo("second");
        assertThat(envelope(deleted).getString("op")).isEqualTo("d");
        assertThat(before(deleted).getString("NAME")).isEqualTo("second");
        assertThat(inserted.sourceOffset().get("commit_lsn")).isEqualTo(updated.sourceOffset().get("commit_lsn"))
                .isEqualTo(deleted.sourceOffset().get("commit_lsn"));
        assertNoDataRecords(Duration.ofSeconds(2));
    }

    @Test
    void shouldCaptureJdbcBatchInsertsAndSetBasedUpdatesAndDeletes() throws Exception {
        startAndVerifySnapshot();
        try (var statement = connection.prepareStatement("INSERT INTO " + tableName() + " VALUES(?,?,?)")) {
            for (int id = 2; id <= 6; id++) {
                statement.setInt(1, id);
                statement.setString(2, "batch " + id);
                statement.setBigDecimal(3, new java.math.BigDecimal(id + ".250000"));
                statement.addBatch();
            }
            assertThat(statement.executeBatch()).hasSize(5);
        }
        connection.commit();
        final Set<Integer> inserted = new HashSet<>();
        for (int index = 0; index < 5; index++) {
            final var record = nextDataRecord();
            assertThat(envelope(record).getString("op")).isEqualTo("c");
            final int id = after(record).getInt32("ID");
            inserted.add(id);
            assertThat(after(record).getString("NAME")).isEqualTo("batch " + id);
        }
        assertThat(inserted).containsExactlyInAnyOrder(2, 3, 4, 5, 6);

        execute("UPDATE " + tableName() + " SET NAME='set update',AMOUNT=AMOUNT+1 WHERE ID>=2");
        connection.commit();
        final Set<Integer> updated = new HashSet<>();
        for (int index = 0; index < 5; index++) {
            final var record = nextDataRecord();
            assertThat(envelope(record).getString("op")).isEqualTo("u");
            final int id = after(record).getInt32("ID");
            updated.add(id);
            assertThat(before(record).getString("NAME")).isEqualTo("batch " + id);
            assertThat(after(record).getString("NAME")).isEqualTo("set update");
            assertThat(before(record).get("AMOUNT")).isEqualTo(new java.math.BigDecimal(id + ".250000"));
            assertThat(after(record).get("AMOUNT")).isEqualTo(new java.math.BigDecimal((id + 1) + ".250000"));
        }
        assertThat(updated).isEqualTo(inserted);

        execute("DELETE FROM " + tableName() + " WHERE ID>=2");
        connection.commit();
        final Set<Integer> deleted = new HashSet<>();
        for (int index = 0; index < 5; index++) {
            final var record = nextDataRecord();
            assertThat(envelope(record).getString("op")).isEqualTo("d");
            deleted.add(before(record).getInt32("ID"));
            assertThat(before(record).getString("NAME")).isEqualTo("set update");
        }
        assertThat(deleted).isEqualTo(inserted);
        assertNoDataRecords(Duration.ofSeconds(1));
    }

    @Test
    void shouldCaptureNewColumnAndRestoreSchemaAfterRestart() throws Exception {
        startAndVerifySnapshot();
        execute("ALTER TABLE " + tableName() + " ADD EXTRA VARCHAR(100)");
        execute("INSERT INTO " + tableName() + " (ID,NAME,AMOUNT,EXTRA) VALUES(2,'new schema',2,'extra')");
        connection.commit();
        assertThat(after(nextDataRecord()).getString("EXTRA")).isEqualTo("extra");
        stopConnector();
        startEngine();
        execute("INSERT INTO " + tableName() + " (ID,NAME,AMOUNT,EXTRA) VALUES(3,'restored schema',3,'restored')");
        connection.commit();
        assertThat(after(nextDataRecord()).getString("EXTRA")).isEqualTo("restored");
    }

    @Test
    void shouldReplayCommentedDdlAndNonTableStatementsThenRecoverTheirHistoricalSchema() throws Exception {
        final var procedure = "P_" + table;
        final var index = "I_" + table;
        startAndVerifySnapshot();
        stopConnector();
        try {
            execute("// 授予权限\r\nGRANT SELECT ON " + tableName() + " TO PUBLIC;");
            execute("/* 权限不改变表结构 */ REVOKE SELECT ON " + tableName() + " FROM PUBLIC");
            execute("COMMENT ON TABLE " + tableName() + " IS q'[it's; -- /* a comment]'");
            execute("COMMENT ON COLUMN " + tableName() + ".NAME IS 'name; // comment'");
            execute("CREATE INDEX " + index + " ON " + tableName() + "(NAME)");
            execute("DROP INDEX " + index);
            execute("CREATE PROCEDURE " + procedure + " AS BEGIN NULL; BEGIN NULL; END; END;");
            execute("DROP PROCEDURE " + procedure);
            execute("ALTER/*词法边界，正文中的 /* 只是文本*/TABLE " + tableName() + " ADD EXTRA VARCHAR(100) DEFAULT q'[it's;--/*//]'");
            execute("INSERT INTO " + tableName() + " (ID,NAME,AMOUNT) VALUES(2,'historical',2)");
            connection.commit();
            startEngine();
            final var record = after(nextDataRecord());
            assertThat(record.get("ID")).isEqualTo(2);
            assertThat(record.getString("EXTRA")).isEqualTo("it's;--/*//");
            assertThat(record.schema().field("EXTRA").schema().defaultValue()).isEqualTo("it's;--/*//");
            assertNoDataRecords(Duration.ofSeconds(1));
            stopConnector();
            startEngine();
            execute("INSERT INTO " + tableName() + " (ID,NAME,AMOUNT) VALUES(3,'recovered',3)");
            connection.commit();
            final var recovered = after(nextDataRecord());
            assertThat(recovered.get("ID")).isEqualTo(3);
            assertThat(recovered.getString("EXTRA")).isEqualTo("it's;--/*//");
            assertThat(recovered.schema()).isEqualTo(record.schema());
        }
        finally {
            stopConnector();
            connection.rollback();
            execute("DROP PROCEDURE IF EXISTS " + procedure);
            execute("DROP INDEX IF EXISTS " + index);
            connection.commit();
        }
    }

    @Test
    void shouldRefreshPhysicalMappingAcrossLiveDdl() throws Exception {
        config = config.edit().with("table.include.list", SCHEMA + "\\." + table + "(_RENAMED)?").build();
        startAndVerifySnapshot();
        // Populate the physical mapping before each DDL; keep the same streaming source alive.
        execute("INSERT INTO " + tableName() + " VALUES(2,'before DDL',2)");
        connection.commit();
        assertThat(after(nextDataRecord()).getString("NAME")).isEqualTo("before DDL");

        execute("ALTER TABLE " + tableName() + " ADD EXTRA VARCHAR(100)");
        execute("INSERT INTO " + tableName() + " VALUES(3,'added',3,'new column')");
        connection.commit();
        assertThat(after(nextDataRecord()).getString("EXTRA")).isEqualTo("new column");

        execute("ALTER TABLE " + tableName() + " RENAME COLUMN EXTRA TO LABEL");
        execute("UPDATE " + tableName() + " SET LABEL='renamed column' WHERE ID=3");
        connection.commit();
        final var renamedColumn = nextDataRecord();
        assertThat(before(renamedColumn).getString("LABEL")).isEqualTo("new column");
        assertThat(after(renamedColumn).getString("LABEL")).isEqualTo("renamed column");
        assertThat(after(renamedColumn).schema().field("EXTRA")).isNull();

        execute("ALTER TABLE " + tableName() + " RENAME TO \"" + table + "_RENAMED\"");
        table = table + "_RENAMED";
        execute("UPDATE " + tableName() + " SET LABEL='renamed table' WHERE ID=3");
        connection.commit();
        final var renamedTable = nextDataRecord();
        assertThat(renamedTable.topic()).endsWith("." + table);
        assertThat(after(renamedTable).getString("LABEL")).isEqualTo("renamed table");

        execute("DROP TABLE " + tableName());
        execute("CREATE TABLE " + tableName() + " (ID INT PRIMARY KEY, COUNTER BIGINT, LABEL VARCHAR(100))");
        execute("INSERT INTO " + tableName() + " VALUES(4,9223372036854775807,'recreated')");
        connection.commit();
        final var recreated = after(nextDataRecord());
        assertThat(recreated.getInt32("ID")).isEqualTo(4);
        assertThat(recreated.getInt64("COUNTER")).isEqualTo(Long.MAX_VALUE);
        assertThat(recreated.getString("LABEL")).isEqualTo("recreated");
        assertThat(recreated.schema().field("NAME")).isNull();
        assertThat(recreated.schema().field("AMOUNT")).isNull();
        assertNoDataRecords(Duration.ofSeconds(1));
    }

    @Test
    void shouldReplayConsecutiveDdlUsingEachHistoricalSchema() throws Exception {
        startAndVerifySnapshot();
        stopConnector();
        execute("ALTER TABLE " + tableName() + " ADD EXTRA VARCHAR(100)");
        execute("INSERT INTO " + tableName() + " (ID,NAME,AMOUNT,EXTRA) VALUES(2,'added',2,'old name')");
        connection.commit();
        execute("ALTER TABLE " + tableName() + " RENAME COLUMN EXTRA TO LABEL");
        execute("INSERT INTO " + tableName() + " (ID,NAME,AMOUNT,LABEL) VALUES(3,'renamed',3,'new name')");
        connection.commit();
        execute("ALTER TABLE " + tableName() + " DROP COLUMN LABEL");
        execute("INSERT INTO " + tableName() + " VALUES(4,'dropped',4)");
        connection.commit();

        startEngine();
        final var added = after(nextDataRecord());
        assertThat(added.get("ID")).isEqualTo(2);
        assertThat(added.getString("EXTRA")).isEqualTo("old name");
        assertThat(added.schema().field("LABEL")).isNull();
        final var renamed = after(nextDataRecord());
        assertThat(renamed.get("ID")).isEqualTo(3);
        assertThat(renamed.getString("LABEL")).isEqualTo("new name");
        assertThat(renamed.schema().field("EXTRA")).isNull();
        final var dropped = after(nextDataRecord());
        assertThat(dropped.get("ID")).isEqualTo(4);
        assertThat(dropped.schema().field("LABEL")).isNull();
        assertThat(dropped.schema().field("EXTRA")).isNull();
        assertNoDataRecords(Duration.ofSeconds(1));

        stopConnector();
        startEngine();
        execute("INSERT INTO " + tableName() + " VALUES(5,'restored final schema',5)");
        connection.commit();
        final var recovered = after(nextDataRecord());
        assertThat(recovered.get("ID")).isEqualTo(5);
        assertThat(recovered.schema()).isEqualTo(dropped.schema());
    }

    @Test
    void shouldRetainHistoricalOrdinalsAfterDroppingAMiddleColumn() throws Exception {
        startAndVerifySnapshot();
        stopConnector();
        execute("INSERT INTO " + tableName() + " VALUES(2,'original middle column',2.250000)");
        connection.commit();
        execute("ALTER TABLE " + tableName() + " RENAME COLUMN NAME TO LABEL");
        execute("INSERT INTO " + tableName() + " VALUES(3,'renamed middle column',3.250000)");
        connection.commit();
        execute("ALTER TABLE " + tableName() + " DROP COLUMN LABEL");
        execute("ALTER TABLE " + tableName() + " ADD COUNTER BIGINT");
        execute("INSERT INTO " + tableName() + " (ID,AMOUNT,COUNTER) VALUES(4,4.250000,9223372036854775807)");
        connection.commit();

        startEngine();
        final var original = after(nextDataRecord());
        assertThat(original.get("ID")).isEqualTo(2);
        assertThat(original.getString("NAME")).isEqualTo("original middle column");
        assertThat(original.get("AMOUNT")).isEqualTo(new java.math.BigDecimal("2.250000"));
        final var renamed = after(nextDataRecord());
        assertThat(renamed.get("ID")).isEqualTo(3);
        assertThat(renamed.getString("LABEL")).isEqualTo("renamed middle column");
        assertThat(renamed.get("AMOUNT")).isEqualTo(new java.math.BigDecimal("3.250000"));
        final var replaced = after(nextDataRecord());
        assertThat(replaced.get("ID")).isEqualTo(4);
        assertThat(replaced.schema().field("NAME")).isNull();
        assertThat(replaced.schema().field("LABEL")).isNull();
        assertThat(replaced.get("AMOUNT")).isEqualTo(new java.math.BigDecimal("4.250000"));
        assertThat(replaced.get("COUNTER")).isEqualTo(Long.MAX_VALUE);

        execute("UPDATE " + tableName() + " SET COUNTER=-9223372036854775808 WHERE ID=4");
        connection.commit();
        final var updated = nextDataRecord();
        assertThat(before(updated).get("COUNTER")).isEqualTo(Long.MAX_VALUE);
        assertThat(after(updated).get("COUNTER")).isEqualTo(Long.MIN_VALUE);
        assertThat(after(updated).get("AMOUNT")).isEqualTo(new java.math.BigDecimal("4.250000"));
    }

    @Test
    void shouldEmitTruncateAndContinueUsingTheSameSchema() throws Exception {
        config = config.edit().with("skipped.operations", "none").build();
        startAndVerifySnapshot();
        execute("TRUNCATE TABLE " + tableName());
        connection.commit();
        final var truncated = nextDataRecord();
        assertThat(envelope(truncated).getString("op")).isEqualTo("t");
        assertThat(truncated.key()).isNull();
        assertThat(envelope(truncated).get("before")).isNull();
        assertThat(envelope(truncated).get("after")).isNull();

        execute("INSERT INTO " + tableName() + " VALUES(2,'after truncate',2)");
        connection.commit();
        final var inserted = nextDataRecord();
        assertThat(envelope(inserted).getString("op")).isEqualTo("c");
        assertThat(after(inserted).get("ID")).isEqualTo(2);
        assertThat(after(inserted).getString("NAME")).isEqualTo("after truncate");
    }

    @Test
    void shouldSkipTruncateEventsByDefault() throws Exception {
        startAndVerifySnapshot();
        execute("TRUNCATE TABLE " + tableName());
        connection.commit();
        execute("INSERT INTO " + tableName() + " VALUES(2,'after skipped truncate',2)");
        connection.commit();

        final var record = nextDataRecord();
        assertThat(envelope(record).getString("op")).isEqualTo("c");
        assertThat(after(record).get("ID")).isEqualTo(2);
        assertNoDataRecords(Duration.ofSeconds(1));
    }

    @ParameterizedTest
    @ValueSource(strings = { "c", "u", "d" })
    void shouldSkipOnlyTheConfiguredDmlOperation(String skippedOperation) throws Exception {
        config = config.edit().with("skipped.operations", skippedOperation).build();
        startAndVerifySnapshot();
        execute("INSERT INTO " + tableName() + " VALUES(2,'created',2)");
        execute("UPDATE " + tableName() + " SET NAME='updated' WHERE ID=2");
        execute("DELETE FROM " + tableName() + " WHERE ID=2");
        connection.commit();

        final List<String> expected = List.of("c", "u", "d").stream().filter(operation -> !operation.equals(skippedOperation)).toList();
        for (String operation : expected) {
            final var record = nextDataRecord();
            assertThat(envelope(record).getString("op")).isEqualTo(operation);
            assertThat(((Struct) record.key()).get("ID")).isEqualTo(2);
        }
        assertNoDataRecords(Duration.ofSeconds(1));
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void shouldApplyUnchangedMessagePolicyAfterColumnFiltering(boolean skipUnchanged) throws Exception {
        config = config.edit().with("column.exclude.list", SCHEMA + "\\." + table + "\\.AMOUNT")
                .with("skip.messages.without.change", skipUnchanged).build();
        startAndVerifySnapshot();
        execute("UPDATE " + tableName() + " SET AMOUNT=2 WHERE ID=1");
        execute("UPDATE " + tableName() + " SET NAME='visible change' WHERE ID=1");
        connection.commit();

        if (!skipUnchanged) {
            final var unchanged = nextDataRecord();
            assertThat(envelope(unchanged).getString("op")).isEqualTo("u");
            assertThat(before(unchanged)).isEqualTo(after(unchanged));
            assertThat(after(unchanged).getString("NAME")).isEqualTo("snapshot");
        }
        final var changed = nextDataRecord();
        assertThat(envelope(changed).getString("op")).isEqualTo("u");
        assertThat(before(changed).getString("NAME")).isEqualTo("snapshot");
        assertThat(after(changed).getString("NAME")).isEqualTo("visible change");
        assertThat(after(changed).schema().field("AMOUNT")).isNull();
        assertNoDataRecords(Duration.ofSeconds(1));
    }

    @Test
    void shouldCaptureANewTableMatchingTheIncludeFilter() throws Exception {
        config = config.edit().with("table.include.list", SCHEMA + "\\." + table + "(_NEW)?").build();
        startAndVerifySnapshot();
        final String added = "\"" + SCHEMA + "\".\"" + table + "_NEW\"";
        execute("CREATE TABLE " + added + " (ID INT PRIMARY KEY, LABEL VARCHAR(100))");
        try {
            execute("INSERT INTO " + added + " VALUES(2,'new table')");
            connection.commit();
            final var inserted = nextDataRecord();
            assertThat(inserted.topic()).endsWith("." + table + "_NEW");
            assertThat(after(inserted).get("ID")).isEqualTo(2);
            assertThat(after(inserted).getString("LABEL")).isEqualTo("new table");
            stopConnector();
            startEngine();
            execute("UPDATE " + added + " SET LABEL='restored new table' WHERE ID=2");
            connection.commit();
            assertThat(after(nextDataRecord()).getString("LABEL")).isEqualTo("restored new table");
        }
        finally {
            stopConnector();
            execute("DROP TABLE " + added);
            connection.commit();
        }
    }

    @Test
    void shouldRecoverTableRenameAndUseTheNewTopic() throws Exception {
        config = config.edit().with("table.include.list", SCHEMA + "\\." + table + "(_RENAMED)?").build();
        startAndVerifySnapshot();
        stopConnector();
        execute("ALTER TABLE " + tableName() + " RENAME TO \"" + table + "_RENAMED\"");
        table = table + "_RENAMED";
        execute("INSERT INTO " + tableName() + " VALUES(2,'after rename',2)");
        connection.commit();
        startEngine();
        final var inserted = nextDataRecord();
        assertThat(inserted.topic()).endsWith("." + table);
        assertThat(after(inserted).get("ID")).isEqualTo(2);
        assertThat(after(inserted).getString("NAME")).isEqualTo("after rename");
        stopConnector();
        startEngine();
        execute("UPDATE " + tableName() + " SET NAME='rename recovered' WHERE ID=2");
        connection.commit();
        final var updated = nextDataRecord();
        assertThat(updated.topic()).endsWith("." + table);
        assertThat(after(updated).getString("NAME")).isEqualTo("rename recovered");
    }

    @Test
    void shouldRecoverSchemaWhenRenamingOutOfAndBackIntoTheCaptureFilter() throws Exception {
        final String original = table;
        final String checkpoint = '"' + SCHEMA + "\".\"" + original + "_KEEP\"";
        config = config.edit().with("table.include.list", SCHEMA + "\\." + original + "(_KEEP)?")
                .with("schema.history.internal.store.only.captured.tables.ddl", true).build();
        try {
            startAndVerifySnapshot();
            execute("CREATE TABLE " + checkpoint + " (ID INT PRIMARY KEY)");
            execute("INSERT INTO " + checkpoint + " VALUES(1)");
            connection.commit();
            assertThat(nextDataRecord().topic()).endsWith("." + original + "_KEEP");
            execute("ALTER TABLE " + tableName() + " RENAME TO \"" + original + "_EXCLUDED\"");
            table = original + "_EXCLUDED";
            execute("INSERT INTO " + tableName() + " VALUES(2,'excluded',2)");
            execute("INSERT INTO " + checkpoint + " VALUES(2)");
            connection.commit();
            final var renameCheckpoint = nextDataRecord();
            assertThat(renameCheckpoint.topic()).endsWith("." + original + "_KEEP");
            assertThat(after(renameCheckpoint).getInt32("ID")).isEqualTo(2);
            assertThat(renameCheckpoint.sourceOffset().get("mining_transaction_id"))
                    .isEqualTo(envelope(renameCheckpoint).getStruct("source").getString("transaction_id"));
            stopConnector();
            // Persist the acknowledged checkpoint explicitly so at-least-once
            // replay cannot obscure which historical schema is being recovered.
            storeOffsets(config, Map.of(renameCheckpoint.sourcePartition(), renameCheckpoint.sourceOffset()));
            consumedLines.clear();

            startEngine();
            execute("ALTER TABLE " + tableName() + " ADD EXTRA VARCHAR(40)");
            execute("UPDATE " + tableName() + " SET EXTRA='excluded change' WHERE ID=2");
            execute("INSERT INTO " + checkpoint + " VALUES(3)");
            connection.commit();
            final var alterCheckpoint = nextDataRecord();
            assertThat(alterCheckpoint.topic()).endsWith("." + original + "_KEEP");
            assertThat(after(alterCheckpoint).getInt32("ID")).isEqualTo(3);
            assertThat(alterCheckpoint.sourceOffset().get("mining_transaction_id"))
                    .isEqualTo(envelope(alterCheckpoint).getStruct("source").getString("transaction_id"));
            stopConnector();
            storeOffsets(config, Map.of(alterCheckpoint.sourcePartition(), alterCheckpoint.sourceOffset()));
            consumedLines.clear();

            startEngine();
            execute("ALTER TABLE " + tableName() + " RENAME TO \"" + original + "\"");
            table = original;
            execute("INSERT INTO " + tableName() + " VALUES(4,'captured again',4,'historical column')");
            connection.commit();
            final var inserted = nextDataRecord();
            assertThat(inserted.topic()).endsWith("." + original);
            assertThat(after(inserted).get("ID")).isEqualTo(4);
            assertThat(after(inserted).getString("EXTRA")).isEqualTo("historical column");
            assertNoDataRecords(Duration.ofSeconds(1));
        }
        finally {
            stopConnector();
            execute("DROP TABLE " + checkpoint);
            connection.commit();
        }
    }

    @Test
    void shouldDiscardDroppedTableSchemaBeforeRecreatingTheSameName() throws Exception {
        startAndVerifySnapshot();
        stopConnector();
        execute("DROP TABLE " + tableName());
        execute("CREATE TABLE " + tableName() + " (ID INT PRIMARY KEY, COUNTER BIGINT, LABEL VARCHAR(100))");
        execute("INSERT INTO " + tableName() + " VALUES(2,9223372036854775807,'replacement')");
        connection.commit();
        startEngine();
        final var inserted = after(nextDataRecord());
        assertThat(inserted.get("ID")).isEqualTo(2);
        assertThat(inserted.get("COUNTER")).isEqualTo(Long.MAX_VALUE);
        assertThat(inserted.getString("LABEL")).isEqualTo("replacement");
        assertThat(inserted.schema().field("NAME")).isNull();
        assertThat(inserted.schema().field("AMOUNT")).isNull();
    }

    @Test
    void shouldPublishTransactionBoundariesAndPerTableCounts() throws Exception {
        config = config.edit().with("snapshot.mode", "no_data").with("provide.transaction.metadata", true).build();
        startEngine();
        waitForStreamingRunning("dameng", "dameng");
        execute("INSERT INTO " + tableName() + " VALUES(2,'first',2)");
        execute("UPDATE " + tableName() + " SET NAME='second' WHERE ID=2");
        execute("INSERT INTO " + tableName() + " VALUES(3,'third',3)");
        connection.commit();

        final var begin = nextTransactionRecord();
        final String transactionId = assertBeginTransaction(begin);
        assertThat(begin.topic()).isEqualTo("dameng.transaction");
        final var first = nextDataRecord();
        final var second = nextDataRecord();
        final var third = nextDataRecord();
        assertRecordTransactionMetadata(first, transactionId, 1, 1);
        assertRecordTransactionMetadata(second, transactionId, 2, 2);
        assertRecordTransactionMetadata(third, transactionId, 3, 3);
        assertThat(envelope(first).getString("op")).isEqualTo("c");
        assertThat(envelope(second).getString("op")).isEqualTo("u");
        assertThat(envelope(third).getString("op")).isEqualTo("c");
        assertEndTransaction(nextTransactionRecord(), transactionId, 3, Map.of(SCHEMA + "." + table, 3));
        assertNoDataRecords(Duration.ofSeconds(1));
    }

    @Test
    void shouldCaptureRedoSqlLongerThanFourThousandCharacters() throws Exception {
        execute("ALTER TABLE " + tableName() + " ADD FIRST_TEXT VARCHAR(1500)");
        execute("ALTER TABLE " + tableName() + " ADD SECOND_TEXT VARCHAR(1500)");
        connection.commit();
        startAndVerifySnapshot();
        // Escaped quotes exceed the SQL_REDO fragment length while the stored row
        // remains below the DM image's per-row page limit.
        final String first = "'".repeat(1100) + "tail";
        final String second = "'".repeat(1100) + ", AND 'quoted'";
        try (var statement = connection.prepareStatement("INSERT INTO " + tableName() + " VALUES(2,'long SQL',2,?,?)")) {
            statement.setString(1, first);
            statement.setString(2, second);
            statement.executeUpdate();
        }
        connection.commit();
        final var inserted = after(nextDataRecord());
        assertThat(inserted.getString("FIRST_TEXT")).isEqualTo(first);
        assertThat(inserted.getString("SECOND_TEXT")).isEqualTo(second);

        try (var statement = connection.prepareStatement("UPDATE " + tableName() + " SET FIRST_TEXT=? WHERE ID=2")) {
            statement.setString(1, second);
            statement.executeUpdate();
        }
        connection.commit();
        final var updated = nextDataRecord();
        assertThat(before(updated).getString("FIRST_TEXT")).isEqualTo(first);
        assertThat(before(updated).getString("SECOND_TEXT")).isEqualTo(second);
        assertThat(after(updated).getString("FIRST_TEXT")).isEqualTo(second);
        assertThat(after(updated).getString("SECOND_TEXT")).isEqualTo(second);

        execute("DELETE FROM " + tableName() + " WHERE ID=2");
        connection.commit();
        final var deleted = nextDataRecord();
        assertThat(envelope(deleted).getString("op")).isEqualTo("d");
        assertThat(before(deleted).getString("FIRST_TEXT")).isEqualTo(second);
        assertThat(before(deleted).getString("SECOND_TEXT")).isEqualTo(second);
    }

    @Test
    void shouldCaptureNullAndLiteralNullWithoutConfusingThem() throws Exception {
        startAndVerifySnapshot();
        execute("INSERT INTO " + tableName() + " VALUES(2,'null',NULL)");
        connection.commit();
        final var inserted = nextDataRecord();
        assertThat(after(inserted).getString("NAME")).isEqualTo("null");
        assertThat(after(inserted).get("AMOUNT")).isNull();
        execute("UPDATE " + tableName() + " SET NAME=NULL,AMOUNT=1.250000 WHERE ID=2");
        connection.commit();
        final var updated = nextDataRecord();
        assertThat(before(updated).getString("NAME")).isEqualTo("null");
        assertThat(after(updated).get("NAME")).isNull();
        assertThat(after(updated).get("AMOUNT")).isEqualTo(new java.math.BigDecimal("1.250000"));
    }

    @Test
    void shouldApplyTableExclusionDuringStreaming() throws Exception {
        startAndVerifySnapshot();
        final String excluded = table + "_OTHER";
        try {
            execute("CREATE TABLE \"" + SCHEMA + "\".\"" + excluded + "\" (ID INT)");
            execute("INSERT INTO \"" + SCHEMA + "\".\"" + excluded + "\" VALUES(99)");
            execute("INSERT INTO " + tableName() + " VALUES(2,'included',2)");
            connection.commit();
            final var record = nextDataRecord();
            assertThat(record.topic()).endsWith("." + table);
            assertThat(after(record).get("ID")).isEqualTo(2);
            assertNoDataRecords(Duration.ofSeconds(2));
        }
        finally {
            execute("DROP TABLE \"" + SCHEMA + "\".\"" + excluded + "\"");
            connection.commit();
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "include", "exclude" })
    void shouldApplyColumnFiltersDuringSnapshotAndStreaming(String filterType) throws Exception {
        final String prefix = SCHEMA + "\\." + table + "\\.";
        config = config.edit().with("column." + filterType + ".list", prefix + (filterType.equals("include") ? "(ID|NAME)" : "AMOUNT")).build();
        startEngine();
        final var snapshot = nextDataRecord();
        assertThat(envelope(snapshot).getString("op")).isEqualTo("r");
        assertThat(after(snapshot).get("ID")).isEqualTo(1);
        assertThat(after(snapshot).schema().field("AMOUNT")).isNull();

        execute("INSERT INTO " + tableName() + " VALUES(2,'filtered',2)");
        connection.commit();
        final var inserted = after(nextDataRecord());
        assertThat(inserted.get("ID")).isEqualTo(2);
        assertThat(inserted.getString("NAME")).isEqualTo("filtered");
        assertThat(inserted.schema().field("AMOUNT")).isNull();
        execute("UPDATE " + tableName() + " SET NAME='updated',AMOUNT=3 WHERE ID=2");
        connection.commit();
        final var updated = nextDataRecord();
        assertThat(before(updated).getString("NAME")).isEqualTo("filtered");
        assertThat(after(updated).getString("NAME")).isEqualTo("updated");
        assertThat(before(updated).schema().field("AMOUNT")).isNull();
        assertThat(after(updated).schema().field("AMOUNT")).isNull();
    }

    @Test
    void shouldCaptureATableWithoutAPrimaryKey() throws Exception {
        execute("ALTER TABLE " + tableName() + " DROP PRIMARY KEY");
        connection.commit();
        startEngine();
        final var snapshot = nextDataRecord();
        assertThat(envelope(snapshot).getString("op")).isEqualTo("r");
        assertThat(snapshot.key()).isNull();
        execute("UPDATE " + tableName() + " SET NAME='without key' WHERE ID=1");
        connection.commit();
        final var updated = nextDataRecord();
        assertThat(updated.key()).isNull();
        assertThat(before(updated).getString("NAME")).isEqualTo("snapshot");
        assertThat(after(updated).getString("NAME")).isEqualTo("without key");
        execute("DELETE FROM " + tableName() + " WHERE ID=1");
        connection.commit();
        final var deleted = nextDataRecord();
        assertThat(deleted.key()).isNull();
        assertThat(envelope(deleted).getString("op")).isEqualTo("d");
        assertThat(before(deleted).get("ID")).isEqualTo(1);
    }

    @Test
    void shouldRefreshCharacterPaddingAfterLiveAndHistoricalDdl() throws Exception {
        execute("ALTER TABLE " + tableName() + " ADD FIXED CHAR(8)");
        execute("UPDATE " + tableName() + " SET FIXED='ab' WHERE ID=1");
        connection.commit();
        startEngine();
        assertThat(after(nextDataRecord()).getString("FIXED")).isEqualTo("ab      ");

        execute("INSERT INTO " + tableName() + " VALUES(2,'before DDL',2,'cd')");
        connection.commit();
        assertThat(after(nextDataRecord()).getString("FIXED")).isEqualTo("cd      ");
        execute("ALTER TABLE " + tableName() + " MODIFY FIXED CHAR(12)");
        execute("INSERT INTO " + tableName() + " VALUES(3,'after DDL',3,'ef')");
        connection.commit();
        final var checkpoint = nextDataRecord();
        assertThat(after(checkpoint).getString("FIXED")).isEqualTo("ef          ");
        stopConnector();
        storeOffsets(config, Map.of(checkpoint.sourcePartition(), checkpoint.sourceOffset()));
        consumedLines.clear();

        execute("INSERT INTO " + tableName() + " VALUES(4,'old history',4,'gh')");
        connection.commit();
        execute("ALTER TABLE " + tableName() + " MODIFY FIXED CHAR(16)");
        execute("INSERT INTO " + tableName() + " VALUES(5,'new history',5,'ij')");
        connection.commit();
        startEngine();
        assertThat(after(nextDataRecord()).getString("FIXED")).isEqualTo("gh          ");
        assertThat(after(nextDataRecord()).getString("FIXED")).isEqualTo("ij              ");
        assertNoDataRecords(Duration.ofSeconds(1));
    }

    @Test
    void shouldUseTheSameTypeMappingsForSnapshotAndStreaming() throws Exception {
        execute("ALTER TABLE " + tableName() + " ADD CREATED TIMESTAMP(6)");
        execute("ALTER TABLE " + tableName() + " ADD BINARY_VALUE VARBINARY(50)");
        execute("ALTER TABLE " + tableName() + " ADD LARGE_NUMBER BIGINT");
        execute("DELETE FROM " + tableName());
        insertTypes(1);
        connection.commit();
        startEngine();
        final var snapshot = nextDataRecord();
        assertThat(envelope(snapshot).getString("op")).isEqualTo("r");
        insertTypes(2);
        connection.commit();
        final var streamed = nextDataRecord();
        assertThat(envelope(streamed).getString("op")).isEqualTo("c");
        for (String column : List.of("NAME", "AMOUNT", "CREATED", "BINARY_VALUE", "LARGE_NUMBER")) {
            assertThat(after(streamed).get(column)).as(column).isEqualTo(after(snapshot).get(column));
            assertThat(after(streamed).schema().field(column).schema()).as(column + " schema")
                    .isEqualTo(after(snapshot).schema().field(column).schema());
        }
        assertThat(after(streamed).getString("NAME")).isEqualTo("达梦 O'Brien, and null");
        assertThat(after(streamed).get("AMOUNT")).isEqualTo(new java.math.BigDecimal("12345678901234.123456"));
        assertThat(after(streamed).get("LARGE_NUMBER")).isEqualTo(Long.MAX_VALUE);
    }

    @Test
    void shouldSnapshotFullLobsAndMarkUnavailableStreamingLobs() throws Exception {
        execute("ALTER TABLE " + tableName() + " ADD TEXT_LOB CLOB");
        execute("ALTER TABLE " + tableName() + " ADD BINARY_LOB BLOB");
        final String snapshotText = "snapshot text".repeat(600) + "中文尾部";
        final byte[] largeBinary = new byte[6000];
        java.util.Arrays.fill(largeBinary, (byte) 0xa5);
        try (var statement = connection.prepareStatement("UPDATE " + tableName() + " SET TEXT_LOB=?,BINARY_LOB=? WHERE ID=1")) {
            statement.setString(1, snapshotText);
            statement.setBytes(2, largeBinary);
            statement.executeUpdate();
        }
        connection.commit();
        final String placeholder = "[DM LOB unavailable]";
        config = config.edit().with("unavailable.value.placeholder", placeholder).build();
        startEngine();
        final var snapshot = after(nextDataRecord());
        assertThat(snapshot.getString("TEXT_LOB")).isEqualTo(snapshotText);
        assertThat(binaryValue(snapshot, "BINARY_LOB")).containsExactly(largeBinary);

        final byte[] inlineBinary = { 0, 1, (byte) 0x80, (byte) 0xff };
        try (var statement = connection.prepareStatement("INSERT INTO " + tableName() + " VALUES(2,'inline LOB',2,?,?)")) {
            statement.setString(1, "inline O'Brien 中文");
            statement.setBytes(2, inlineBinary);
            statement.executeUpdate();
        }
        connection.commit();
        final var inline = after(nextDataRecord());
        assertThat(inline.getString("TEXT_LOB")).isEqualTo("inline O'Brien 中文");
        assertThat(binaryValue(inline, "BINARY_LOB")).containsExactly(inlineBinary);

        try (var statement = connection.prepareStatement("INSERT INTO " + tableName() + " VALUES(3,'external LOB',3,?,?)")) {
            statement.setString(1, "streamed".repeat(900));
            statement.setBytes(2, largeBinary);
            statement.executeUpdate();
        }
        connection.commit();
        final var external = after(nextDataRecord());
        assertThat(external.getString("TEXT_LOB")).isEqualTo(placeholder);
        assertThat(binaryValue(external, "BINARY_LOB")).containsExactly(placeholder.getBytes(StandardCharsets.UTF_8));

        execute("UPDATE " + tableName() + " SET NAME='unchanged LOB' WHERE ID=3");
        connection.commit();
        final var updated = nextDataRecord();
        assertThat(before(updated).getString("TEXT_LOB")).isEqualTo(placeholder);
        assertThat(after(updated).getString("TEXT_LOB")).isEqualTo(placeholder);
        assertThat(binaryValue(before(updated), "BINARY_LOB")).containsExactly(placeholder.getBytes(StandardCharsets.UTF_8));
        assertThat(binaryValue(after(updated), "BINARY_LOB")).containsExactly(placeholder.getBytes(StandardCharsets.UTF_8));

        execute("DELETE FROM " + tableName() + " WHERE ID=3");
        connection.commit();
        final var deleted = nextDataRecord();
        assertThat(envelope(deleted).getString("op")).isEqualTo("d");
        assertThat(before(deleted).getString("TEXT_LOB")).isEqualTo(placeholder);
        assertThat(binaryValue(before(deleted), "BINARY_LOB")).containsExactly(placeholder.getBytes(StandardCharsets.UTF_8));
    }

    @Test
    void shouldEmitDeleteAndCreateWhenPrimaryKeyChanges() throws Exception {
        startAndVerifySnapshot();
        execute("UPDATE " + tableName() + " SET ID=2 WHERE ID=1");
        connection.commit();
        final var deleted = nextDataRecord();
        final var created = nextDataRecord();
        assertThat(envelope(deleted).getString("op")).isEqualTo("d");
        assertThat(before(deleted).get("ID")).isEqualTo(1);
        assertThat(envelope(created).getString("op")).isEqualTo("c");
        assertThat(after(created).get("ID")).isEqualTo(2);
        assertThat(after(created).getString("NAME")).isEqualTo("snapshot");
        assertThat(((Struct) deleted.headers().lastWithName("__debezium.newkey").value()).get("ID")).isEqualTo(2);
        assertThat(((Struct) created.headers().lastWithName("__debezium.oldkey").value()).get("ID")).isEqualTo(1);
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void shouldReplayCompositeKeyChangeAfterAcknowledgingOnlyItsDelete(boolean tombstones) throws Exception {
        execute("ALTER TABLE " + tableName() + " DROP PRIMARY KEY");
        execute("ALTER TABLE " + tableName() + " ADD TENANT INT DEFAULT 7 NOT NULL");
        execute("ALTER TABLE " + tableName() + " ADD PRIMARY KEY(TENANT,ID)");
        connection.commit();
        config = config.edit().with("tombstones.on.delete", tombstones).build();
        startAndVerifySnapshot();
        execute("UPDATE " + tableName() + " SET TENANT=8,ID=2,NAME='moved' WHERE TENANT=7 AND ID=1");
        connection.commit();
        final var deleted = nextDataRecord();
        assertThat(envelope(deleted).getString("op")).isEqualTo("d");
        assertThat(((Struct) deleted.key()).getInt32("TENANT")).isEqualTo(7);
        assertThat(((Struct) deleted.key()).getInt32("ID")).isEqualTo(1);
        final var newKey = (Struct) deleted.headers().lastWithName("__debezium.newkey").value();
        assertThat(newKey.getInt32("TENANT")).isEqualTo(8);
        assertThat(newKey.getInt32("ID")).isEqualTo(2);
        SourceRecord checkpoint = deleted;
        if (tombstones) {
            assertThat(waitForAvailableRecords(30, TimeUnit.SECONDS)).isTrue();
            checkpoint = consumeRecord();
            assertThat(checkpoint.value()).isNull();
            assertThat(checkpoint.key()).isEqualTo(deleted.key());
            assertThat(checkpoint.sourceOffset()).isEqualTo(deleted.sourceOffset());
        }
        stopConnector();
        storeOffsets(config, Map.of(checkpoint.sourcePartition(), checkpoint.sourceOffset()));
        consumedLines.clear();
        startEngine();
        final var replayedDelete = nextDataRecord();
        final var created = nextDataRecord();
        assertThat(envelope(replayedDelete).getString("op")).isEqualTo("d");
        assertThat(replayedDelete.key()).isEqualTo(deleted.key());
        assertThat(envelope(created).getString("op")).isEqualTo("c");
        assertThat(created.key()).isEqualTo(newKey);
        assertThat(after(created).getInt32("TENANT")).isEqualTo(8);
        assertThat(after(created).getInt32("ID")).isEqualTo(2);
        assertThat(after(created).getString("NAME")).isEqualTo("moved");
        assertThat(created.headers().lastWithName("__debezium.oldkey").value()).isEqualTo(deleted.key());
        assertNoDataRecords(Duration.ofSeconds(1));
    }

    @Test
    void shouldReplayHistoricalDecimalAndTimestampPrecisionChanges() throws Exception {
        execute("ALTER TABLE " + tableName() + " ADD STAMP TIMESTAMP(3)");
        execute("ALTER TABLE " + tableName() + " ADD FLEX INT");
        connection.commit();
        startAndVerifySnapshot();
        stopConnector();
        execute("INSERT INTO " + tableName() + " VALUES(2,'old precision',12.345678,TIMESTAMP'2026-10-10 12:34:56.123',123)");
        connection.commit();
        execute("ALTER TABLE " + tableName() + " MODIFY AMOUNT DECIMAL(30,9)");
        execute("ALTER TABLE " + tableName() + " MODIFY STAMP TIMESTAMP(6)");
        execute("UPDATE " + tableName() + " SET NAME='new precision',AMOUNT=123456789012345678901.123456789,"
                + "STAMP=TIMESTAMP'2026-10-10 12:34:56.654321' WHERE ID=2");
        connection.commit();
        startEngine();
        final var old = nextDataRecord();
        assertThat(envelope(old).getString("op")).isEqualTo("c");
        assertThat(after(old).get("AMOUNT")).isEqualTo(new java.math.BigDecimal("12.345678"));
        assertThat(after(old).getInt32("FLEX")).isEqualTo(123);
        assertThat(after(old).schema().field("AMOUNT").schema().parameters().get("scale")).isEqualTo("6");
        assertThat(after(old).get("STAMP")).isEqualTo(java.time.LocalDateTime.of(2026, 10, 10, 12, 34, 56, 123_000_000)
                .toInstant(java.time.ZoneOffset.UTC).toEpochMilli());
        final var updated = nextDataRecord();
        assertThat(envelope(updated).getString("op")).isEqualTo("u");
        assertThat(before(updated).get("AMOUNT")).isEqualTo(new java.math.BigDecimal("12.345678000"));
        assertThat(after(updated).get("AMOUNT")).isEqualTo(new java.math.BigDecimal("123456789012345678901.123456789"));
        assertThat(after(updated).schema().field("AMOUNT").schema().parameters().get("scale")).isEqualTo("9");
        assertThat(after(updated).get("STAMP")).isEqualTo(java.time.LocalDateTime.of(2026, 10, 10, 12, 34, 56)
                .toEpochSecond(java.time.ZoneOffset.UTC) * 1_000_000 + 654_321);
        stopConnector();
        storeOffsets(config, Map.of(updated.sourcePartition(), updated.sourceOffset()));
        consumedLines.clear();
        execute("DELETE FROM " + tableName() + " WHERE ID=2");
        connection.commit();
        // All remaining FLEX values are NULL, permitting an unrelated type change.
        execute("ALTER TABLE " + tableName() + " MODIFY FLEX VARCHAR(50)");
        execute("INSERT INTO " + tableName() + " VALUES(3,'text type',3,NULL,'000123.4500')");
        connection.commit();
        startEngine();
        final var deleted = nextDataRecord();
        assertThat(envelope(deleted).getString("op")).isEqualTo("d");
        assertThat(before(deleted)).isEqualTo(after(updated));
        final var text = nextDataRecord();
        assertThat(envelope(text).getString("op")).isEqualTo("c");
        assertThat(after(text).getString("FLEX")).isEqualTo("000123.4500");
        assertThat(after(text).schema().field("FLEX").schema().type()).isEqualTo(Schema.Type.STRING);
        assertNoDataRecords(Duration.ofSeconds(1));
    }

    @Test
    void shouldPreserveMultiTableTransactionMetadataAfterInterruptedDispatch() throws Exception {
        final var otherTable = table + "_OTHER";
        execute("CREATE TABLE " + qualified(otherTable) + " (ID INT PRIMARY KEY, NAME VARCHAR(200), AMOUNT DECIMAL(20,6))");
        connection.commit();
        try {
            config = config.edit()
                    .with("table.include.list", SCHEMA + "\\.(" + table + "|" + otherTable + ")")
                    .with("snapshot.mode", "no_data").with("provide.transaction.metadata", true).build();
            startEngine();
            waitForStreamingRunning("dameng", "dameng");
            execute("INSERT INTO " + tableName() + " VALUES(2,'first A',2)");
            execute("INSERT INTO " + qualified(otherTable) + " VALUES(10,'first B',10)");
            execute("UPDATE " + tableName() + " SET NAME='second A' WHERE ID=2");
            execute("UPDATE " + qualified(otherTable) + " SET NAME='second B' WHERE ID=10");
            connection.commit();
            final var transactionId = assertBeginTransaction(nextTransactionRecord());
            final var checkpoint = nextDataRecord();
            assertRecordTransactionMetadata(checkpoint, transactionId, 1, 1);
            assertThat(checkpoint.topic()).endsWith("." + table);
            stopConnector();
            storeOffsets(config, Map.of(checkpoint.sourcePartition(), checkpoint.sourceOffset()));
            consumedLines.clear();
            startEngine();
            final var firstB = nextDataRecord();
            final var secondA = nextDataRecord();
            final var secondB = nextDataRecord();
            assertRecordTransactionMetadata(firstB, transactionId, 2, 1);
            assertRecordTransactionMetadata(secondA, transactionId, 3, 2);
            assertRecordTransactionMetadata(secondB, transactionId, 4, 2);
            assertThat(firstB.topic()).endsWith("." + otherTable);
            assertThat(after(firstB).getString("NAME")).isEqualTo("first B");
            assertThat(before(secondA).getString("NAME")).isEqualTo("first A");
            assertThat(after(secondA).getString("NAME")).isEqualTo("second A");
            assertThat(before(secondB).getString("NAME")).isEqualTo("first B");
            assertThat(after(secondB).getString("NAME")).isEqualTo("second B");
            assertEndTransaction(nextTransactionRecord(), transactionId, 4, Map.of(SCHEMA + "." + table, 2, SCHEMA + "." + otherTable, 2));
            assertNoDataRecords(Duration.ofSeconds(1));
        }
        finally {
            stopConnector();
            execute("DROP TABLE " + qualified(otherTable));
            connection.commit();
        }
    }

    private static String qualified(String name) {
        return '"' + SCHEMA + "\".\"" + name + '"';
    }

    @Test
    void shouldMatchAnIndependentSeededTransactionModelAcrossRestart() throws Exception {
        startEngine();
        final var snapshot = nextDataRecord();
        assertThat(envelope(snapshot).getString("op")).isEqualTo("r");
        final var random = new Random(0x444D434443L);
        Map<Integer, ModelRow> model = new LinkedHashMap<>();
        model.put(1, new ModelRow("snapshot", new BigDecimal("1.250000")));
        final List<ModelChange> expected = new ArrayList<>();
        int nextId = 2;
        for (int transaction = 0; transaction < 40; transaction++) {
            Map<Integer, ModelRow> working = new LinkedHashMap<>(model);
            final List<ModelChange> changes = new ArrayList<>();
            Map<Integer, ModelRow> savepointState = null;
            java.sql.Savepoint savepoint = null;
            for (int operation = 0; operation < 3; operation++) {
                final int action = working.isEmpty() ? 0 : random.nextInt(3);
                final int id = action == 0 ? nextId++ : new ArrayList<>(working.keySet()).get(random.nextInt(working.size()));
                final var previous = working.get(id);
                final var replacement = new ModelRow(transaction % 4 == 0 ? null : "tx" + transaction + ":" + operation + " O'Brien, \\ 中文\nnext",
                        transaction % 6 == 0 ? null : BigDecimal.valueOf(random.nextInt(2_000_000) - 1_000_000, 6));
                final var sql = switch (action) {
                    case 0 -> "INSERT INTO " + tableName() + " (NAME,AMOUNT,ID) VALUES(?,?,?)";
                    case 1 -> "UPDATE " + tableName() + " SET NAME=?,AMOUNT=? WHERE ID=?";
                    default -> "DELETE FROM " + tableName() + " WHERE ID=?";
                };
                try (var statement = connection.prepareStatement(sql)) {
                    if (action == 2) {
                        statement.setInt(1, id);
                    }
                    else {
                        statement.setString(1, replacement.name());
                        statement.setBigDecimal(2, replacement.amount());
                        statement.setInt(3, id);
                    }
                    assertThat(statement.executeUpdate()).isEqualTo(1);
                }
                if (action == 2) {
                    working.remove(id);
                    changes.add(new ModelChange("d", id, previous, null));
                }
                else {
                    working.put(id, replacement);
                    changes.add(new ModelChange(action == 0 ? "c" : "u", id, previous, replacement));
                }
                if (operation == 0 && transaction % 5 == 0) {
                    savepoint = connection.setSavepoint("MODEL_SAVEPOINT");
                    savepointState = new LinkedHashMap<>(working);
                }
            }
            if (transaction % 7 == 0) {
                connection.rollback();
            }
            else {
                if (savepoint != null) {
                    connection.rollback(savepoint);
                    working = savepointState;
                    changes.subList(1, changes.size()).clear();
                }
                connection.commit();
                model = working;
                expected.addAll(changes);
            }
            if (transaction == 19) {
                assertDatabaseModel(model);
                final var checkpoint = assertModelChanges(expected);
                stopConnector();
                storeOffsets(config, Map.of(checkpoint.sourcePartition(), checkpoint.sourceOffset()));
                consumedLines.clear();
                expected.clear();
            }
        }
        assertDatabaseModel(model);
        startEngine();
        assertModelChanges(expected);
        assertNoDataRecords(Duration.ofSeconds(2));
    }

    private SourceRecord assertModelChanges(List<ModelChange> expected) throws Exception {
        assertThat(expected).isNotEmpty();
        SourceRecord last = null;
        for (var change : expected) {
            last = nextDataRecord();
            assertThat(envelope(last).getString("op")).isEqualTo(change.operation());
            assertThat(((Struct) last.key()).getInt32("ID")).isEqualTo(change.id());
            assertModelRow(before(last), change.id(), change.before());
            assertModelRow(after(last), change.id(), change.after());
        }
        return last;
    }

    private static void assertModelRow(Struct row, int id, ModelRow expected) {
        if (expected == null) {
            assertThat(row).isNull();
        }
        else {
            assertThat(row.getInt32("ID")).isEqualTo(id);
            assertThat(row.getString("NAME")).isEqualTo(expected.name());
            assertThat(row.get("AMOUNT")).isEqualTo(expected.amount());
        }
    }

    private void assertDatabaseModel(Map<Integer, ModelRow> expected) throws SQLException {
        final Map<Integer, ModelRow> actual = new LinkedHashMap<>();
        try (var statement = connection.createStatement(); var result = statement.executeQuery("SELECT ID,NAME,AMOUNT FROM " + tableName())) {
            while (result.next()) {
                actual.put(result.getInt(1), new ModelRow(result.getString(2), result.getBigDecimal(3)));
            }
        }
        assertThat(actual).containsExactlyInAnyOrderEntriesOf(expected);
    }

    private record ModelRow(String name, BigDecimal amount) {
    }

    private record ModelChange(String operation, int id, ModelRow before, ModelRow after) {
    }

    @Test
    void shouldEmitATombstoneAfterTheDeleteEnvelopeWhenEnabled() throws Exception {
        config = config.edit().with("tombstones.on.delete", true).build();
        startAndVerifySnapshot();
        execute("DELETE FROM " + tableName() + " WHERE ID=1");
        connection.commit();
        final var deleted = nextDataRecord();
        assertThat(envelope(deleted).getString("op")).isEqualTo("d");
        assertThat(waitForAvailableRecords(30, TimeUnit.SECONDS)).isTrue();
        final var tombstone = consumeRecord();
        assertThat(tombstone.value()).isNull();
        assertThat(tombstone.valueSchema()).isNull();
        assertThat(tombstone.topic()).isEqualTo(deleted.topic());
        assertThat(tombstone.key()).isEqualTo(deleted.key());
        assertThat(tombstone.sourceOffset()).isEqualTo(deleted.sourceOffset());
    }

    @Test
    void shouldPreserveQuotedMixedCaseTableAndColumnIdentifiers() throws Exception {
        execute("ALTER TABLE " + tableName() + " RENAME TO \"" + table + "_Mixed\"");
        table = table + "_Mixed";
        execute("ALTER TABLE " + tableName() + " RENAME COLUMN ID TO \"KeyId\"");
        execute("ALTER TABLE " + tableName() + " RENAME COLUMN NAME TO \"DisplayName\"");
        execute("ALTER TABLE " + tableName() + " RENAME COLUMN AMOUNT TO \"Amount\"");
        connection.commit();
        config = config.edit().with("table.include.list", SCHEMA + "\\." + table).build();
        startEngine();
        final var snapshot = nextDataRecord();
        assertThat(envelope(snapshot).getString("op")).isEqualTo("r");
        assertThat(snapshot.topic()).endsWith("." + table);
        assertThat(((Struct) snapshot.key()).getInt32("KeyId")).isEqualTo(1);
        assertThat(after(snapshot).getString("DisplayName")).isEqualTo("snapshot");
        assertThat(after(snapshot).schema().field("NAME")).isNull();

        execute("UPDATE " + tableName() + " SET \"DisplayName\"='mixed case' WHERE \"KeyId\"=1");
        connection.commit();
        final var updated = nextDataRecord();
        assertThat(updated.topic()).isEqualTo(snapshot.topic());
        assertThat(((Struct) updated.key()).getInt32("KeyId")).isEqualTo(1);
        assertThat(before(updated).getString("DisplayName")).isEqualTo("snapshot");
        assertThat(after(updated).getString("DisplayName")).isEqualTo("mixed case");
        assertThat(after(updated).get("Amount")).isEqualTo(new java.math.BigDecimal("1.250000"));
    }

    private void insertTypes(int id) throws SQLException {
        try (var statement = connection.prepareStatement("INSERT INTO " + tableName() + " VALUES(?,?,?,?,?,?)")) {
            statement.setInt(1, id);
            statement.setString(2, "达梦 O'Brien, and null");
            statement.setBigDecimal(3, new java.math.BigDecimal("12345678901234.123456"));
            statement.setTimestamp(4, java.sql.Timestamp.valueOf("2026-09-24 12:34:56.123456"));
            statement.setBytes(5, new byte[]{ 0, 1, (byte) 0x80, (byte) 0xff });
            statement.setLong(6, Long.MAX_VALUE);
            statement.executeUpdate();
        }
    }

    private void startAndVerifySnapshot() throws Exception {
        startEngine();
        final var snapshot = nextDataRecord();
        assertThat(envelope(snapshot).getString("op")).isEqualTo("r");
        assertThat(after(snapshot).get("ID")).isEqualTo(1);
        assertThat(after(snapshot).getString("NAME")).isEqualTo("snapshot");
        assertThat(envelope(snapshot).getStruct("source").getString("connector")).isEqualTo("dameng");
    }

    private SourceRecord nextDataRecord() throws Exception {
        final long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
        while (System.nanoTime() < deadline) {
            assertConnectorHealthy();
            if (waitForAvailableRecords(1, TimeUnit.SECONDS)) {
                final var record = consumeRecord();
                if (isData(record)) {
                    envelope(record).validate();
                    if (record.key() instanceof Struct key) {
                        key.validate();
                    }
                    return record;
                }
            }
        }
        throw new AssertionError("No Dameng change event received within 60 seconds");
    }

    private void assertNoDataRecords(Duration duration) throws Exception {
        final long deadline = System.nanoTime() + duration.toNanos();
        final List<SourceRecord> unexpected = new ArrayList<>();
        while (System.nanoTime() < deadline) {
            assertConnectorHealthy();
            if (waitForAvailableRecords(100, TimeUnit.MILLISECONDS)) {
                final var record = consumeRecord();
                if (isData(record)) {
                    unexpected.add(record);
                }
            }
        }
        assertThat(unexpected).isEmpty();
    }

    private SourceRecord nextTransactionRecord() throws Exception {
        final long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
        while (System.nanoTime() < deadline) {
            assertConnectorHealthy();
            if (waitForAvailableRecords(1, TimeUnit.SECONDS)) {
                final var record = consumeRecord();
                if (record != null && record.value() instanceof Struct value && value.schema().field("status") != null) {
                    value.validate();
                    return record;
                }
                assertThat(isData(record)).as("transaction boundary must surround its data records").isFalse();
            }
        }
        throw new AssertionError("No Dameng transaction metadata received within 60 seconds");
    }

    private void startEngine() {
        connectorFailure.set(null);
        start(DamengConnector.class, config, (success, message, error) -> {
            if (!success) {
                connectorFailure.set(error != null ? error : new IllegalStateException(message));
            }
        }, null);
        assertConnectorHealthy();
    }

    private void assertConnectorHealthy() {
        final Throwable failure = connectorFailure.get();
        if (failure != null) {
            throw new AssertionError("Dameng connector failed", failure);
        }
    }

    private boolean isData(SourceRecord record) {
        return record != null && record.value() instanceof Struct value && value.schema().field("op") != null;
    }

    private static Struct envelope(SourceRecord record) {
        return (Struct) record.value();
    }

    private static Struct before(SourceRecord record) {
        return envelope(record).getStruct("before");
    }

    private static Struct after(SourceRecord record) {
        return envelope(record).getStruct("after");
    }

    private static byte[] binaryValue(Struct value, String column) {
        final var buffer = ((ByteBuffer) value.get(column)).duplicate();
        final byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return bytes;
    }

    private String tableName() {
        return "\"" + SCHEMA + "\".\"" + table + "\"";
    }

    private void execute(String sql) throws SQLException {
        try (var statement = connection.createStatement()) {
            statement.execute(sql);
        }
    }
}
