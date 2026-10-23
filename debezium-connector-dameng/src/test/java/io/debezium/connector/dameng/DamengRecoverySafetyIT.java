/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.EOFException;
import java.io.StreamCorruptedException;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.document.DocumentReader;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;
import io.debezium.junit.logging.LogInterceptor;
import io.debezium.relational.TableId;
import io.debezium.relational.history.HistoryRecord;
import io.debezium.relational.history.JsonTableChangeSerializer;
import io.debezium.storage.file.history.FileSchemaHistory;

import ch.qos.logback.classic.Logger;

/** Disconnects only task sessions belonging to a unique test account in a test-owned database. */
class DamengRecoverySafetyIT extends AbstractAsyncEngineConnectorTest {
    private static final String PASSWORD = "DbzRecovery_42";

    @TempDir
    Path directory;

    private Connection admin;
    private String username;
    private String table;
    private String checkConstraint;
    private boolean userCreated;
    private boolean tableCreated;
    private Configuration config;
    private final AtomicReference<Throwable> failure = new AtomicReference<>();

    @BeforeAll
    static void startDatabase() throws Exception {
        Assumptions.assumeTrue(System.getProperty("dameng.hostname") == null,
                "Session fault injection and test-user grants run only in the container owned by this test suite");
        DamengTestDatabase.start();
    }

    @AfterAll
    static void stopDatabase() {
        DamengTestDatabase.stop();
    }

    @BeforeEach
    void before() throws Exception {
        admin = DriverManager.getConnection(DamengTestDatabase.jdbcUrl(), DamengTestDatabase.user(), DamengTestDatabase.password());
        final String suffix = UUID.randomUUID().toString().replace("-", "").substring(0, 12).toUpperCase();
        username = "DBZ_CDC_" + suffix;
        table = "DBZ_RECOVERY_" + suffix;
        checkConstraint = "DBZ_CHECK_" + suffix;
        execute("CREATE USER " + username + " IDENTIFIED BY \"" + PASSWORD + "\"");
        userCreated = true;
        execute("CREATE TABLE " + qualifiedTable() + " (ID INT, SCORE INT, LABEL VARCHAR(50), CONSTRAINT DBZ_PK_" + suffix
                + " PRIMARY KEY(ID), CONSTRAINT " + checkConstraint + " CHECK(SCORE>0))");
        tableCreated = true;
        execute("INSERT INTO " + qualifiedTable() + " VALUES(1,1,'snapshot')");
        // DM does not permit object-level grants on SYS.SYSOBJECTS/SYSCONS.
        // Its documented SOI and VTI roles allow system-catalog/dynamic-view reads.
        // Grant only SELECT on the captured business table, without DBA or ANY TABLE.
        execute("GRANT SELECT ON " + qualifiedTable() + " TO " + username);
        execute("GRANT SOI TO " + username);
        execute("GRANT VTI TO " + username);
        execute("GRANT EXECUTE ON DBMS_LOGMNR TO " + username);
        config = Configuration.create()
                .with("log.mining.window.strategy", System.getProperty("dameng.test.window.strategy", "archive_bytes"))
                .with("topic.prefix", "dameng")
                .with("database.hostname", DamengTestDatabase.hostname()).with("database.port", DamengTestDatabase.port())
                .with("database.user", username).with("database.password", PASSWORD)
                .with("database.dbname", System.getProperty("dameng.database", "DAMENG"))
                .with("table.include.list", DamengTestDatabase.schema() + "\\." + table)
                .with("schema.history.internal", FileSchemaHistory.class.getName())
                .with("schema.history.internal.file.filename", directory.resolve("schema-history.dat").toString())
                .with("snapshot.mode", "initial").with("poll.interval.ms", 100)
                .with("retriable.restart.connector.wait.ms", 100).with("offset.flush.interval.ms", 100)
                .with("include.schema.changes", false).with("tombstones.on.delete", false).build();
    }

    @AfterEach
    void after() throws Exception {
        stopConnector();
        if (admin != null) {
            try {
                if (tableCreated) {
                    execute("DROP TABLE " + qualifiedTable());
                }
                if (userCreated) {
                    execute("DROP USER " + username);
                }
            }
            finally {
                admin.close();
            }
        }
    }

    @Test
    void shouldWarnAboutAnIncludedTableWithoutSelectPrivilegesAndContinueCapturingVisibleTables() throws Exception {
        final String hiddenTable = table + "_NO_SELECT";
        final String qualifiedHidden = '"' + DamengTestDatabase.schema() + "\".\"" + hiddenTable + '"';
        execute("CREATE TABLE " + qualifiedHidden + " (ID INT, LABEL VARCHAR(50))");
        final var logs = LogInterceptor.forClass(DamengSnapshotChangeEventSource.class);
        try {
            execute("INSERT INTO " + qualifiedHidden + " VALUES(1,'not visible')");
            config = config.edit().with("table.include.list", DamengTestDatabase.schema() + "\\." + table + ","
                    + DamengTestDatabase.schema() + "\\." + hiddenTable).build();
            try (var restricted = new DamengConnection(new DamengConnectorConfig(config).getJdbcConfig())) {
                assertThat(restricted.getAllTableIds(null))
                        .contains(new TableId(null, DamengTestDatabase.schema(), table))
                        .doesNotContain(new TableId(null, DamengTestDatabase.schema(), hiddenTable));
                assertThatThrownBy(() -> restricted.query("SELECT 1 FROM " + qualifiedHidden + " WHERE 1=0", rows -> {
                })).isInstanceOf(SQLException.class).satisfies(error -> assertThat(((SQLException) error).getErrorCode()).isEqualTo(-5504));
            }

            start(DamengConnector.class, config, (success, message, error) -> {
                if (!success) {
                    failure.set(error == null ? new IllegalStateException(message) : error);
                }
            }, null);
            waitForStreamingRunning("dameng", "dameng");
            final var snapshot = nextDataRecord();
            assertThat(envelope(snapshot).getString("op")).isEqualTo("r");
            assertThat(snapshot.topic()).endsWith("." + table);
            assertThat(logs.containsWarnMessage("table.include.list rule '" + DamengTestDatabase.schema() + "\\." + hiddenTable
                    + "' did not select any table visible to database user '" + username + "'")).isTrue();
            assertThat(logs.containsWarnMessage("table.include.list rule '" + DamengTestDatabase.schema() + "\\." + table + "'")).isFalse();
            assertThat(logs.containsMessage("Dameng tables selected for schema capture: [" + DamengTestDatabase.schema() + "." + table + "]")).isTrue();
            final List<TableId> storedTables = new ArrayList<>();
            for (String line : java.nio.file.Files.readAllLines(directory.resolve("schema-history.dat"))) {
                final var record = DocumentReader.defaultReader().read(line);
                for (var change : new JsonTableChangeSerializer().deserialize(record.getArray(HistoryRecord.Fields.TABLE_CHANGES), false)) {
                    storedTables.add(change.getId());
                }
            }
            assertThat(storedTables).contains(new TableId(null, DamengTestDatabase.schema(), table))
                    .doesNotContain(new TableId(null, DamengTestDatabase.schema(), hiddenTable));

            execute("INSERT INTO " + qualifiedTable() + " VALUES(2,2,'streaming after warning')");
            final var streamed = nextDataRecord();
            assertThat(envelope(streamed).getString("op")).isEqualTo("c");
            assertThat(envelope(streamed).getStruct("after").getString("LABEL")).isEqualTo("streaming after warning");
            assertThat(failure.get()).isNull();
        }
        finally {
            stopConnector();
            logs.stop();
            ((Logger) LoggerFactory.getLogger(DamengSnapshotChangeEventSource.class)).detachAppender(logs);
            execute("DROP TABLE " + qualifiedHidden);
        }
    }

    @Test
    void shouldRecoverAfterTheAdministratorClosesTaskSessionsWithoutDbaPrivileges() throws Exception {
        start(DamengConnector.class, config, (success, message, error) -> {
            if (!success) {
                failure.set(error == null ? new IllegalStateException(message) : error);
            }
        }, null);
        waitForStreamingRunning("dameng", "dameng");
        final var snapshot = nextDataRecord();
        assertThat(envelope(snapshot).getString("op")).isEqualTo("r");
        assertThat(((Struct) snapshot.key()).getInt32("ID")).isEqualTo(1);
        execute("INSERT INTO " + qualifiedTable() + " VALUES(2,2,'before disconnect')");
        final var beforeDisconnect = nextDataRecord();
        assertThat(envelope(beforeDisconnect).getString("op")).isEqualTo("c");
        assertThat(envelope(beforeDisconnect).getStruct("after").getInt32("ID")).isEqualTo(2);
        waitForDurableOffset(beforeDisconnect, config);
        final List<Session> oldSessions = sessions();
        assertThat(oldSessions).as("Only the connector uses the unique CDC account").isNotEmpty();

        // SP_CLOSE_SESSION returns before the connection is necessarily released.
        // USER_NAME identifies this test's account, never an unrelated DBA session.
        for (Session session : oldSessions) {
            execute("SP_CLOSE_SESSION(" + session.id() + ")");
        }
        execute("UPDATE " + qualifiedTable() + " SET SCORE=3,LABEL='after reconnect' WHERE ID=2");
        execute("ALTER TABLE " + qualifiedTable() + " DROP CONSTRAINT " + checkConstraint);
        execute("INSERT INTO " + qualifiedTable() + " VALUES(3,-1,'constraint removed')");

        final Set<Integer> delivered = new HashSet<>();
        final long recoveryDeadline = System.nanoTime() + Duration.ofSeconds(90).toNanos();
        while (delivered.size() < 2 && System.nanoTime() < recoveryDeadline) {
            final var record = nextDataRecord();
            final var value = envelope(record);
            assertThat(value.getString("op")).as("Retry resumes offsets without repeating the initial snapshot").isNotEqualTo("r");
            assertThat(record.sourceOffset().get(DamengOffsetContext.CAPTURE_SCOPE_KEY))
                    .isEqualTo(beforeDisconnect.sourceOffset().get(DamengOffsetContext.CAPTURE_SCOPE_KEY));
            final var after = value.getStruct("after");
            if (after.getInt32("ID") == 2 && value.getString("op").equals("u")) {
                assertThat(value.getStruct("before").getInt32("SCORE")).isEqualTo(2);
                assertThat(after.getInt32("SCORE")).isEqualTo(3);
                assertThat(after.getString("LABEL")).isEqualTo("after reconnect");
                delivered.add(2);
            }
            else if (after.getInt32("ID") == 3) {
                assertThat(value.getString("op")).isEqualTo("c");
                assertThat(after.getInt32("SCORE")).isEqualTo(-1);
                delivered.add(3);
            }
        }
        assertThat(delivered).containsExactlyInAnyOrder(2, 3);
        Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            final List<Session> active = sessions();
            assertThat(active).isNotEmpty().doesNotContainAnyElementsOf(oldSessions);
        });
        assertThat(failure.get()).isNull();
    }

    @Test
    void shouldRecoverFromRepeatedConnectionFailuresDuringStartupAndStreaming() throws Exception {
        try (var proxy = new DamengTcpProxy(DamengTestDatabase.hostname(), DamengTestDatabase.port())) {
            final var proxyConfig = config.edit().with("database.hostname", "127.0.0.1").with("database.port", proxy.port())
                    .with("database.socket.timeout.ms", 3000).with("database.query.timeout.ms", 3000)
                    .with("errors.max.retries", 50).with("retriable.restart.connector.wait.ms", 200).build();
            try {
                // Engine configuration validation probes the database before
                // starting a task. Allow that one connection, then reject the
                // task's initial attempts before a coordinator even exists.
                proxy.allowOneConnectionThenReject();
                start(DamengConnector.class, proxyConfig, (success, message, error) -> {
                    if (!success) {
                        failure.set(error == null ? new IllegalStateException(message) : error);
                    }
                }, null);
                waitForRejectedConnections(proxy, 3);
                proxy.setAvailable(true);
                waitForStreamingRunning("dameng", "dameng");
                final var snapshot = nextDataRecord();
                assertThat(envelope(snapshot).getString("op")).isEqualTo("r");
                assertThat(envelope(snapshot).getStruct("after").getInt32("ID")).isEqualTo(1);

                execute("INSERT INTO " + qualifiedTable() + " VALUES(2,2,'before network failure')");
                final var checkpoint = nextDataRecord();
                assertThat(envelope(checkpoint).getString("op")).isEqualTo("c");
                assertThat(envelope(checkpoint).getStruct("after").getInt32("ID")).isEqualTo(2);
                waitForDurableOffset(checkpoint, proxyConfig);

                final int previousRejections = proxy.rejectedConnections();
                proxy.setAvailable(false);
                // Closing established sockets tests the streaming failure path;
                // refusing reconnects tests the subsequent partial-start cleanup.
                waitForRejectedConnections(proxy, previousRejections + 3);
                execute("UPDATE " + qualifiedTable() + " SET SCORE=4,LABEL='written while disconnected' WHERE ID=2");
                execute("INSERT INTO " + qualifiedTable() + " VALUES(3,3,'written while disconnected')");
                proxy.setAvailable(true);

                final Set<Integer> delivered = new HashSet<>();
                final long recoveryDeadline = System.nanoTime() + Duration.ofSeconds(90).toNanos();
                while (delivered.size() < 2 && System.nanoTime() < recoveryDeadline) {
                    final var record = nextDataRecord();
                    final var value = envelope(record);
                    final var operation = value.getString("op");
                    assertThat(operation).as("A durable streaming offset must prevent another snapshot").isNotEqualTo("r");
                    assertThat(record.sourceOffset().get(DamengOffsetContext.CAPTURE_SCOPE_KEY))
                            .isEqualTo(checkpoint.sourceOffset().get(DamengOffsetContext.CAPTURE_SCOPE_KEY));
                    final var after = value.getStruct("after");
                    final int id = after.getInt32("ID");
                    if (id == 2 && operation.equals("u")) {
                        assertThat(value.getStruct("before").getInt32("SCORE")).isEqualTo(2);
                        assertThat(after.getInt32("SCORE")).isEqualTo(4);
                        assertThat(after.getString("LABEL")).isEqualTo("written while disconnected");
                        delivered.add(2);
                    }
                    else if (id == 3) {
                        assertThat(operation).isEqualTo("c");
                        assertThat(after.getInt32("SCORE")).isEqualTo(3);
                        assertThat(after.getString("LABEL")).isEqualTo("written while disconnected");
                        delivered.add(3);
                    }
                    else {
                        // At-least-once delivery can replay the previously
                        // delivered streaming record, but no unknown record.
                        assertThat(id).isEqualTo(2);
                        assertThat(operation).isEqualTo("c");
                        assertThat(after.getInt32("SCORE")).isEqualTo(2);
                        assertThat(after.getString("LABEL")).isEqualTo("before network failure");
                    }
                }
                assertThat(delivered).containsExactlyInAnyOrder(2, 3);
                assertThat(failure.get()).isNull();
                proxy.checkHealthy();
            }
            finally {
                // Close the task before its proxy to avoid injecting a second
                // unintended fault during resource cleanup.
                stopConnector();
            }
        }
    }

    private void waitForRejectedConnections(DamengTcpProxy proxy, int expected) {
        Awaitility.await().atMost(Duration.ofSeconds(60)).untilAsserted(() -> {
            assertThat(failure.get()).as("Connection failures must remain recoverable").isNull();
            assertThat(proxy.rejectedConnections()).isGreaterThanOrEqualTo(expected);
            final var retries = new AtomicInteger(-1);
            engine.runWithTask(task -> retries.set(((DamengConnectorTask) task).getErrorHandler()
                    .map(handler -> handler.getRetries()).orElse(-1)));
            assertThat(retries.get()).as("The task must retry, rather than only the JDBC driver").isGreaterThanOrEqualTo(3);
        });
    }

    private void waitForDurableOffset(SourceRecord checkpoint, Configuration engineConfig) {
        // A read concurrent with FileOffsetBackingStore's rewrite can see a
        // truncated serialization. Retry only that bounded transient condition.
        Awaitility.await().atMost(Duration.ofSeconds(30))
                .ignoreExceptionsMatching(error -> error instanceof ConnectException
                        && (error.getCause() instanceof EOFException || error.getCause() instanceof StreamCorruptedException))
                .untilAsserted(() -> {
                    final var stored = readLastCommittedOffset(engineConfig, checkpoint.sourcePartition());
                    assertThat(stored).isNotNull();
                    assertThat(((Number) stored.get("commit_lsn")).longValue())
                            .isGreaterThanOrEqualTo(((Number) checkpoint.sourceOffset().get("commit_lsn")).longValue());
                    assertThat(stored.get(DamengOffsetContext.CAPTURE_SCOPE_KEY))
                            .isEqualTo(checkpoint.sourceOffset().get(DamengOffsetContext.CAPTURE_SCOPE_KEY));
                });
    }

    private List<Session> sessions() throws SQLException {
        final List<Session> sessions = new ArrayList<>();
        try (var statement = admin.prepareStatement("SELECT SESS_ID, SESS_SEQ FROM V$SESSIONS WHERE USER_NAME=?")) {
            statement.setString(1, username);
            try (var result = statement.executeQuery()) {
                while (result.next()) {
                    sessions.add(new Session(result.getLong(1), result.getLong(2)));
                }
            }
        }
        return sessions;
    }

    private SourceRecord nextDataRecord() throws Exception {
        final long deadline = System.nanoTime() + Duration.ofSeconds(90).toNanos();
        while (System.nanoTime() < deadline) {
            if (failure.get() != null) {
                throw new AssertionError("Dameng task did not recover after session disconnect", failure.get());
            }
            if (waitForAvailableRecords(1, TimeUnit.SECONDS)) {
                final var record = consumeRecord();
                if (record.topic().equals("dameng." + DamengTestDatabase.schema() + "." + table)
                        && record.value() instanceof Struct value && value.schema().field("op") != null) {
                    value.validate();
                    return record;
                }
            }
        }
        throw new AssertionError("No Dameng recovery event within 90 seconds");
    }

    private void execute(String sql) throws SQLException {
        try (var statement = admin.createStatement()) {
            statement.execute(sql);
        }
        catch (SQLException e) {
            final String operation = sql.startsWith("CREATE USER") ? "CREATE USER " + username : sql;
            throw new SQLException("Dameng test setup/action failed: " + operation, e.getSQLState(), e.getErrorCode(), e);
        }
    }

    private String qualifiedTable() {
        return '"' + DamengTestDatabase.schema() + "\".\"" + table + '"';
    }

    private static Struct envelope(SourceRecord record) {
        return (Struct) record.value();
    }

    private record Session(long id, long sequence) {
    }
}
