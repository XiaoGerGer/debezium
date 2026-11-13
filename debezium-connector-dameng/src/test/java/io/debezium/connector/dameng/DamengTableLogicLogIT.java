/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.nio.file.Path;
import java.sql.DriverManager;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.debezium.config.Configuration;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;
import io.debezium.storage.file.history.FileSchemaHistory;

/** Verifies table-level redo and online DDL when the database does not log every table. */
class DamengTableLogicLogIT extends AbstractAsyncEngineConnectorTest {
    @TempDir
    Path directory;

    private final AtomicReference<Throwable> connectorFailure = new AtomicReference<>();

    @BeforeAll
    static void startDatabase() throws Exception {
        assumeTrue(System.getProperty("dameng.hostname") == null, "The test must own the database with RLOG_IGNORE_TABLE_SET=0");
        assumeTrue(DamengTestDatabase.requestedCompatibilityMode() == 0, "The connector supports native mode only");
        DamengTestDatabase.start(0, 0, 1, 0);
    }

    @AfterAll
    static void stopDatabase() {
        DamengTestDatabase.stop();
    }

    @Test
    void shouldCaptureAfterEnablingLogicLogOnAnOnlineTableAndAfterRestart() throws Exception {
        final String table = "DBZ_LOG_" + UUID.randomUUID().toString().replace("-", "").substring(0, 12).toUpperCase();
        final String qualified = DamengTestDatabase.schema() + "." + table;
        final var config = Configuration.create()
                .with("topic.prefix", "dameng")
                .with("database.hosts", DamengTestDatabase.hosts())
                .with("database.user", DamengTestDatabase.user()).with("database.password", DamengTestDatabase.password())
                .with("database.dbname", "DAMENG").with("table.include.list", qualified.replace(".", "\\."))
                .with("schema.history.internal", FileSchemaHistory.class.getName())
                .with("schema.history.internal.file.filename", directory.resolve("history.dat").toString())
                .with("snapshot.mode", "initial").with("snapshot.locking.mode", "shared").with("poll.interval.ms", 100)
                .with("include.schema.changes", false).with("tombstones.on.delete", false).build();
        try (var jdbc = DriverManager.getConnection(DamengTestDatabase.jdbcUrl(), DamengTestDatabase.user(), DamengTestDatabase.password())) {
            jdbc.setAutoCommit(true);
            try {
                execute(jdbc, "CREATE TABLE " + qualified + " (ID INT PRIMARY KEY, V VARCHAR(80))");
                execute(jdbc, "INSERT INTO " + qualified + " VALUES(1,'snapshot')");
                startEngine(config);
                final var snapshot = nextDataRecord();
                assertThat(snapshot.getString("op")).isEqualTo("r");
                assertThat(snapshot.getStruct("after").getInt32("ID")).isEqualTo(1);
                waitForStreamingRunning("dameng", "dameng");

                execute(jdbc, "ALTER TABLE " + qualified + " ADD LOGIC LOG");
                execute(jdbc, "INSERT INTO " + qualified + " VALUES(2,'streamed')");
                final var streamed = nextDataRecord();
                assertThat(streamed.getString("op")).isEqualTo("c");
                assertThat(streamed.getStruct("after").getInt32("ID")).isEqualTo(2);
                assertThat(streamed.getStruct("after").getString("V")).isEqualTo("streamed");

                stopConnector();
                startEngine(config);
                waitForStreamingRunning("dameng", "dameng");
                execute(jdbc, "INSERT INTO " + qualified + " VALUES(3,'restarted')");
                final var restarted = nextDataRecord();
                assertThat(restarted.getString("op")).isEqualTo("c");
                assertThat(restarted.getStruct("after").getInt32("ID")).isEqualTo(3);
                assertThat(restarted.getStruct("after").getString("V")).isEqualTo("restarted");
            }
            finally {
                stopConnector();
                execute(jdbc, "DROP TABLE " + qualified);
            }
        }
    }

    private static void execute(java.sql.Connection jdbc, String sql) throws Exception {
        try (var statement = jdbc.createStatement()) {
            statement.execute(sql);
        }
    }

    private void startEngine(Configuration config) {
        connectorFailure.set(null);
        start(DamengConnector.class, config, (success, message, error) -> {
            if (!success) {
                connectorFailure.set(error == null ? new IllegalStateException(message) : error);
            }
        }, null);
    }

    private Struct nextDataRecord() throws Exception {
        final long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
        while (System.nanoTime() < deadline) {
            assertThat(connectorFailure.get()).as("Connector failure").isNull();
            if (waitForAvailableRecords(1, TimeUnit.SECONDS)) {
                final var record = consumeRecord();
                if (record != null && record.value() instanceof Struct envelope && envelope.schema().field("op") != null) {
                    envelope.validate();
                    return envelope;
                }
            }
        }
        throw new AssertionError("No CDC event in 60 seconds");
    }
}
