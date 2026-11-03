/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.debezium.config.Configuration;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;
import io.debezium.storage.file.history.FileSchemaHistory;

/** Replays native binary defaults and JSON constraints while unrelated view DDL passes through the live engine. */
class DamengAntlrStreamingIT extends AbstractAsyncEngineConnectorTest {
    @TempDir
    Path directory;

    private final AtomicReference<Throwable> failure = new AtomicReference<>();
    private Connection jdbc;
    private Configuration config;
    private String table;
    private String control;
    private String view;

    @BeforeAll
    static void startDatabase() throws Exception {
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
                execute("DROP VIEW IF EXISTS " + view);
                execute("DROP TABLE IF EXISTS " + table);
                execute("DROP TABLE IF EXISTS " + control);
            }
            finally {
                jdbc.close();
            }
        }
    }

    @Test
    void shouldReplayBinaryDefaultsAndJsonChecksAcrossDowntimeWithoutBlockingOnComplexViews() throws Exception {
        final var prefix = "DBZ_ANTLR_" + UUID.randomUUID().toString().replace("-", "").substring(0, 10).toUpperCase();
        final var schema = DamengTestDatabase.schema();
        control = schema + "." + prefix + "_CONTROL";
        table = schema + "." + prefix + "_DATA";
        view = schema + "." + prefix + "_VIEW";
        final var constraint = prefix + "_JSON";
        final var history = directory.resolve("history.dat");
        jdbc = DriverManager.getConnection(DamengTestDatabase.jdbcUrl(), DamengTestDatabase.user(), DamengTestDatabase.password());
        jdbc.setAutoCommit(true);
        execute("CREATE TABLE " + control + "(ID INT PRIMARY KEY)");
        execute("INSERT INTO " + control + " VALUES(0)");
        config = Configuration.create()
                .with("topic.prefix", "dameng")
                .with("database.hostname", DamengTestDatabase.hostname()).with("database.port", DamengTestDatabase.port())
                .with("database.user", DamengTestDatabase.user()).with("database.password", DamengTestDatabase.password())
                .with("database.dbname", System.getProperty("dameng.database", "DAMENG"))
                .with("table.include.list", Pattern.quote(schema) + "\\." + prefix + ".*")
                .with("schema.history.internal", FileSchemaHistory.class.getName())
                .with("schema.history.internal.file.filename", history.toString())
                .with("snapshot.mode", "initial").with("poll.interval.ms", 100)
                .with("include.schema.changes", false).with("tombstones.on.delete", false).build();
        startEngine();
        next(control, "r", 0);
        waitForStreamingRunning("dameng", "dameng");

        // This definition is absent from the snapshot and must be learned from actual DDL redo.
        execute("CREATE TABLE " + table + "(ID INT PRIMARY KEY,PAYLOAD VARBINARY(4) DEFAULT 0xF,FIXED BINARY(4) DEFAULT 0xF,"
                + "DOC VARCHAR(200) DEFAULT '{\"phase\":1}',CONSTRAINT " + constraint
                + " CHECK(DOC IS JSON (STRICT) WITH UNIQUE KEYS))");
        execute("CREATE VIEW " + view + " AS WITH R AS (SELECT ID,ROW_NUMBER() OVER(ORDER BY ID) RN FROM " + table + ") "
                + "SELECT ID,CASE WHEN RN=1 THEN 'first' ELSE 'later' END LABEL FROM R");
        execute("INSERT INTO " + table + "(ID) VALUES(1)");
        final var inserted = after(next(table, "c", 1));
        assertThat(binary(inserted, "PAYLOAD")).containsExactly((byte) 0x0f);
        assertThat(inserted.getString("DOC")).isEqualTo("{\"phase\":1}");
        assertDefault(inserted, "PAYLOAD", new byte[]{ 0x0f });
        assertThat(binary(inserted, "FIXED")).containsExactly((byte) 0x0f, (byte) 0, (byte) 0, (byte) 0);
        assertDefault(inserted, "FIXED", new byte[]{ 0x0f, 0, 0, 0 });

        execute("UPDATE " + table + " SET PAYLOAD=0x007f80ff,DOC='{\"phase\":2}' WHERE ID=1");
        final var updated = next(table, "u", 1);
        assertThat(binary(before(updated), "PAYLOAD")).containsExactly((byte) 0x0f);
        assertThat(binary(after(updated), "PAYLOAD")).containsExactly((byte) 0, (byte) 0x7f, (byte) 0x80, (byte) 0xff);
        assertThat(after(updated).getString("DOC")).isEqualTo("{\"phase\":2}");
        checkpoint(updated);
        assertThat(java.nio.file.Files.size(history)).isPositive();

        // Queue a row under each schema while stopped. Today's metadata cannot explain the first row.
        execute("INSERT INTO " + table + "(ID) VALUES(2)");
        execute("ALTER TABLE " + table + " ALTER COLUMN PAYLOAD SET DEFAULT 0X123");
        execute("ALTER TABLE " + table + " ADD EXTRA VARBINARY(4) DEFAULT 0X80ff");
        execute("ALTER TABLE " + table + " DROP CONSTRAINT " + constraint);
        execute("ALTER TABLE " + table + " ADD CONSTRAINT " + constraint + " CHECK(DOC IS JSON LAX WITHOUT UNIQUE KEYS)");
        execute("INSERT INTO " + table + "(ID) VALUES(3)");
        startEngine();
        final var oldSchema = after(next(table, "c", 2));
        assertThat(oldSchema.schema().field("EXTRA")).isNull();
        assertThat(binary(oldSchema, "PAYLOAD")).containsExactly((byte) 0x0f);
        assertDefault(oldSchema, "PAYLOAD", new byte[]{ 0x0f });
        assertThat(binary(oldSchema, "FIXED")).containsExactly((byte) 0x0f, (byte) 0, (byte) 0, (byte) 0);
        assertDefault(oldSchema, "FIXED", new byte[]{ 0x0f, 0, 0, 0 });
        final var recovered = next(table, "c", 3);
        final var newSchema = after(recovered);
        assertThat(binary(newSchema, "PAYLOAD")).containsExactly((byte) 0x01, (byte) 0x23);
        assertThat(binary(newSchema, "EXTRA")).containsExactly((byte) 0x80, (byte) 0xff);
        assertDefault(newSchema, "PAYLOAD", new byte[]{ 0x01, 0x23 });
        assertDefault(newSchema, "EXTRA", new byte[]{ (byte) 0x80, (byte) 0xff });
        assertThat(binary(newSchema, "FIXED")).containsExactly((byte) 0x0f, (byte) 0, (byte) 0, (byte) 0);
        assertDefault(newSchema, "FIXED", new byte[]{ 0x0f, 0, 0, 0 });
        checkpoint(recovered);

        // A second start recovers the persisted history rather than replaying these DDL records again.
        startEngine();
        waitForStreamingRunning("dameng", "dameng");
        execute("UPDATE " + table + " SET PAYLOAD=0x00ff,DOC='{\"phase\":3}' WHERE ID=3");
        final var restartedUpdate = next(table, "u", 3);
        assertThat(after(restartedUpdate).schema()).isEqualTo(newSchema.schema());
        assertThat(binary(before(restartedUpdate), "PAYLOAD")).containsExactly((byte) 0x01, (byte) 0x23);
        assertThat(binary(after(restartedUpdate), "PAYLOAD")).containsExactly((byte) 0, (byte) 0xff);
        assertDefault(after(restartedUpdate), "PAYLOAD", new byte[]{ 0x01, 0x23 });
        assertThat(binary(after(restartedUpdate), "FIXED")).containsExactly((byte) 0x0f, (byte) 0, (byte) 0, (byte) 0);
        assertDefault(after(restartedUpdate), "FIXED", new byte[]{ 0x0f, 0, 0, 0 });
        execute("DELETE FROM " + table + " WHERE ID=3");
        final var deleted = next(table, "d", 3);
        assertThat(binary(before(deleted), "PAYLOAD")).containsExactly((byte) 0, (byte) 0xff);
        assertThat(((Struct) deleted.value()).get("after")).isNull();
        assertThat(failure.get()).as("connector failure after view DDL and history recovery").isNull();
    }

    private void checkpoint(SourceRecord record) throws Exception {
        stopConnector();
        storeOffsets(config, Map.of(record.sourcePartition(), record.sourceOffset()));
        consumedLines.clear();
    }

    private void execute(String sql) throws SQLException {
        try (var statement = jdbc.createStatement()) {
            statement.execute(sql);
        }
    }

    private void startEngine() {
        failure.set(null);
        start(DamengConnector.class, config, (success, message, error) -> {
            if (!success) {
                failure.set(error == null ? new IllegalStateException(message) : error);
            }
        }, null);
    }

    private SourceRecord next(String qualifiedTable, String operation, int id) throws Exception {
        final long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
        while (System.nanoTime() < deadline) {
            assertThat(failure.get()).as("connector failure").isNull();
            if (waitForAvailableRecords(1, TimeUnit.SECONDS)) {
                final var record = consumeRecord();
                if (record != null && record.topic().equals("dameng." + qualifiedTable) && record.value() instanceof Struct envelope) {
                    envelope.validate();
                    assertThat(envelope.getString("op")).isEqualTo(operation);
                    final var row = envelope.getStruct(operation.equals("d") ? "before" : "after");
                    assertThat(row.getInt32("ID")).isEqualTo(id);
                    return record;
                }
            }
        }
        throw new AssertionError("No " + operation + " event for " + qualifiedTable + " ID=" + id + " in 60 seconds");
    }

    private static Struct before(SourceRecord record) {
        return ((Struct) record.value()).getStruct("before");
    }

    private static Struct after(SourceRecord record) {
        return ((Struct) record.value()).getStruct("after");
    }

    private static byte[] binary(Struct row, String name) {
        final var buffer = ((ByteBuffer) row.get(name)).duplicate();
        final var bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return bytes;
    }

    private static void assertDefault(Struct row, String name, byte[] expected) {
        assertThat(row.schema().field(name).schema().defaultValue()).isEqualTo(ByteBuffer.wrap(expected));
    }
}
