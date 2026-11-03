/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.debezium.config.Configuration;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;
import io.debezium.storage.file.history.FileSchemaHistory;

class DamengBitmapJoinIndexFilterIT extends AbstractAsyncEngineConnectorTest {
    private static final String SCHEMA = "DBZ_BMJ";
    private static final String CREATE_INDEX = "CREATE BITMAP INDEX BI ON DBZ_BMJ.FACT(D.V)"
            + " FROM DBZ_BMJ.FACT F, DBZ_BMJ.DIMENSION D WHERE F.ID=D.ID";
    @TempDir
    Path directory;
    private final AtomicReference<Throwable> failure = new AtomicReference<>();
    private Connection writer;
    private Configuration config;

    @BeforeAll
    static void startDatabase() throws Exception {
        assumeTrue(System.getProperty("dameng.hostname") == null, "Bitmap join index fixtures require a test-owned database");
        assumeTrue(DamengTestDatabase.requestedCompatibilityMode() == 0, "Bitmap join index fixtures use COMPATIBLE_MODE=0");
        DamengTestDatabase.start();
    }

    @AfterAll
    static void stopDatabase() {
        DamengTestDatabase.stop();
    }

    @AfterEach
    void closeConnectorAndWriter() throws Exception {
        try {
            stopConnector();
        }
        finally {
            if (writer != null) {
                writer.close();
            }
        }
    }

    @Test
    void shouldExcludeBitmapStorageDuringSnapshotStreamingAndRestart() throws Exception {
        writer = DriverManager.getConnection(DamengTestDatabase.jdbcUrl(), DamengTestDatabase.user(), DamengTestDatabase.password());
        execute("CREATE SCHEMA " + SCHEMA);
        for (final var name : List.of("FACT", "DIMENSION", "MARKER")) {
            execute("CREATE TABLE " + SCHEMA + '.' + name + "(ID INT NOT CLUSTER PRIMARY KEY,V VARCHAR(40))");
            execute("INSERT INTO " + SCHEMA + '.' + name + " VALUES(1,'initial')");
        }
        execute(CREATE_INDEX);
        try (var statement = writer.createStatement();
                var rows = statement.executeQuery("SELECT COUNT(*) FROM DBZ_BMJ.\"BMJ$_BI\"")) {
            assertThat(rows.next()).isTrue();
            assertThat(rows.getLong(1)).as("the real auxiliary table is populated before snapshot").isPositive();
        }
        config = Configuration.create().with("topic.prefix", "dameng")
                .with("database.hostname", DamengTestDatabase.hostname()).with("database.port", DamengTestDatabase.port())
                .with("database.user", DamengTestDatabase.user()).with("database.password", DamengTestDatabase.password())
                .with("database.dbname", System.getProperty("dameng.database", "DAMENG"))
                .with("schema.include.list", SCHEMA)
                .with("schema.history.internal", FileSchemaHistory.class.getName())
                .with("schema.history.internal.file.filename", directory.resolve("history.dat").toString())
                .with("snapshot.mode", "initial").with("poll.interval.ms", 100).with("offset.flush.interval.ms", 100)
                .with("include.schema.changes", false).with("tombstones.on.delete", false).build();
        // Select the whole schema so the built-in filter must exclude the actual BMJ$_ table.
        startEngine();
        final List<String> snapshots = new ArrayList<>();
        for (int index = 0; index < 3; index++) {
            snapshots.add(signature(nextDataRecord()));
        }
        assertThat(snapshots).containsExactlyInAnyOrder("FACT:r:1:initial", "DIMENSION:r:1:initial", "MARKER:r:1:initial");

        execute("DROP INDEX DBZ_BMJ.BI");
        execute("UPDATE DBZ_BMJ.FACT SET V='online' WHERE ID=1");
        execute(CREATE_INDEX);
        execute("DROP INDEX DBZ_BMJ.BI");
        execute("INSERT INTO DBZ_BMJ.MARKER VALUES(2,'checkpoint')");
        assertThat(signature(nextDataRecord())).isEqualTo("FACT:u:1:online");
        final var checkpoint = nextDataRecord();
        assertThat(signature(checkpoint)).isEqualTo("MARKER:c:2:checkpoint");
        stopConnector();
        storeOffsets(config, Map.of(checkpoint.sourcePartition(), checkpoint.sourceOffset()));
        consumedLines.clear();

        // The auxiliary object no longer exists when its CREATE/DROP redo is replayed.
        execute(CREATE_INDEX);
        execute("DROP INDEX DBZ_BMJ.BI");
        execute("UPDATE DBZ_BMJ.FACT SET V='offline' WHERE ID=1");
        execute("INSERT INTO DBZ_BMJ.MARKER VALUES(3,'replayed')");
        startEngine();
        assertThat(signature(nextDataRecord())).isEqualTo("FACT:u:1:offline");
        assertThat(signature(nextDataRecord())).isEqualTo("MARKER:c:3:replayed");
        stopConnector();
        assertThat(java.nio.file.Files.readString(directory.resolve("history.dat"))).doesNotContain("BMJ$_");
    }

    private void startEngine() throws Exception {
        failure.set(null);
        start(DamengConnector.class, config, (success, message, error) -> {
            if (!success) {
                failure.set(error == null ? new IllegalStateException(message) : error);
            }
        }, null);
        Awaitility.await("bitmap join index filter connector starts streaming").atMost(Duration.ofSeconds(60))
                .pollInterval(Duration.ofMillis(100)).until(() -> {
                    if (failure.get() != null) {
                        throw new AssertionError("Connector failed during startup", failure.get());
                    }
                    return isStreamingRunning("dameng", "dameng");
                });
    }

    private SourceRecord nextDataRecord() throws Exception {
        final long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
        while (System.nanoTime() < deadline) {
            assertThat(failure.get()).as("connector remains healthy").isNull();
            if (waitForAvailableRecords(100, TimeUnit.MILLISECONDS)) {
                final var record = consumeRecord();
                if (record != null && record.value() instanceof Struct value && value.schema().field("op") != null) {
                    assertThat(record.topic()).isIn("dameng.DBZ_BMJ.FACT", "dameng.DBZ_BMJ.DIMENSION", "dameng.DBZ_BMJ.MARKER");
                    assertThat(value.getStruct("after").schema().fields()).extracting(field -> field.name()).containsExactly("ID", "V");
                    return record;
                }
            }
        }
        throw new AssertionError("No business-table event within 60 seconds; connector failure=" + failure.get());
    }

    private static String signature(SourceRecord record) {
        final var envelope = (Struct) record.value();
        final var row = envelope.getStruct("after");
        return record.topic().substring(record.topic().lastIndexOf('.') + 1) + ':' + envelope.getString("op") + ':'
                + row.getInt32("ID") + ':' + row.getString("V");
    }

    private void execute(String sql) throws SQLException {
        try (var statement = writer.createStatement()) {
            statement.setQueryTimeout(15);
            statement.execute(sql);
        }
    }
}
