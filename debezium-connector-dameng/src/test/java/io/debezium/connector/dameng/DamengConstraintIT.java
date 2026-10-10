/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.debezium.config.Configuration;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;
import io.debezium.storage.file.history.FileSchemaHistory;

/** Exercises named constraints that exist before the initial snapshot and are dropped during downtime. */
class DamengConstraintIT extends AbstractAsyncEngineConnectorTest {
    @TempDir
    Path directory;

    private Connection connection;
    private String table;
    private String parentTable;
    private Configuration config;
    private final AtomicReference<Throwable> failure = new AtomicReference<>();

    @BeforeAll
    static void startDatabase() throws Exception {
        DamengTestDatabase.start();
    }

    @AfterAll
    static void stopDatabase() {
        DamengTestDatabase.stop();
    }

    @BeforeEach
    void before() throws Exception {
        connection = DriverManager.getConnection(DamengTestDatabase.jdbcUrl(), DamengTestDatabase.user(), DamengTestDatabase.password());
        final String prefix = "DBZ_CONS_" + UUID.randomUUID().toString().replace("-", "").substring(0, 12).toUpperCase();
        table = prefix;
        parentTable = prefix + "_P";
        execute("CREATE TABLE " + qualified(parentTable) + " (ID INT PRIMARY KEY)");
        execute("CREATE TABLE " + qualified(table) + " (ID INT NOT NULL, UNIQUE_ID INT, PARENT_ID INT, SCORE INT, LABEL VARCHAR(50), "
                + "CONSTRAINT " + constraint("PK") + " PRIMARY KEY(ID), "
                + "CONSTRAINT " + constraint("UK") + " UNIQUE(UNIQUE_ID), "
                + "CONSTRAINT " + constraint("FK") + " FOREIGN KEY(PARENT_ID) REFERENCES " + qualified(parentTable) + "(ID), "
                + "CONSTRAINT " + constraint("CK") + " CHECK(SCORE>0))");
        execute("INSERT INTO " + qualified(parentTable) + " VALUES(1)");
        execute("INSERT INTO " + qualified(table) + " VALUES(1,10,1,1,'snapshot')");
        config = Configuration.create().with("topic.prefix", "dameng")
                .with("database.hostname", DamengTestDatabase.hostname())
                .with("database.port", DamengTestDatabase.port())
                .with("database.user", DamengTestDatabase.user())
                .with("database.password", DamengTestDatabase.password())
                .with("database.dbname", System.getProperty("dameng.database", "DAMENG"))
                .with("table.include.list", DamengTestDatabase.schema() + "\\." + table)
                .with("schema.history.internal", FileSchemaHistory.class.getName())
                .with("schema.history.internal.file.filename", directory.resolve("schema-history.dat").toString())
                .with("snapshot.mode", "initial").with("poll.interval.ms", 100)
                .with("include.schema.changes", false).with("tombstones.on.delete", false).build();
    }

    @AfterEach
    void after() throws Exception {
        stopConnector();
        if (connection != null) {
            try {
                execute("DROP TABLE IF EXISTS " + qualified(table));
                execute("DROP TABLE IF EXISTS " + qualified(parentTable));
            }
            finally {
                connection.close();
            }
        }
    }

    @Test
    void shouldRecoverNamedSnapshotConstraintsAndStreamTheirRemoval() throws Exception {
        startEngine();
        final var snapshot = nextDataRecord();
        assertThat(((Struct) snapshot.value()).getString("op")).isEqualTo("r");
        assertThat(((Struct) snapshot.key()).getInt32("ID")).isEqualTo(1);
        execute("INSERT INTO " + qualified(table) + " VALUES(2,20,1,2,'before stop')");
        final var beforeStop = nextDataRecord();
        assertThat(((Struct) beforeStop.key()).getInt32("ID")).isEqualTo(2);
        stopConnector();
        storeOffsets(config, Map.of(beforeStop.sourcePartition(), beforeStop.sourceOffset()));
        consumedLines.clear();

        // Current metadata has no constraints at restart. Historical attributes
        // must resolve every DROP without replacing the recovered table shape.
        for (String kind : new String[]{ "FK", "UK", "CK", "PK" }) {
            execute("ALTER TABLE " + qualified(table) + " DROP CONSTRAINT " + constraint(kind));
        }
        execute("INSERT INTO " + qualified(table) + " VALUES(3,10,999,-1,'after drops')");

        startEngine();
        final var afterDrops = nextDataRecord();
        assertThat(afterDrops.key()).isNull();
        final var value = (Struct) afterDrops.value();
        assertThat(value.getString("op")).isEqualTo("c");
        assertThat(value.getStruct("after").getInt32("ID")).isEqualTo(3);
        assertThat(value.getStruct("after").getInt32("UNIQUE_ID")).isEqualTo(10);
        assertThat(value.getStruct("after").getInt32("PARENT_ID")).isEqualTo(999);
        assertThat(value.getStruct("after").getInt32("SCORE")).isEqualTo(-1);

        stopConnector();
        storeOffsets(config, Map.of(afterDrops.sourcePartition(), afterDrops.sourceOffset()));
        consumedLines.clear();
        execute("INSERT INTO " + qualified(table) + " VALUES(4,10,999,-2,'second restart')");
        startEngine();
        final var restored = nextDataRecord();
        assertThat(restored.key()).isNull();
        assertThat(((Struct) restored.value()).getStruct("after").getInt32("ID")).isEqualTo(4);
    }

    @Test
    void shouldNotPromoteAUniqueConstraintToAnEventKeyWhenNoPrimaryKeyExists() throws Exception {
        execute("ALTER TABLE " + qualified(table) + " DROP CONSTRAINT " + constraint("PK"));
        startEngine();
        final var snapshot = nextDataRecord();
        assertThat(((Struct) snapshot.value()).getString("op")).isEqualTo("r");
        assertThat(snapshot.key()).as("A UNIQUE constraint is not an event primary key").isNull();
        execute("INSERT INTO " + qualified(table) + " VALUES(2,20,1,2,'before unique drop')");
        final var beforeStop = nextDataRecord();
        assertThat(beforeStop.key()).isNull();
        stopConnector();
        storeOffsets(config, Map.of(beforeStop.sourcePartition(), beforeStop.sourceOffset()));
        consumedLines.clear();

        execute("ALTER TABLE " + qualified(table) + " DROP CONSTRAINT " + constraint("UK"));
        execute("INSERT INTO " + qualified(table) + " VALUES(3,10,1,3,'duplicate unique value')");
        startEngine();
        final var duplicate = nextDataRecord();
        assertThat(duplicate.key()).isNull();
        assertThat(((Struct) duplicate.value()).getString("op")).isEqualTo("c");
        assertThat(((Struct) duplicate.value()).getStruct("after").getInt32("UNIQUE_ID")).isEqualTo(10);
        assertThat(((Struct) duplicate.value()).getStruct("after").getInt32("ID")).isEqualTo(3);
    }

    private void startEngine() throws Exception {
        failure.set(null);
        start(DamengConnector.class, config, (success, message, error) -> {
            if (!success) {
                failure.set(error == null ? new IllegalStateException(message) : error);
            }
        }, null);
        waitForStreamingRunning("dameng", "dameng");
    }

    private SourceRecord nextDataRecord() throws Exception {
        final long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
        while (System.nanoTime() < deadline) {
            if (failure.get() != null) {
                throw new AssertionError("Dameng named constraint connector failed", failure.get());
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
        throw new AssertionError("No Dameng named constraint event within 60 seconds");
    }

    private void execute(String sql) throws SQLException {
        try (var statement = connection.createStatement()) {
            statement.execute(sql);
        }
    }

    private String constraint(String kind) {
        return '"' + table + '_' + kind + '"';
    }

    private static String qualified(String table) {
        return '"' + DamengTestDatabase.schema() + "\".\"" + table + '"';
    }
}
