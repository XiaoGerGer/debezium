/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.sql.Connection;
import java.time.Duration;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Deployment regression for the packaged plugin in a real distributed Connect
 * worker. Kafka owns schema history and offsets; a replacement worker has no local
 * state from its predecessor. Delivery assertions allow at-least-once duplicates.
 */
class DamengKafkaConnectIT {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final HttpClient HTTP = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
    private static final Duration TIMEOUT = Duration.ofMinutes(2);
    private static final String CONNECTOR_CLASS = "io.debezium.connector.dameng.DamengConnector";

    private final String suffix = UUID.randomUUID().toString().replace("-", "").substring(0, 12);
    private final String connectorName = "dameng-connect-" + suffix;
    private final String prefix = "dameng_" + suffix;
    private final String table = "DBZ_CONNECT_" + suffix.toUpperCase();
    private final String topic = prefix + ".SYSDBA." + table;
    private final String historyTopic = prefix + ".history";
    private final Map<String, ExpectedChange> knownEvents = new HashMap<>();
    private long latestCommitLsn;

    @Test
    void shouldResumeDurableRecoveryBaselineAfterWorkerKillBeforeAnyOffsetCommit() throws Exception {
        try (var environment = new DamengKafkaConnectEnvironment()) {
            // Deliberately keep source offsets absent. Kafka schema history must
            // durably fix the cut even if the very first worker is killed.
            environment.setOffsetFlushIntervalMillis(3_600_000);
            environment.start();
            try (var database = environment.databaseConnection(); var consumer = consumer(environment, topic)) {
                execute(database, "CREATE TABLE " + qualifiedTable() + " (ID INT PRIMARY KEY, LABEL VARCHAR(100))");
                execute(database, "INSERT INTO " + qualifiedTable() + " VALUES(1,'skipped before recovery')");
                database.commit();
                registerConnector(environment, Map.of("recovery.restart.nonce", "incident-first"));
                awaitRunning(environment);
                final var baseline = awaitRecoveryBaseline(environment);
                assertThat(baseline.path("recovery_nonce").asText()).isEqualTo("incident-first");
                execute(database, "INSERT INTO " + qualifiedTable() + " VALUES(2,'after baseline')");
                database.commit();
                awaitChanges(consumer, false, change("c", 2, null, "after baseline"));
                environment.killWorker();
                try (var offsets = consumer(environment, DamengKafkaConnectEnvironment.OFFSET_TOPIC)) {
                    final var partitions = offsets.partitionsFor(DamengKafkaConnectEnvironment.OFFSET_TOPIC).stream()
                            .map(info -> new org.apache.kafka.common.TopicPartition(info.topic(), info.partition())).toList();
                    assertThat(partitions).isNotEmpty();
                    assertThat(offsets.endOffsets(partitions).values()).as("no source offset was ever written before SIGKILL").containsOnly(0L);
                }
                execute(database, "ALTER TABLE " + qualifiedTable() + " ADD EXTRA VARCHAR(40)");
                execute(database, "INSERT INTO " + qualifiedTable() + " VALUES(3,'while stopped','new schema')");
                database.commit();
                environment.startWorker();
                awaitRunning(environment);
                awaitChanges(consumer, false, new ExpectedChange("c", 3, null, "while stopped", null, "new schema"));
                assertThat(awaitRecoveryBaseline(environment)).isEqualTo(baseline);
                // Removing the incident parameter is not a request for another cut.
                final var response = request(environment, "GET", "/connectors/" + connectorName + "/config", null);
                final var updated = (com.fasterxml.jackson.databind.node.ObjectNode) JSON.readTree(response.body());
                updated.remove("recovery.restart.nonce");
                assertThat(request(environment, "PUT", "/connectors/" + connectorName + "/config", updated.toString()).statusCode()).isEqualTo(200);
                execute(database, "INSERT INTO " + qualifiedTable() + " VALUES(4,'nonce removed','same generation')");
                database.commit();
                awaitChanges(consumer, false, new ExpectedChange("c", 4, null, "nonce removed", null, "same generation"));
                assertOnlyKnownReplays(consumer);
                awaitRunning(environment);
            }
        }
    }

    private JsonNode awaitRecoveryBaseline(DamengKafkaConnectEnvironment environment) throws Exception {
        try (var consumer = consumer(environment, historyTopic)) {
            final long deadline = System.nanoTime() + TIMEOUT.toNanos();
            while (System.nanoTime() < deadline) {
                for (var record : consumer.poll(Duration.ofMillis(200))) {
                    final var position = JSON.readTree(record.value()).path("position");
                    if (position.has("dameng_recovery_baseline")) {
                        assertThat(position.path("recovery_baseline_lsn").asLong()).isPositive();
                        return position;
                    }
                }
            }
            throw new AssertionError("Recovery baseline was not persisted in Kafka schema history");
        }
    }

    @Test
    void shouldRecoverPackagedConnectorAfterTaskRestartAndWorkerKill() throws Exception {
        try (var environment = new DamengKafkaConnectEnvironment()) {
            environment.start();
            assertPluginIsLoaded(environment);
            try (var database = environment.databaseConnection();
                    var consumer = consumer(environment, topic)) {
                execute(database, "CREATE TABLE " + qualifiedTable() + " (ID INT PRIMARY KEY, LABEL VARCHAR(100))");
                execute(database, "INSERT INTO " + qualifiedTable() + " VALUES(1,'snapshot')");
                database.commit();
                registerConnector(environment);
                awaitRunning(environment);

                awaitChanges(consumer, true, change("r", 1, null, "snapshot"));
                execute(database, "INSERT INTO " + qualifiedTable() + " VALUES(2,'streamed')");
                execute(database, "UPDATE " + qualifiedTable() + " SET LABEL='before restart' WHERE ID=1");
                database.commit();
                awaitChanges(consumer, false, change("c", 2, null, "streamed"), change("u", 1, "snapshot", "before restart"));
                awaitKafkaHistory(environment);
                awaitKafkaOffset(environment, latestCommitLsn);

                // Observe the stopped state before resuming, so the assertion
                // cannot accidentally see the old RUNNING state of a pending restart.
                final var stopped = request(environment, "PUT", "/connectors/" + connectorName + "/stop", "");
                assertThat(stopped.statusCode()).isIn(202, 204);
                Awaitility.await("connector and task stop").atMost(TIMEOUT).untilAsserted(() -> {
                    final var response = request(environment, "GET", "/connectors/" + connectorName + "/status", null);
                    assertThat(response.statusCode()).as(response.body()).isEqualTo(200);
                    final var status = JSON.readTree(response.body());
                    assertThat(status.path("connector").path("state").asText()).isEqualTo("STOPPED");
                    assertThat(status.path("tasks").size()).isZero();
                });
                final var resumed = request(environment, "PUT", "/connectors/" + connectorName + "/resume", "");
                assertThat(resumed.statusCode()).isIn(202, 204);
                awaitRunning(environment);
                execute(database, "INSERT INTO " + qualifiedTable() + " VALUES(3,'after task restart')");
                database.commit();
                awaitChanges(consumer, false, change("c", 3, null, "after task restart"));
                awaitKafkaOffset(environment, latestCommitLsn);

                // Kill only our worker. The broker and database remain available.
                // Replacing the container also removes every local worker file.
                environment.killWorker();
                execute(database, "ALTER TABLE " + qualifiedTable() + " ADD EXTRA VARCHAR(40)");
                execute(database, "UPDATE " + qualifiedTable() + " SET LABEL='while stopped', EXTRA='recovered DDL' WHERE ID=2");
                execute(database, "DELETE FROM " + qualifiedTable() + " WHERE ID=1");
                execute(database, "INSERT INTO " + qualifiedTable() + " VALUES(4,'while worker stopped','offline insert')");
                database.commit();
                environment.startWorker();
                assertPluginIsLoaded(environment);
                // Do not register again: the new worker must recover its config,
                // source offset and schema history from the existing Kafka topics.
                awaitRunning(environment);
                awaitChanges(consumer, false,
                        new ExpectedChange("u", 2, "streamed", "while stopped", null, "recovered DDL"),
                        change("d", 1, "before restart", null),
                        new ExpectedChange("c", 4, null, "while worker stopped", null, "offline insert"));
                execute(database, "INSERT INTO " + qualifiedTable() + " VALUES(5,'after replacement','live insert')");
                database.commit();
                awaitChanges(consumer, false, new ExpectedChange("c", 5, null, "after replacement", null, "live insert"));
                awaitKafkaOffset(environment, latestCommitLsn);
                assertOnlyKnownReplays(consumer);
                awaitRunning(environment);
            }
        }
    }

    @Test
    void shouldRecoverEveryRowAfterWorkerKillDuringTransactionPublication() throws Exception {
        final int transactionRows = 3_000;
        final int allowedSends = 8;
        try (var environment = new DamengKafkaConnectEnvironment()) {
            environment.start();
            try (var database = environment.databaseConnection();
                    var consumer = consumer(environment, topic)) {
                execute(database, "CREATE TABLE " + qualifiedTable() + " (ID INT PRIMARY KEY, LABEL VARCHAR(100))");
                execute(database, "INSERT INTO " + qualifiedTable() + " VALUES(1,'snapshot')");
                database.commit();
                registerConnector(environment);
                awaitRunning(environment);
                awaitChanges(consumer, true, change("r", 1, null, "snapshot"));

                execute(database, "INSERT INTO " + qualifiedTable() + " VALUES(2,'durable baseline')");
                database.commit();
                awaitChanges(consumer, false, change("c", 2, null, "durable baseline"));
                awaitKafkaHistory(environment);
                awaitKafkaOffset(environment, latestCommitLsn);
                final var baselineEndOffsets = consumer.endOffsets(consumer.assignment());
                assertThat(baselineEndOffsets).hasSize(1);

                environment.killWorker();
                environment.startGatedWorker(topic, allowedSends);
                awaitRunning(environment);

                final List<ExpectedChange> transaction = new ArrayList<>();
                try (var statement = database.prepareStatement("INSERT INTO " + qualifiedTable() + " VALUES(?,?)")) {
                    for (int row = 0; row < transactionRows; row++) {
                        final int id = row + 100;
                        final var expected = change("c", id, null, "committed transaction row " + id);
                        transaction.add(expected);
                        knownEvents.put(expected.signature(), expected);
                        statement.setInt(1, id);
                        statement.setString(2, expected.afterLabel());
                        statement.addBatch();
                    }
                    assertThat(statement.executeBatch()).hasSize(transactionRows);
                }
                database.commit();

                // Gate producer send number nine, after the database has committed
                // the whole transaction. The I/O thread can acknowledge earlier
                // sends, but the worker cannot publish the remaining rows.
                environment.awaitWorkerSendGate();
                final Set<String> deliveredBeforeKill = new HashSet<>();
                final long deadline = System.nanoTime() + TIMEOUT.toNanos();
                while (deliveredBeforeKill.isEmpty() && System.nanoTime() < deadline) {
                    for (var record : consumer.poll(Duration.ofMillis(200))) {
                        final var signature = assertChange(record, false);
                        if (knownEvents.get(signature).id() >= 100) {
                            deliveredBeforeKill.add(signature);
                        }
                    }
                }
                assertThat(deliveredBeforeKill).as("a committed transaction is only partly visible in Kafka before SIGKILL")
                        .isNotEmpty().hasSizeLessThan(transactionRows);
                environment.killWorker();
                final var interruptedEndOffsets = consumer.endOffsets(consumer.assignment());
                final long publishedRows = interruptedEndOffsets.entrySet().stream()
                        .mapToLong(entry -> entry.getValue() - baselineEndOffsets.get(entry.getKey())).sum();
                assertThat(publishedRows).as("broker offsets prove the worker was killed before the full transaction was published")
                        .isBetween(1L, (long) allowedSends);

                // A fresh worker has no gate and no previous local files. Any
                // intermediate source offsets already flushed are legitimate;
                // together, pre-crash delivery and replay must contain every row.
                environment.startWorker();
                assertPluginIsLoaded(environment);
                awaitRunning(environment);
                awaitChanges(consumer, false, transaction.stream()
                        .filter(expected -> !deliveredBeforeKill.contains(expected.signature()))
                        .toArray(ExpectedChange[]::new));

                execute(database, "INSERT INTO " + qualifiedTable() + " VALUES(10000,'after interrupted transaction')");
                database.commit();
                awaitChanges(consumer, false, change("c", 10000, null, "after interrupted transaction"));
                awaitKafkaOffset(environment, latestCommitLsn);
                assertOnlyKnownReplays(consumer);
                awaitRunning(environment);
            }
        }
    }

    @Test
    void shouldPreserveTypedCrudThroughBrokerOutageAndWorkerReassignment() throws Exception {
        try (var environment = new DamengKafkaConnectEnvironment(3)) {
            environment.start();
            try (var database = environment.databaseConnection(); var consumer = consumer(environment, topic)) {
                execute(database, "CREATE TABLE " + qualifiedTable() + " (ID INT PRIMARY KEY, LABEL VARCHAR(100), "
                        + "AMOUNT DECIMAL(30,6), COUNTER BIGINT, STAMP TIMESTAMP(6), RAW_VALUE VARBINARY(8), FIXED CHAR(8), FLAG BIT)");
                execute(database, "INSERT INTO " + qualifiedTable() + " VALUES(1,'snapshot',12345678901234567890.123456,"
                        + "9223372036854775807,TIMESTAMP'1965-02-03 12:34:56.123456',0x000180FF,'abc',1)");
                database.commit();
                final var initial = storedTypedRow(database, 1);
                registerConnector(environment, Map.of("decimal.handling.mode", "string", "binary.handling.mode", "base64"));
                awaitRunning(environment);
                final Map<String, TypedChange> known = new HashMap<>();
                awaitTypedChanges(consumer, known, List.of(new TypedChange("r", 1, null, initial)));
                final int oldLeader = environment.pauseTopicLeader(topic);
                final var changed = JSON.createObjectNode();
                try {
                    execute(database, "UPDATE " + qualifiedTable() + " SET LABEL='offline',AMOUNT=-12345678901234567890.654321,"
                            + "COUNTER=-9223372036854775808,STAMP=TIMESTAMP'2024-02-29 23:59:59.999999',RAW_VALUE=0xFF800100,FIXED='xy',FLAG=0 WHERE ID=1");
                    database.commit();
                    changed.setAll((com.fasterxml.jackson.databind.node.ObjectNode) storedTypedRow(database, 1));
                    // Replicated Kafka must elect a replacement and publish the
                    // complete source change while the former leader is paused.
                    awaitTypedChanges(consumer, known, List.of(new TypedChange("u", 1, initial, changed)));
                    Awaitility.await("Kafka elects a replacement topic leader")
                            .atMost(TIMEOUT)
                            .ignoreExceptions()
                            .untilAsserted(() -> assertThat(environment.topicLeader(topic)).isNotEqualTo(oldLeader));
                }
                finally {
                    environment.resumeBroker();
                }
                awaitKafkaOffset(environment, latestCommitLsn);

                environment.addWorker();
                awaitRunning(environment);
                final var status = JSON.readTree(request(environment, "GET", "/connectors/" + connectorName + "/status", null).body());
                environment.killAssignedWorker(status.path("tasks").get(0).path("worker_id").asText());
                // The surviving worker must recover the existing config, offset
                // and history, without registration or a new initial snapshot.
                execute(database, "UPDATE " + qualifiedTable() + " SET ID=2,LABEL='moved' WHERE ID=1");
                database.commit();
                final var moved = storedTypedRow(database, 2);
                execute(database, "INSERT INTO " + qualifiedTable() + " VALUES(3,'nullable',NULL,NULL,NULL,NULL,NULL,NULL)");
                database.commit();
                final var nullable = storedTypedRow(database, 3);
                execute(database, "DELETE FROM " + qualifiedTable() + " WHERE ID=2");
                database.commit();
                awaitRunning(environment);
                awaitTypedChanges(consumer, known, List.of(
                        new TypedChange("d", 1, changed, null), new TypedChange("c", 2, null, moved),
                        new TypedChange("c", 3, null, nullable), new TypedChange("d", 2, moved, null)));
                awaitKafkaOffset(environment, latestCommitLsn);
                assertThat(storedTypedRow(database, 3)).isEqualTo(nullable);
                try (var statement = database.createStatement(); var result = statement.executeQuery("SELECT COUNT(*) FROM " + qualifiedTable())) {
                    assertThat(result.next()).isTrue();
                    assertThat(result.getInt(1)).isEqualTo(1);
                }
            }
        }
    }

    private JsonNode storedTypedRow(Connection database, int id) throws Exception {
        final var row = JSON.createObjectNode();
        try (var statement = database.createStatement(); var result = statement.executeQuery("SELECT * FROM " + qualifiedTable() + " WHERE ID=" + id)) {
            assertThat(result.next()).isTrue();
            row.put("ID", result.getInt("ID"));
            row.put("LABEL", result.getString("LABEL"));
            row.put("AMOUNT", result.getBigDecimal("AMOUNT") == null ? null : result.getBigDecimal("AMOUNT").toPlainString());
            if (result.getObject("COUNTER") == null) {
                row.putNull("COUNTER");
            }
            else {
                row.put("COUNTER", result.getLong("COUNTER"));
            }
            if (result.getObject("STAMP") == null) {
                row.putNull("STAMP");
            }
            else {
                final var time = result.getTimestamp("STAMP").toLocalDateTime();
                row.put("STAMP", time.toEpochSecond(ZoneOffset.UTC) * 1_000_000 + time.getNano() / 1_000);
            }
            row.put("RAW_VALUE", result.getBytes("RAW_VALUE") == null ? null : Base64.getEncoder().encodeToString(result.getBytes("RAW_VALUE")));
            row.put("FIXED", result.getString("FIXED"));
            if (result.getObject("FLAG") == null) {
                row.putNull("FLAG");
            }
            else {
                row.put("FLAG", result.getBoolean("FLAG"));
            }
            assertThat(result.next()).isFalse();
        }
        return row;
    }

    private void awaitTypedChanges(KafkaConsumer<String, String> consumer, Map<String, TypedChange> known, List<TypedChange> expected) throws Exception {
        final Set<String> remaining = new HashSet<>();
        expected.forEach(change -> {
            known.put(change.signature(), change);
            remaining.add(change.signature());
        });
        final List<String> observed = new ArrayList<>();
        final boolean allowSnapshot = expected.stream().anyMatch(change -> change.operation().equals("r"));
        final long deadline = System.nanoTime() + TIMEOUT.toNanos();
        while (!remaining.isEmpty() && System.nanoTime() < deadline) {
            for (var record : consumer.poll(Duration.ofMillis(200))) {
                final var value = JSON.readTree(record.value());
                if (!allowSnapshot) {
                    assertThat(value.path("op").asText()).as("durable Kafka offsets prevent another initial snapshot").isNotEqualTo("r");
                }
                final var signature = value.path("op").asText() + ":" + JSON.readTree(record.key()).path("ID").asInt();
                final var change = known.get(signature);
                assertThat(change).as("unexpected typed event %s", value).isNotNull();
                assertThat(value.path("before")).isEqualTo(change.before() == null ? JSON.getNodeFactory().nullNode() : change.before());
                assertThat(value.path("after")).isEqualTo(change.after() == null ? JSON.getNodeFactory().nullNode() : change.after());
                assertThat(value.path("source").path("connector").asText()).isEqualTo("dameng");
                latestCommitLsn = Math.max(latestCommitLsn, value.path("source").path("commit_lsn").asLong());
                if (remaining.remove(signature)) {
                    observed.add(signature);
                }
            }
        }
        assertThat(remaining).isEmpty();
        assertThat(observed).as("first delivery retains the source operation order")
                .containsExactlyElementsOf(expected.stream().map(TypedChange::signature).toList());
    }

    private record TypedChange(String operation, int id, JsonNode before, JsonNode after) {
        String signature() {
            return operation + ":" + id;
        }
    }

    private void assertPluginIsLoaded(DamengKafkaConnectEnvironment environment) throws Exception {
        final var response = request(environment, "GET", "/connector-plugins", null);
        assertThat(response.statusCode()).isEqualTo(200);
        final var plugins = JSON.readTree(response.body());
        assertThat(plugins.isArray()).isTrue();
        boolean found = false;
        for (JsonNode plugin : plugins) {
            if (CONNECTOR_CLASS.equals(plugin.path("class").asText())) {
                assertThat(plugin.path("type").asText()).isEqualTo("source");
                assertThat(plugin.path("version").asText()).isEqualTo(Module.version());
                found = true;
            }
        }
        assertThat(found).as("packaged Dameng connector is discovered through plugin.path").isTrue();
    }

    private void registerConnector(DamengKafkaConnectEnvironment environment) throws Exception {
        registerConnector(environment, Map.of());
    }

    private void registerConnector(DamengKafkaConnectEnvironment environment, Map<String, String> overrides) throws Exception {
        final Map<String, String> config = new HashMap<>();
        config.put("connector.class", CONNECTOR_CLASS);
        config.put("tasks.max", "1");
        config.put("topic.prefix", prefix);
        config.put("database.hostname", "dameng");
        config.put("database.port", "5236");
        config.put("database.user", DamengKafkaConnectEnvironment.DATABASE_USER);
        config.put("database.password", DamengKafkaConnectEnvironment.DATABASE_PASSWORD);
        config.put("database.dbname", "DAMENG");
        config.put("table.include.list", "SYSDBA\\." + table);
        config.put("snapshot.mode", "initial");
        config.put("schema.history.internal.kafka.bootstrap.servers", environment.internalBootstrapServers());
        config.put("schema.history.internal.kafka.topic", historyTopic);
        config.put("include.schema.changes", "false");
        config.put("tombstones.on.delete", "false");
        config.put("heartbeat.interval.ms", "1000");
        config.put("poll.interval.ms", "100");
        config.putAll(overrides);
        final var response = request(environment, "POST", "/connectors", JSON.writeValueAsString(Map.of("name", connectorName, "config", config)));
        assertThat(response.statusCode()).as(response.body()).isEqualTo(201);
    }

    private void awaitRunning(DamengKafkaConnectEnvironment environment) {
        Awaitility.await("connector and task are RUNNING").atMost(TIMEOUT).pollInterval(Duration.ofMillis(250)).untilAsserted(() -> {
            final var response = request(environment, "GET", "/connectors/" + connectorName + "/status", null);
            assertThat(response.statusCode()).as(response.body()).isEqualTo(200);
            final var status = JSON.readTree(response.body());
            assertThat(status.path("connector").path("state").asText()).as(response.body()).isEqualTo("RUNNING");
            assertThat(status.path("tasks").size()).as(response.body()).isEqualTo(1);
            assertThat(status.path("tasks").get(0).path("state").asText()).as(response.body()).isEqualTo("RUNNING");
        });
    }

    private void awaitKafkaHistory(DamengKafkaConnectEnvironment environment) throws Exception {
        try (var consumer = consumer(environment, historyTopic)) {
            final long deadline = System.nanoTime() + TIMEOUT.toNanos();
            while (System.nanoTime() < deadline) {
                for (var record : consumer.poll(Duration.ofMillis(200))) {
                    if (record.value() != null && record.value().contains(table) && record.value().contains("dameng.object.id")) {
                        assertThat(JSON.readTree(record.value()).path("tableChanges").isArray()).isTrue();
                        return;
                    }
                }
            }
            throw new AssertionError("Kafka schema history did not persist the captured table and physical object identity");
        }
    }

    private void awaitKafkaOffset(DamengKafkaConnectEnvironment environment, long commitLsn) throws Exception {
        assertThat(commitLsn).isPositive();
        try (var consumer = consumer(environment, DamengKafkaConnectEnvironment.OFFSET_TOPIC)) {
            final long deadline = System.nanoTime() + TIMEOUT.toNanos();
            while (System.nanoTime() < deadline) {
                for (var record : consumer.poll(Duration.ofMillis(200))) {
                    if (record.key() != null && record.key().contains(connectorName) && record.value() != null) {
                        final var offset = JSON.readTree(record.value());
                        if (offset.path("commit_lsn").asLong() >= commitLsn) {
                            assertThat(offset.path("lsn").asLong()).isPositive();
                            assertThat(offset.path("database_magic").asLong()).isNotZero();
                            return;
                        }
                    }
                }
            }
            throw new AssertionError("Kafka offset topic did not durably record commit LSN " + commitLsn);
        }
    }

    private void awaitChanges(KafkaConsumer<String, String> consumer, boolean allowSnapshot, ExpectedChange... expected) throws Exception {
        final Set<String> remaining = new HashSet<>();
        for (var change : expected) {
            knownEvents.put(change.signature(), change);
            remaining.add(change.signature());
        }
        final long deadline = System.nanoTime() + TIMEOUT.toNanos();
        while (!remaining.isEmpty() && System.nanoTime() < deadline) {
            for (var record : consumer.poll(Duration.ofMillis(200))) {
                remaining.remove(assertChange(record, allowSnapshot));
            }
        }
        assertThat(remaining).as("every expected committed change reaches Kafka; at-least-once duplicates are allowed").isEmpty();
    }

    private String assertChange(ConsumerRecord<String, String> record, boolean allowSnapshot) throws Exception {
        assertThat(record.topic()).isEqualTo(topic);
        assertThat(record.value()).isNotNull();
        final var value = JSON.readTree(record.value());
        final var key = JSON.readTree(record.key());
        final String operation = value.path("op").asText();
        if (!allowSnapshot) {
            assertThat(operation).as("stored Kafka offset prevents another initial snapshot").isNotEqualTo("r");
        }
        final int id = key.path("ID").asInt();
        final String signature = operation + ":" + id + ":" + text(value.path("after"), "LABEL");
        final var expected = knownEvents.get(signature);
        assertThat(expected).as("unexpected event: %s", value).isNotNull();
        assertRow(value.get("before"), expected.id(), expected.beforeLabel(), expected.beforeExtra());
        assertRow(value.get("after"), expected.id(), expected.afterLabel(), expected.afterExtra());
        assertThat(value.path("source").path("connector").asText()).isEqualTo("dameng");
        assertThat(value.path("source").path("schema").asText()).isEqualTo("SYSDBA");
        assertThat(value.path("source").path("table").asText()).isEqualTo(table);
        latestCommitLsn = Math.max(latestCommitLsn, value.path("source").path("commit_lsn").asLong());
        return signature;
    }

    private void assertOnlyKnownReplays(KafkaConsumer<String, String> consumer) throws Exception {
        final long deadline = System.nanoTime() + Duration.ofSeconds(2).toNanos();
        while (System.nanoTime() < deadline) {
            for (var record : consumer.poll(Duration.ofMillis(100))) {
                assertChange(record, false);
            }
        }
    }

    private static void assertRow(JsonNode row, int id, String label, String extra) {
        if (label == null) {
            assertThat(row == null || row.isNull()).isTrue();
        }
        else {
            assertThat(row).isNotNull();
            assertThat(row.path("ID").asInt()).isEqualTo(id);
            assertThat(text(row, "LABEL")).isEqualTo(label);
            assertThat(text(row, "EXTRA")).isEqualTo(extra);
        }
    }

    private static String text(JsonNode node, String field) {
        return node.path(field).isMissingNode() || node.path(field).isNull() ? null : node.path(field).asText();
    }

    private static KafkaConsumer<String, String> consumer(DamengKafkaConnectEnvironment environment, String topic) {
        final var consumer = new KafkaConsumer<String, String>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, environment.bootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "dameng-connect-reader-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"), new StringDeserializer(), new StringDeserializer());
        consumer.subscribe(List.of(topic));
        return consumer;
    }

    private static HttpResponse<String> request(DamengKafkaConnectEnvironment environment, String method, String path, String body) throws Exception {
        final var request = HttpRequest.newBuilder(URI.create(environment.workerUrl() + path)).timeout(Duration.ofSeconds(30))
                .header("Content-Type", "application/json")
                .method(method, body == null ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofString(body)).build();
        return HTTP.send(request, HttpResponse.BodyHandlers.ofString());
    }

    private static ExpectedChange change(String operation, int id, String before, String after) {
        return new ExpectedChange(operation, id, before, after, null, null);
    }

    private String qualifiedTable() {
        return "\"SYSDBA\".\"" + table + "\"";
    }

    private static void execute(Connection connection, String sql) throws Exception {
        try (var statement = connection.createStatement()) {
            statement.execute(sql);
        }
    }

    private record ExpectedChange(String operation, int id, String beforeLabel, String afterLabel, String beforeExtra, String afterExtra) {
        private String signature() {
            return operation + ":" + id + ":" + afterLabel;
        }
    }
}
