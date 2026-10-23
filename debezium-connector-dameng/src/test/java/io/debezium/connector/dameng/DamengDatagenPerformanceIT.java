/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.StringWriter;
import java.lang.management.ManagementFactory;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.sql.Connection;
import java.sql.DriverManager;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.zip.ZipInputStream;

import javax.management.Attribute;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.openmbean.CompositeData;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIf;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import jdk.management.jfr.FlightRecorderMXBean;

/**
 * Opt-in bounded BSEG workload through an isolated distributed worker and real Kafka.
 * The default broker, worker and database are test-owned. The BSEG generator is
 * compiled from test sources in this checkout. An external broker/JAR can still be
 * selected explicitly for comparisons. This is not a machine-independent throughput SLA.
 */
@EnabledIf("enabled")
class DamengDatagenPerformanceIT {
    static boolean enabled() {
        return Boolean.getBoolean("dameng.perf.enabled") || System.getProperty("dameng.perf.datagen.jar") != null;
    }

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final HttpClient HTTP = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
    private static final long GIB = 1024L * 1024 * 1024;
    private static final List<String> METRICS = List.of("LatestArchivedLsn", "ScannedUpperLsn", "OldestReplayLsn",
            "WindowBufferRecords", "WindowBufferEstimatedBytes", "OpenTransactions", "LastMiningDurationMillis",
            "MaxMiningDurationMillis", "QueueRemainingCapacity", "TotalNumberOfEventsSeen", "MilliSecondsBehindSource",
            "MiningSessions", "ReconstructionSessions", "MinedRows", "ReconstructedRows",
            "TransactionBufferMemoryBytes", "TransactionBufferDiskBytes", "TransactionBufferRecords", "SpilledTransactions",
            "MiningWindowByteBudget", "SelectedArchiveFileBytes", "SelectedArchiveFileCount", "CurrentMiningBatchSize", "LastWindowLsnSpan",
            "QueueEnqueueWaitDurationMillis", "ForwardMiningDurationMillis", "ReconstructionDurationMillis", "DispatchDurationMillis",
            "LastForwardMiningDurationMillis", "LastReconstructionDurationMillis", "LastDispatchDurationMillis");
    private final Path directory = Path.of(System.getProperty("dameng.perf.directory", "target/dameng-datagen-performance")).toAbsolutePath();
    private final String prefix = "dbzperf_" + UUID.randomUUID().toString().replace("-", "");
    private final String topic = prefix + ".BIZ.BSEG";
    private final List<String> topics = List.of(topic, prefix + "_history", prefix + "_config", prefix + "_offset", prefix + "_status");
    private final Map<String, String> mirror = new HashMap<>();
    private final List<String> columns = new ArrayList<>();
    private final List<Boolean> decimals = new ArrayList<>();
    private final Map<String, Long> counts = new HashMap<>();
    private final StringBuilder evidence = new StringBuilder();
    private final StringBuilder downstreamSamples = new StringBuilder();
    private final StringBuilder samples = new StringBuilder("elapsed_ms,delivered,generator_running,free_bytes,database_archived_upper_lsn,kafka_end_offset,"
            + String.join(",", METRICS) + ",heap_used_bytes,heap_committed_bytes,heap_max_bytes,gc_count,gc_time_ms,process_cpu_ns\n");
    private final Path disk = Path.of(System.getProperty("dameng.perf.disk.path", "."));
    private long initialFree;
    private long initialDockerFree;
    private long minimumFree;
    private long received;
    private long serializedValueBytes;
    private int maximumSerializedValueBytes;
    private long started;
    private int restPort;
    private int jmxPort;
    private Process generator;
    private GenericContainer<?> worker;
    private GenericContainer<?> broker;

    @TempDir
    Path temporaryDirectory;

    @Test
    @Timeout(value = 15, unit = TimeUnit.MINUTES)
    void shouldMeasureBoundedWideTableWorkloadThroughKafka() throws Exception {
        assumeTrue(System.getProperty("dameng.hostname") == null, "Performance probe owns its database");
        final var externalJar = System.getProperty("dameng.perf.datagen.jar");
        final Path jar = externalJar == null ? null : Path.of(externalJar);
        if (jar != null) {
            assertThat(jar).exists();
        }
        final int cycles = Integer.getInteger("dameng.perf.cycles", 3);
        final int initialRows = Integer.getInteger("dameng.perf.initial.rows", 5_000);
        final int large = Integer.getInteger("dameng.perf.large.size", 10_000);
        final int small = Integer.getInteger("dameng.perf.small.size", 100);
        final int interval = Integer.getInteger("dameng.perf.interval.ms", 7_000);
        final long expected = (large + 9L * small) * cycles;
        assertThat(cycles).isBetween(1, 10);
        assertThat(initialRows).isBetween(1_000, 20_000);
        assertThat(expected).isBetween(1L, 110_000L);
        assertThat(interval).isBetween(0, 30_000);
        Files.createDirectories(directory);
        initialFree = Files.getFileStore(disk).getUsableSpace();
        minimumFree = initialFree;
        assertThat(Long.getLong("dameng.perf.min.free.gib", 15)).isPositive();
        assertThat(Long.getLong("dameng.perf.max.growth.gib", 4)).isPositive();
        guardDisk();
        note("monitoredFilesystem=" + disk.toAbsolutePath());
        note("prefix=" + prefix + " expectedDml=" + expected + " initialRows=" + initialRows
                + " large=" + large + " small=" + small + " cycles=" + cycles + " intervalMs=" + interval);
        note(jar == null ? "datagen=compiled test sources in this checkout"
                : "datagenSha256=" + HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(jar))));
        note("hostJava=" + System.getProperty("java.runtime.version") + " hostHeap="
                + ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getMax());
        try {
            final var bootstrap = bootstrapServers();
            try (Admin admin = Admin.create(Map.of("bootstrap.servers", bootstrap))) {
                try {
                    DamengTestDatabase.start();
                    final var config = generatorConfig(initialRows, large, small, interval, cycles);
                    generator = launchGenerator(jar, config, true);
                    awaitGenerator(Duration.ofMinutes(2));
                    try (var database = open()) {
                        note("database=" + database.getMetaData().getDatabaseProductVersion());
                        readDatabase(database, mirror);
                    }
                    assertThat(mirror).hasSize(initialRows);
                    createTopics(admin);
                    startWorker(bootstrap);
                    try (var jmx = JMXConnectorFactory.connect(new JMXServiceURL("service:jmx:rmi:///jndi/rmi://127.0.0.1:" + jmxPort + "/jmxrmi"));
                            var consumer = new KafkaConsumer<String, String>(Map.of("bootstrap.servers", bootstrap,
                                    "enable.auto.commit", "false", "auto.offset.reset", "earliest", "max.poll.records", "2000"),
                                    new StringDeserializer(), new StringDeserializer())) {
                        consumer.assign(List.of(new TopicPartition(topic, 0)));
                        consumer.seekToBeginning(consumer.assignment());
                        registerConnector(bootstrap);
                        final var mbeans = jmx.getMBeanServerConnection();
                        final var metric = new ObjectName("debezium.dameng:type=connector-metrics,context=streaming,server=" + prefix);
                        Awaitility.await("streaming after schema snapshot").atMost(Duration.ofMinutes(2))
                                .until(() -> mbeans.isRegistered(metric) && Boolean.TRUE.equals(mbeans.getAttribute(metric, "Connected")));
                        assertRunning();
                        if (Boolean.getBoolean("dameng.perf.detailed.waits")) {
                            final var recorder = ManagementFactory.newPlatformMXBeanProxy(mbeans, FlightRecorderMXBean.MXBEAN_NAME, FlightRecorderMXBean.class);
                            final long recordingId = recorder.getRecordings().stream().filter(recording -> recording.getName().equals("perf"))
                                    .findFirst().orElseThrow().getId();
                            final Map<String, String> settings = new HashMap<>(recorder.getRecordingSettings(recordingId));
                            settings.put("jdk.ThreadPark#threshold", "0 ms");
                            recorder.setRecordingSettings(recordingId, settings);
                            note("detailedWaits=true ThreadParkThreshold=0ms");
                        }
                        sampleDownstream(mbeans, 0);
                        note("workloadStartedAt=" + java.time.Instant.now());
                        started = System.nanoTime();
                        generator = launchGenerator(jar, config, false);
                        final long deadline = started + Duration.ofSeconds(Long.getLong("dameng.perf.timeout.seconds", 240)).toNanos();
                        long lastSample = 0;
                        long generationFinished = 0;
                        while (System.nanoTime() < deadline && (generator.isAlive() || received < expected)) {
                            for (var record : consumer.poll(Duration.ofMillis(100))) {
                                verifyChange(JSON.readTree(record.value()));
                                serializedValueBytes += record.serializedValueSize();
                                maximumSerializedValueBytes = Math.max(maximumSerializedValueBytes, record.serializedValueSize());
                            }
                            assertThat(received).as("no excess CDC events").isLessThanOrEqualTo(expected);
                            if (!generator.isAlive() && generationFinished == 0) {
                                generationFinished = System.nanoTime();
                                assertThat(generator.exitValue()).as("generator exit; see workload.log").isZero();
                                note("generationObservedFinishedMs=" + millis(generationFinished - started)
                                        + " deliveredAtGeneratorFinish=" + received);
                            }
                            if (System.nanoTime() - lastSample >= Duration.ofSeconds(1).toNanos()) {
                                guardDisk();
                                assertRunning();
                                sample(mbeans, metric, consumer);
                                lastSample = System.nanoTime();
                            }
                        }
                        sample(mbeans, metric, consumer);
                        note("elapsedMs=" + millis(System.nanoTime() - started) + " received=" + received + " counts=" + counts);
                        note("serializedValueBytes=" + serializedValueBytes + " maximumSerializedValueBytes=" + maximumSerializedValueBytes);
                        dumpFlightRecording(mbeans);
                        recordContainerMemory();
                        assertThat(received).as("all committed DML reaches Kafka before the deadline; see samples.csv").isEqualTo(expected);
                        assertThat(generator.isAlive()).isFalse();
                        assertThat(generator.exitValue()).isZero();
                        assertThat(counts).containsEntry("c", expected * 7 / 100).containsEntry("u", expected * 90 / 100)
                                .containsEntry("d", expected * 3 / 100).hasSize(3);
                        final Map<String, String> databaseState = new HashMap<>();
                        try (var database = open()) {
                            readDatabase(database, databaseState);
                        }
                        assertThat(mirror).as("all 221 columns of final Kafka-derived state equal the database").isEqualTo(databaseState);
                        // Restarting initialization must recognize existing generator rows without changing them.
                        generator = launchGenerator(jar, config, true);
                        awaitGenerator(Duration.ofMinutes(2));
                        final Map<String, String> restartedState = new HashMap<>();
                        try (var database = open()) {
                            readDatabase(database, restartedState);
                        }
                        assertThat(restartedState).as("generator initialization is restartable").isEqualTo(databaseState);
                        final long quietUntil = System.nanoTime() + Duration.ofSeconds(3).toNanos();
                        while (System.nanoTime() < quietUntil) {
                            assertThat(consumer.poll(Duration.ofMillis(200)).isEmpty()).as("no duplicate trailing DML").isTrue();
                        }
                        assertRunning();
                        note("ALL_EVENT_COUNTS_BEFORE_IMAGES_AND_FINAL_STATE_PASSED=true finalRows=" + mirror.size());
                    }
                }
                finally {
                    try {
                        try {
                            stopGenerator();
                        }
                        finally {
                            if (worker != null && worker.isRunning()) {
                                try {
                                    if (!Files.exists(directory.resolve("container-memory.txt"))) {
                                        recordContainerMemory();
                                    }
                                    if (!Files.exists(directory.resolve("worker.jfr"))) {
                                        try (var jmx = JMXConnectorFactory.connect(new JMXServiceURL(
                                                "service:jmx:rmi:///jndi/rmi://127.0.0.1:" + jmxPort + "/jmxrmi"))) {
                                            dumpFlightRecording(jmx.getMBeanServerConnection());
                                        }
                                        catch (Exception diagnosticFailure) {
                                            note("jfrUnavailable=" + diagnosticFailure);
                                        }
                                    }
                                    Files.writeString(directory.resolve("worker.log"), worker.getLogs());
                                }
                                finally {
                                    worker.stop();
                                }
                            }
                        }
                    }
                    finally {
                        try {
                            // Never list and delete unrelated topics, even after a failed run.
                            try {
                                final var existing = admin.listTopics().names().get(15, TimeUnit.SECONDS);
                                final var owned = topics.stream().filter(existing::contains).toList();
                                if (!owned.isEmpty()) {
                                    admin.deleteTopics(owned).all().get(30, TimeUnit.SECONDS);
                                }
                            }
                            finally {
                                admin.close(Duration.ofSeconds(5));
                            }
                        }
                        finally {
                            DamengTestDatabase.stop();
                            note("initialFreeBytes=" + initialFree + " minimumFreeBytes=" + minimumFree);
                            Files.writeString(directory.resolve("evidence.txt"), evidence);
                            Files.writeString(directory.resolve("samples.csv"), samples);
                        }
                    }
                }
            }
        }
        finally {
            if (broker != null) {
                try {
                    Files.writeString(directory.resolve("broker.log"), broker.getLogs());
                }
                finally {
                    broker.stop();
                }
            }
        }
    }

    private Path generatorConfig(int initial, int large, int small, int interval, int cycles) throws Exception {
        final var properties = new Properties();
        properties.setProperty("database.hostname", DamengTestDatabase.hostname());
        properties.setProperty("database.port", Integer.toString(DamengTestDatabase.port()));
        properties.setProperty("database.user", DamengTestDatabase.user());
        properties.setProperty("database.password", DamengTestDatabase.password());
        properties.setProperty("schema", "BIZ");
        properties.setProperty("table", "BSEG");
        properties.setProperty("initial.rows", Integer.toString(initial));
        properties.setProperty("transaction.large.size", Integer.toString(large));
        properties.setProperty("transaction.small.size", Integer.toString(small));
        properties.setProperty("cycles", Integer.toString(cycles));
        properties.setProperty("interval.ms", Integer.toString(interval));
        properties.setProperty("random.seed", "20261015");
        final var path = temporaryDirectory.resolve("datagen.properties");
        writeProperties(path, properties);
        return path;
    }

    private Process launchGenerator(Path jar, Path config, boolean init) throws Exception {
        final List<String> command = new ArrayList<>(List.of(Path.of(System.getProperty("java.home"), "bin", "java").toString(), "-Xmx256m"));
        if (jar == null) {
            command.addAll(List.of("-cp", System.getProperty("surefire.test.class.path", System.getProperty("java.class.path")),
                    "io.debezium.connector.dameng.datagen.Main"));
        }
        else {
            command.addAll(List.of("-jar", jar.toAbsolutePath().toString()));
        }
        command.addAll(List.of("--config", config.toString()));
        if (init) {
            command.add("--init-only");
        }
        final var builder = new ProcessBuilder(command).redirectErrorStream(true)
                .redirectOutput(directory.resolve(init ? "init.log" : "workload.log").toFile());
        builder.environment().remove("DM_PASSWORD");
        return builder.start();
    }

    private void awaitGenerator(Duration timeout) throws Exception {
        final long deadline = System.nanoTime() + timeout.toNanos();
        while (generator.isAlive() && System.nanoTime() < deadline) {
            guardDisk();
            generator.waitFor(1, TimeUnit.SECONDS);
        }
        assertThat(generator.isAlive()).as("generator must finish in time").isFalse();
        assertThat(generator.exitValue()).as("generator exit; see init.log").isZero();
    }

    private void stopGenerator() throws Exception {
        if (generator != null && generator.isAlive()) {
            generator.destroy();
            if (!generator.waitFor(10, TimeUnit.SECONDS)) {
                generator.destroyForcibly();
                generator.waitFor(10, TimeUnit.SECONDS);
            }
        }
    }

    private void guardDisk() throws Exception {
        final long free = Files.getFileStore(disk).getUsableSpace();
        minimumFree = Math.min(minimumFree, free);
        assertThat(free).as("minimum free space at %s", disk).isGreaterThan(Long.getLong("dameng.perf.min.free.gib", 15) * GIB);
        assertThat(initialFree - free).as("maximum filesystem growth at %s", disk).isLessThan(Long.getLong("dameng.perf.max.growth.gib", 4) * GIB);
        if (broker != null && broker.isRunning()) {
            final var result = broker.execInContainer("df", "-Pk", "/");
            assertThat(result.getExitCode()).as("read owned Docker storage usage").isZero();
            final var lines = result.getStdout().strip().split("\\R");
            final long dockerFree = Long.parseLong(lines[lines.length - 1].strip().split("\\s+")[3]) * 1024;
            if (initialDockerFree == 0) {
                initialDockerFree = dockerFree;
                note("initialDockerFreeBytes=" + dockerFree);
            }
            assertThat(dockerFree).as("minimum free space in Docker storage")
                    .isGreaterThan(Long.getLong("dameng.perf.docker.min.free.gib", 15) * GIB);
            assertThat(initialDockerFree - dockerFree).as("maximum Docker filesystem growth")
                    .isLessThan(Long.getLong("dameng.perf.max.growth.gib", 4) * GIB);
        }
    }

    private String bootstrapServers() throws Exception {
        final var external = System.getProperty("dameng.perf.bootstrap.servers");
        if (external != null && !external.isBlank()) {
            note("externalKafka=" + external);
            return external;
        }
        final int port = freePort();
        int controller = freePort();
        while (controller == port) {
            controller = freePort();
        }
        broker = new GenericContainer<>(DockerImageName.parse(System.getProperty("dameng.perf.broker.image", "apache/kafka:3.9.1")))
                .withNetworkMode("host")
                .withEnv("KAFKA_NODE_ID", "1")
                .withEnv("KAFKA_PROCESS_ROLES", "broker,controller")
                .withEnv("KAFKA_LISTENERS", "PLAINTEXT://127.0.0.1:" + port + ",CONTROLLER://127.0.0.1:" + controller)
                .withEnv("KAFKA_ADVERTISED_LISTENERS", "PLAINTEXT://127.0.0.1:" + port)
                .withEnv("KAFKA_CONTROLLER_QUORUM_VOTERS", "1@127.0.0.1:" + controller)
                .withEnv("KAFKA_CONTROLLER_LISTENER_NAMES", "CONTROLLER")
                .withEnv("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP", "PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT")
                .withEnv("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1")
                .withEnv("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
                .withEnv("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
                .withEnv("KAFKA_DEFAULT_REPLICATION_FACTOR", "1")
                .withEnv("KAFKA_NUM_PARTITIONS", "1")
                .withEnv("KAFKA_LOG_RETENTION_BYTES", "1073741824")
                .withEnv("KAFKA_LOG_SEGMENT_BYTES", "33554432")
                .withEnv("KAFKA_HEAP_OPTS", "-Xms256m -Xmx512m")
                .waitingFor(Wait.forLogMessage(".*Kafka Server started.*", 1).withStartupTimeout(Duration.ofMinutes(2)));
        broker.start();
        guardDisk();
        note("ownedKafka=" + broker.getDockerImageName() + " replicationFactor=1 port=" + port);
        return "127.0.0.1:" + port;
    }

    private void createTopics(Admin admin) throws Exception {
        final List<NewTopic> created = new ArrayList<>();
        for (var name : topics) {
            final Map<String, String> settings;
            if (name.equals(topic)) {
                settings = Map.of("min.insync.replicas", "1", "retention.ms", "3600000", "segment.bytes", "33554432");
            }
            else if (name.equals(topics.get(1))) {
                settings = Map.of("min.insync.replicas", "1", "cleanup.policy", "delete", "retention.ms", "-1", "retention.bytes", "-1");
            }
            else {
                settings = Map.of("min.insync.replicas", "1", "cleanup.policy", "compact");
            }
            created.add(new NewTopic(name, 1, (short) 1).configs(settings));
        }
        admin.createTopics(created).all().get(30, TimeUnit.SECONDS);
        for (var description : admin.describeTopics(topics).allTopicNames().get(15, TimeUnit.SECONDS).values()) {
            assertThat(description.partitions()).hasSize(1);
            assertThat(description.partitions().get(0).replicas()).hasSize(1);
        }
        note("topicPartitions=1 replicationFactor=1 minInSyncReplicas=1");
    }

    private void startWorker(String bootstrap) throws Exception {
        final var build = Path.of(System.getProperty("project.build.directory", "target")).toAbsolutePath();
        final var plugin = Files.createDirectories(temporaryDirectory.resolve("plugins"));
        final var archive = build.resolve("debezium-connector-dameng-" + Module.version() + "-plugin.zip");
        assertThat(archive).as("Build this checkout with -Passembly").exists();
        try (var zip = new ZipInputStream(Files.newInputStream(archive))) {
            for (var entry = zip.getNextEntry(); entry != null; entry = zip.getNextEntry()) {
                final var path = plugin.resolve(entry.getName()).normalize();
                assertThat(path.startsWith(plugin)).isTrue();
                if (entry.isDirectory()) {
                    Files.createDirectories(path);
                }
                else {
                    Files.createDirectories(path.getParent());
                    Files.copy(zip, path);
                }
            }
        }
        final var module = "debezium-connector-dameng-" + Module.version() + ".jar";
        try (var files = Files.walk(plugin)) {
            final var packaged = files.filter(path -> path.getFileName().toString().equals(module)).findFirst().orElseThrow();
            final var digest = MessageDigest.getInstance("SHA-256");
            assertThat(digest.digest(Files.readAllBytes(packaged))).isEqualTo(digest.digest(Files.readAllBytes(build.resolve(module))));
            note("moduleSha256=" + HexFormat.of().formatHex(digest.digest(Files.readAllBytes(packaged))));
        }
        restPort = freePort();
        do {
            jmxPort = freePort();
        } while (jmxPort == restPort);
        final var properties = new Properties();
        properties.putAll(Map.of("bootstrap.servers", bootstrap, "group.id", prefix, "config.storage.topic", topics.get(2),
                "offset.storage.topic", topics.get(3), "status.storage.topic", topics.get(4),
                "config.storage.replication.factor", "1", "offset.storage.replication.factor", "1", "status.storage.replication.factor", "1",
                "offset.storage.partitions", "1", "status.storage.partitions", "1"));
        properties.putAll(Map.of("key.converter", "org.apache.kafka.connect.json.JsonConverter",
                "value.converter", "org.apache.kafka.connect.json.JsonConverter", "key.converter.schemas.enable", "false",
                "value.converter.schemas.enable", "false", "listeners", "http://127.0.0.1:" + restPort,
                "rest.advertised.host.name", "127.0.0.1", "rest.advertised.port", Integer.toString(restPort),
                "plugin.path", "/tmp/perf-plugins", "plugin.discovery", "service_load", "topic.creation.enable", "false"));
        properties.putAll(Map.of("producer.enable.idempotence", "false", "producer.acks", "all", "offset.flush.interval.ms", "1000"));
        final int producerBatchBytes = Integer.getInteger("dameng.perf.producer.batch.size", 16384);
        assertThat(producerBatchBytes).isBetween(16384, 1048576);
        properties.put("producer.batch.size", Integer.toString(producerBatchBytes));
        note("producerBatchBytes=" + producerBatchBytes);
        final var config = temporaryDirectory.resolve("worker.properties");
        writeProperties(config, properties);
        final var heap = System.getProperty("dameng.perf.worker.heap.opts", "-Xms512m -Xmx1g");
        final long memoryMiB = Long.getLong("dameng.perf.worker.memory.mib", 0L);
        assertThat(memoryMiB).isBetween(0L, 16_384L);
        worker = new GenericContainer<>(DockerImageName.parse(System.getProperty("dameng.perf.worker.image", "apache/kafka:3.9.1")))
                .withImagePullPolicy(org.testcontainers.images.PullPolicy.defaultPolicy())
                .withNetworkMode("host")
                .withCreateContainerCmdModifier(command -> command.withEntrypoint("/bin/bash"))
                .withCreateContainerCmdModifier(command -> {
                    if (memoryMiB > 0) {
                        // Equal RAM and total RAM+swap limits disable container swap.
                        command.getHostConfig().withMemory(memoryMiB * 1024 * 1024).withMemorySwap(memoryMiB * 1024 * 1024);
                    }
                })
                .withCommand("-c", "exec /opt/kafka/bin/connect-distributed.sh /tmp/perf-worker.properties")
                .withEnv("KAFKA_HEAP_OPTS", heap)
                .withEnv("KAFKA_JMX_OPTS", "-Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.authenticate=false "
                        + "-Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.host=127.0.0.1 "
                        + "-Djava.rmi.server.hostname=127.0.0.1 -Dcom.sun.management.jmxremote.port=" + jmxPort
                        + " -Dcom.sun.management.jmxremote.rmi.port=" + jmxPort)
                .withEnv("KAFKA_OPTS", "-XX:StartFlightRecording=name=perf,settings=profile,maxsize=64m,dumponexit=true,filename=/tmp/perf.jfr")
                .withCopyFileToContainer(MountableFile.forHostPath(plugin, 0755), "/tmp/perf-plugins")
                .withCopyFileToContainer(MountableFile.forHostPath(config, 0644), "/tmp/perf-worker.properties")
                .waitingFor(Wait.forLogMessage(".*Kafka Connect started.*", 1).withStartupTimeout(Duration.ofMinutes(2)));
        worker.start();
        if (memoryMiB > 0) {
            assertThat(worker.getContainerInfo().getHostConfig().getMemory()).isEqualTo(memoryMiB * 1024 * 1024);
            assertThat(worker.getContainerInfo().getHostConfig().getMemorySwap()).isEqualTo(memoryMiB * 1024 * 1024);
        }
        note("workerImage=" + worker.getDockerImageName() + " workerHeapOpts=" + heap + " containerMemoryMiB=" + memoryMiB);
    }

    private void registerConnector(String bootstrap) throws Exception {
        final Map<String, String> config = new LinkedHashMap<>();
        config.put("connector.class", DamengConnector.class.getName());
        config.put("tasks.max", "1");
        config.put("topic.prefix", prefix);
        config.put("database.hostname", DamengTestDatabase.hostname());
        config.put("database.port", Integer.toString(DamengTestDatabase.port()));
        config.put("database.user", DamengTestDatabase.user());
        config.put("database.password", DamengTestDatabase.password());
        config.put("database.dbname", "DAMENG");
        config.put("table.include.list", "BIZ\\.BSEG");
        config.put("snapshot.mode", "no_data");
        config.put("schema.history.internal.kafka.bootstrap.servers", bootstrap);
        config.put("schema.history.internal.kafka.topic", topics.get(1));
        config.put("include.schema.changes", "false");
        config.put("tombstones.on.delete", "false");
        config.put("decimal.handling.mode", "string");
        config.put("poll.interval.ms", "100");
        config.put("query.fetch.size", "2000");
        config.put("log.mining.window.strategy", System.getProperty("dameng.perf.window.strategy", "fixed"));
        config.put("log.mining.window.max.bytes", System.getProperty("dameng.perf.window.max.bytes", "67108864"));
        config.put("log.mining.batch.size", System.getProperty("dameng.perf.mining.batch.size", "1000"));
        config.put("log.mining.buffer.memory.bytes", System.getProperty("dameng.perf.buffer.memory.bytes", "67108864"));
        // Only pass the retired setting when explicitly requested for compatibility regression.
        final var legacyBudget = System.getProperty("dameng.perf.buffer.max.bytes");
        if (legacyBudget != null) {
            config.put("log.mining.buffer.max.bytes", legacyBudget);
        }
        final var response = request("POST", "/connectors", JSON.writeValueAsString(Map.of("name", prefix, "config", config)));
        assertThat(response.statusCode()).as("connector registration").isEqualTo(201);
        note("windowStrategy=" + config.get("log.mining.window.strategy") + " archiveByteBudget=" + config.get("log.mining.window.max.bytes"));
        note("logMiningBatchLsn=" + config.get("log.mining.batch.size") + " transactionCacheBytes=" + config.get("log.mining.buffer.memory.bytes")
                + " ignoredLegacyWindowBudgetBytes=" + legacyBudget
                + " queryFetchSize=2000 snapshot=no_data decimal=string SQL_OPTIONS=2048");
    }

    private void assertRunning() throws Exception {
        final var response = request("GET", "/connectors/" + prefix + "/status", null);
        assertThat(response.statusCode()).isEqualTo(200);
        final var status = JSON.readTree(response.body());
        assertThat(status.path("connector").path("state").asText()).as(response.body()).isEqualTo("RUNNING");
        assertThat(status.path("tasks").size()).as(response.body()).isEqualTo(1);
        assertThat(status.path("tasks").get(0).path("state").asText()).as(response.body()).isEqualTo("RUNNING");
    }

    private HttpResponse<String> request(String method, String path, String body) throws Exception {
        return HTTP.send(HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + restPort + path))
                .timeout(Duration.ofSeconds(10)).header("Content-Type", "application/json")
                .method(method, body == null ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofString(body)).build(),
                HttpResponse.BodyHandlers.ofString());
    }

    private void sample(MBeanServerConnection mbeans, ObjectName metric, KafkaConsumer<String, String> consumer) throws Exception {
        final long archived;
        try (var database = open();
                var statement = database.createStatement();
                var rows = statement.executeQuery("SELECT MAX(CLSN) FROM V$ARCH_FILE WHERE PATH IS NOT NULL")) {
            assertThat(rows.next()).isTrue();
            archived = rows.getLong(1);
        }
        samples.append(millis(System.nanoTime() - started)).append(',').append(received).append(',').append(generator.isAlive())
                .append(',').append(Files.getFileStore(disk).getUsableSpace()).append(',').append(archived)
                .append(',').append(consumer.endOffsets(consumer.assignment()).get(new TopicPartition(topic, 0)));
        for (var name : METRICS) {
            samples.append(',').append(mbeans.getAttribute(metric, name));
        }
        final var heap = (CompositeData) mbeans.getAttribute(new ObjectName("java.lang:type=Memory"), "HeapMemoryUsage");
        long gcCount = 0;
        long gcTime = 0;
        for (var collector : mbeans.queryNames(new ObjectName("java.lang:type=GarbageCollector,name=*"), null)) {
            gcCount += (Long) mbeans.getAttribute(collector, "CollectionCount");
            gcTime += (Long) mbeans.getAttribute(collector, "CollectionTime");
        }
        samples.append(',').append(heap.get("used")).append(',').append(heap.get("committed")).append(',').append(heap.get("max"))
                .append(',').append(gcCount).append(',').append(gcTime)
                .append(',').append(mbeans.getAttribute(new ObjectName("java.lang:type=OperatingSystem"), "ProcessCpuTime"));
        samples.append('\n');
        Files.writeString(directory.resolve("samples.csv"), samples);
        sampleDownstream(mbeans, millis(System.nanoTime() - started));
    }

    private void sampleDownstream(MBeanServerConnection mbeans, long elapsed) throws Exception {
        final Map<String, Object> values = new LinkedHashMap<>();
        for (var name : List.of(
                new ObjectName("kafka.producer:type=producer-metrics,client-id=connector-producer-" + prefix + "-0"),
                new ObjectName("kafka.connect:type=source-task-metrics,connector=" + prefix + ",task=0"))) {
            assertThat(mbeans.isRegistered(name)).as("Downstream metrics registered: %s", name).isTrue();
            final var attributes = java.util.Arrays.stream(mbeans.getMBeanInfo(name).getAttributes())
                    .filter(attribute -> attribute.isReadable()).map(attribute -> attribute.getName()).toArray(String[]::new);
            final Map<String, Object> metrics = new LinkedHashMap<>();
            for (Attribute attribute : mbeans.getAttributes(name, attributes).asList()) {
                if (attribute.getValue() instanceof Number) {
                    metrics.put(attribute.getName(), attribute.getValue());
                }
            }
            values.put(name.toString(), metrics);
        }
        downstreamSamples.append(JSON.writeValueAsString(Map.of("elapsed_ms", elapsed, "sampled_at", java.time.Instant.now().toString(), "mbeans", values)))
                .append('\n');
        Files.writeString(directory.resolve("downstream.jsonl"), downstreamSamples);
    }

    private void recordContainerMemory() throws Exception {
        final var result = worker.execInContainer("sh", "-c",
                "if [ -f /sys/fs/cgroup/memory.peak ]; then "
                        + "cat /sys/fs/cgroup/memory.max /sys/fs/cgroup/memory.swap.max /sys/fs/cgroup/memory.peak /sys/fs/cgroup/memory.events; "
                        + "else cat /sys/fs/cgroup/memory/memory.limit_in_bytes /sys/fs/cgroup/memory/memory.max_usage_in_bytes "
                        + "/sys/fs/cgroup/memory/memory.failcnt; fi");
        assertThat(result.getExitCode()).as("read cgroup memory evidence: %s", result.getStderr()).isZero();
        Files.writeString(directory.resolve("container-memory.txt"), result.getStdout());
    }

    private void dumpFlightRecording(MBeanServerConnection mbeans) throws Exception {
        final var result = mbeans.invoke(new ObjectName("com.sun.management:type=DiagnosticCommand"), "jfrDump",
                new Object[]{ new String[]{ "name=perf", "filename=/tmp/perf.jfr" } }, new String[]{ "[Ljava.lang.String;" });
        note("jfrDump=" + result);
        worker.copyFileFromContainer("/tmp/perf.jfr", directory.resolve("worker.jfr").toString());
    }

    private void verifyChange(JsonNode event) throws Exception {
        final var operation = event.path("op").asText();
        assertThat(operation).isIn("c", "u", "d");
        final var before = event.get("before");
        final var after = event.get("after");
        final var key = key(operation.equals("d") ? before : after);
        if (operation.equals("c")) {
            assertThat(mirror).as("insert key must be new").doesNotContainKey(key);
        }
        else {
            assertThat(hash(before)).as("before image of %s", key).isEqualTo(mirror.get(key));
        }
        if (operation.equals("d")) {
            mirror.remove(key);
        }
        else {
            mirror.put(key, hash(after));
        }
        counts.merge(operation, 1L, Long::sum);
        received++;
    }

    private void readDatabase(Connection database, Map<String, String> state) throws Exception {
        try (var statement = database.createStatement(); var rows = statement.executeQuery("SELECT * FROM BIZ.BSEG")) {
            if (columns.isEmpty()) {
                final var metadata = rows.getMetaData();
                for (int index = 1; index <= metadata.getColumnCount(); index++) {
                    columns.add(metadata.getColumnName(index));
                    decimals.add(metadata.getColumnType(index) == java.sql.Types.DECIMAL || metadata.getColumnType(index) == java.sql.Types.NUMERIC);
                }
                assertThat(columns).hasSize(221);
            }
            while (rows.next()) {
                final var row = JSON.createObjectNode();
                for (int index = 0; index < columns.size(); index++) {
                    row.put(columns.get(index), rows.getString(index + 1));
                }
                state.put(key(row), hash(row));
            }
        }
    }

    private String hash(JsonNode row) throws Exception {
        final var digest = MessageDigest.getInstance("SHA-256");
        for (int index = 0; index < columns.size(); index++) {
            final var node = row.get(columns.get(index));
            assertThat(node).as("column %s is present", columns.get(index)).isNotNull();
            String value = node.isNull() ? "<null>" : node.asText();
            if (decimals.get(index) && !node.isNull()) {
                value = new java.math.BigDecimal(value).stripTrailingZeros().toPlainString();
            }
            digest.update(value.getBytes(StandardCharsets.UTF_8));
            digest.update((byte) 0);
        }
        return HexFormat.of().formatHex(digest.digest());
    }

    private static String key(JsonNode row) {
        return row.path("MANDT").asText() + "/" + row.path("BUKRS").asText() + "/" + row.path("BELNR").asText()
                + "/" + row.path("GJAHR").asText() + "/" + row.path("BUZEI").asText();
    }

    private static Connection open() throws Exception {
        return DriverManager.getConnection(DamengTestDatabase.jdbcUrl(), DamengTestDatabase.user(), DamengTestDatabase.password());
    }

    private static int freePort() throws Exception {
        try (var socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private static void writeProperties(Path file, Properties properties) throws Exception {
        try (var writer = new StringWriter()) {
            properties.store(writer, "Test-owned bounded performance probe");
            Files.writeString(file, writer.toString());
        }
    }

    private static long millis(long nanos) {
        return TimeUnit.NANOSECONDS.toMillis(nanos);
    }

    private void note(String message) {
        evidence.append(message).append('\n');
        System.out.println("DATAGEN_PERF " + message);
    }
}
