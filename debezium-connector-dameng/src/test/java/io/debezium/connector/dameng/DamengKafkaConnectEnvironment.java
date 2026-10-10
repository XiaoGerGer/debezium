/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.zip.ZipInputStream;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.awaitility.Awaitility;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.ImagePullPolicy;
import org.testcontainers.images.PullPolicy;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import com.github.dockerjava.api.command.InspectContainerResponse;
import com.github.dockerjava.api.model.ExposedPort;

/** Runs an isolated KRaft broker and distributed Connect worker from the resolved Kafka runtime. */
final class DamengKafkaConnectEnvironment implements AutoCloseable {
    static final String CONFIG_TOPIC = "dameng-connect-configs";
    static final String OFFSET_TOPIC = "dameng-connect-offsets";
    static final String STATUS_TOPIC = "dameng-connect-status";
    static final String DATABASE_USER = "SYSDBA";
    static final String DATABASE_PASSWORD = "SYSDBA001";

    private static final String JAVA_IMAGE = System.getProperty("dameng.connect.java.image", "hub.badxiao.top/library/flink:2.2.1-java21");
    private static final String DATABASE_IMAGE = System.getProperty("dameng.image", "hub.badxiao.top/xuxuclassmate/dameng:latest");
    private static final Duration START_TIMEOUT = Duration.ofMinutes(3);
    private static final String SEND_GATE_MARKER = "/tmp/dameng-send-gate-blocked";
    private static final String LOGGING = "<configuration><appender name=\"CONSOLE\" class=\"ch.qos.logback.core.ConsoleAppender\">"
            + "<encoder><pattern>%d %-5level %logger - %msg%n</pattern></encoder></appender>"
            + "<root level=\"INFO\"><appender-ref ref=\"CONSOLE\"/></root></configuration>";

    private final Path buildDirectory = Path.of(System.getProperty("project.build.directory", "target")).toAbsolutePath();
    private final Path runtimeDirectory = buildDirectory.resolve("kafka-connect-runtime");
    private final Path pluginDirectory;
    private final Path logsDirectory;
    private final Path sendGateJar;
    private final Network network = Network.newNetwork();
    private GenericContainer<?> database;
    private KafkaRuntimeContainer kafka;
    private final List<KafkaRuntimeContainer> brokers = new ArrayList<>();
    private final int brokerCount;
    private KafkaRuntimeContainer pausedBroker;
    private GenericContainer<?> worker;
    private GenericContainer<?> secondaryWorker;
    private int databasePort;
    private int workerNumber;

    DamengKafkaConnectEnvironment() throws Exception {
        this(1);
    }

    DamengKafkaConnectEnvironment(int brokerCount) throws Exception {
        assertThat(brokerCount).isIn(1, 3);
        this.brokerCount = brokerCount;
        pluginDirectory = Files.createTempDirectory(buildDirectory, "dameng-connect-plugin-");
        logsDirectory = Files.createTempDirectory(Files.createDirectories(buildDirectory.resolve("dameng-connect-logs")), "run-");
        sendGateJar = createSendGateJar();
        extractCurrentPlugin();
        try (var files = Files.list(runtimeDirectory)) {
            final var libraries = files.map(path -> path.getFileName().toString()).toList();
            assertThat(libraries).anyMatch(name -> name.startsWith("kafka_2.13-"));
            assertThat(libraries).anyMatch(name -> name.startsWith("connect-runtime-"));
            assertThat(libraries).noneMatch(name -> name.startsWith("debezium-"));
            assertThat(libraries).noneMatch(name -> name.startsWith("DmJdbcDriver"));
        }
    }

    void start() throws Exception {
        database = new GenericContainer<>(DockerImageName.parse(DATABASE_IMAGE))
                .withImagePullPolicy(pullPolicy())
                .withNetwork(network).withNetworkAliases("dameng")
                .withLabel("io.debezium.dameng.connect-test", "database")
                .withExposedPorts(5236)
                .waitingFor(Wait.forLogMessage(".*SYSTEM IS READY.*", 1).withStartupTimeout(START_TIMEOUT));
        database.start();
        databasePort = database.getMappedPort(5236);
        prepareDatabase();

        for (int node = 1; node <= brokerCount; node++) {
            brokers.add(new KafkaRuntimeContainer(node));
        }
        kafka = brokers.get(0);
        Startables.deepStart(brokers.stream()).join();
        try (Admin admin = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers()))) {
            Awaitility.await("Kafka KRaft broker is ready").atMost(START_TIMEOUT).untilAsserted(
                    () -> assertThat(admin.describeCluster().nodes().get()).hasSize(brokerCount));
        }
        startWorker();
    }

    Connection databaseConnection() throws SQLException {
        final var connection = DriverManager.getConnection("jdbc:dm://" + database.getHost() + ":" + databasePort,
                DATABASE_USER, DATABASE_PASSWORD);
        connection.setAutoCommit(false);
        return connection;
    }

    String bootstrapServers() {
        return brokers.stream().map(broker -> broker.getHost() + ":" + broker.getMappedPort(9094)).collect(Collectors.joining(","));
    }

    String internalBootstrapServers() {
        return IntStream.rangeClosed(1, brokerCount).mapToObj(node -> brokerAlias(node) + ":9092").collect(Collectors.joining(","));
    }

    private static String brokerAlias(int node) {
        return node == 1 ? "broker" : "broker" + node;
    }

    String workerUrl() {
        final var active = worker == null ? secondaryWorker : worker;
        return "http://" + active.getHost() + ":" + active.getMappedPort(8083);
    }

    void killWorker() throws Exception {
        assertThat(worker.isRunning()).isTrue();
        worker.getDockerClient().killContainerCmd(worker.getContainerId()).withSignal("KILL").exec();
        Awaitility.await("worker process is killed").atMost(Duration.ofSeconds(30)).until(() -> !worker.isRunning());
        saveLogs(worker, "worker-" + workerNumber);
        worker.stop();
        worker = null;
    }

    void startWorker() {
        startWorker(null, 0);
    }

    void addWorker() {
        assertThat(secondaryWorker).isNull();
        secondaryWorker = createWorker("connect-secondary", null, 0);
    }

    void killAssignedWorker(String workerId) throws Exception {
        if (workerId.equals("connect-secondary:8083")) {
            assertThat(secondaryWorker).isNotNull();
            secondaryWorker.getDockerClient().killContainerCmd(secondaryWorker.getContainerId()).withSignal("KILL").exec();
            Awaitility.await().atMost(Duration.ofSeconds(30)).until(() -> !secondaryWorker.isRunning());
            saveLogs(secondaryWorker, "worker-secondary");
            secondaryWorker.stop();
            secondaryWorker = null;
        }
        else {
            assertThat(workerId).isEqualTo("connect:8083");
            killWorker();
        }
    }

    int pauseTopicLeader(String topic) throws Exception {
        final int leader = topicLeader(topic);
        pausedBroker = brokers.get(leader - 1);
        pausedBroker.getDockerClient().pauseContainerCmd(pausedBroker.getContainerId()).exec();
        assertThat(pausedBroker.getDockerClient().inspectContainerCmd(pausedBroker.getContainerId()).exec().getState().getPaused()).isTrue();
        return leader;
    }

    int topicLeader(String topic) throws Exception {
        try (Admin admin = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers()))) {
            return admin.describeTopics(List.of(topic)).allTopicNames().get(15, TimeUnit.SECONDS)
                    .get(topic).partitions().get(0).leader().id();
        }
    }

    void resumeBroker() {
        pausedBroker.getDockerClient().unpauseContainerCmd(pausedBroker.getContainerId()).exec();
        pausedBroker = null;
    }

    void startGatedWorker(String topic, int allowedRecords) {
        assertThat(topic).isNotBlank();
        assertThat(allowedRecords).isPositive();
        startWorker(topic, allowedRecords);
    }

    void awaitWorkerSendGate() {
        Awaitility.await("source producer is blocked before sending the remaining transaction rows")
                .atMost(START_TIMEOUT).untilAsserted(() -> assertThat(worker.execInContainer("cat", SEND_GATE_MARKER).getExitCode()).isZero());
    }

    private void startWorker(String gatedTopic, int allowedRecords) {
        workerNumber++;
        worker = createWorker("connect", gatedTopic, allowedRecords);
    }

    private GenericContainer<?> createWorker(String alias, String gatedTopic, int allowedRecords) {
        final var properties = new Properties();
        properties.setProperty("bootstrap.servers", internalBootstrapServers());
        properties.setProperty("group.id", "dameng-connect-e2e");
        properties.setProperty("config.storage.topic", CONFIG_TOPIC);
        properties.setProperty("offset.storage.topic", OFFSET_TOPIC);
        properties.setProperty("status.storage.topic", STATUS_TOPIC);
        properties.setProperty("config.storage.replication.factor", Integer.toString(brokerCount));
        properties.setProperty("offset.storage.replication.factor", Integer.toString(brokerCount));
        properties.setProperty("status.storage.replication.factor", Integer.toString(brokerCount));
        properties.setProperty("offset.storage.partitions", "1");
        properties.setProperty("status.storage.partitions", "1");
        properties.setProperty("key.converter", "org.apache.kafka.connect.json.JsonConverter");
        properties.setProperty("value.converter", "org.apache.kafka.connect.json.JsonConverter");
        properties.setProperty("key.converter.schemas.enable", "false");
        properties.setProperty("value.converter.schemas.enable", "false");
        properties.setProperty("offset.flush.interval.ms", "1000");
        properties.setProperty("offset.flush.timeout.ms", "30000");
        properties.setProperty("scheduled.rebalance.max.delay.ms", "0");
        properties.setProperty("listeners", "http://0.0.0.0:8083");
        properties.setProperty("rest.advertised.host.name", alias);
        properties.setProperty("rest.advertised.port", "8083");
        properties.setProperty("plugin.path", "/plugins");
        properties.setProperty("plugin.discovery", "service_load");
        if (gatedTopic != null) {
            properties.setProperty("producer.interceptor.classes", DamengKafkaConnectSendGate.class.getName());
            properties.setProperty("producer.dameng.test.gate.topic", gatedTopic);
            properties.setProperty("producer.dameng.test.gate.allowed.records", Integer.toString(allowedRecords));
        }
        final var created = new GenericContainer<>(DockerImageName.parse(JAVA_IMAGE))
                .withImagePullPolicy(pullPolicy()).withNetwork(network).withNetworkAliases(alias)
                .withLabel("io.debezium.dameng.connect-test", "worker")
                .withExposedPorts(8083).withEnv("JAVA_TOOL_OPTIONS", "-Xmx1g")
                .withCreateContainerCmdModifier(command -> command.withEntrypoint("/bin/sh"))
                .withCommand("-c", "exec java -Dlogback.configurationFile=/runtime/logback.xml -cp '/runtime/*' "
                        + "org.apache.kafka.connect.cli.ConnectDistributed /runtime/worker.properties")
                .withCopyFileToContainer(MountableFile.forHostPath(runtimeDirectory), "/runtime")
                // Files.createTempDirectory uses mode 0700 on Linux. The Java image
                // runs as a non-root user, so copied plugin paths must be readable.
                .withCopyFileToContainer(MountableFile.forHostPath(pluginDirectory, 0755), "/plugins")
                .withCopyToContainer(Transferable.of(LOGGING), "/runtime/logback.xml")
                .withCopyToContainer(Transferable.of(propertiesText(properties)), "/runtime/worker.properties")
                .waitingFor(Wait.forHttp("/connectors").forPort(8083).withStartupTimeout(START_TIMEOUT));
        if (gatedTopic != null) {
            // This test fixture is deliberately outside the packaged connector.
            // The replacement worker runs without either the jar or the gate.
            created.withCopyFileToContainer(MountableFile.forHostPath(sendGateJar, 0644), "/runtime/dameng-send-gate.jar");
        }
        created.start();
        return created;
    }

    private Path createSendGateJar() throws IOException {
        final var jar = Files.createTempFile(buildDirectory, "dameng-connect-send-gate-", ".jar");
        final var resource = DamengKafkaConnectSendGate.class.getName().replace('.', '/') + ".class";
        try (var input = DamengKafkaConnectSendGate.class.getClassLoader().getResourceAsStream(resource);
                var output = new JarOutputStream(Files.newOutputStream(jar))) {
            if (input == null) {
                throw new IOException("Missing compiled Kafka producer gate fixture: " + resource);
            }
            output.putNextEntry(new JarEntry(resource));
            input.transferTo(output);
            output.closeEntry();
        }
        return jar;
    }

    private void prepareDatabase() throws Exception {
        try (var connection = databaseConnection(); var statement = connection.createStatement()) {
            connection.setAutoCommit(true);
            statement.execute("SP_SET_PARA_VALUE(2, 'RLOG_APPEND_LOGIC', 2)");
            statement.execute("SP_SET_PARA_VALUE(2, 'RLOG_IGNORE_TABLE_SET', 1)");
            try (var rows = statement.executeQuery("SELECT PARA_NAME FROM V$DM_INI WHERE PARA_NAME='RLOG_LLOG_COMPRESS'")) {
                if (rows.next()) {
                    try (var compression = connection.createStatement()) {
                        compression.execute("SP_SET_PARA_VALUE(2, 'RLOG_LLOG_COMPRESS', 0)");
                    }
                }
            }
            statement.execute("ALTER DATABASE MOUNT");
            assertThat(database.execInContainer("mkdir", "-p", "/opt/dmdbms/data/DAMENG/dbz_connect_archive").getExitCode()).isZero();
            statement.execute("ALTER DATABASE ARCHIVELOG");
            statement.execute("ALTER DATABASE ADD ARCHIVELOG 'DEST=/opt/dmdbms/data/DAMENG/dbz_connect_archive, TYPE=LOCAL, FILE_SIZE=64, SPACE_LIMIT=1024'");
            statement.execute("ALTER DATABASE OPEN");
        }
        database.getDockerClient().restartContainerCmd(database.getContainerId()).exec();
        final var inspect = database.getDockerClient().inspectContainerCmd(database.getContainerId()).exec();
        databasePort = Integer.parseInt(inspect.getNetworkSettings().getPorts().getBindings().get(ExposedPort.tcp(5236))[0].getHostPortSpec());
        Awaitility.await("DM is ready after CDC configuration").atMost(START_TIMEOUT).until(() -> {
            try (var ignored = databaseConnection()) {
                return true;
            }
            catch (SQLException e) {
                return false;
            }
        });
    }

    private void extractCurrentPlugin() throws Exception {
        final String stem = "debezium-connector-dameng-" + Module.version();
        final Path archive = buildDirectory.resolve(stem + "-plugin.zip");
        final Path moduleJar = buildDirectory.resolve(stem + ".jar");
        assertThat(archive).as("Build the current plugin with -Pdameng-connect-tests,assembly").exists();
        Path packagedModule = null;
        try (var input = new ZipInputStream(Files.newInputStream(archive))) {
            for (var entry = input.getNextEntry(); entry != null; entry = input.getNextEntry()) {
                final Path destination = pluginDirectory.resolve(entry.getName()).normalize();
                if (!destination.startsWith(pluginDirectory)) {
                    throw new IOException("Plugin archive entry escapes extraction directory: " + entry.getName());
                }
                if (entry.isDirectory()) {
                    Files.createDirectories(destination);
                }
                else {
                    Files.createDirectories(destination.getParent());
                    Files.copy(input, destination);
                    if (destination.getFileName().toString().equals(stem + ".jar")) {
                        packagedModule = destination;
                    }
                }
            }
        }
        assertThat(packagedModule).as("Connector module is included in the deployment archive").isNotNull();
        final var digest = MessageDigest.getInstance("SHA-256");
        assertThat(digest.digest(Files.readAllBytes(packagedModule))).as("Plugin archive contains this build's connector jar")
                .isEqualTo(digest.digest(Files.readAllBytes(moduleJar)));
    }

    private static ImagePullPolicy pullPolicy() {
        return Boolean.getBoolean("dameng.connect.allow.image.pull") ? PullPolicy.defaultPolicy() : ignored -> false;
    }

    private static String propertiesText(Properties properties) {
        try (var writer = new StringWriter()) {
            properties.store(writer, "Dameng distributed Kafka Connect regression");
            return writer.toString();
        }
        catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private void saveLogs(GenericContainer<?> container, String name) throws IOException {
        if (container != null && container.getContainerId() != null) {
            Files.writeString(logsDirectory.resolve(name + ".log"), container.getLogs(), StandardCharsets.UTF_8);
        }
    }

    @Override
    public void close() throws Exception {
        try {
            try {
                saveLogs(worker, "worker-" + workerNumber);
            }
            finally {
                try {
                    if (worker != null) {
                        worker.stop();
                    }
                }
                finally {
                    try {
                        saveLogs(secondaryWorker, "worker-secondary");
                    }
                    finally {
                        if (secondaryWorker != null) {
                            secondaryWorker.stop();
                        }
                    }
                }
            }
        }
        finally {
            try {
                try {
                    saveLogs(kafka, "kafka");
                }
                finally {
                    if (kafka != null) {
                        for (var broker : brokers) {
                            try {
                                saveLogs(broker, "kafka-" + broker.node);
                            }
                            finally {
                                broker.stop();
                            }
                        }
                    }
                }
            }
            finally {
                try {
                    try {
                        saveLogs(database, "dameng");
                    }
                    finally {
                        if (database != null) {
                            database.stop();
                        }
                    }
                }
                finally {
                    network.close();
                }
            }
        }
    }

    private final class KafkaRuntimeContainer extends GenericContainer<KafkaRuntimeContainer> {
        private final int node;

        private KafkaRuntimeContainer(int node) {
            super(DockerImageName.parse(JAVA_IMAGE));
            this.node = node;
            withImagePullPolicy(pullPolicy());
            withNetwork(network).withNetworkAliases(brokerAlias(node));
            withLabel("io.debezium.dameng.connect-test", "broker");
            withExposedPorts(9094);
            withEnv("JAVA_TOOL_OPTIONS", "-Xmx1g");
            withCreateContainerCmdModifier(command -> command.withEntrypoint("/bin/sh"));
            withCommand("-c", "while [ ! -f /runtime/broker.properties ]; do sleep 0.1; done; "
                    + "java -Dlogback.configurationFile=/runtime/logback.xml -cp '/runtime/*' kafka.tools.StorageTool "
                    + "format --ignore-formatted -t 5L6g3nShT-eMCtK--X86sw -c /runtime/broker.properties && "
                    + "exec java -Dlogback.configurationFile=/runtime/logback.xml -cp '/runtime/*' kafka.Kafka /runtime/broker.properties");
            withCopyFileToContainer(MountableFile.forHostPath(runtimeDirectory), "/runtime");
            withCopyToContainer(Transferable.of(LOGGING), "/runtime/logback.xml");
            waitingFor(Wait.forListeningPort().withStartupTimeout(START_TIMEOUT));
        }

        @Override
        protected void containerIsStarting(InspectContainerResponse containerInfo) {
            final var properties = new Properties();
            properties.setProperty("node.id", Integer.toString(node));
            properties.setProperty("process.roles", "broker,controller");
            properties.setProperty("controller.quorum.voters", IntStream.rangeClosed(1, brokerCount)
                    .mapToObj(member -> member + "@" + brokerAlias(member) + ":9093").collect(Collectors.joining(",")));
            properties.setProperty("controller.listener.names", "CONTROLLER");
            properties.setProperty("listeners", "INTERNAL://0.0.0.0:9092,EXTERNAL://0.0.0.0:9094,CONTROLLER://0.0.0.0:9093");
            properties.setProperty("advertised.listeners", "INTERNAL://" + brokerAlias(node) + ":9092,EXTERNAL://" + getHost() + ":" + getMappedPort(9094));
            properties.setProperty("listener.security.protocol.map", "INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT");
            properties.setProperty("inter.broker.listener.name", "INTERNAL");
            properties.setProperty("log.dirs", "/tmp/kafka-logs");
            properties.setProperty("num.partitions", "1");
            properties.setProperty("default.replication.factor", Integer.toString(brokerCount));
            properties.setProperty("offsets.topic.replication.factor", Integer.toString(brokerCount));
            properties.setProperty("transaction.state.log.replication.factor", Integer.toString(brokerCount));
            properties.setProperty("transaction.state.log.min.isr", brokerCount == 1 ? "1" : "2");
            properties.setProperty("min.insync.replicas", brokerCount == 1 ? "1" : "2");
            properties.setProperty("group.initial.rebalance.delay.ms", "0");
            copyFileToContainer(Transferable.of(propertiesText(properties)), "/runtime/broker.properties");
        }
    }
}
