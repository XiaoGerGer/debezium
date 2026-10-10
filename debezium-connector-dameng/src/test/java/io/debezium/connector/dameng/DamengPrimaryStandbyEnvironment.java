/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.awaitility.Awaitility;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

/**
 * Isolated physical DM Data Watch primary/standby pair. The standby is restored
 * from a real DMRMAN backup; independent initialized databases are not replicas.
 * Switching is performed through a MANUAL dmwatcher group and dmmonitor.
 */
final class DamengPrimaryStandbyEnvironment implements AutoCloseable {
    static final String USER = "SYSDBA";
    static final String PASSWORD = "SYSDBA001";
    private static final String BIN = "/opt/dmdbms/bin";
    private static final String DATA = "/opt/dmdbms/data/DAMENG";
    private static final String INI = DATA + "/dm.ini";
    private static final String GROUP = "DBZ_HA";
    private final Network network = Network.newNetwork();
    private final List<GenericContainer<?>> nodes = new ArrayList<>();
    private final String[] instanceNames = new String[2];
    private final int oguid = 100_000 + Math.floorMod(UUID.randomUUID().hashCode(), 900_000);
    private final Path logDirectory;
    private int primary;

    DamengPrimaryStandbyEnvironment() throws IOException {
        Files.createDirectories(Path.of("target"));
        logDirectory = Files.createTempDirectory(Path.of("target"), "dameng-ha-");
        final String suffix = UUID.randomUUID().toString().replace("-", "").substring(0, 6).toUpperCase();
        instanceNames[0] = "DBZ_" + suffix + "_A";
        instanceNames[1] = "DBZ_" + suffix + "_B";
    }

    void start() throws Exception {
        Class.forName("dm.jdbc.driver.DmDriver");
        try {
            for (int i = 0; i < 2; i++) {
                final var container = new GenericContainer<>(DockerImageName.parse(
                        System.getProperty("dameng.image", "hub.badxiao.top/xuxuclassmate/dameng:latest")))
                        .withNetwork(network).withExposedPorts(5236).withCommand("sleep", "infinity")
                        .waitingFor(Wait.forSuccessfulCommand("test -x " + BIN + "/dminit"));
                nodes.add(container);
                container.start();
                command(i, "cd " + BIN + " && ./dminit PATH=/opt/dmdbms/data DB_NAME=DAMENG INSTANCE_NAME=" + instanceNames[i]
                        + " SYSDBA_PWD=" + PASSWORD + " PAGE_SIZE=8 CASE_SENSITIVE=1 UNICODE_FLAG=1 LENGTH_IN_CHAR=0 LOG_SIZE=256");
                startServer(i, false);
                awaitConnection(i);
                sql(i, "SHUTDOWN NORMAL");
                awaitServerStopped(i);
                command(i, "cd " + BIN + " && ./DmAPService start");
            }
            command(0, "cd " + BIN + " && ./dmrman CTLSTMT=\"BACKUP DATABASE '" + INI
                    + "' FULL TO DBZ_HA_BACKUP BACKUPSET '/tmp/dbz_ha_backup'\"");
            command(0, "tar -C /tmp -czf /tmp/dbz_ha_backup.tgz dbz_ha_backup");
            final Path backup = logDirectory.resolve("backup.tgz");
            nodes.get(0).copyFileFromContainer("/tmp/dbz_ha_backup.tgz", backup.toString());
            nodes.get(1).copyFileToContainer(MountableFile.forHostPath(backup), "/tmp/dbz_ha_backup.tgz");
            command(1, "tar -C /tmp -xzf /tmp/dbz_ha_backup.tgz");
            command(1, "cd " + BIN + " && ./dmrman CTLSTMT=\"RESTORE DATABASE '" + INI
                    + "' FROM BACKUPSET '/tmp/dbz_ha_backup'\"");
            command(1, "cd " + BIN + " && ./dmrman CTLSTMT=\"RECOVER DATABASE '" + INI + "' UPDATE DB_MAGIC\"");
            Files.delete(backup);
            for (int i = 0; i < 2; i++) {
                configure(i);
                startServer(i, true);
                awaitConnection(i);
                sql(i, "SP_SET_OGUID(" + oguid + ")", "ALTER DATABASE " + (i == 0 ? "PRIMARY" : "STANDBY"),
                        "SP_SET_PARA_VALUE(1,'ALTER_MODE_STATUS',0)");
            }
            for (int i = 0; i < 2; i++) {
                command(i, "cd " + BIN + " && nohup ./dmwatcher " + DATA + "/dmwatcher.ini > /tmp/watcher.log 2>&1 < /dev/null &");
            }
            awaitRoles();
        }
        catch (Exception | Error e) {
            close();
            throw e;
        }
    }

    private void configure(int node) throws Exception {
        final Map<String, String> values = new LinkedHashMap<>();
        values.put("INSTANCE_NAME", instanceNames[node]);
        values.put("PORT_NUM", "5236");
        values.put("DW_INACTIVE_INTERVAL", "60");
        values.put("ALTER_MODE_STATUS", "1");
        values.put("ENABLE_OFFLINE_TS", "2");
        values.put("MAL_INI", "1");
        values.put("ARCH_INI", "1");
        values.put("RLOG_SEND_APPLY_MON", "64");
        values.put("RLOG_APPEND_LOGIC", "2");
        values.put("RLOG_IGNORE_TABLE_SET", "1");
        values.put("RLOG_LLOG_COMPRESS", "0");
        values.put("MEMORY_POOL", "128");
        values.put("BUFFER", "256");
        final var ini = new StringBuilder();
        for (String line : command(node, "cat " + INI).split("\\R")) {
            final String key = line.split("=", 2)[0].trim();
            ini.append(values.containsKey(key) ? key + " = " + values.remove(key) : line).append('\n');
        }
        values.forEach((key, value) -> ini.append(key).append(" = ").append(value).append('\n'));
        write(node, "dm.ini", ini.toString());
        final var mal = new StringBuilder("MAL_CHECK_INTERVAL=2\nMAL_CONN_FAIL_INTERVAL=3\n");
        for (int i = 0; i < 2; i++) {
            final String address = nodes.get(i).getDockerClient().inspectContainerCmd(nodes.get(i).getContainerId()).exec()
                    .getNetworkSettings().getNetworks().values().iterator().next().getIpAddress();
            mal.append("[MAL_").append(i).append("]\nMAL_INST_NAME=").append(instanceNames[i]).append("\nMAL_HOST=").append(address)
                    .append("\nMAL_PORT=61141\nMAL_INST_HOST=").append(address)
                    .append("\nMAL_INST_PORT=5236\nMAL_DW_PORT=52141\nMAL_INST_DW_PORT=33141\n");
        }
        write(node, "dmmal.ini", mal.toString());
        command(node, "mkdir -p " + DATA + "/dbz_archive");
        write(node, "dmarch.ini", "[ARCHIVE_REALTIME]\nARCH_TYPE=REALTIME\nARCH_DEST=" + instanceNames[1 - node]
                + "\n[ARCHIVE_LOCAL1]\nARCH_TYPE=LOCAL\nARCH_DEST=" + DATA
                + "/dbz_archive\nARCH_FILE_SIZE=64\nARCH_SPACE_LIMIT=1024\n");
        write(node, "dmwatcher.ini", "[" + GROUP + "]\nDW_TYPE=GLOBAL\nDW_MODE=MANUAL\nDW_ERROR_TIME=10\nINST_RECOVER_TIME=3"
                + "\nINST_ERROR_TIME=10\nINST_OGUID=" + oguid + "\nINST_INI=" + INI
                + "\nINST_AUTO_RESTART=0\nINST_STARTUP_CMD=" + BIN + "/dmserver\nRLOG_SEND_THRESHOLD=0\nRLOG_APPLY_THRESHOLD=0\n");
        final var monitor = new StringBuilder("MON_DW_CONFIRM=0\nMON_LOG_PATH=/tmp\n[" + GROUP + "]\nMON_INST_OGUID=" + oguid + "\n");
        for (GenericContainer<?> container : nodes) {
            final String address = container.getDockerClient().inspectContainerCmd(container.getContainerId()).exec()
                    .getNetworkSettings().getNetworks().values().iterator().next().getIpAddress();
            monitor.append("MON_DW_IP=").append(address).append(":52141\n");
        }
        write(node, "dmmonitor.ini", monitor.toString());
    }

    private void write(int node, String filename, String text) throws IOException {
        final Path file = logDirectory.resolve(node + "-" + filename);
        Files.writeString(file, text, StandardCharsets.UTF_8);
        nodes.get(node).copyFileToContainer(MountableFile.forHostPath(file), DATA + "/" + filename);
    }

    private void startServer(int node, boolean mount) throws Exception {
        command(node, "cd " + BIN + " && nohup ./dmserver " + INI + (mount ? " mount" : "")
                + " -noconsole > /tmp/server.log 2>&1 < /dev/null &");
    }

    private void awaitConnection(int node) {
        Awaitility.await("DM HA node " + node + " accepts connections").atMost(Duration.ofMinutes(2)).pollInterval(Duration.ofSeconds(1))
                .until(() -> {
                    try (var ignored = connection(node)) {
                        return true;
                    }
                    catch (SQLException e) {
                        return false;
                    }
                });
    }

    private void awaitServerStopped(int node) {
        Awaitility.await("DM HA node " + node + " exits cleanly").atMost(Duration.ofMinutes(1)).pollInterval(Duration.ofSeconds(1))
                .until(() -> nodes.get(node).execInContainer("bash", "-c", "! pgrep -x dmserver").getExitCode() == 0);
    }

    private void awaitRoles() {
        Awaitility.await("DM HA primary and standby are OPEN").atMost(Duration.ofMinutes(2)).pollInterval(Duration.ofSeconds(1))
                .until(() -> hasRole(primary, "PRIMARY") && hasRole(1 - primary, "STANDBY")
                        && hasValidArchive(0) && hasValidArchive(1));
    }

    private boolean hasValidArchive(int node) {
        try (var connection = connection(node);
                var statement = connection.createStatement();
                var result = statement.executeQuery("SELECT ARCH_STATUS FROM V$ARCH_STATUS WHERE ARCH_TYPE='REALTIME'")) {
            return result.next() && "VALID".equals(result.getString(1));
        }
        catch (SQLException e) {
            return false;
        }
    }

    private boolean hasRole(int node, String role) {
        try (var connection = connection(node);
                var statement = connection.createStatement();
                var result = statement.executeQuery("SELECT MODE$,STATUS$ FROM V$INSTANCE")) {
            return result.next() && role.equals(result.getString(1)) && "OPEN".equals(result.getString(2));
        }
        catch (SQLException e) {
            return false;
        }
    }

    void switchover() throws Exception {
        final int nextPrimary = 1 - primary;
        final String output = monitor("switchover " + GROUP + "." + instanceNames[nextPrimary]);
        if (!output.contains("Switchover instance " + instanceNames[nextPrimary] + " success")) {
            throw new IllegalStateException("DM switchover failed: " + output);
        }
        primary = nextPrimary;
        awaitRoles();
    }

    void killPrimaryAndTakeover() throws Exception {
        final int nextPrimary = 1 - primary;
        command(primary, "pkill -KILL -x dmserver");
        Awaitility.await("DM monitor allows normal takeover of the failed primary")
                .atMost(Duration.ofMinutes(1)).pollInterval(Duration.ofSeconds(2))
                .until(() -> monitor(nextPrimary, "choose takeover " + GROUP).contains("1: " + instanceNames[nextPrimary]));
        final String output = monitor(nextPrimary, "takeover " + GROUP + "." + instanceNames[nextPrimary]);
        if (!output.contains("Success to takeover use instance " + instanceNames[nextPrimary])) {
            throw new IllegalStateException("DM normal takeover failed: " + output);
        }
        primary = nextPrimary;
        Awaitility.await("DM takeover primary is OPEN").atMost(Duration.ofMinutes(1))
                .until(() -> hasRole(primary, "PRIMARY"));
    }

    String monitor(String operation) throws Exception {
        return monitor(primary, operation);
    }

    private String monitor(int node, String operation) throws Exception {
        final String input = "login\n" + USER + "\n" + PASSWORD + "\n" + operation + "\nexit\n";
        final String command = "cd " + BIN + " && printf '%s' '" + input.replace("'", "'\\''") + "' | ./dmmonitor " + DATA + "/dmmonitor.ini";
        final String output = command(node, command);
        Files.writeString(logDirectory.resolve("monitor-" + System.nanoTime() + ".log"), output);
        return output;
    }

    String endpoints() {
        return endpoint(0) + "," + endpoint(1);
    }

    String primaryJdbcUrl() {
        return jdbcUrl(primary);
    }

    String standbyJdbcUrl() {
        return jdbcUrl(1 - primary);
    }

    String jdbcUrl(int node) {
        return "jdbc:dm://" + endpoint(node) + "?connectTimeout=2000&socketTimeout=15000";
    }

    private String endpoint(int node) {
        return nodes.get(node).getHost() + ":" + nodes.get(node).getMappedPort(5236);
    }

    Connection connection(int node) throws SQLException {
        return DriverManager.getConnection(jdbcUrl(node), USER, PASSWORD);
    }

    Connection primaryConnection() throws SQLException {
        return connection(primary);
    }

    void sql(int node, String... statements) throws SQLException {
        try (var connection = connection(node); var statement = connection.createStatement()) {
            statement.setQueryTimeout(15);
            for (String sql : statements) {
                statement.execute(sql);
            }
        }
    }

    private String command(int node, String command) throws Exception {
        final var result = nodes.get(node).execInContainer("bash", "-c", command);
        if (result.getExitCode() != 0) {
            throw new IllegalStateException("DM HA command failed: " + result.getStdout() + result.getStderr());
        }
        return result.getStdout() + result.getStderr();
    }

    @Override
    public void close() {
        for (int i = 0; i < nodes.size(); i++) {
            final GenericContainer<?> container = nodes.get(i);
            if (container.isRunning()) {
                for (String logfile : List.of("server.log", "watcher.log")) {
                    try {
                        container.copyFileFromContainer("/tmp/" + logfile, logDirectory.resolve(i + "-" + logfile).toString());
                    }
                    catch (RuntimeException e) {
                        // Setup can fail before either process has created its log.
                    }
                }
            }
        }
        // MANUAL mode and INST_AUTO_RESTART=0 prevent promotion or restart.
        // Stop this isolated primary before the standby; no user database is addressed.
        for (int i : new int[]{ primary, 1 - primary }) {
            if (i < nodes.size()) {
                nodes.get(i).stop();
            }
        }
        network.close();
    }
}
