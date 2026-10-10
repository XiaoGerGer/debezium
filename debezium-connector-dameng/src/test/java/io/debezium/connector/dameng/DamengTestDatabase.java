/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import org.awaitility.Awaitility;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import com.github.dockerjava.api.model.ExposedPort;

/** Database lifecycle shared by the engine and direct JDBC integration suites. */
final class DamengTestDatabase {
    private static final String USER = System.getProperty("dameng.user", "SYSDBA");
    private static final String PASSWORD = System.getProperty("dameng.password", "SYSDBA001");
    private static final String SCHEMA = System.getProperty("dameng.schema", "SYSDBA");
    private static GenericContainer<?> container;
    private static String jdbcUrl;
    private static String hostname;
    private static int port;
    private static int users;
    private static final List<String> hiddenArchives = new ArrayList<>();

    private DamengTestDatabase() {
    }

    static synchronized void start() throws Exception {
        if (users++ > 0) {
            return;
        }
        try {
            Class.forName("dm.jdbc.driver.DmDriver");
            hostname = System.getProperty("dameng.hostname");
            port = Integer.getInteger("dameng.port", 5236);
            if (hostname == null) {
                container = new GenericContainer<>(DockerImageName.parse(System.getProperty("dameng.image", "hub.badxiao.top/xuxuclassmate/dameng:latest")))
                        .withExposedPorts(5236)
                        .waitingFor(Wait.forLogMessage(".*SYSTEM IS READY.*", 1).withStartupTimeout(Duration.ofMinutes(5)));
                container.start();
                hostname = container.getHost();
                port = container.getMappedPort(5236);
                jdbcUrl = "jdbc:dm://" + hostname + ":" + port;
                prepareContainerForCdc();
            }
            jdbcUrl = "jdbc:dm://" + hostname + ":" + port;
        }
        catch (Exception | AssertionError e) {
            users = 0;
            if (container != null) {
                container.stop();
                container = null;
            }
            throw e;
        }
    }

    static synchronized void stop() {
        if (users > 0 && --users == 0 && container != null) {
            container.stop();
            container = null;
        }
    }

    /**
     * The published DM image starts in native mode without archive logging. Configure only the
     * container created by this test class. An explicitly supplied hostname is never changed.
     */
    private static void prepareContainerForCdc() throws Exception {
        executeAdmin("SP_SET_PARA_VALUE(2, 'RLOG_APPEND_LOGIC', 2)");
        executeAdmin("SP_SET_PARA_VALUE(2, 'RLOG_IGNORE_TABLE_SET', 1)");
        if (hasIniParameter("RLOG_LLOG_COMPRESS")) {
            executeAdmin("SP_SET_PARA_VALUE(2, 'RLOG_LLOG_COMPRESS', 0)");
        }
        executeAdmin("ALTER DATABASE MOUNT");
        final var directory = container.execInContainer("mkdir", "-p", "/opt/dmdbms/data/DAMENG/dbz_archive");
        assertThat(directory.getExitCode()).as("archive directory creation").isZero();
        executeAdmin("ALTER DATABASE ARCHIVELOG");
        executeAdmin("ALTER DATABASE ADD ARCHIVELOG 'DEST=/opt/dmdbms/data/DAMENG/dbz_archive, TYPE=LOCAL, FILE_SIZE=64, SPACE_LIMIT=1024'");
        executeAdmin("ALTER DATABASE OPEN");
        container.getDockerClient().restartContainerCmd(container.getContainerId()).exec();
        final var restarted = container.getDockerClient().inspectContainerCmd(container.getContainerId()).exec();
        final var bindings = restarted.getNetworkSettings().getPorts().getBindings().get(ExposedPort.tcp(5236));
        assertThat(bindings).as("DM listener is published after restart").isNotEmpty();
        port = Integer.parseInt(bindings[0].getHostPortSpec());
        jdbcUrl = "jdbc:dm://" + hostname + ":" + port;
        Awaitility.await("DM database is ready after archive configuration")
                .atMost(Duration.ofMinutes(3))
                .pollInterval(Duration.ofSeconds(2))
                .until(DamengTestDatabase::canConnect);
    }

    private static void executeAdmin(String sql) throws SQLException {
        try (Connection admin = DriverManager.getConnection(jdbcUrl, USER, PASSWORD);
                Statement statement = admin.createStatement()) {
            statement.execute(sql);
        }
    }

    private static boolean hasIniParameter(String name) throws SQLException {
        try (Connection admin = DriverManager.getConnection(jdbcUrl, USER, PASSWORD);
                var statement = admin.prepareStatement("SELECT PARA_NAME FROM V$DM_INI WHERE PARA_NAME=?")) {
            statement.setString(1, name);
            try (var result = statement.executeQuery()) {
                return result.next();
            }
        }
    }

    private static boolean canConnect() {
        try (Connection ignored = DriverManager.getConnection(jdbcUrl, USER, PASSWORD)) {
            return true;
        }
        catch (SQLException e) {
            return false;
        }
    }

    static String hostname() {
        return hostname;
    }

    static int port() {
        return port;
    }

    static String jdbcUrl() {
        return jdbcUrl;
    }

    static String user() {
        return USER;
    }

    static String password() {
        return PASSWORD;
    }

    static String schema() {
        return SCHEMA;
    }

    /** Removes only this fixture's archived files from the database's search path. */
    static synchronized void hideManagedArchives(List<String> paths) throws Exception {
        assertThat(container).as("archive fault injection requires a fixture-owned disposable database").isNotNull();
        assertThat(paths).isNotEmpty();
        final var quarantine = "/opt/dmdbms/data/DAMENG/dbz_missing_archive";
        assertThat(container.execInContainer("mkdir", "-p", quarantine).getExitCode()).isZero();
        for (var path : paths) {
            assertThat(path).startsWith("/opt/dmdbms/data/DAMENG/dbz_archive/").doesNotContain("/../");
            final var moved = container.execInContainer("mv", "--", path, quarantine + "/");
            assertThat(moved.getExitCode()).as(moved.getStderr()).isZero();
            hiddenArchives.add(path);
        }
    }

    static synchronized void restoreManagedArchives() throws Exception {
        for (var path : List.copyOf(hiddenArchives)) {
            final var source = "/opt/dmdbms/data/DAMENG/dbz_missing_archive/" + path.substring(path.lastIndexOf('/') + 1);
            final var restored = container.execInContainer("mv", "--", source, path);
            assertThat(restored.getExitCode()).as(restored.getStderr()).isZero();
            hiddenArchives.remove(path);
        }
    }
}
