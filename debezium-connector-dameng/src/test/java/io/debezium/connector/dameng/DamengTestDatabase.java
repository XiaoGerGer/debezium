/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import org.awaitility.Awaitility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.utility.DockerImageName;

import com.github.dockerjava.api.model.ExposedPort;

/** Database lifecycle shared by the engine and direct JDBC integration suites. */
final class DamengTestDatabase {
    private static final Logger LOGGER = LoggerFactory.getLogger(DamengTestDatabase.class);
    private static final String USER = System.getProperty("dameng.user", "SYSDBA");
    private static final String PASSWORD = System.getProperty("dameng.password", "SYSDBA001");
    private static final String SCHEMA = System.getProperty("dameng.schema", "SYSDBA");
    private static GenericContainer<?> container;
    private static String jdbcUrl;
    private static String hostname;
    private static int port;
    private static int users;
    private static Charset charset = StandardCharsets.UTF_8;
    private static final List<String> hiddenArchives = new ArrayList<>();

    private DamengTestDatabase() {
    }

    static synchronized void start() throws Exception {
        start(0, 0);
    }

    static synchronized void start(int backslashEscape, int nestedComments) throws Exception {
        start(backslashEscape, nestedComments, 1);
    }

    static synchronized void start(int backslashEscape, int nestedComments, int caseSensitive) throws Exception {
        if (caseSensitive != 0 && caseSensitive != 1) {
            throw new IllegalArgumentException("CASE_SENSITIVE must be 0 or 1");
        }
        if (users++ > 0) {
            return;
        }
        try {
            Class.forName("dm.jdbc.driver.DmDriver");
            final int mode = requestedCompatibilityMode();
            hostname = System.getProperty("dameng.hostname");
            port = Integer.getInteger("dameng.port", 5236);
            if (hostname == null) {
                container = new GenericContainer<>(DockerImageName.parse(System.getProperty("dameng.image",
                        "dameng:8.1.3.162-20240621-rev232765")))
                        .withEnv("MODE", "dmsingle")
                        .withEnv("SYSDBA_PWD", PASSWORD)
                        .withEnv("CASE_SENSITIVE", Integer.toString(caseSensitive))
                        .withExposedPorts(5236)
                        .waitingFor(Wait.forListeningPort().withStartupTimeout(Duration.ofMinutes(5)));
                container.start();
                hostname = container.getHost();
                port = container.getMappedPort(5236);
                jdbcUrl = "jdbc:dm://" + hostname + ":" + port;
                Awaitility.await("DM database accepts JDBC connections")
                        .atMost(Duration.ofMinutes(5))
                        .pollInterval(Duration.ofSeconds(2))
                        .until(DamengTestDatabase::canConnect);
                prepareContainerForCdc(mode, backslashEscape, nestedComments);
            }
            jdbcUrl = "jdbc:dm://" + hostname + ":" + port;
            verifyCompatibilityMode(mode);
            verifyLexicalParameter("BACKSLASH_ESCAPE", backslashEscape);
            verifyLexicalParameter("NESTED_C_STYLE_COMMENT", nestedComments);
            try (var admin = DriverManager.getConnection(jdbcUrl, USER, PASSWORD);
                    var statement = admin.createStatement();
                    var result = statement.executeQuery("SELECT SF_GET_CASE_SENSITIVE_FLAG()")) {
                assertThat(result.next()).isTrue();
                assertThat(result.getInt(1)).as("CASE_SENSITIVE initialization setting").isEqualTo(caseSensitive);
            }
            recordEnvironment();
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

    static String executeDisql(String sql) throws Exception {
        if (container == null) {
            throw new IllegalStateException("DISQL lexical tests require a test-owned database container");
        }
        final var path = "/tmp/dbz-lexical-test.sql";
        container.copyFileToContainer(Transferable.of((sql + "\nEXIT;\n").getBytes(StandardCharsets.UTF_8)), path);
        final var result = container.execInContainer("env", "LD_LIBRARY_PATH=/opt/dmdbms/bin:/opt/dmdbms/lib", "/opt/dmdbms/bin/disql",
                USER + "/" + PASSWORD + "@127.0.0.1:5236", "`" + path);
        assertThat(result.getExitCode()).as("DISQL exit: %s", result.getStderr()).isZero();
        return result.getStdout() + result.getStderr();
    }

    /** Freeze inputs for paired miner benchmarks; never operates on an external database. */
    static List<String> freezeBenchmarkArchives(List<String> sources, Path evidenceDirectory) throws Exception {
        if (container == null) {
            throw new IllegalStateException("Archive benchmark requires a test-owned database container");
        }
        final var directory = "/tmp/dbz-miner-benchmark";
        assertThat(container.execInContainer("mkdir", "-p", directory).getExitCode()).isZero();
        final List<String> copies = new ArrayList<>();
        final var manifest = new StringBuilder();
        Files.createDirectories(evidenceDirectory.resolve("archives"));
        for (var source : sources) {
            final var target = directory + "/" + Path.of(source).getFileName();
            assertThat(container.execInContainer("cp", source, target).getExitCode()).isZero();
            final var hash = container.execInContainer("sha256sum", target);
            final var size = container.execInContainer("stat", "-c", "%s", target);
            assertThat(hash.getExitCode()).isZero();
            assertThat(size.getExitCode()).isZero();
            final var local = evidenceDirectory.resolve("archives").resolve(Path.of(source).getFileName());
            container.copyFileFromContainer(target, local.toAbsolutePath().toString());
            final var digest = java.security.MessageDigest.getInstance("SHA-256");
            try (var input = Files.newInputStream(local)) {
                final byte[] buffer = new byte[64 * 1024];
                int read;
                while ((read = input.read(buffer)) != -1) {
                    digest.update(buffer, 0, read);
                }
            }
            assertThat(java.util.HexFormat.of().formatHex(digest.digest())).isEqualTo(hash.getStdout().split(" ")[0]);
            manifest.append(hash.getStdout().trim()).append(" bytes=").append(size.getStdout().trim()).append('\n');
            copies.add(target);
        }
        Files.writeString(evidenceDirectory.resolve("archives.txt"), manifest);
        return copies;
    }

    /**
     * The published DM image starts in native mode without archive logging. Configure only the
     * container created by this test class. An explicitly supplied hostname is never changed.
     */
    private static void prepareContainerForCdc(int mode, int backslashEscape, int nestedComments) throws Exception {
        try {
            executeAdmin("SP_SET_PARA_VALUE(2, 'COMPATIBLE_MODE', " + mode + ")");
        }
        catch (SQLException rejected) {
            throw new SQLException("Fixture cannot set COMPATIBLE_MODE=" + mode + " on this database image", rejected);
        }
        executeAdmin("SP_SET_PARA_VALUE(2, 'RLOG_APPEND_LOGIC', 2)");
        executeAdmin("SP_SET_PARA_VALUE(2, 'BACKSLASH_ESCAPE', " + backslashEscape + ")");
        executeAdmin("SP_SET_PARA_VALUE(2, 'NESTED_C_STYLE_COMMENT', " + nestedComments + ")");
        executeAdmin("SP_SET_PARA_VALUE(2, 'RLOG_IGNORE_TABLE_SET', 1)");
        if (hasIniParameter("RLOG_LLOG_COMPRESS")) {
            executeAdmin("SP_SET_PARA_VALUE(2, 'RLOG_LLOG_COMPRESS', 0)");
        }
        executeAdmin("ALTER DATABASE MOUNT");
        final var directory = container.execInContainer("mkdir", "-p", "/opt/dmdbms/data/DAMENG/dbz_archive");
        assertThat(directory.getExitCode()).as("archive directory creation").isZero();
        // The vendor entrypoint runs dmserver as the data directory owner, while Docker exec uses root.
        assertThat(container.execInContainer("chown", "--reference=/opt/dmdbms/data/DAMENG",
                "/opt/dmdbms/data/DAMENG/dbz_archive").getExitCode()).as("archive directory ownership").isZero();
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

    static int requestedCompatibilityMode() {
        final var value = System.getProperty("dameng.compatible.mode", "0");
        final int mode = Integer.parseInt(value);
        if (mode < 0) {
            throw new IllegalArgumentException("dameng.compatible.mode must be nonnegative: " + value);
        }
        return mode;
    }

    private static void verifyCompatibilityMode(int mode) throws SQLException {
        try (Connection admin = DriverManager.getConnection(jdbcUrl, USER, PASSWORD);
                var statement = admin.prepareStatement(
                        "SELECT PARA_VALUE, SESS_VALUE, FILE_VALUE FROM V$DM_INI WHERE PARA_NAME='COMPATIBLE_MODE'")) {
            try (var result = statement.executeQuery()) {
                assertThat(result.next()).as("COMPATIBLE_MODE is readable").isTrue();
                for (int index = 1; index <= 3; index++) {
                    assertThat(result.getInt(index)).as("COMPATIBLE_MODE system/session/file value %s", index).isEqualTo(mode);
                }
            }
        }
    }

    private static void verifyLexicalParameter(String name, int expected) throws SQLException {
        try (Connection admin = DriverManager.getConnection(jdbcUrl, USER, PASSWORD);
                var statement = admin.prepareStatement("SELECT PARA_VALUE, SESS_VALUE, FILE_VALUE FROM V$DM_INI WHERE PARA_NAME=?")) {
            statement.setString(1, name);
            try (var result = statement.executeQuery()) {
                assertThat(result.next()).as("%s is readable", name).isTrue();
                for (int index = 1; index <= 3; index++) {
                    assertThat(result.getInt(index)).as("%s system/session/file value %s", name, index).isEqualTo(expected);
                }
            }
        }
    }

    private static void recordEnvironment() throws Exception {
        final var evidence = new StringBuilder();
        try (Connection admin = DriverManager.getConnection(jdbcUrl, USER, PASSWORD);
                Statement statement = admin.createStatement()) {
            final var metadata = admin.getMetaData();
            evidence.append("product=").append(metadata.getDatabaseProductName()).append('\n')
                    .append("version=").append(metadata.getDatabaseProductVersion()).append('\n')
                    .append("driver=").append(metadata.getDriverVersion()).append('\n');
            try (var result = statement.executeQuery("SELECT SF_GET_CASE_SENSITIVE_FLAG()")) {
                assertThat(result.next()).isTrue();
                evidence.append("CASE_SENSITIVE=").append(result.getInt(1)).append('\n');
            }
            try (var result = statement.executeQuery("SELECT SF_GET_UNICODE_FLAG()")) {
                assertThat(result.next()).isTrue();
                charset = switch (result.getInt(1)) {
                    case 0 -> Charset.forName("GB18030");
                    case 1 -> StandardCharsets.UTF_8;
                    case 2 -> Charset.forName("EUC-KR");
                    default -> throw new IllegalStateException("Unknown test database character set");
                };
                evidence.append("charset=").append(charset.name()).append('\n');
            }
            try (var result = statement.executeQuery("SELECT * FROM V$VERSION")) {
                while (result.next()) {
                    evidence.append("build=").append(result.getString(1)).append('\n');
                }
            }
            try (var result = statement.executeQuery("SELECT PARA_NAME, PARA_VALUE, SESS_VALUE, FILE_VALUE FROM V$DM_INI "
                    + "WHERE PARA_NAME IN ('COMPATIBLE_MODE','LENGTH_IN_CHAR','DECIMAL_ENHANCED','IFUN_DATETIME_MODE',"
                    + "'NUMBER_MODE','FLOAT_MODE','ORA_DATE_FMT','MY_STRICT_TABLES','NVARCHAR_LENGTH_IN_CHAR','RLOG_APPEND_LOGIC','RLOG_IGNORE_TABLE_SET','RLOG_LLOG_COMPRESS',"
                    + "'BACKSLASH_ESCAPE','NESTED_C_STYLE_COMMENT') ORDER BY PARA_NAME")) {
                while (result.next()) {
                    evidence.append(result.getString(1)).append('=').append(result.getString(2))
                            .append(";session=").append(result.getString(3)).append(";file=").append(result.getString(4)).append('\n');
                }
            }
        }
        LOGGER.info("DM datatype test environment:\n{}", evidence);
        final var directory = Path.of(System.getProperty("dameng.evidence.directory",
                System.getProperty("project.build.directory", "target") + "/dameng-mode-evidence"));
        Files.createDirectories(directory);
        Files.writeString(directory.resolve("environment-mode-" + requestedCompatibilityMode() + ".txt"), evidence);
        final var selection = java.util.regex.Pattern.compile(System.getProperty("dameng.datatype.case", ".*"));
        final var catalog = new StringBuilder();
        final var cases = DamengDatatypeCases.boundaries().filter(type -> selection.matcher(type.name()).find()).toList();
        for (int index = 0; index < cases.size(); index++) {
            final var type = cases.get(index);
            catalog.append(index + 1).append('\t').append(type.name()).append('\t')
                    .append("settings=").append(type.settings()).append('\t').append("values=").append(type.values().size())
                    .append('\t').append("rejected=").append(type.rejected().size()).append('\n');
        }
        Files.writeString(directory.resolve("case-catalog.txt"), catalog);
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

    static Charset charset() {
        return charset;
    }

    static void recordCapability(String capability, String outcome) throws Exception {
        final var directory = Path.of(System.getProperty("dameng.evidence.directory",
                System.getProperty("project.build.directory", "target") + "/dameng-mode-evidence"));
        Files.writeString(directory.resolve("capabilities-mode-" + requestedCompatibilityMode() + ".txt"),
                capability + "=" + outcome.replace('\n', ' ') + '\n', java.nio.file.StandardOpenOption.CREATE, java.nio.file.StandardOpenOption.APPEND);
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
