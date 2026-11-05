/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.UUID;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

/** Manual database object availability and dependency diagnostics; does not run the connector. */
class DamengNonTableDdlProbeIT {
    private static final ObjectMapper JSON = new ObjectMapper();
    private final List<String> cleanup = new ArrayList<>();
    private final ObjectNode evidence = JSON.createObjectNode();
    private final ArrayNode attempts = evidence.putArray("statements");
    private Connection jdbc;
    private String evidenceProperty = "dameng.non.table.ddl.probe.evidence";
    private String evidenceDefault = "target/non-table-ddl-probe.json";

    private enum DropDependency {
        TYPE_COLUMN,
        CLASS_COLUMN,
        NESTED_TYPE_MEMBER,
        SUBTYPE
    }

    @BeforeAll
    static void startDatabase() throws Exception {
        assumeTrue(System.getProperty("dameng.hostname") == null, "Object lifecycle tests require a test-owned database");
        // RECYCLEBIN is static: set it before the fixture configures and restarts its own database.
        final var parameter = "dameng.test.ini.RECYCLEBIN";
        final var previous = System.setProperty(parameter, "1");
        try {
            DamengTestDatabase.start();
        }
        finally {
            if (previous == null) {
                System.clearProperty(parameter);
            }
            else {
                System.setProperty(parameter, previous);
            }
        }
    }

    @AfterAll
    static void stopDatabase() {
        DamengTestDatabase.stop();
    }

    @AfterEach
    void cleanup() throws Exception {
        if (jdbc != null) {
            try {
                // Every name belongs to this test. No CASCADE is needed or allowed.
                for (int position = cleanup.size() - 1; position >= 0; position--) {
                    try {
                        execute(cleanup.get(position));
                    }
                    catch (SQLException ignored) {
                        // Objects successfully dropped by the test are already absent.
                    }
                }
            }
            finally {
                jdbc.close();
            }
        }
        writeEvidence();
    }

    @Test
    void shouldRecordLibraryDefinitionAvailability() throws Exception {
        evidenceProperty = "dameng.non.table.ddl.library.probe.evidence";
        evidenceDefault = "target/non-table-ddl-library-probe.json";
        final var name = "DBZ_NTL_" + UUID.randomUUID().toString().replace("-", "").substring(0, 10).toUpperCase();
        jdbc = DriverManager.getConnection(DamengTestDatabase.jdbcUrl(), DamengTestDatabase.user(), DamengTestDatabase.password());
        jdbc.setAutoCommit(true);
        evidence.put("databaseVersion", jdbc.getMetaData().getDatabaseProductVersion());
        evidence.put("compatibleMode", DamengTestDatabase.requestedCompatibilityMode());
        evidence.put("scope", "database-only documented LIBRARY availability probe; no connector support conclusion");
        evidence.put("connectorSupportVerified", false);
        evidence.put("validationPassed", false);
        final List<String> libraries = List.of(name + "_SIMPLE", DamengTestDatabase.schema() + "." + name + "_REPLACED");
        int accepted = 0;
        for (int position = 0; position < libraries.size(); position++) {
            final var library = libraries.get(position);
            final var attempt = attempts.addObject();
            final var create = attempt.putObject("create");
            create.put("sql", (position == 0 ? "CREATE LIBRARY " : "CREATE OR REPLACE LIBRARY ")
                    + library + " AS '/opt/dmdbms/bin/libdodbc.so'");
            try {
                execute(create.required("sql").asText());
                create.put("jdbc", "accepted");
                cleanup.add("DROP LIBRARY " + library);
            }
            catch (SQLException error) {
                create.put("jdbc", "rejected");
                create.put("jdbcError", error.toString());
                create.put("sqlState", error.getSQLState());
                create.put("vendorCode", error.getErrorCode());
                create.put("conclusion", "database rejected this LIBRARY form; connector support not demonstrated");
                writeEvidence();
                assertThat(error.getErrorCode()).as("only the observed syntax rejection is an expected LIBRARY availability result").isEqualTo(-2007);
                continue;
            }
            final var drop = attempt.putObject("drop");
            drop.put("sql", "DROP LIBRARY " + library);
            try {
                execute(drop.required("sql").asText());
                drop.put("jdbc", "accepted");
                accepted++;
            }
            catch (SQLException error) {
                drop.put("jdbc", "rejected");
                drop.put("jdbcError", error.toString());
                drop.put("sqlState", error.getSQLState());
                drop.put("vendorCode", error.getErrorCode());
                writeEvidence();
                throw error;
            }
            attempt.put("conclusion", "database accepted this LIBRARY lifecycle; archived redo and CDC remain unverified");
        }
        assertThat(attempts).hasSize(2);
        evidence.put("acceptedLifecycles", accepted);
        evidence.put("conclusion", "database availability recorded separately for minimal and qualified OR REPLACE syntax; connector support not demonstrated");
        evidence.put("validationPassed", true);
        writeEvidence();
    }

    @ParameterizedTest
    @EnumSource(DropDependency.class)
    void shouldRejectDefaultTypeOrClassDropWhileReferencedByATable(DropDependency dependency) throws Exception {
        evidenceProperty = "dameng.non.table.ddl.type.drop.probe.evidence";
        evidenceDefault = "target/non-table-ddl-type-drop-" + dependency.name().toLowerCase(Locale.ROOT) + ".json";
        final var prefix = "DBZ_NTD_" + UUID.randomUUID().toString().replace("-", "").substring(0, 10).toUpperCase();
        final var owner = DamengTestDatabase.schema();
        final var type = owner + "." + prefix + "_TYPE";
        final var dependent = owner + "." + prefix + "_DEPENDENT";
        final var referenced = owner + "." + prefix + "_REF";
        final var kind = dependency == DropDependency.CLASS_COLUMN ? "CLASS" : "TYPE";
        final boolean indirect = dependency == DropDependency.NESTED_TYPE_MEMBER || dependency == DropDependency.SUBTYPE;
        jdbc = DriverManager.getConnection(DamengTestDatabase.jdbcUrl(), DamengTestDatabase.user(), DamengTestDatabase.password());
        jdbc.setAutoCommit(true);
        evidence.put("databaseVersion", jdbc.getMetaData().getDatabaseProductVersion());
        evidence.put("compatibleMode", DamengTestDatabase.requestedCompatibilityMode());
        evidence.put("dependency", dependency.name());
        evidence.put("scope", "database dependency constraint; default DROP must preserve referencing tables and their data");
        evidence.put("connectorSupportVerified", false);
        evidence.put("validationPassed", false);
        cleanup.add("DROP " + kind + " " + type + " RESTRICT");
        execute(switch (dependency) {
            case CLASS_COLUMN -> "CREATE CLASS " + type + " AS ID INT; END;";
            case SUBTYPE -> "CREATE TYPE " + type + " AS OBJECT(ID INT) NOT FINAL";
            default -> "CREATE TYPE " + type + " AS OBJECT(ID INT)";
        });
        if (indirect) {
            cleanup.add("DROP TYPE " + dependent + " RESTRICT");
            execute(dependency == DropDependency.NESTED_TYPE_MEMBER
                    ? "CREATE TYPE " + dependent + " AS OBJECT(V " + type + ")"
                    : "CREATE TYPE " + dependent + " UNDER " + type + "(V INT)");
        }
        cleanup.add("DROP TABLE " + referenced);
        execute("CREATE TABLE " + referenced + "(ID INT PRIMARY KEY,V " + (indirect ? dependent : type) + ")");
        execute("INSERT INTO " + referenced + "(ID) VALUES(1)");
        final var drop = evidence.putObject("drop");
        drop.put("sql", "DROP " + kind + " " + type);
        assertThatThrownBy(() -> {
            execute(drop.required("sql").asText());
            drop.put("jdbc", "accepted");
        }).as("default DROP must reject the %s dependency", dependency)
                .isInstanceOfSatisfying(SQLException.class, error -> {
                    drop.put("jdbc", "rejected");
                    drop.put("jdbcError", error.toString());
                    drop.put("sqlState", error.getSQLState());
                    drop.put("vendorCode", error.getErrorCode());
                });
        final var remaining = evidence.putObject("referencingTableAfterDrop");
        try (var statement = jdbc.createStatement(); var rows = statement.executeQuery("SELECT ID,V FROM " + referenced)) {
            assertThat(rows.getMetaData().getColumnCount()).isEqualTo(2);
            assertThat(rows.getMetaData().getColumnName(2)).isEqualTo("V");
            assertThat(rows.next()).isTrue();
            assertThat(rows.getInt(1)).isEqualTo(1);
            assertThat(rows.next()).isFalse();
            remaining.put("readable", true);
            remaining.put("dataAndColumnPreserved", true);
        }
        execute("DROP TABLE " + referenced);
        if (indirect) {
            execute("DROP TYPE " + dependent);
        }
        // The identical DROP must succeed without dependencies, excluding syntax/privilege errors as false positives.
        execute(drop.required("sql").asText());
        evidence.put("defaultDropAcceptedAfterRemovingDependencies", true);
        evidence.put("validationPassed", true);
        writeEvidence();
    }

    @Test
    void shouldRejectDroppingATablespaceThatStillContainsATable() throws Exception {
        evidenceProperty = "dameng.non.table.ddl.tablespace.drop.probe.evidence";
        evidenceDefault = "target/non-table-ddl-tablespace-drop-probe.json";
        final var tablespace = "DBZ_NTS_" + UUID.randomUUID().toString().replace("-", "").substring(0, 10).toUpperCase();
        final var referenced = DamengTestDatabase.schema() + "." + tablespace + "_DATA";
        final var datafile = "/opt/dmdbms/data/DAMENG/" + tablespace + ".DBF";
        jdbc = DriverManager.getConnection(DamengTestDatabase.jdbcUrl(), DamengTestDatabase.user(), DamengTestDatabase.password());
        jdbc.setAutoCommit(true);
        evidence.put("databaseVersion", jdbc.getMetaData().getDatabaseProductVersion());
        evidence.put("compatibleMode", DamengTestDatabase.requestedCompatibilityMode());
        evidence.put("scope", "Database-only nonempty TABLESPACE deletion constraint on the recorded DM version; not a claim for other versions or CDC");
        evidence.put("connectorSupportVerified", false);
        evidence.put("validationPassed", false);
        evidence.put("tablespace", tablespace);
        evidence.put("datafile", datafile);
        cleanup.add("DROP TABLESPACE " + tablespace);
        cleanup.add("DROP TABLE " + referenced);
        execute("CREATE TABLESPACE " + tablespace + " DATAFILE '" + datafile + "' SIZE 50");
        execute("CREATE TABLE " + referenced + "(ID INT PRIMARY KEY) STORAGE(ON " + tablespace + ")");
        execute("INSERT INTO " + referenced + " VALUES(1)");

        final var drop = evidence.putObject("dropWhileUsed");
        drop.put("sql", "DROP TABLESPACE " + tablespace);
        assertThatThrownBy(() -> execute(drop.required("sql").asText()))
                .as("a tablespace containing the private table cannot be dropped")
                .isInstanceOfSatisfying(SQLException.class, error -> {
                    drop.put("jdbc", "rejected");
                    drop.put("jdbcError", error.toString());
                    drop.put("sqlState", error.getSQLState());
                    drop.put("vendorCode", error.getErrorCode());
                });
        try (var statement = jdbc.createStatement(); var rows = statement.executeQuery("SELECT ID FROM " + referenced)) {
            assertThat(rows.next()).isTrue();
            assertThat(rows.getInt(1)).isEqualTo(1);
            assertThat(rows.next()).isFalse();
        }
        evidence.put("tableDataPreservedAfterRejectedDrop", true);
        // The recycle-bin fixture must remove the physical object before the tablespace is empty.
        execute("DROP TABLE " + referenced + " PURGE");
        execute("DROP TABLESPACE " + tablespace);
        evidence.put("emptyTablespaceDropAccepted", true);
        evidence.put("validationPassed", true);
        writeEvidence();
    }

    private void execute(String sql) throws SQLException {
        try (var statement = jdbc.createStatement()) {
            statement.execute(sql);
        }
    }

    private void writeEvidence() throws Exception {
        final var output = Path.of(System.getProperty(evidenceProperty, evidenceDefault));
        java.nio.file.Files.createDirectories(output.toAbsolutePath().getParent());
        JSON.writerWithDefaultPrettyPrinter().writeValue(output.toFile(), evidence);
    }
}
