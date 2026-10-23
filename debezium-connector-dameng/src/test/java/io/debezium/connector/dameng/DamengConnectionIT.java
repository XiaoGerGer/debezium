/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.sql.Types;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.dameng.compatibility.DamengCompatibilityMode;
import io.debezium.connector.dameng.compatibility.DamengCompatibilityRegistry;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;

/** Verifies native JDBC metadata and archived redo against a real DM database. */
class DamengConnectionIT {
    private DamengConnection connection;
    private TableId tableId;
    private boolean tableCreated;

    @BeforeAll
    static void startDatabase() throws Exception {
        DamengTestDatabase.start();
    }

    @AfterAll
    static void stopDatabase() {
        DamengTestDatabase.stop();
    }

    @BeforeEach
    void before() {
        connection = new DamengConnection(Configuration.create()
                .with("hostname", DamengTestDatabase.hostname())
                .with("port", DamengTestDatabase.port())
                .with("user", DamengTestDatabase.user())
                .with("password", DamengTestDatabase.password())
                .build());
        tableId = new TableId(null, DamengTestDatabase.schema(),
                "DBZ_JDBC_" + UUID.randomUUID().toString().replace("-", "").substring(0, 12).toUpperCase());
    }

    @AfterEach
    void after() throws Exception {
        if (connection != null) {
            try {
                connection.endLogMining();
                if (tableCreated) {
                    connection.execute("DROP TABLE " + tableName());
                }
            }
            finally {
                connection.close();
            }
        }
    }

    @Test
    void shouldExposeArchiveFileByteLengths() throws Exception {
        final var archives = connection.readArchiveLogFiles();
        assertThat(archives).isNotEmpty().allSatisfy(file -> assertThat(file.bytes()).isPositive());
        connection.query("SELECT PATH, LEN, FREE, ARCH_LSN, CLSN FROM V$ARCH_FILE WHERE PATH IS NOT NULL ORDER BY ARCH_LSN", rows -> {
            int count = 0;
            while (rows.next()) {
                assertThat(rows.getLong(2)).as("Archive LEN in bytes: %s", rows.getString(1)).isPositive();
                assertThat(rows.getLong(3)).as("Archive write offset").isBetween(0L, rows.getLong(2));
                if (count++ < 3) {
                    System.out.println("ARCHIVE_BYTES_PROBE path=" + rows.getString(1) + " length=" + rows.getLong(2)
                            + " writeOffset=" + rows.getLong(3) + " first=" + rows.getLong(4) + " last=" + rows.getLong(5));
                }
            }
            assertThat(count).isPositive();
        });
    }

    @Test
    void shouldReadDatabaseVersionFromTheLiveJdbcConnection() throws Exception {
        final var metadata = connection.connection().getMetaData();
        final var version = connection.getDatabaseVersion();
        assertThat(version.productName()).isEqualTo(metadata.getDatabaseProductName()).isNotBlank();
        assertThat(version.productVersion()).isEqualTo(metadata.getDatabaseProductVersion()).isNotBlank();
        assertThat(version.major()).isEqualTo(metadata.getDatabaseMajorVersion()).isPositive();
        assertThat(version.minor()).isEqualTo(metadata.getDatabaseMinorVersion());
    }

    @Test
    void shouldCreateAndReuseTheSignalTableWithoutChangingExistingRows() throws Exception {
        assertThat(connection.ensureSignalTable(tableId)).isTrue();
        tableCreated = true;
        connection.execute("INSERT INTO " + tableName() + " VALUES ('kept','execute-snapshot','{}')");
        assertThat(connection.ensureSignalTable(tableId)).isFalse();
        connection.query("SELECT * FROM " + tableName(), rows -> {
            assertThat(rows.getMetaData().getColumnCount()).isEqualTo(3);
            assertThat(rows.next()).isTrue();
            assertThat(rows.getString(1)).isEqualTo("kept");
            assertThat(rows.getString(2)).isEqualTo("execute-snapshot");
            assertThat(rows.getString(3)).isEqualTo("{}");
            assertThat(rows.next()).isFalse();
        });
        final var tables = new Tables();
        connection.readSchema(tables, null, tableId.schema(), tableId::equals, null, false);
        assertThat(tables.forTable(tableId).primaryKeyColumnNames()).containsExactly("ID");
    }

    @Test
    void shouldCreateSignalTablesWithQuotedNames() throws Exception {
        tableId = new TableId(null, tableId.schema(), tableId.table() + " 'Signal\"");
        assertThat(connection.ensureSignalTable(tableId)).isTrue();
        tableCreated = true;
        assertThat(connection.ensureSignalTable(tableId)).isFalse();
    }

    @Test
    void shouldRejectAnIncompatibleSignalTableWithoutChangingIt() throws Exception {
        createTable("ID VARCHAR(64) PRIMARY KEY, TYPE VARCHAR(64), DATA VARCHAR(2000), EXTRA INT");
        connection.execute("INSERT INTO " + tableName() + " VALUES ('kept','execute-snapshot','{}',7)");
        assertThatThrownBy(() -> connection.ensureSignalTable(tableId)).hasMessageContaining("exactly three columns");
        connection.query("SELECT EXTRA FROM " + tableName() + " WHERE ID='kept'", rows -> {
            assertThat(rows.next()).isTrue();
            assertThat(rows.getInt(1)).isEqualTo(7);
        });
    }

    @Test
    void shouldExplainMissingCreatePermissionAndAllowAnAdministratorToPrecreateTheSignalTable() throws Exception {
        Assumptions.assumeTrue(System.getProperty("dameng.hostname") == null,
                "Test users are created only in the database owned by this test suite");
        final var username = "DBZ_SIG_" + UUID.randomUUID().toString().replace("-", "").substring(0, 12).toUpperCase();
        final var password = "DbzSignal_42";
        connection.execute("CREATE USER " + username + " IDENTIFIED BY \"" + password + "\"");
        try {
            final var restrictedConfig = Configuration.create()
                    .with("hostname", DamengTestDatabase.hostname()).with("port", DamengTestDatabase.port())
                    .with("user", username).with("password", password).build();
            try (var restricted = new DamengConnection(restrictedConfig)) {
                assertThatThrownBy(() -> restricted.ensureSignalTable(tableId))
                        .hasMessageContaining("Could not create Dameng signal table", "CREATE TABLE permission", "pre-create");
            }
            assertThat(connection.ensureSignalTable(tableId)).isTrue();
            tableCreated = true;
            connection.execute("GRANT SELECT, INSERT ON " + tableName() + " TO " + username);
            try (var restricted = new DamengConnection(restrictedConfig)) {
                assertThat(restricted.ensureSignalTable(tableId)).isFalse();
                restricted.execute("INSERT INTO " + tableName() + " VALUES ('granted','execute-snapshot','{}')");
            }
            connection.query("SELECT ID FROM " + tableName(), rows -> {
                assertThat(rows.next()).isTrue();
                assertThat(rows.getString(1)).isEqualTo("granted");
                assertThat(rows.next()).isFalse();
            });
        }
        finally {
            connection.execute("DROP USER " + username);
        }
    }

    @Test
    void shouldSelectNativeCompatibilityRulesFromTheActualServerParameters() throws Exception {
        final var environment = connection.getCompatibilityEnvironment();
        assertThat(environment.mode()).isEqualTo(new DamengCompatibilityMode("0"));
        assertThat(environment.parameter("LENGTH_IN_CHAR")).isEqualTo("0");
        assertThat(DamengCompatibilityRegistry.builtIn().resolve(environment.mode()).id()).isEqualTo("native-v1");
        createTable("ID INT PRIMARY KEY, D DATE, C CHAR(8)");
        final var physical = new Tables();
        connection.readSchema(physical, null, tableId.schema(), tableId::equals, null, false);
        final var historical = new Tables();
        connection.createDdlParser().parse("CREATE TABLE " + tableName() + " (ID INT PRIMARY KEY, D DATE, C CHAR(8))", historical, tableId.schema());
        for (var name : List.of("D", "C")) {
            final var jdbcColumn = physical.forTable(tableId).columnWithName(name);
            final var ddlColumn = historical.forTable(tableId).columnWithName(name);
            assertThat(ddlColumn.jdbcType()).isEqualTo(jdbcColumn.jdbcType());
            assertThat(ddlColumn.scale()).isEqualTo(jdbcColumn.scale());
            assertThat(ddlColumn.typeExpression()).isEqualTo(jdbcColumn.typeExpression());
        }
    }

    @Test
    void shouldPreserveOriginalDdlWithoutDelimiter() throws Exception {
        final long start = connection.currentLsn();
        final var create = "CREATE TABLE " + tableName() + " (ID INT PRIMARY KEY, NAME VARCHAR(30))";
        connection.execute(create);
        tableCreated = true;
        final var add = "ALTER TABLE " + tableName() + " ADD EXTRA INT";
        final var rename = "ALTER TABLE " + tableName() + " RENAME COLUMN NAME TO RENAMED";
        connection.execute(add, rename);

        assertThat(mine(start).stream().filter(row -> row.operation() == 5).map(MinedRow::sql))
                .contains(create, add, rename);
    }

    @Test
    void shouldMineHistoricalColumnOrdinalsAcrossRenameDropAndAdd() throws Exception {
        createTable("ID INT PRIMARY KEY, BEFORENAME VARCHAR(30), RETAINED INT");
        final long objectId = connection.getTableObjectId(tableId);
        final long start = connection.currentLsn();
        connection.execute("INSERT INTO " + tableName() + " VALUES(1,'before',11)");
        connection.execute("ALTER TABLE " + tableName() + " RENAME COLUMN BEFORENAME TO AFTERNAME");
        connection.execute("INSERT INTO " + tableName() + " VALUES(2,'renamed',22)");
        connection.execute("ALTER TABLE " + tableName() + " DROP COLUMN AFTERNAME");
        connection.execute("INSERT INTO " + tableName() + " VALUES(3,33)");
        connection.execute("ALTER TABLE " + tableName() + " ADD FINALCOL VARCHAR(30)");
        connection.execute("INSERT INTO " + tableName() + " VALUES(4,44,'tail')");

        final var inserts = mine(start).stream().filter(row -> row.operation() == 1).toList();
        assertThat(inserts).hasSize(4);
        assertThat(inserts).allSatisfy(row -> assertThat(row.objectId()).isEqualTo(objectId));
        assertThat(inserts.get(0).sql()).contains("\"OBJ# " + objectId + "\"", "\"COL 1\", \"COL 2\", \"COL 3\"", "VALUES(1, 'before', 11)");
        assertThat(inserts.get(1).sql()).contains("VALUES(2, 'renamed', 22)");
        // Dropping a middle column compacts physical ordinals in subsequent records.
        assertThat(inserts.get(2).sql()).contains("\"COL 1\", \"COL 2\"", "VALUES(3, 33)").doesNotContain("COL 3");
        assertThat(inserts.get(3).sql()).contains("\"COL 1\", \"COL 2\", \"COL 3\"", "VALUES(4, 44, 'tail')");
        assertThat(inserts.get(1).objectVersion()).isGreaterThan(inserts.get(0).objectVersion());
        assertThat(inserts.get(2).objectVersion()).isGreaterThan(inserts.get(1).objectVersion());
        assertThat(inserts.get(3).objectVersion()).isGreaterThan(inserts.get(2).objectVersion());
    }

    @Test
    void shouldExposeRepeatableFragmentsAndObjectVersionsAcrossDdl() throws Exception {
        final long start = connection.currentLsn();
        createTable("ID INT PRIMARY KEY, A VARCHAR(1500), B VARCHAR(1500)");
        final long originalId = connection.getTableObjectId(tableId);
        try (var insert = connection.connection().prepareStatement("INSERT INTO " + tableName() + " VALUES(?,?,?)")) {
            insert.setInt(1, 1);
            insert.setString(2, "'".repeat(1200));
            insert.setString(3, "'".repeat(1200));
            insert.executeUpdate();
        }
        connection.execute("ALTER TABLE " + tableName() + " RENAME COLUMN A TO RENAMED");
        // Keep the stored row below DM's page limit; the UPDATE includes both
        // before and after images and expands to at least three SQL_REDO pieces.
        try (var update = connection.connection().prepareStatement("UPDATE " + tableName() + " SET ID=2, RENAMED=?, B=?")) {
            update.setString(1, "'".repeat(1450));
            update.setString(2, "'".repeat(1450));
            update.executeUpdate();
        }
        connection.execute("TRUNCATE TABLE " + tableName());
        connection.execute("INSERT INTO " + tableName() + "(ID) VALUES(3)");
        connection.execute("DROP TABLE " + tableName());
        tableCreated = false;
        createTable("ID INT PRIMARY KEY");
        final long recreatedId = connection.getTableObjectId(tableId);
        connection.execute("INSERT INTO " + tableName() + " VALUES(4)");
        final long end = connection.currentLsn();
        Awaitility.await().atMost(Duration.ofSeconds(30)).until(() -> connection.latestArchivedLsn() >= end);

        final var first = mineMetadata(start, end);
        connection.endLogMining();
        // A new mining session must reproduce the same coordinates and versions.
        assertThat(mineMetadata(start, end)).containsExactlyElementsOf(first);
        final var inserts = first.stream().filter(row -> row.operation() == 1 && row.sequence() == 0).toList();
        assertThat(inserts).hasSize(3);
        final var insertPieces = first.stream().filter(row -> row.lsn() == inserts.get(0).lsn()).toList();
        assertThat(insertPieces).hasSizeGreaterThanOrEqualTo(2);
        assertSqlFragments(insertPieces);
        final String insertSql = insertPieces.stream().map(MiningMetadata::sql).collect(java.util.stream.Collectors.joining());
        final String quotedValue = "'".repeat(2402);
        assertThat(insertSql).endsWith("VALUES(1, " + quotedValue + ", " + quotedValue + ");");
        assertThat(inserts.get(0).objectVersion()).isZero();
        assertThat(inserts.get(0).objectId()).isEqualTo(originalId);
        final var updates = first.stream().filter(row -> row.operation() == 3).toList();
        assertThat(updates).hasSizeGreaterThanOrEqualTo(3);
        assertSqlFragments(updates);
        final var update = updates.get(0);
        assertThat(update.objectVersion()).isGreaterThan(inserts.get(0).objectVersion());
        // TRUNCATE need not change the version; it cannot be used as a DDL counter.
        assertThat(inserts.get(1).objectVersion()).isEqualTo(update.objectVersion());
        assertThat(inserts.get(2).objectId()).isEqualTo(recreatedId);
        assertThat(inserts.get(2).objectVersion()).isZero();
        assertThat(first.stream().filter(row -> row.operation() == 5 && row.sql().startsWith("CREATE TABLE")))
                .hasSize(2).allSatisfy(row -> assertThat(row.objectVersion()).isNull());
        assertThat(first.stream().filter(row -> row.operation() == 6 || row.operation() == 7))
                .isNotEmpty().allSatisfy(row -> {
                    assertThat(row.objectId()).isNull();
                    assertThat(row.sequence()).isZero();
                    assertThat(row.continuation()).isZero();
                    assertThat(row.rollback()).isZero();
                });
    }

    private static void assertSqlFragments(List<MiningMetadata> pieces) {
        final var first = pieces.get(0);
        for (int i = 0; i < pieces.size(); i++) {
            final var piece = pieces.get(i);
            assertThat(piece.sequence()).isEqualTo(i);
            assertThat(piece.continuation()).isEqualTo(i == pieces.size() - 1 ? 0 : 1);
            assertThat(piece.lsn()).isEqualTo(first.lsn());
            assertThat(piece.recordSetId()).isEqualTo(first.recordSetId());
            assertThat(piece.objectId()).isEqualTo(first.objectId());
            assertThat(piece.objectVersion()).isEqualTo(first.objectVersion());
            assertThat(piece.block()).isEqualTo(first.block());
            assertThat(piece.byteOffset()).isEqualTo(first.byteOffset());
            assertThat(piece.sql()).isNotEmpty();
        }
    }

    private List<MiningMetadata> mineMetadata(long start, long end) throws Exception {
        connection.startLogMining(start, end, false);
        return connection.queryAndMap("SELECT SCN, OPERATION_CODE, ROLL_BACK, DATA_OBJ#, DATA_OBJV#, RS_ID, SSN, CSF, RBABLK, RBABYTE, SQL_REDO "
                + "FROM V$LOGMNR_CONTENTS WHERE SCN >= " + start + " AND SCN <= " + end
                + " ORDER BY SCN, RBASQN, RBABLK, RBABYTE, SSN", rows -> {
                    final List<MiningMetadata> result = new ArrayList<>();
                    while (rows.next()) {
                        final long objectId = rows.getLong("DATA_OBJ#");
                        final Long nullableId = rows.wasNull() ? null : objectId;
                        final long version = rows.getLong("DATA_OBJV#");
                        final Long nullableVersion = rows.wasNull() ? null : version;
                        result.add(new MiningMetadata(rows.getLong("SCN"), rows.getInt("OPERATION_CODE"), rows.getInt("ROLL_BACK"),
                                nullableId, nullableVersion, rows.getString("RS_ID"), rows.getInt("SSN"), rows.getInt("CSF"),
                                rows.getLong("RBABLK"), rows.getInt("RBABYTE"), rows.getString("SQL_REDO")));
                        for (String field : List.of("SCN", "OPERATION_CODE", "ROLL_BACK", "SSN", "CSF")) {
                            rows.getLong(field);
                            assertThat(rows.wasNull()).as(field + " at LSN " + result.get(result.size() - 1).lsn()).isFalse();
                        }
                    }
                    return result;
                });
    }

    private record MiningMetadata(long lsn, int operation, int rollback, Long objectId, Long objectVersion, String recordSetId,
            int sequence, int continuation, long block, int byteOffset, String sql) {
    }

    @Test
    void shouldNormalizeTimeZoneMetadataAndReadLobValues() throws Exception {
        createTable("ID INT PRIMARY KEY, TZ TIMESTAMP(6) WITH TIME ZONE, TTZ TIME(3) WITH TIME ZONE, C CLOB, B BLOB");
        final var text = "达梦".repeat(3_000);
        final byte[] binary = new byte[6_000];
        for (int i = 0; i < binary.length; i++) {
            binary[i] = (byte) i;
        }
        try (var statement = connection.connection().prepareStatement("INSERT INTO " + tableName() + " VALUES(?,?,?,?,?)")) {
            statement.setInt(1, 1);
            statement.setString(2, "2026-09-24 12:34:56.123456 +08:00");
            statement.setString(3, "12:34:56.123 +08:00");
            statement.setString(4, text);
            statement.setBytes(5, binary);
            statement.executeUpdate();
        }
        final var tables = new Tables();
        connection.readSchema(tables, null, tableId.schema(), id -> id.equals(tableId), null, false);
        final var table = tables.forTable(tableId);
        assertThat(table.columnWithName("TZ").jdbcType()).isEqualTo(Types.TIMESTAMP_WITH_TIMEZONE);
        assertThat(table.columnWithName("TTZ").jdbcType()).isEqualTo(Types.TIME_WITH_TIMEZONE);
        connection.query("SELECT TZ, TTZ, C, B FROM " + tableName(), result -> {
            assertThat(result.next()).isTrue();
            assertThat(connection.getColumnValue(result, 1, table.columnWithName("TZ"), table))
                    .isEqualTo(OffsetDateTime.parse("2026-09-24T12:34:56.123456+08:00"));
            assertThat(connection.getColumnValue(result, 2, table.columnWithName("TTZ"), table))
                    .isEqualTo(OffsetTime.parse("12:34:56.123+08:00"));
            assertThat(connection.getColumnValue(result, 3, table.columnWithName("C"), table)).isEqualTo(text);
            assertThat((byte[]) connection.getColumnValue(result, 4, table.columnWithName("B"), table)).isEqualTo(binary);
        });
    }

    @Test
    void shouldResolveQuotedObjectIdentity() throws Exception {
        tableId = new TableId(null, tableId.schema(), tableId.table() + " 'Quoted\"");
        createTable("ID INT PRIMARY KEY");
        final long start = connection.currentLsn();
        final long objectId = connection.getTableObjectId(tableId);
        connection.execute("INSERT INTO " + tableName() + " VALUES(1)");
        assertThat(mine(start).stream().filter(row -> row.operation() == 1))
                .singleElement().satisfies(row -> assertThat(row.objectId()).isEqualTo(objectId));
    }

    private void createTable(String columns) throws Exception {
        connection.execute("CREATE TABLE " + tableName() + " (" + columns + ")");
        tableCreated = true;
    }

    private List<MinedRow> mine(long start) throws Exception {
        final long end = connection.currentLsn();
        Awaitility.await("the committed changes are archived").atMost(Duration.ofSeconds(30))
                .pollInterval(Duration.ofMillis(100)).until(() -> connection.latestArchivedLsn() >= end);
        connection.startLogMining(start, end, false);
        final long objectId = connection.getTableObjectId(tableId);
        return connection.queryAndMap("SELECT OPERATION_CODE, DATA_OBJ#, DATA_OBJV#, SQL_REDO FROM V$LOGMNR_CONTENTS "
                + "WHERE SCN >= " + start + " AND SCN <= " + end + " AND DATA_OBJ#=" + objectId + " ORDER BY SCN, SSN", result -> {
                    List<MinedRow> rows = new ArrayList<>();
                    while (result.next()) {
                        rows.add(new MinedRow(result.getInt(1), result.getLong(2), result.getLong(3), result.getString(4)));
                    }
                    return rows;
                });
    }

    private String tableName() {
        return "\"" + tableId.schema().replace("\"", "\"\"") + "\".\"" + tableId.table().replace("\"", "\"\"") + "\"";
    }

    private record MinedRow(int operation, long objectId, long objectVersion, String sql) {
    }
}
