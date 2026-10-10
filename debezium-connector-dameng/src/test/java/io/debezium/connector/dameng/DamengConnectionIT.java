/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng;

import static org.assertj.core.api.Assertions.assertThat;

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
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;
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
