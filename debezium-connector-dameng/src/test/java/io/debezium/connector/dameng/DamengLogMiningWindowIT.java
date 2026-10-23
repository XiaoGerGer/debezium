/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.UUID;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import io.debezium.config.Configuration;

/** Verifies transaction boundaries and rollback visibility across real LogMiner sessions. */
class DamengLogMiningWindowIT {
    @BeforeAll
    static void startDatabase() throws Exception {
        DamengTestDatabase.start();
    }

    @AfterAll
    static void stopDatabase() {
        DamengTestDatabase.stop();
    }

    @ParameterizedTest
    @ValueSource(strings = { "commit", "rollback", "statement-failure", "savepoint-only", "bulk-update", "bulk-delete" })
    void shouldInspectRollbackMarkersWithoutAssumingExplicitSavepoints(String outcome) throws Exception {
        final var table = DamengTestDatabase.schema() + ".DBZ_BOUNDARY_" + UUID.randomUUID().toString().replace("-", "").substring(0, 12);
        try (var miner = new DamengConnection(Configuration.create().with("hostname", DamengTestDatabase.hostname())
                .with("port", DamengTestDatabase.port()).with("user", DamengTestDatabase.user()).with("password", DamengTestDatabase.password()).build());
                var writer = DriverManager.getConnection(DamengTestDatabase.jdbcUrl(), DamengTestDatabase.user(), DamengTestDatabase.password())) {
            miner.execute("CREATE TABLE " + table + " (ID INT PRIMARY KEY, NAME VARCHAR(30), CHECK(ID>0))");
            try {
                writer.setAutoCommit(false);
                final long start = miner.currentLsn();
                try (var statement = writer.createStatement()) {
                    statement.execute("INSERT INTO " + table + " VALUES(1,'keep')");
                    final long boundary = miner.currentLsn();
                    final var before = mine(miner, start, boundary);
                    if (outcome.equals("statement-failure")) {
                        assertThatThrownBy(() -> statement.execute("INSERT INTO " + table + " SELECT 2,'failed' FROM DUAL UNION ALL SELECT -1,'failed' FROM DUAL"))
                                .isInstanceOf(SQLException.class);
                        statement.execute("INSERT INTO " + table + " VALUES(3,'after failure')");
                    }
                    else if (outcome.equals("savepoint-only")) {
                        writer.setSavepoint("UNUSED_POINT");
                    }
                    else if (outcome.equals("bulk-update")) {
                        statement.execute("UPDATE " + table + " SET NAME='updated'");
                    }
                    else if (outcome.equals("bulk-delete")) {
                        statement.execute("DELETE FROM " + table);
                    }
                    if (outcome.equals("rollback")) {
                        writer.rollback();
                    }
                    else {
                        writer.commit();
                    }
                    final long end = miner.currentLsn();
                    final var forward = mine(miner, boundary + 1, end);
                    final var complete = mine(miner, start, end);
                    System.out.println("BOUNDARY_PROBE " + outcome + " before=" + before + " forward=" + forward + " complete=" + complete);
                    miner.startLogMining(start, end, false);
                    try {
                        miner.query("SELECT * FROM V$LOGMNR_CONTENTS WHERE OPERATION_CODE=36 ORDER BY SCN, SSN", rows -> {
                            while (rows.next()) {
                                final var detail = new StringBuilder("MARKER_DETAIL " + outcome);
                                for (int i = 1; i <= rows.getMetaData().getColumnCount(); i++) {
                                    detail.append(' ').append(rows.getMetaData().getColumnLabel(i)).append('=');
                                    final var value = rows.getObject(i);
                                    detail.append(value instanceof byte[] bytes ? HexFormat.of().formatHex(bytes) : value);
                                }
                                System.out.println(detail);
                            }
                        });
                    }
                    finally {
                        miner.endLogMining();
                    }
                    assertThat(complete).isNotEmpty();
                }
            }
            finally {
                writer.rollback();
                miner.execute("DROP TABLE " + table);
            }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "commit", "savepoint", "rollback" })
    void shouldExposeTransactionOutcomeWhenMiningStartsInsideATransaction(String outcome) throws Exception {
        final var table = DamengTestDatabase.schema() + ".DBZ_WINDOW_" + UUID.randomUUID().toString().replace("-", "").substring(0, 12);
        try (var miner = new DamengConnection(Configuration.create().with("hostname", DamengTestDatabase.hostname())
                .with("port", DamengTestDatabase.port()).with("user", DamengTestDatabase.user()).with("password", DamengTestDatabase.password()).build());
                var writer = DriverManager.getConnection(DamengTestDatabase.jdbcUrl(), DamengTestDatabase.user(), DamengTestDatabase.password())) {
            miner.execute("CREATE TABLE " + table + " (ID INT PRIMARY KEY, NAME VARCHAR(30))");
            try {
                writer.setAutoCommit(false);
                final long start = miner.currentLsn();
                try (var statement = writer.createStatement()) {
                    statement.execute("INSERT INTO " + table + " VALUES(1,'keep')");
                    final var savepoint = writer.setSavepoint("WINDOW_POINT");
                    statement.execute("INSERT INTO " + table + " VALUES(2,'discard')");
                    final long boundary = miner.currentLsn();
                    final var before = mine(miner, start, boundary);
                    final var inserted = before.stream().filter(row -> row.sql() != null && row.sql().contains("'discard'")).findFirst().orElseThrow();
                    assertThat(inserted.rollback()).isZero();
                    if (outcome.equals("rollback")) {
                        writer.rollback();
                    }
                    else {
                        if (outcome.equals("savepoint")) {
                            writer.rollback(savepoint);
                        }
                        writer.commit();
                    }
                    final long end = miner.currentLsn();
                    final var forward = mine(miner, boundary + 1, end).stream().filter(row -> row.xid().equals(inserted.xid())).toList();
                    final var complete = mine(miner, start, end).stream().filter(row -> row.xid().equals(inserted.xid())).toList();
                    System.out.println("WINDOW_PROBE " + outcome + " start=" + start + " boundary=" + boundary + " end=" + end
                            + " before=" + before + " forward=" + forward + " complete=" + complete);
                    assertThat(forward).anySatisfy(row -> assertThat(row.operation()).isEqualTo(outcome.equals("rollback") ? 36 : 7));
                    final var finalInsert = complete.stream().filter(row -> inserted.lsn() == row.lsn() && row.operation() == 1).findFirst().orElseThrow();
                    assertThat(finalInsert.rollback()).isEqualTo(outcome.equals("commit") ? 0 : 1);
                }
            }
            finally {
                writer.rollback();
                miner.execute("DROP TABLE " + table);
            }
        }
    }

    private List<Row> mine(DamengConnection miner, long start, long end) throws Exception {
        Awaitility.await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(100)).until(() -> miner.latestArchivedLsn() >= end);
        miner.startLogMining(start, end, false);
        try {
            return miner.queryAndMap("SELECT SCN, START_SCN, COMMIT_SCN, XID, OPERATION_CODE, ROLL_BACK, SQL_REDO, RS_ID FROM V$LOGMNR_CONTENTS "
                    + "WHERE SCN >= " + start + " AND SCN <= " + end + " ORDER BY SCN, SSN", rows -> {
                        final List<Row> result = new ArrayList<>();
                        while (rows.next()) {
                            final var xid = rows.getBytes(4);
                            result.add(new Row(rows.getLong(1), rows.getLong(2), rows.getLong(3), xid == null ? "" : HexFormat.of().formatHex(xid),
                                    rows.getInt(5), rows.getInt(6), rows.getString(7), rows.getString(8)));
                        }
                        return result;
                    });
        }
        finally {
            miner.endLogMining();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void shouldDistinguishEmptySavepointRollbackFromFullRollback(boolean full) throws Exception {
        final var table = DamengTestDatabase.schema() + ".DBZ_EMPTY_" + UUID.randomUUID().toString().replace("-", "").substring(0, 12);
        try (var miner = new DamengConnection(Configuration.create().with("hostname", DamengTestDatabase.hostname())
                .with("port", DamengTestDatabase.port()).with("user", DamengTestDatabase.user()).with("password", DamengTestDatabase.password()).build());
                var writer = DriverManager.getConnection(DamengTestDatabase.jdbcUrl(), DamengTestDatabase.user(), DamengTestDatabase.password())) {
            miner.execute("CREATE TABLE " + table + " (ID INT PRIMARY KEY)");
            try {
                writer.setAutoCommit(false);
                final long start = miner.currentLsn();
                final var point = writer.setSavepoint("BEFORE_FIRST_DML");
                try (var statement = writer.createStatement()) {
                    statement.execute("INSERT INTO " + table + " VALUES(1)");
                    writer.setSavepoint("SECOND_POINT");
                    statement.execute("INSERT INTO " + table + " VALUES(2)");
                    final var before = mine(miner, start, miner.currentLsn());
                    final var xid = before.stream().filter(row -> row.operation() == 1).findFirst().orElseThrow().xid();
                    if (full) {
                        writer.rollback();
                    }
                    else {
                        writer.rollback(point);
                    }
                    final long end = miner.currentLsn();
                    final var rolled = mine(miner, start, end).stream().filter(row -> row.xid().equals(xid)).toList();
                    System.out.println("EMPTY_ROLLBACK full=" + full + " before=" + before + " after=" + rolled);
                    assertThat(rolled.stream().filter(row -> row.operation() == 1)).hasSize(2)
                            .allSatisfy(row -> assertThat(row.rollback()).isEqualTo(1));
                    assertThat(rolled.stream().filter(row -> row.operation() == 6)).hasSize(1)
                            .allSatisfy(row -> assertThat(row.rollback()).isEqualTo(1));
                    final var markers = rolled.stream().filter(row -> row.operation() == 36).toList();
                    assertThat(markers).isNotEmpty();
                    final var terminal = markers.get(markers.size() - 1);
                    assertThat(terminal.rollback()).isEqualTo(full ? 1 : 0);
                    if (full) {
                        assertThat(terminal.recordSetId()).isEqualTo("-1");
                    }
                    else {
                        assertThat(terminal.recordSetId()).isNotEqualTo("-1");
                    }
                    if (!full) {
                        statement.execute("INSERT INTO " + table + " VALUES(3)");
                        writer.commit();
                        final var completed = mine(miner, start, miner.currentLsn());
                        assertThat(completed).anySatisfy(row -> {
                            assertThat(row.xid()).isEqualTo(xid);
                            assertThat(row.operation()).isEqualTo(7);
                        });
                    }
                }
            }
            finally {
                writer.rollback();
                miner.execute("DROP TABLE " + table);
            }
        }
    }

    private record Row(long lsn, long startLsn, long commitLsn, String xid, int operation, int rollback, String sql, String recordSetId) {
    }
}
