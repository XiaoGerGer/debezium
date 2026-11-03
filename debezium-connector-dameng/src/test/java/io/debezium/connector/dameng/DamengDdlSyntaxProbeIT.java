/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import io.debezium.config.Configuration;
import io.debezium.document.Document;
import io.debezium.document.DocumentWriter;
import io.debezium.relational.TableId;

/** Records actual server acceptance and archived SQL; this probe does not enable a compatibility profile. */
class DamengDdlSyntaxProbeIT {
    @BeforeAll
    static void startDatabase() throws Exception {
        org.junit.jupiter.api.Assumptions.assumeTrue(System.getProperty("dameng.hostname") == null,
                "DDL syntax probes run only in a database owned by this test suite");
        DamengTestDatabase.start();
    }

    @AfterAll
    static void stopDatabase() {
        DamengTestDatabase.stop();
    }

    private record SyntaxCase(String name, String prefix, String defaultExpression, boolean nativeRequired) {
        @Override
        public String toString() {
            return name;
        }
    }

    static Stream<SyntaxCase> syntaxCases() {
        return Stream.of(
                new SyntaxCase("dash-lf", "-- 中文注释; DROP TABLE ignored\n", "'https://host/--/*;#'", true),
                new SyntaxCase("dash-crlf", "-- 中文注释\r\n", "'text'", true),
                new SyntaxCase("dash-cr", "-- 中文注释\r", "'text'", false),
                new SyntaxCase("slash-lf", "// 中文注释; DROP TABLE ignored\n", "'text'", true),
                new SyntaxCase("slash-crlf", "// 中文注释\r\n", "'text'", true),
                new SyntaxCase("slash-cr", "// 中文注释\r", "'text'", false),
                new SyntaxCase("block", "/* 中文注释; DROP TABLE ignored */", "'text'", true),
                new SyntaxCase("hint", "/*+ SOME_HINT */", "'text'", true),
                new SyntaxCase("nested-block", "/* outer /* nested */ outer */", "'text'", false),
                new SyntaxCase("hash", "# MySQL comment\n", "'text'", false),
                new SyntaxCase("mysql-version-comment", "/*! SELECT 1; */", "'text'", false),
                new SyntaxCase("oracle-q-quote", "", "q'[https://host/--/*;#]'", false),
                new SyntaxCase("postgres-dollar-quote", "", "$$https://host/--/*;#$$", false),
                new SyntaxCase("postgres-tagged-dollar-quote", "", "$tag$https://host/--/*;#$tag$", false));
    }

    @Test
    void shouldRecordCreateIfNotExistsWhenTheTableAlreadyExists() throws Exception {
        final var table = "DBZ_IF_" + UUID.randomUUID().toString().replace("-", "").substring(0, 12).toUpperCase();
        final var qualified = '"' + DamengTestDatabase.schema() + "\".\"" + table + '"';
        final var sql = "CREATE TABLE IF NOT EXISTS " + qualified + " (ID INT PRIMARY KEY)";
        final var evidence = new StringBuilder();
        evidence.append("compatibleMode=").append(DamengTestDatabase.requestedCompatibilityMode()).append('\n');
        evidence.append("sql=").append(sql).append('\n');
        try (var connection = new DamengConnection(Configuration.create().with("hostname", DamengTestDatabase.hostname())
                .with("port", DamengTestDatabase.port()).with("user", DamengTestDatabase.user())
                .with("password", DamengTestDatabase.password()).build())) {
            evidence.append("version=").append(connection.connection().getMetaData().getDatabaseProductVersion()).append('\n');
            final long start = connection.currentLsn();
            boolean created = false;
            try {
                try {
                    connection.execute(sql);
                    created = true;
                    evidence.append("firstExecution=accepted\n");
                }
                catch (SQLException rejected) {
                    evidence.append("firstExecution=rejected; code=").append(rejected.getErrorCode())
                            .append("; message=").append(rejected.getMessage()).append('\n');
                    return;
                }
                final long firstEnd = connection.currentLsn();
                final long originalObjectId = connection.getTableObjectId(new TableId(null, DamengTestDatabase.schema(), table));
                evidence.append("firstEndLsn=").append(firstEnd).append("; objectId=").append(originalObjectId).append('\n');
                try {
                    connection.execute(sql);
                    evidence.append("secondExecution=accepted\n");
                    assertThat(connection.getTableObjectId(new TableId(null, DamengTestDatabase.schema(), table)))
                            .as("IF NOT EXISTS must not replace the existing table").isEqualTo(originalObjectId);
                }
                catch (SQLException rejected) {
                    evidence.append("secondExecution=rejected; code=").append(rejected.getErrorCode())
                            .append("; message=").append(rejected.getMessage()).append('\n');
                }
                final long secondEnd = connection.currentLsn();
                evidence.append("secondEndLsn=").append(secondEnd).append('\n');
                Awaitility.await("conditional CREATE is archived").atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(100))
                        .until(() -> connection.latestArchivedLsn() >= secondEnd);
                connection.startLogMining(start, secondEnd, false);
                final List<String> redo = connection.queryAndMap("SELECT SCN,DATA_OBJ#,SQL_REDO,CSF FROM V$LOGMNR_CONTENTS WHERE OPERATION_CODE=5 "
                        + "AND SCN > " + start + " AND SCN <= " + secondEnd + " ORDER BY SCN,SSN", result -> {
                            final List<String> records = new ArrayList<>();
                            while (result.next()) {
                                final var statement = result.getString("SQL_REDO");
                                if (statement != null && statement.contains(table)) {
                                    assertThat(result.getInt("CSF")).as("short conditional CREATE must not be fragmented").isZero();
                                    records.add("lsn=" + result.getLong("SCN") + "; objectId=" + result.getLong("DATA_OBJ#")
                                            + "; sql=" + statement);
                                }
                            }
                            return records;
                        });
                redo.forEach(record -> evidence.append("redo=").append(record).append('\n'));
                evidence.append("matchingDdlRecords=").append(redo.size()).append('\n');
                assertThat(redo).as("the first CREATE must appear in archived LogMiner DDL").isNotEmpty();
            }
            finally {
                connection.endLogMining();
                if (created) {
                    connection.execute("DROP TABLE " + qualified);
                }
                final var directory = Path.of(System.getProperty("dameng.evidence.directory", "target/ddl-syntax-probe"));
                Files.createDirectories(directory);
                Files.writeString(directory.resolve("mode-" + DamengTestDatabase.requestedCompatibilityMode() + "-create-if-not-exists.txt"), evidence);
            }
        }
    }

    @ParameterizedTest
    @MethodSource("syntaxCases")
    void shouldRecordServerAcceptanceAndArchivedSql(SyntaxCase sample) throws Exception {
        final var table = "DBZ_SQL_" + UUID.randomUUID().toString().replace("-", "").substring(0, 12).toUpperCase();
        final var qualified = '"' + DamengTestDatabase.schema() + "\".\"" + table + '"';
        final var sql = sample.prefix() + "CREATE TABLE " + qualified + " (ID INT PRIMARY KEY, VALUE VARCHAR(100) DEFAULT "
                + sample.defaultExpression() + ")";
        final var evidence = Document.create().setString("case", sample.name())
                .setNumber("compatibleMode", DamengTestDatabase.requestedCompatibilityMode()).setString("sql", sql);
        try (var connection = new DamengConnection(Configuration.create().with("hostname", DamengTestDatabase.hostname())
                .with("port", DamengTestDatabase.port()).with("user", DamengTestDatabase.user())
                .with("password", DamengTestDatabase.password()).build())) {
            final long start = connection.currentLsn();
            boolean created = false;
            try {
                try {
                    connection.execute(sql);
                    created = true;
                }
                catch (SQLException rejected) {
                    evidence.setString("status", "rejected").setNumber("errorCode", rejected.getErrorCode())
                            .setString("sqlState", rejected.getSQLState()).setString("message", rejected.getMessage());
                    if (sample.nativeRequired() && DamengTestDatabase.requestedCompatibilityMode() == 0) {
                        throw rejected;
                    }
                    return;
                }
                evidence.setString("status", "accepted");
                connection.query("SELECT VALUE FROM " + qualified + " WHERE 1=0", result -> assertThat(result.next()).isFalse());
                final long end = connection.currentLsn();
                Awaitility.await("DDL archive is available").atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(100))
                        .until(() -> connection.latestArchivedLsn() >= end);
                connection.startLogMining(start, end, false);
                final List<String> redo = connection.queryAndMap("SELECT SQL_REDO FROM V$LOGMNR_CONTENTS WHERE OPERATION_CODE=5 "
                        + "AND SCN >= " + start + " AND SCN <= " + end, result -> {
                            final List<String> records = new ArrayList<>();
                            while (result.next()) {
                                final var statement = result.getString(1);
                                if (statement != null && statement.contains(table)) {
                                    records.add(statement);
                                }
                            }
                            return records;
                        });
                evidence.setString("redo", String.join("\n", redo));
                assertThat(redo).as("accepted DDL must have an archived record").isNotEmpty();
            }
            finally {
                connection.endLogMining();
                if (created) {
                    connection.execute("DROP TABLE " + qualified);
                }
                final var directory = Path.of(System.getProperty("dameng.evidence.directory", "target/ddl-syntax-probe"));
                java.nio.file.Files.createDirectories(directory);
                java.nio.file.Files.writeString(directory.resolve("mode-" + DamengTestDatabase.requestedCompatibilityMode() + "-" + sample.name() + ".json"),
                        DocumentWriter.defaultWriter().write(evidence));
            }
        }
    }
}
