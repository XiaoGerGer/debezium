/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.lang.reflect.Method;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.sql.Connection;
import java.sql.DriverManager;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import io.debezium.config.Configuration;
import io.debezium.connector.dameng.logminer.LogMinerSql;

/** Opt-in diagnostic benchmark, deliberately independent of Connector/Kafka throughput. */
@EnabledIfSystemProperty(named = "dameng.benchmark.jni.jar", matches = ".+")
class DamengLogMinerBenchmarkIT {
    private static final int FETCH_SIZE = 2_000;
    private static final int SQL_OPTIONS = Integer.getInteger("dameng.benchmark.sql.options", 16);
    private static final int JNI_OPTIONS = Integer.getInteger("dameng.benchmark.jni.options", -1);
    private static final String COLUMNS = "SCN,START_SCN,COMMIT_SCN,XID,OPERATION_CODE,ROLL_BACK,SEG_OWNER,TABLE_NAME,USERNAME,SQL_REDO,RS_ID,SSN,CSF,DATA_OBJ#,RBASQN,RBABLK,RBABYTE";
    private static final String ORDER = " ORDER BY COMMIT_SCN,SCN,RBASQN,RBABLK,RBABYTE,SSN";
    private static final List<String> GETTERS = List.of("getScn", "getStartScn", "getCommitScn", "getXid", "getOperationCode",
            "getRollBack", "getSegOwner", "getTableName", "getUserName", "getSqlRedo", "getRsId", "getSsn", "getCsf", "getDataObj",
            "getRbasqn", "getRbablk", "getRbabyte");
    private final Path directory = Path.of(System.getProperty("dameng.benchmark.directory", "target/dameng-logminer-benchmark"));
    private final StringBuilder evidence = new StringBuilder();
    private final StringBuilder timings = new StringBuilder(
            "round,method,rows,setup_ms,start_ms,first_batch_ms,fetch_ms,sort_ms,end_ms,total_ms,rows_per_second,order_inversions,native_get_data_ms\n");
    private long objectId;

    @Test
    @Timeout(value = 20, unit = java.util.concurrent.TimeUnit.MINUTES)
    void shouldCompareSqlAndJniOnIdenticalFrozenArchives() throws Exception {
        assumeTrue(System.getProperty("dameng.hostname") == null, "Benchmark owns its database");
        final int count = Integer.getInteger("dameng.benchmark.rows", 100_000);
        final int rounds = Integer.getInteger("dameng.benchmark.rounds", 5);
        assertThat(count).isGreaterThanOrEqualTo(100).isEqualTo(count / 100 * 100);
        assertThat(rounds).isPositive();
        Files.createDirectories(directory);
        note("JAVA=" + System.getProperty("java.runtime.version"));
        note("JNI_JAR=" + System.getProperty("dameng.benchmark.jni.jar"));
        note("LIBRARY_PATH=" + System.getProperty("java.library.path"));
        note("SQL_OPTIONS=" + SQL_OPTIONS + " JNI_OPTIONS=" + JNI_OPTIONS + " FETCH_SIZE=" + FETCH_SIZE + " JNI_PARALLEL=2,8 TRX_END=1");
        DamengTestDatabase.start();
        try (var jdbc = open();
                var miner = new DamengConnection(configuration());
                var jni = new NativeMiner(Path.of(System.getProperty("dameng.benchmark.jni.jar")))) {
            note("DATABASE=" + jdbc.getMetaData().getDatabaseProductVersion() + " DRIVER=" + jdbc.getMetaData().getDriverVersion());
            try (var statement = jdbc.createStatement();
                    var result = statement.executeQuery(
                            "SELECT PARA_NAME,PARA_VALUE FROM V$DM_INI WHERE PARA_NAME LIKE 'LOGMNR%' ORDER BY PARA_NAME")) {
                while (result.next()) {
                    note(result.getString(1) + "=" + result.getString(2));
                }
            }
            execute(jdbc, "CREATE TABLE BENCH_MINER(ID INT PRIMARY KEY, AMOUNT DECIMAL(18,4), PAYLOAD VARCHAR(256))");
            try (var statement = jdbc.createStatement(); var rows = statement.executeQuery("SELECT ID FROM SYS.SYSOBJECTS WHERE NAME='BENCH_MINER'")) {
                assertThat(rows.next()).isTrue();
                objectId = rows.getLong(1);
            }
            final long start = miner.currentLsn();
            generate(jdbc, count);
            final long end = miner.currentLsn();
            Awaitility.await().atMost(Duration.ofSeconds(60)).pollInterval(Duration.ofMillis(200))
                    .until(() -> miner.latestArchivedLsn() >= end);
            // A separate immutable copy prevents background writes changing paired rounds.
            final var files = DamengTestDatabase.freezeBenchmarkArchives(
                    miner.archiveLogsForRange(start, end).stream().map(DamengConnection.ArchiveLogFile::path).toList(), directory);
            note("WORKLOAD_RANGE=" + start + ".." + end + " OBJECT=" + objectId + " FILES=" + files.size());
            final List<String> variants = Boolean.getBoolean("dameng.benchmark.jni.diagnostics")
                    ? List.of("sql-ordered", "jni-2", "jni-2-batch200", "jni-2-cache2048", "jni-2-batch200-cache2048", "jni-8-cache2048")
                    : List.of("sql-ordered", "jni-2", "sql-unordered", "jni-8");
            note("VARIANTS=" + variants);
            List<Row> reference = null;
            // Round zero warms both APIs. Rotate order to reduce page-cache/JIT/order bias.
            for (int round = 0; round <= rounds; round++) {
                final var methods = new ArrayList<>(variants);
                java.util.Collections.rotate(methods, round % methods.size());
                for (var method : methods) {
                    final var measurement = method.startsWith("jni") ? mineNative(jni, files, Integer.parseInt(method.split("-")[1]),
                            method.contains("batch200") ? 200 : FETCH_SIZE, method.contains("cache2048") ? 2048 : 256)
                            : mineSql(files, method.equals("sql-ordered"));
                    record(round, method, measurement);
                    if (reference == null) {
                        reference = measurement.rows();
                        verifyWorkload(reference, count);
                        Files.writeString(directory.resolve("reference-sample.txt"), reference.stream().limit(40).map(Row::toString)
                                .collect(java.util.stream.Collectors.joining("\n")));
                    }
                    else {
                        compare(reference, measurement.rows(), round + "-" + method);
                    }
                }
            }
            note("ALL_ROUNDS_COMPARISON_PASSED=true allowLegacyDifferences=" + Boolean.getBoolean("dameng.benchmark.allow.legacy.differences"));
        }
        finally {
            try {
                Files.writeString(directory.resolve("evidence.txt"), evidence);
                Files.writeString(directory.resolve("timings.csv"), timings);
            }
            finally {
                DamengTestDatabase.stop();
            }
        }
    }

    private void generate(Connection jdbc, int count) throws Exception {
        final long started = System.nanoTime();
        jdbc.setAutoCommit(false);
        try (var insert = jdbc.prepareStatement("INSERT INTO BENCH_MINER VALUES(?,?,?)")) {
            for (int id = 1; id <= count; id++) {
                insert.setInt(1, id);
                insert.setBigDecimal(2, java.math.BigDecimal.valueOf(id, 4));
                insert.setString(3, "payload-" + id + "-" + "abcdef0123456789".repeat(8));
                insert.addBatch();
                if (id % 100 == 0) {
                    insert.executeBatch();
                    // First half: 100 rows per transaction. Second half: one large transaction.
                    if (id <= count / 2) {
                        jdbc.commit();
                    }
                }
            }
        }
        jdbc.commit();
        execute(jdbc, "UPDATE BENCH_MINER SET AMOUNT=AMOUNT+1 WHERE ID<=" + count / 2);
        jdbc.commit();
        execute(jdbc, "DELETE FROM BENCH_MINER WHERE ID>" + count * 3 / 4);
        jdbc.commit();
        // Rollback must be preserved, not traded for a faster but unsafe API setting.
        execute(jdbc, "UPDATE BENCH_MINER SET PAYLOAD='rollback-probe' WHERE ID<=100");
        jdbc.rollback();
        jdbc.setAutoCommit(true);
        note("GENERATED inserts=" + count + " updates=" + count / 2 + " deletes=" + count / 4
                + " rollbackUpdates=100 generate_ms=" + ms(System.nanoTime() - started));
    }

    private Measurement mineSql(List<String> files, boolean ordered) throws Exception {
        final long begin = System.nanoTime();
        final List<Row> result = new ArrayList<>();
        try (var jdbc = open()) {
            for (var file : files) {
                execute(jdbc, LogMinerSql.addLogFile(file));
            }
            final long setup = System.nanoTime();
            execute(jdbc, "BEGIN DBMS_LOGMNR.START_LOGMNR(OPTIONS=>" + SQL_OPTIONS + "); END;");
            final long start = System.nanoTime();
            long first;
            try (var statement = jdbc.prepareStatement("SELECT " + COLUMNS + " FROM V$LOGMNR_CONTENTS" + (ordered ? ORDER : ""))) {
                statement.setFetchSize(FETCH_SIZE);
                statement.setQueryTimeout(300);
                try (var rows = statement.executeQuery()) {
                    first = System.nanoTime();
                    while (rows.next()) {
                        final List<String> values = new ArrayList<>(GETTERS.size());
                        for (int index = 1; index <= GETTERS.size(); index++) {
                            if (index == 4) {
                                final var bytes = rows.getBytes(index);
                                values.add(bytes == null ? "" : HexFormat.of().withUpperCase().formatHex(bytes));
                            }
                            else if (index <= 6 || index >= 12) {
                                values.add(Long.toString(rows.getLong(index)));
                            }
                            else {
                                values.add(empty(rows.getString(index)));
                            }
                        }
                        result.add(new Row(values));
                    }
                }
            }
            final long fetched = System.nanoTime();
            final long inversions = inversions(result);
            result.sort(Row.ORDERING);
            final long sorted = System.nanoTime();
            execute(jdbc, LogMinerSql.end());
            final long ended = System.nanoTime();
            return new Measurement(result, setup - begin, start - setup, first - start, fetched - start,
                    sorted - fetched, ended - sorted, ended - begin, inversions, 0);
        }
    }

    private Measurement mineNative(NativeMiner jni, List<String> files, int parallelism, int batchSize, int contentBuffers) throws Exception {
        final long begin = System.nanoTime();
        final List<Row> rows = new ArrayList<>();
        final long connection = ((Number) jni.call("createConnect", DamengTestDatabase.hostname(), DamengTestDatabase.port(),
                DamengTestDatabase.user(), DamengTestDatabase.password())).longValue();
        assertThat(connection).isPositive();
        try {
            jni.ok("setAttr", connection, 10001, parallelism);
            jni.ok("setAttr", connection, 10003, contentBuffers);
            // Older native clients have no OPTIONS attribute. Minus one leaves their default unchanged; exact record comparison must still pass.
            if (JNI_OPTIONS >= 0) {
                jni.ok("setAttr", connection, 10008, JNI_OPTIONS);
            }
            jni.ok("setAttr", connection, 10005, 1);
            for (var file : files) {
                jni.ok("addLogFile", connection, directory.resolve("archives").resolve(Path.of(file).getFileName()).toAbsolutePath().toString(), 3);
            }
            final long setup = System.nanoTime();
            jni.ok("startLogmnr", connection, -1L, null, null);
            final long started = System.nanoTime();
            long first = 0;
            long nativeCalls = 0;
            while (true) {
                final long beforeCall = System.nanoTime();
                final var batch = (Object[]) jni.call("getData", connection, batchSize);
                nativeCalls += System.nanoTime() - beforeCall;
                if (first == 0) {
                    first = System.nanoTime();
                }
                if (batch == null || batch.length == 0) {
                    break;
                }
                for (var row : batch) {
                    final List<String> values = new ArrayList<>(GETTERS.size());
                    for (var getter : jni.getters) {
                        values.add(empty(getter.invoke(row)));
                    }
                    // JNI prefixes the same binary transaction identifier with 0x.
                    if (values.get(3).startsWith("0x")) {
                        values.set(3, values.get(3).substring(2).toUpperCase(Locale.ROOT));
                    }
                    rows.add(new Row(values));
                }
            }
            final long fetched = System.nanoTime();
            final long inversions = inversions(rows);
            rows.sort(Row.ORDERING);
            final long sorted = System.nanoTime();
            jni.ok("endLogmnr", connection, 0);
            final long ended = System.nanoTime();
            return new Measurement(rows, setup - begin, started - setup, first - started, fetched - started,
                    sorted - fetched, ended - sorted, ended - begin, inversions, nativeCalls);
        }
        finally {
            jni.ok("closeConnect", connection);
        }
    }

    private void verifyWorkload(List<Row> rows, int count) {
        final Map<String, Long> operations = new LinkedHashMap<>();
        rows.stream().filter(row -> row.number(13) == objectId).forEach(row -> operations.merge(
                "op=" + row.values().get(4) + ",rollback=" + row.values().get(5), 1L, Long::sum));
        note("BUSINESS_OPERATIONS=" + operations);
        assertThat(operations.get("op=1,rollback=0")).isEqualTo((long) count);
        assertThat(operations.get("op=3,rollback=0")).isEqualTo((long) count / 2);
        assertThat(operations.get("op=2,rollback=0")).isEqualTo((long) count / 4);
        assertThat(rows.stream().filter(row -> row.number(13) == objectId && row.number(5) != 0).count()).isGreaterThanOrEqualTo(100);
    }

    private void compare(List<Row> expected, List<Row> actual, String label) throws Exception {
        // Exact multiset comparison includes duplicate multiplicity and SQL text, independent of API order.
        final Map<Row, Integer> differences = new HashMap<>();
        final boolean allowLegacyDifferences = label.contains("jni-") && Boolean.getBoolean("dameng.benchmark.allow.legacy.differences");
        if (allowLegacyDifferences) {
            // Explicit diagnostic exception for the old native library, NOT a production workaround.
            // Other allowances below cover only unused boundary metadata and rollback sentinels.
            note("JNI_RBABYTE_TRUNCATION_EXPECTED " + label + " rows=" + expected.stream().filter(row -> row.number(16) > 65535).count());
        }
        expected.forEach(row -> differences.merge(comparisonRow(row, allowLegacyDifferences, true), 1, Integer::sum));
        actual.forEach(row -> differences.merge(comparisonRow(row, allowLegacyDifferences, false), -1, Integer::sum));
        differences.values().removeIf(count -> count == 0);
        if (!differences.isEmpty()) {
            Files.writeString(directory.resolve("difference-" + label + ".txt"), differences.entrySet().stream().limit(100)
                    .map(Object::toString).collect(java.util.stream.Collectors.joining("\n")));
        }
        assertThat(differences.size()).as("Exact raw-record multiset for %s; see difference file", label).isZero();
        note("EQUAL " + label + " rows=" + actual.size() + " allowLegacyDifferences=" + allowLegacyDifferences);
    }

    private static Row comparisonRow(Row row, boolean allowLegacyDifferences, boolean sql) {
        if (!allowLegacyDifferences) {
            return row;
        }
        final List<String> values = new ArrayList<>(row.values());
        // Old JNI has different unset sentinels for a rolled-back transaction's COMMIT_SCN.
        if (row.number(5) != 0 && row.number(2) <= 0) {
            values.set(2, "0");
        }
        // Transaction boundaries are not table operations. Old JNI may retain the last
        // table's name and an unset object id (-1) in these otherwise unused fields.
        if (row.number(4) == 6 || row.number(4) == 7 || row.number(4) == 36) {
            values.set(6, "");
            values.set(7, "");
            values.set(13, "0");
        }
        if (sql) {
            values.set(16, Long.toString(row.number(16) & 0xffffL));
        }
        return new Row(values);
    }

    private void record(int round, String method, Measurement m) throws Exception {
        final var line = String.format(Locale.ROOT, "%d,%s,%d,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.1f,%d,%.3f%n", round, method,
                m.rows().size(), ms(m.setup()), ms(m.start()), ms(m.first()), ms(m.fetch()), ms(m.sort()), ms(m.end()), ms(m.total()),
                m.rows().size() * 1_000_000_000.0 / m.total(), m.inversions(), ms(m.nativeCalls()));
        timings.append(line);
        note(line.strip());
        Files.writeString(directory.resolve("timings.csv"), timings);
        final var digest = MessageDigest.getInstance("SHA-256");
        // Digest is informational only; equality above checks every field without hash collision risk.
        for (var row : m.rows()) {
            digest.update(row.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8));
        }
        note("DIGEST " + round + " " + method + " " + HexFormat.of().formatHex(digest.digest()));
    }

    private static long inversions(List<Row> rows) {
        long count = 0;
        for (int i = 1; i < rows.size(); i++) {
            if (Row.ORDERING.compare(rows.get(i - 1), rows.get(i)) > 0) {
                count++;
            }
        }
        return count;
    }

    private record Row(List<String> values) {
        private static final Comparator<Row> ORDERING = Comparator.comparingLong((Row row) -> row.number(2))
                .thenComparingLong(row -> row.number(0)).thenComparingLong(row -> row.number(14))
                .thenComparingLong(row -> row.number(15)).thenComparingLong(row -> row.number(16)).thenComparingLong(row -> row.number(11));

        long number(int index) {
            return values.get(index).isEmpty() ? 0 : Long.parseLong(values.get(index));
        }
    }

    private record Measurement(List<Row> rows, long setup, long start, long first, long fetch, long sort, long end, long total, long inversions, long nativeCalls) {
    }

    private static final class NativeMiner implements AutoCloseable {
        private final URLClassLoader loader;
        private final Map<String, Method> methods = new HashMap<>();
        private final List<Method> getters = new ArrayList<>();

        NativeMiner(Path jar) throws Exception {
            loader = new URLClassLoader(new java.net.URL[]{ jar.toUri().toURL() }, getClass().getClassLoader());
            final var api = Class.forName("com.dameng.logmnr.LogmnrDll", true, loader);
            for (var method : api.getDeclaredMethods()) {
                methods.put(method.getName(), method);
            }
            final var record = loader.loadClass("com.dameng.logmnr.LogmnrRecord");
            for (var getter : GETTERS) {
                getters.add(record.getMethod(getter));
            }
            ok("initLogmnr");
        }

        Object call(String name, Object... arguments) throws Exception {
            return methods.get(name).invoke(null, arguments);
        }

        void ok(String name, Object... arguments) throws Exception {
            assertThat(((Number) call(name, arguments)).intValue()).as("JNI %s", name).isZero();
        }

        @Override
        public void close() throws Exception {
            try {
                ok("deinitLogmnr");
            }
            finally {
                loader.close();
            }
        }
    }

    private static String empty(Object value) {
        return value == null ? "" : value.toString();
    }

    private static double ms(long nanos) {
        return nanos / 1_000_000.0;
    }

    private void note(String text) {
        evidence.append(text).append('\n');
        System.out.println("MINER_BENCHMARK " + text);
    }

    private static Connection open() throws Exception {
        return DriverManager.getConnection(DamengTestDatabase.jdbcUrl(), DamengTestDatabase.user(), DamengTestDatabase.password());
    }

    private static void execute(Connection jdbc, String sql) throws Exception {
        try (var statement = jdbc.createStatement()) {
            statement.setQueryTimeout(300);
            statement.execute(sql);
        }
    }

    private static Configuration configuration() {
        return Configuration.create().with("hostname", DamengTestDatabase.hostname()).with("port", DamengTestDatabase.port())
                .with("user", DamengTestDatabase.user()).with("password", DamengTestDatabase.password()).build();
    }
}
