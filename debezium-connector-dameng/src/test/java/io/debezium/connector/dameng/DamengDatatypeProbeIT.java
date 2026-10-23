/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;

import java.math.BigDecimal;
import java.nio.file.Path;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import io.debezium.config.Configuration;
import io.debezium.connector.dameng.DamengDatatypeCases.TypeCase;
import io.debezium.connector.dameng.DamengDatatypeCases.Value;
import io.debezium.connector.dameng.logminer.LogMinerRow;
import io.debezium.connector.dameng.logminer.LogMinerSql;
import io.debezium.connector.dameng.logminer.LogMinerTransactionBuffer;
import io.debezium.connector.dameng.logminer.parser.LogMinerDmlParser;
import io.debezium.data.Envelope.Operation;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;

/**
 * Component consistency across server modes, including those the production task rejects.
 * Invokes real JDBC, archived LogMiner, DML/DDL parsers and converters without mocking
 * validation or enabling a mode in the connector. This is not a complete-engine acceptance test.
 */
class DamengDatatypeProbeIT {
    private DamengConnection connection;
    private DamengConnectorConfig config;
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
        config = new DamengConnectorConfig(Configuration.create()
                .with("topic.prefix", "dameng")
                .with("database.hostname", DamengTestDatabase.hostname())
                .with("database.port", DamengTestDatabase.port())
                .with("database.user", DamengTestDatabase.user())
                .with("database.password", DamengTestDatabase.password())
                .with("database.dbname", System.getProperty("dameng.database", "DAMENG"))
                .build());
        connection = new DamengConnection(config.getJdbcConfig());
        tableId = new TableId(null, DamengTestDatabase.schema(),
                "DBZ_TYPE_PROBE_" + UUID.randomUUID().toString().replace("-", "").substring(0, 12).toUpperCase());
    }

    @AfterEach
    void after() throws Exception {
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

    static Stream<TypeCase> boundaryTypes() {
        return DamengDatatypesIT.boundaryTypes();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("boundaryTypes")
    void shouldCompareJdbcAndArchivedCrudAgainstStoredBoundaries(TypeCase type) throws Exception {
        final var builder = config.getConfig().edit();
        type.settings().forEach(builder::with);
        config = new DamengConnectorConfig(builder.build());
        final var create = "CREATE TABLE " + tableName() + " (ID INT PRIMARY KEY, VALUE " + type.declaration() + ")" + type.storage();
        final long start = connection.currentLsn();
        connection.execute(create);
        tableCreated = true;
        final var tables = new Tables();
        connection.readSchema(tables, null, tableId.schema(), id -> id.equals(tableId), null, false);
        final var table = tables.forTable(tableId);
        final var converters = new DamengValueConverters(config, connection.getDatabaseCharset());
        for (var column : table.columns()) {
            DamengTestDatabase.recordCapability(type.name() + "/metadata/" + column.name(),
                    "jdbc=" + column.jdbcType() + "; type=" + column.typeName() + "; expression=" + column.typeExpression()
                            + "; length=" + column.length() + "; scale=" + column.scale());
        }
        final List<Executable> checks = new ArrayList<>();
        final List<ExpectedEvent> expectedEvents = new ArrayList<>();
        int id = 1;
        for (var value : Stream.concat(type.values().stream(), Stream.of(Value.sql("NULL"))).toList()) {
            final int currentId = id++;
            value.insert(connection.connection(), tableName(), currentId);
            final var inserted = storedRow(currentId);
            expectedEvents.add(new ExpectedEvent(Operation.CREATE, null, inserted));
            final var snapshotValues = readSnapshotValues(table, currentId);
            DamengTestDatabase.recordCapability(type.name() + "/source/id=" + currentId,
                    "input=" + value.description() + "; JDBC=" + Arrays.deepToString(snapshotValues) + "; expected=" + inserted);
            recordJsonEvidence(type.name() + "/id=" + currentId, table, converters, snapshotValues);
            checks.add(() -> DamengDatatypeAssertions.assertRow(convertRow(table, converters, snapshotValues), inserted, "JDBC snapshot/id=" + currentId));
            connection.execute("UPDATE " + tableName() + " SET VALUE=NULL WHERE ID=" + currentId);
            final var nulled = storedRow(currentId);
            expectedEvents.add(new ExpectedEvent(Operation.UPDATE, inserted, nulled));
            value.update(connection.connection(), tableName(), currentId);
            final var restored = storedRow(currentId);
            expectedEvents.add(new ExpectedEvent(Operation.UPDATE, nulled, restored));
            connection.execute("DELETE FROM " + tableName() + " WHERE ID=" + currentId);
            expectedEvents.add(new ExpectedEvent(Operation.DELETE, restored, null));
        }
        final var rows = mine(start);
        for (var row : rows) {
            DamengTestDatabase.recordCapability(type.name() + "/archive/LSN=" + row.lsn(), "operation=" + row.operation() + "; SQL_REDO=" + row.redo());
        }
        final var objectId = connection.getTableObjectId(tableId);
        final var data = rows.stream().filter(row -> row.objectId() == objectId && row.operation() >= 1 && row.operation() <= 3).toList();
        checks.add(() -> assertThat(data).as("archived committed CRUD count").hasSize(expectedEvents.size()));
        final var physical = Table.editor().tableId(new TableId(null, null, "OBJ# " + objectId));
        for (var column : table.columns()) {
            physical.addColumn(column.edit().name("COL " + column.position()).create());
        }
        final var physicalTable = physical.create();
        final var parser = new LogMinerDmlParser();
        for (int index = 0; index < Math.min(data.size(), expectedEvents.size()); index++) {
            final var row = data.get(index);
            final var expected = expectedEvents.get(index);
            final int eventIndex = index;
            checks.add(() -> {
                final var parsed = parser.parse(row.redo(), physicalTable);
                assertAll("archived event=" + eventIndex + "/LSN=" + row.lsn(),
                        () -> assertThat(parsed.operation()).isEqualTo(expected.operation()),
                        () -> DamengDatatypeAssertions.assertRow(expected.before() == null ? null : convertRow(table, converters, parsed.oldValues()),
                                expected.before(), "redo/before/event=" + eventIndex),
                        () -> DamengDatatypeAssertions.assertRow(expected.after() == null ? null : convertRow(table, converters, parsed.newValues()),
                                expected.after(), "redo/after/event=" + eventIndex));
            });
        }
        checks.add(() -> {
            final var ddl = rows.stream().filter(row -> row.operation() == 5 && create.equals(row.redo())).findFirst().orElseThrow();
            final var history = new Tables();
            new DamengDdlParser(connection.getDatabaseCharset(), true, false, false).parse(ddl.redo(), history, tableId.schema());
            final var historicalTable = history.forTable(tableId);
            // Compare output schemas, rather than nominal JDBC aliases of otherwise identical types.
            assertThat(rowSchema(historicalTable, converters)).as("archived DDL and JDBC produce the same logical schema")
                    .isEqualTo(rowSchema(table, converters));
        });
        int rejectedId = 9999;
        for (var value : type.rejected()) {
            final int invalidId = rejectedId++;
            checks.add(() -> verifyOutsideRangeInput(type, value, invalidId, table, physicalTable, converters));
        }
        assertAll("component/mode=" + DamengTestDatabase.requestedCompatibilityMode() + "/" + type, checks);
    }

    private static void recordJsonEvidence(String sample, Table table, DamengValueConverters converters, Object[] values) throws Exception {
        try (var converter = new JsonConverter()) {
            converter.configure(Map.of("schemas.enable", true), false);
            final var row = convertRow(table, converters, values);
            final var encoded = converter.fromConnectData("dameng.types", row.schema(), row);
            final var decoded = converter.toConnectData("dameng.types", encoded);
            DamengTestDatabase.recordCapability(sample + "/JSON", new String(encoded, java.nio.charset.StandardCharsets.UTF_8)
                    + "; decoded=" + decoded.value());
        }
        catch (RuntimeException failure) {
            // Retain diagnostics; the independent strict consistency checks still report the failure.
            DamengTestDatabase.recordCapability(sample + "/JSON", failure.getClass().getName() + ": " + failure.getMessage());
        }
    }

    @Test
    void shouldDiagnoseLocalTimezoneAcrossWriterReaderAndMiningSessions() throws Exception {
        final long start = connection.currentLsn();
        connection.execute("CREATE TABLE " + tableName() + " (ID INT PRIMARY KEY, VALUE TIMESTAMP(6) WITH LOCAL TIME ZONE)");
        tableCreated = true;
        try (var writer = new DamengConnection(config.getJdbcConfig())) {
            writer.execute("SET TIME ZONE '+05:45'",
                    "INSERT INTO " + tableName() + " VALUES(1,TIMESTAMP'2000-02-29 12:34:56.123456')",
                    "INSERT INTO " + tableName() + " VALUES(2,TIMESTAMP'2000-02-29 12:34:56.123456 -12:59')");
        }
        for (var zone : List.of("+08:00", "+00:00", "-12:59")) {
            connection.execute("SET TIME ZONE '" + zone + "'");
            final List<String> values = connection.queryAndMap("SELECT ID, VALUE FROM " + tableName() + " ORDER BY ID", result -> {
                final List<String> samples = new ArrayList<>();
                while (result.next()) {
                    samples.add("id=" + result.getInt(1) + "; stored=" + result.getString(2) + "; timestamp=" + result.getTimestamp(2));
                }
                return samples;
            });
            for (var value : values) {
                DamengTestDatabase.recordCapability("LOCAL/session=" + zone, value);
            }
            for (var row : mine(start)) {
                if (row.operation() == 1) {
                    DamengTestDatabase.recordCapability("LOCAL/miner=" + zone + "/LSN=" + row.lsn(), row.redo());
                }
            }
            connection.endLogMining();
        }
    }

    @Test
    void shouldDiagnoseMineValueAvailabilityWithoutReplacingHistoricalRowsWithCurrentData() throws Exception {
        final long start = connection.currentLsn();
        connection.execute("CREATE TABLE " + tableName() + " (ID INT PRIMARY KEY, VALUE DOUBLE, TZ TIME(6) WITH TIME ZONE)");
        tableCreated = true;
        connection.execute("INSERT INTO " + tableName() + " VALUES(1,1.23456789012345,TIME'12:34:56.123456 +05:45')",
                "UPDATE " + tableName() + " SET VALUE=0.000000123456789 WHERE ID=1",
                "DELETE FROM " + tableName() + " WHERE ID=1");
        final int currentRows = connection.queryAndMap("SELECT COUNT(*) FROM " + tableName(), result -> result.next() ? result.getInt(1) : -1);
        assertThat(currentRows).isZero();
        mine(start);
        for (var parameter : List.of("LOGMNR_DELAY_ANALYZE", "FLOAT_MODE", "DATETIME_FMT_MODE", "RLOG_LLOG_COMPRESS")) {
            DamengTestDatabase.recordCapability("MINE_VALUE/parameter/" + parameter, String.valueOf(connection.getIniParameter(parameter)));
        }
        try {
            final List<String> results = connection.queryAndMap("SELECT OPERATION_CODE, REDO_VALUE, UNDO_VALUE FROM V$LOGMNR_CONTENTS "
                    + "WHERE DATA_OBJ#=" + connection.getTableObjectId(tableId) + " AND OPERATION_CODE IN (1,2,3)", result -> {
                        final List<String> samples = new ArrayList<>();
                        while (result.next()) {
                            samples.add("operation=" + result.getInt(1) + "; redo=" + result.getLong(2) + "; undo=" + result.getLong(3));
                        }
                        return samples;
                    });
            for (var result : results) {
                DamengTestDatabase.recordCapability("MINE_VALUE/handles", result);
            }
        }
        catch (SQLException unavailable) {
            DamengTestDatabase.recordCapability("MINE_VALUE/handles", "unavailable; code=" + unavailable.getErrorCode() + "; " + unavailable.getMessage());
        }
        try {
            final var function = connection.queryAndMap("SELECT DBMS_LOGMNR.MINE_VALUE(0, '" + tableId.schema() + "." + tableId.table()
                    + ".VALUE') FROM DUAL", result -> result.next() ? result.getString(1) : null);
            DamengTestDatabase.recordCapability("MINE_VALUE/function", "available; dummy handle returned " + function);
        }
        catch (SQLException unavailable) {
            DamengTestDatabase.recordCapability("MINE_VALUE/function", "call rejected; code=" + unavailable.getErrorCode() + "; " + unavailable.getMessage());
        }
        recordMinedColumnValues("dictionary-free", List.of(tableId.schema() + "." + tableId.table() + ".ID",
                tableId.schema() + "." + tableId.table() + ".VALUE", tableId.schema() + "." + tableId.table() + ".TZ",
                "OBJ# " + connection.getTableObjectId(tableId) + ".COL 2", tableId.schema() + ".OBJ# " + connection.getTableObjectId(tableId) + ".COL 2"));
        final long end = connection.currentLsn();
        connection.endLogMining();
        for (var file : connection.archiveLogsForRange(start, end)) {
            connection.execute(LogMinerSql.addLogFile(file.path()));
        }
        try {
            connection.execute("BEGIN DBMS_LOGMNR.START_LOGMNR(STARTSCN => " + start + ", ENDSCN => " + end + ", OPTIONS => "
                    + (LogMinerSql.NO_ROWID_IN_STMT | LogMinerSql.DICT_FROM_ONLINE_CATALOG) + "); END;");
            recordMinedColumnValues("online-dictionary", List.of(tableId.schema() + "." + tableId.table() + ".ID",
                    tableId.schema() + "." + tableId.table() + ".VALUE", tableId.schema() + "." + tableId.table() + ".TZ"));
        }
        finally {
            connection.execute(LogMinerSql.end());
        }
    }

    private void recordMinedColumnValues(String dictionary, List<String> names) throws Exception {
        for (var name : names) {
            try {
                final var quotedName = LogMinerSql.quoteLiteral(name);
                final List<String> values = connection.queryAndMap("SELECT OPERATION_CODE, DBMS_LOGMNR.MINE_VALUE(REDO_VALUE, '"
                        + quotedName + "'), DBMS_LOGMNR.MINE_VALUE(UNDO_VALUE, '" + quotedName + "'), DBMS_LOGMNR.COLUMN_PRESENT(REDO_VALUE, '"
                        + quotedName + "'), DBMS_LOGMNR.COLUMN_PRESENT(UNDO_VALUE, '" + quotedName + "'), SQL_REDO FROM V$LOGMNR_CONTENTS WHERE DATA_OBJ#="
                        + connection.getTableObjectId(tableId) + " AND OPERATION_CODE IN (1,2,3)", result -> {
                            final List<String> samples = new ArrayList<>();
                            while (result.next()) {
                                samples.add("operation=" + result.getInt(1) + "; redo=" + result.getString(2) + "; undo=" + result.getString(3)
                                        + "; present-redo=" + result.getInt(4) + "; present-undo=" + result.getInt(5) + "; SQL_REDO=" + result.getString(6));
                            }
                            return samples;
                        });
                for (var value : values) {
                    DamengTestDatabase.recordCapability("MINE_VALUE/" + dictionary + "/column/" + name, value);
                }
            }
            catch (SQLException unavailable) {
                DamengTestDatabase.recordCapability("MINE_VALUE/" + dictionary + "/column/" + name,
                        "unavailable; code=" + unavailable.getErrorCode() + "; " + unavailable.getMessage());
            }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "NUMBER", "NUMERIC", "DECIMAL", "CHAR(8 CHAR)" })
    void shouldDiagnoseBoundaryInputThroughLiteralsAndJdbcBinding(String declaration) throws Exception {
        connection.execute("CREATE TABLE " + tableName() + " (ID INT PRIMARY KEY, VALUE " + declaration + ")");
        tableCreated = true;
        final boolean character = declaration.startsWith("CHAR");
        final List<String> inputs = character ? List.of("梦".repeat(7), "梦".repeat(8), "😀".repeat(8)) : List.of("1E125", "-1E125");
        int id = 1;
        for (var input : inputs) {
            for (var transport : List.of("JDBC", "SQL literal", "SQL scientific notation")) {
                if (character && transport.equals("SQL scientific notation")) {
                    continue;
                }
                final int rowId = id++;
                try {
                    if (transport.equals("JDBC")) {
                        try (var statement = connection.connection().prepareStatement("INSERT INTO " + tableName() + " VALUES(?,?)")) {
                            statement.setInt(1, rowId);
                            if (character) {
                                statement.setString(2, input);
                            }
                            else {
                                statement.setBigDecimal(2, new BigDecimal(input));
                            }
                            statement.executeUpdate();
                        }
                    }
                    else {
                        final var literal = character ? "'" + LogMinerSql.quoteLiteral(input) + "'"
                                : transport.equals("SQL literal") ? new BigDecimal(input).toPlainString() : input;
                        connection.execute("INSERT INTO " + tableName() + " VALUES(" + rowId + "," + literal + ")");
                    }
                }
                catch (SQLException rejected) {
                    DamengTestDatabase.recordCapability("BOUNDARY_INPUT/" + declaration + "/" + transport + "/" + input,
                            "rejected; code=" + rejected.getErrorCode() + "; " + rejected.getMessage());
                    continue;
                }
                final var actual = connection.queryAndMap("SELECT VALUE FROM " + tableName() + " WHERE ID=" + rowId, result -> {
                    assertThat(result.next()).isTrue();
                    return result.getString(1);
                });
                DamengTestDatabase.recordCapability("BOUNDARY_INPUT/" + declaration + "/" + transport + "/" + input, "accepted; stored=" + actual);
                if (character) {
                    assertThat(actual).as("accepted fixed character input must retain every character").startsWith(input);
                    assertThat(actual.substring(input.length())).as("only fixed-width space padding may follow the input")
                            .isEqualTo(" ".repeat(actual.length() - input.length()));
                }
                else {
                    assertThat(new BigDecimal(actual)).as("accepted numeric input must retain its value").isEqualByComparingTo(new BigDecimal(input));
                }
            }
        }
    }

    private void verifyOutsideRangeInput(TypeCase type, Value value, int id, Table table, Table physicalTable, DamengValueConverters converters) throws Exception {
        final long start = connection.currentLsn();
        try {
            value.insert(connection.connection(), tableName(), id);
        }
        catch (SQLException rejected) {
            assertThat(rejected.getErrorCode()).as("database rejects an outside-range input").isNotZero();
            DamengTestDatabase.recordCapability(type.declaration() + "/outside-range/" + value.description(), "rejected; code=" + rejected.getErrorCode());
            return;
        }
        final var expected = storedRow(id);
        final var snapshotValues = readSnapshotValues(table, id);
        final long objectId = connection.getTableObjectId(tableId);
        final var rows = mine(start).stream().filter(row -> row.objectId() == objectId && row.operation() == 1).toList();
        DamengTestDatabase.recordCapability(type.declaration() + "/outside-range/" + value.description(),
                "accepted; actual stored JDBC value=" + expected.get("VALUE") + "; consistency outcome is recorded in test XML");
        assertAll("outside-range input accepted by mode=" + DamengTestDatabase.requestedCompatibilityMode(),
                () -> assertThat(rows).as("accepted outside-range INSERT must appear exactly once in committed redo").hasSize(1),
                () -> DamengDatatypeAssertions.assertRow(convertRow(table, converters, snapshotValues), expected, "accepted outside-range JDBC snapshot"),
                () -> {
                    final var parsed = new LogMinerDmlParser().parse(rows.get(0).redo(), physicalTable);
                    DamengDatatypeAssertions.assertRow(convertRow(table, converters, parsed.newValues()), expected, "accepted outside-range archived INSERT");
                });
    }

    @ParameterizedTest
    @ValueSource(strings = { "REAL", "DOUBLE" })
    void shouldClassifyNonFiniteJdbcInputs(String declaration) throws Exception {
        final long start = connection.currentLsn();
        connection.execute("CREATE TABLE " + tableName() + " (ID INT PRIMARY KEY, VALUE " + declaration + ")");
        tableCreated = true;
        final var tables = new Tables();
        connection.readSchema(tables, null, tableId.schema(), candidate -> candidate.equals(tableId), null, false);
        final var table = tables.forTable(tableId);
        final var converters = new DamengValueConverters(config, connection.getDatabaseCharset());
        final List<Executable> checks = new ArrayList<>();
        final List<Map<String, Object>> accepted = new ArrayList<>();
        int id = 1;
        for (double value : new double[]{ Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY }) {
            final Object parameter;
            if (declaration.equals("REAL")) {
                parameter = (float) value;
            }
            else {
                parameter = value;
            }
            try {
                new Value("?", parameter).insert(connection.connection(), tableName(), id);
            }
            catch (SQLException rejected) {
                // DM documents finite ranges only. A rejection is an input capability,
                // not a consistency pass for a row that was never stored or mined.
                assertThat(rejected.getErrorCode()).as("explicit database rejection of non-finite input").isNotZero();
                DamengTestDatabase.recordCapability(declaration + "/" + value, "rejected; code=" + rejected.getErrorCode() + ";" + rejected.getMessage());
                id++;
                continue;
            }
            try (var statement = connection.connection().createStatement();
                    var result = statement.executeQuery("SELECT VALUE FROM " + tableName() + " WHERE ID=" + id)) {
                assertThat(result.next()).isTrue();
                assertThat(Double.doubleToRawLongBits(result.getDouble(1))).as("accepted non-finite input must not become a finite value")
                        .isEqualTo(Double.doubleToRawLongBits(value));
            }
            DamengTestDatabase.recordCapability(declaration + "/" + value, "accepted; consistency outcome is recorded in test XML");
            final var expected = storedRow(id);
            accepted.add(expected);
            final var snapshotValues = readSnapshotValues(table, id);
            checks.add(() -> DamengDatatypeAssertions.assertRow(convertRow(table, converters, snapshotValues), expected, "non-finite JDBC snapshot"));
            id++;
        }
        if (!accepted.isEmpty()) {
            final var objectId = connection.getTableObjectId(tableId);
            final var physical = Table.editor().tableId(new TableId(null, null, "OBJ# " + objectId));
            table.columns().forEach(column -> physical.addColumn(column.edit().name("COL " + column.position()).create()));
            final var physicalTable = physical.create();
            final var rows = mine(start).stream().filter(row -> row.objectId() == objectId && row.operation() == 1).toList();
            checks.add(() -> assertThat(rows).hasSize(accepted.size()));
            for (int index = 0; index < Math.min(rows.size(), accepted.size()); index++) {
                final var row = rows.get(index);
                final var expected = accepted.get(index);
                checks.add(() -> DamengDatatypeAssertions.assertRow(convertRow(table, converters,
                        new LogMinerDmlParser().parse(row.redo(), physicalTable).newValues()), expected, "non-finite archived redo"));
            }
        }
        assertAll("non-finite/mode=" + DamengTestDatabase.requestedCompatibilityMode() + "/" + declaration, checks);
    }

    @ParameterizedTest
    @ValueSource(strings = { "INTERVAL YEAR(3)", "INTERVAL YEAR(3) TO MONTH", "INTERVAL MONTH(3)", "INTERVAL DAY(3)",
            "INTERVAL DAY(3) TO HOUR", "INTERVAL DAY(3) TO MINUTE", "INTERVAL DAY(3) TO SECOND(6)", "INTERVAL HOUR(3)",
            "INTERVAL HOUR(3) TO MINUTE", "INTERVAL HOUR(3) TO SECOND(6)", "INTERVAL MINUTE(3)", "INTERVAL MINUTE(3) TO SECOND(6)",
            "INTERVAL SECOND(3,6)" })
    void shouldDescribeNativeIntervalMetadataAndRedo(String declaration) throws Exception {
        final long start = connection.currentLsn();
        connection.execute("CREATE TABLE " + tableName() + " (ID INT PRIMARY KEY, VALUE " + declaration + ")");
        tableCreated = true;
        final var qualifier = declaration.substring("INTERVAL ".length());
        final var sample = switch (qualifier) {
            case "YEAR(3)" -> "012";
            case "YEAR(3) TO MONTH" -> "012-03";
            case "MONTH(3)" -> "015";
            case "DAY(3)" -> "002";
            case "DAY(3) TO HOUR" -> "002 03";
            case "DAY(3) TO MINUTE" -> "002 03:04";
            case "DAY(3) TO SECOND(6)" -> "002 03:04:05.123456";
            case "HOUR(3)" -> "027";
            case "HOUR(3) TO MINUTE" -> "027:04";
            case "HOUR(3) TO SECOND(6)" -> "027:04:05.123456";
            case "MINUTE(3)" -> "123";
            case "MINUTE(3) TO SECOND(6)" -> "123:05.123456";
            case "SECOND(3,6)" -> "123.123456";
            default -> throw new IllegalArgumentException(declaration);
        };
        connection.execute("INSERT INTO " + tableName() + " VALUES(1,INTERVAL '" + sample + "' " + qualifier + ")",
                "INSERT INTO " + tableName() + " VALUES(2,INTERVAL '-" + sample + "' " + qualifier + ")",
                "INSERT INTO " + tableName() + " VALUES(3,NULL)");
        connection.query("SELECT * FROM " + tableName() + " ORDER BY ID", result -> {
            final var metadata = result.getMetaData();
            while (result.next()) {
                final var value = result.getObject(2);
                final var description = "jdbc=" + metadata.getColumnType(2) + "; type=" + metadata.getColumnTypeName(2)
                        + "; precision=" + metadata.getPrecision(2) + "; scale=" + metadata.getScale(2)
                        + "; object=" + value + "; class=" + (value == null ? null : value.getClass().getName()) + "; string=" + result.getString(2);
                try {
                    DamengTestDatabase.recordCapability("INTERVAL/" + declaration + "/id=" + result.getInt(1), description);
                }
                catch (Exception failure) {
                    throw new SQLException(failure);
                }
            }
        });
        connection.execute("UPDATE " + tableName() + " SET VALUE=NULL WHERE ID=1", "DELETE FROM " + tableName() + " WHERE ID=2");
        for (var row : mine(start)) {
            DamengTestDatabase.recordCapability("INTERVAL/" + declaration + "/redo/" + row.lsn(), row.redo());
        }
    }

    private Map<String, Object> storedRow(int id) throws Exception {
        return DamengDatatypeAssertions.storedRow(connection.connection(), tableName(), id, config.getConfig());
    }

    private Object[] readSnapshotValues(Table table, int id) throws Exception {
        return connection.queryAndMap("SELECT * FROM " + tableName() + " WHERE ID=" + id, result -> {
            assertThat(result.next()).isTrue();
            final Object[] values = new Object[table.columns().size()];
            for (var column : table.columns()) {
                values[column.position() - 1] = connection.getColumnValue(result, column.position(), column, table);
            }
            return values;
        });
    }

    private static Schema rowSchema(Table table, DamengValueConverters converters) {
        final var builder = SchemaBuilder.struct().name("dameng.probe.Row");
        for (var column : table.columns()) {
            final var field = converters.schemaBuilder(column);
            if (column.isOptional()) {
                field.optional();
            }
            builder.field(column.name(), field.build());
        }
        return builder.build();
    }

    private static Struct convertRow(Table table, DamengValueConverters converters, Object[] values) {
        final var row = new Struct(rowSchema(table, converters));
        for (var column : table.columns()) {
            final var field = row.schema().field(column.name());
            row.put(field, converters.converter(column, field).convert(values[column.position() - 1]));
        }
        row.validate();
        return row;
    }

    private List<LogMinerRow> mine(long start) throws Exception {
        final long end = connection.currentLsn();
        Awaitility.await("datatype CRUD is archived").atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(100))
                .until(() -> connection.latestArchivedLsn() >= end);
        connection.startLogMining(start, end, false);
        try (var buffer = new LogMinerTransactionBuffer(64 * 1024 * 1024, 4 * 1024 * 1024, 10_000,
                Path.of(System.getProperty("java.io.tmpdir")), 256 * 1024 * 1024)) {
            connection.prepareQuery(connection.logMinerContentsQuery(false), statement -> {
                statement.setLong(1, start);
                statement.setLong(2, end);
            }, result -> {
                while (result.next()) {
                    buffer.add(new LogMinerRow(result.getLong("SCN"), result.getLong("START_SCN"), result.getLong("COMMIT_SCN"),
                            result.getString("XID"), result.getInt("OPERATION_CODE"), result.getInt("ROLL_BACK"), result.getString("SEG_OWNER"),
                            result.getString("TABLE_NAME"), result.getString("USERNAME"), result.getString("SQL_REDO"), result.getString("RS_ID"),
                            result.getInt("SSN"), result.getInt("CSF"), result.getTimestamp("TIMESTAMP").toInstant(), result.getLong("DATA_OBJ#")));
                }
            });
            final List<LogMinerRow> committed = new ArrayList<>();
            for (var transaction : buffer.committedTransactions()) {
                try (var cursor = transaction.open()) {
                    LogMinerRow row;
                    while ((row = cursor.next()) != null) {
                        committed.add(row);
                    }
                }
            }
            return List.copyOf(committed);
        }
    }

    private String tableName() {
        return "\"" + tableId.schema().replace("\"", "\"\"") + "\".\"" + tableId.table() + "\"";
    }

    private record ExpectedEvent(Operation operation, Map<String, Object> before, Map<String, Object> after) {
    }
}
