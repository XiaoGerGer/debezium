/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.time.LocalDateTime;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import io.debezium.config.Configuration;
import io.debezium.connector.dameng.DamengConnection.ArchiveLogFile;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.jdbc.JdbcConnection.ResultSetConsumer;
import io.debezium.jdbc.JdbcConnection.StatementPreparer;
import io.debezium.relational.Column;
import io.debezium.relational.TableId;

class DamengConnectionTest {
    @Test
    void shouldNormalizeJdbcTextToTheHistoricalLobType() throws SQLException {
        final var connection = spy(new DamengConnection(JdbcConfiguration.create().build()));
        doReturn("1").when(connection).getCaseSensitiveFlag();
        final var metadata = mock(ResultSet.class);
        when(metadata.getString(4)).thenReturn("VALUE");
        when(metadata.getString(6)).thenReturn("TEXT");
        when(metadata.getInt(5)).thenReturn(Types.LONGVARCHAR);
        final var column = connection.readTableColumn(metadata, new TableId(null, "APP", "T"), null).orElseThrow().create();
        assertThat(column.jdbcType()).isEqualTo(Types.CLOB);
        assertThat(column.typeName()).isEqualTo("TEXT");
    }

    @ParameterizedTest
    @CsvSource({ "NUMBER,3", "DOUBLE,6", "FLOAT,6" })
    void shouldDistinguishDecimalAndBinaryStorageBehindTheJdbcFloatCode(String physicalType, int jdbcType) throws SQLException {
        final var connection = spy(new DamengConnection(JdbcConfiguration.create().build()));
        doReturn("1").when(connection).getCaseSensitiveFlag();
        final var metadata = mock(ResultSet.class);
        when(metadata.getString(4)).thenReturn("F");
        when(metadata.getString(6)).thenReturn("FLOAT");
        when(metadata.getInt(5)).thenReturn(Types.FLOAT);
        when(metadata.getInt(7)).thenReturn(53);
        final var storedType = mock(ResultSet.class);
        when(storedType.next()).thenReturn(true);
        when(storedType.getString(1)).thenReturn(physicalType);
        doAnswer(invocation -> {
            invocation.<ResultSetConsumer> getArgument(2).accept(storedType);
            return connection;
        }).when(connection).prepareQuery(anyString(), any(StatementPreparer.class), any(ResultSetConsumer.class));
        final var column = connection.readTableColumn(metadata, new TableId(null, "APP", "NUMBERS"), null).orElseThrow().create();
        assertThat(column.jdbcType()).isEqualTo(jdbcType);
        assertThat(column.length()).isEqualTo(53);
        assertThat(column.scale()).isEmpty();
        if (jdbcType == Types.DECIMAL) {
            final var rows = mock(ResultSet.class);
            final var value = new BigDecimal("12345678901234567890.123456789");
            when(rows.getObject(1)).thenReturn(value);
            assertThat(connection.getColumnValue(rows, 1, column, null)).isEqualTo(value);
            verify(rows, never()).getDouble(1);
        }
        when(storedType.next()).thenReturn(false);
        assertThatThrownBy(() -> connection.readTableColumn(metadata, new TableId(null, "APP", "NUMBERS"), null))
                .hasMessageContaining("Cannot resolve physical numeric type");
    }

    @Test
    void shouldRetainOracleDateTimeAndEraWhenTheDriverDisplaysOnlyTheDate() throws SQLException {
        final var connection = new DamengConnection(JdbcConfiguration.create().build());
        final var rows = mock(ResultSet.class);
        final var metadata = mock(ResultSetMetaData.class);
        when(rows.getMetaData()).thenReturn(metadata);
        when(metadata.getColumnType(1)).thenReturn(Types.DATE);
        final var column = Column.editor().name("D").jdbcType(Types.TIMESTAMP).type("TIMESTAMP").scale(0).create();
        final var stored = LocalDateTime.of(-1, 1, 1, 12, 34, 56);
        when(rows.getObject(1, LocalDateTime.class)).thenReturn(stored);
        assertThat(connection.getColumnValue(rows, 1, column, null)).isEqualTo(stored);
        verify(rows, never()).getString(1);
        when(rows.getObject(1, LocalDateTime.class)).thenReturn(null);
        assertThat(connection.getColumnValue(rows, 1, column, null)).isNull();
        when(metadata.getColumnType(1)).thenReturn(Types.TIMESTAMP);
        when(rows.getString(1)).thenReturn("-0001-01-01 12:34:56");
        assertThat(connection.getColumnValue(rows, 1, column, null)).isEqualTo(stored);
    }

    @Test
    void shouldCanonicalizeMetadataNamesAndPrimaryKeysWithoutChangingDefaults() throws SQLException {
        final var connection = spy(new DamengConnection(JdbcConfiguration.create().build()));
        doReturn("0").when(connection).getCaseSensitiveFlag();
        final var id = new TableId(null, "APP", "MIXED");
        assertThat(connection.createTableId("DAMENG", "app", "MiXeD")).isEqualTo(id);
        assertThat(connection.createTableId(null, null, "表")).isEqualTo(new TableId(null, null, "表"));
        final var metadata = mock(DatabaseMetaData.class);
        doReturn(List.of("Id")).when(connection).readPrimaryKeyNames(metadata, id);
        assertThat(connection.readPrimaryKeyOrUniqueIndexNames(metadata, id)).containsExactly("ID");
        final var column = mock(java.sql.ResultSet.class);
        when(column.getString(4)).thenReturn("MiXeD");
        when(column.getString(6)).thenReturn("VARCHAR");
        when(column.getInt(5)).thenReturn(java.sql.Types.VARCHAR);
        when(column.getString(13)).thenReturn("'aBc'");
        assertThat(connection.readTableColumn(column, id, (catalog, schema, table, name) -> name.equals("MIXED")))
                .hasValueSatisfying(editor -> {
                    assertThat(editor.name()).isEqualTo("MIXED");
                    assertThat(editor.create().defaultValueExpression()).contains("'aBc'");
                });
        assertThat(connection.readTableColumn(column, id, (catalog, schema, table, name) -> name.equals("OTHER"))).isEmpty();
        verify(connection, org.mockito.Mockito.times(1)).getCaseSensitiveFlag();
    }

    @Test
    void shouldFailMetadataDiscoveryWhenTheCaseFlagCannotBeRead() throws SQLException {
        final var connection = spy(new DamengConnection(JdbcConfiguration.create().build()));
        doThrow(new SQLException("unavailable")).when(connection).getCaseSensitiveFlag();
        assertThatThrownBy(() -> connection.createTableId(null, "APP", "T"))
                .hasMessageContaining("Cannot determine Dameng identifier case semantics");
    }

    @Test
    void shouldReadTheDatabaseVersionFromStandardJdbcMetadata() throws SQLException {
        final var connection = spy(new DamengConnection(JdbcConfiguration.create().build()));
        final var jdbc = mock(Connection.class);
        final var metadata = mock(DatabaseMetaData.class);
        doReturn(jdbc).when(connection).connection();
        when(jdbc.getMetaData()).thenReturn(metadata);
        when(metadata.getDatabaseProductName()).thenReturn("DM DBMS");
        when(metadata.getDatabaseProductVersion()).thenReturn("8.1.3.162");
        when(metadata.getDatabaseMajorVersion()).thenReturn(8);
        when(metadata.getDatabaseMinorVersion()).thenReturn(1);

        assertThat(connection.getDatabaseVersion()).isEqualTo(new DamengDatabaseVersion("DM DBMS", "8.1.3.162", 8, 1));
    }

    @Test
    void shouldUseOnlyDeclaredPrimaryKeysForSnapshotEventKeys() throws SQLException {
        final var connection = spy(new DamengConnection(JdbcConfiguration.create().build()));
        final var metadata = mock(DatabaseMetaData.class);
        final var tableId = new TableId(null, "APP", "WITHOUT_PK");
        doReturn(List.of()).when(connection).readPrimaryKeyNames(metadata, tableId);
        doReturn("1").when(connection).getCaseSensitiveFlag();
        assertThat(connection.readPrimaryKeyOrUniqueIndexNames(metadata, tableId)).isEmpty();
        verify(connection, never()).readTableUniqueIndices(metadata, tableId);

        doReturn(List.of("ID")).when(connection).readPrimaryKeyNames(metadata, tableId);
        assertThat(connection.readPrimaryKeyOrUniqueIndexNames(metadata, tableId)).containsExactly("ID");
    }

    @ParameterizedTest
    @ValueSource(strings = { "0", "2", "4", "7" })
    void shouldIgnoreVarcharLengthUnitInAllModes(String mode) throws SQLException {
        final var connection = validConnection();
        doReturn(mode).when(connection).getIniParameter("COMPATIBLE_MODE");
        doReturn("0").when(connection).getIniParameter("FLOAT_MODE");
        doReturn("0").when(connection).getIniParameter("ORA_DATE_FMT");
        doReturn("1").when(connection).getIniParameter("MY_STRICT_TABLES");
        final var initial = connection.getCompatibilityEnvironment();
        final var config = new DamengConnectorConfig(Configuration.create().with("topic.prefix", "dameng").build());
        final var offset = new DamengOffsetContext(config, 100);
        offset.validateCompatibilityEnvironment(initial);
        final var recovered = new DamengOffsetContext.Loader(config).load(offset.getOffset());
        for (var value : java.util.Arrays.asList(null, "", "0", "1", "2", "unknown")) {
            doReturn(value).when(connection).getIniParameter("LENGTH_IN_CHAR");
            final var observed = connection.getCompatibilityEnvironment();
            assertThat(observed.parameters()).doesNotContainKey("LENGTH_IN_CHAR");
            assertThat(observed).isEqualTo(initial);
            recovered.validateCompatibilityEnvironment(observed);
        }
        verify(connection, never()).getIniParameter("LENGTH_IN_CHAR");
    }

    @Test
    void shouldUseTheRegisteredProfileToConstructTheHistoricalDdlParser() throws SQLException {
        final var connection = validConnection();
        doReturn(StandardCharsets.UTF_8).when(connection).getDatabaseCharset();
        final var tables = new io.debezium.relational.Tables();
        connection.createDdlParser().parse("CREATE TABLE APP.T (D DATE, C CHAR(8), N NCHAR(8))", tables, "APP");
        final var table = tables.forTable(new TableId(null, "APP", "T"));
        assertThat(table.columnWithName("D").jdbcType()).isEqualTo(java.sql.Types.DATE);
        assertThat(table.columnWithName("C").typeExpression()).isEqualTo("CHAR(8 BYTE)");
        assertThat(table.columnWithName("N").typeExpression()).isEqualTo("NCHAR(8 BYTE)");
        assertThat(connection.getCompatibilityEnvironment().parameters()).doesNotContainKey("LENGTH_IN_CHAR");
    }

    @Test
    void shouldPropagateParameterQueryFailuresWithoutSubstitutingDefaults() throws SQLException {
        final var connection = validConnection();
        final var failure = new SQLException("permission denied", "28000");
        doThrow(failure).when(connection).getIniParameter("NUMBER_MODE");
        assertThatThrownBy(connection::getCompatibilityEnvironment).isSameAs(failure);
        doReturn("0").when(connection).getIniParameter("NUMBER_MODE");
        doReturn("0").when(connection).getIniParameter("BACKSLASH_ESCAPE");
        doReturn("0").when(connection).getIniParameter("NESTED_C_STYLE_COMMENT");
        doReturn(null).when(connection).getIniParameter("NLS_LENGTH_SEMANTICS");
        assertThat(connection.getCompatibilityEnvironment().parameters()).doesNotContainKey("NLS_LENGTH_SEMANTICS");
    }

    @Test
    void validatesRestartRetentionWithoutRequiringCurrentRedoToBeArchived() throws SQLException {
        final var connection = archiveConnection(List.of(log("a", 50, 100, 11, 0)));
        doReturn(200L).when(connection).currentLsn();
        final var config = new DamengConnectorConfig(Configuration.create().with("topic.prefix", "dameng").build());
        assertThat(connection.validateLogPosition(null, new DamengOffsetContext(config, 50), config)).isTrue();
        assertThat(connection.validateLogPosition(null, new DamengOffsetContext(config, 150), config)).isTrue();
        assertThat(connection.validateLogPosition(null, new DamengOffsetContext(config, 200), config)).isTrue();
        assertThat(connection.validateLogPosition(null, new DamengOffsetContext(config, 49), config)).isFalse();
        assertThat(connection.validateLogPosition(null, new DamengOffsetContext(config, 201), config)).isFalse();
        doReturn(List.of()).when(connection).readArchiveLogFiles();
        assertThat(connection.validateLogPosition(null, new DamengOffsetContext(config, 150), config)).isFalse();
    }

    private static ArchiveLogFile log(String name, long first, long last, long magic, long previous) {
        return new ArchiveLogFile(name, first, last, magic, previous);
    }

    private static DamengConnection archiveConnection(List<ArchiveLogFile> logs) throws SQLException {
        final var connection = spy(new DamengConnection(JdbcConfiguration.create().build()));
        doReturn(logs).when(connection).readArchiveLogFiles();
        doReturn(11L).when(connection).databaseMagic();
        return connection;
    }

    @ParameterizedTest
    @CsvSource({ "3,escaped", "4,raw" })
    void shouldObserveNativeBackslashSemanticsAndTrackThemInTheSignature(String length, String semantics) throws SQLException {
        final var connection = validConnection();
        doReturn("1").when(connection).getIniParameter("BACKSLASH_ESCAPE");
        final var rows = mock(ResultSet.class);
        when(rows.next()).thenReturn(true);
        when(rows.getString(1)).thenReturn(length);
        doAnswer(invocation -> {
            assertThat((String) invocation.getArgument(0)).isEqualTo("SELECT LENGTH('a\\nb') FROM DUAL");
            return ((io.debezium.jdbc.JdbcConnection.ResultSetMapper<?>) invocation.getArgument(1)).apply(rows);
        }).when(connection).queryAndMap(anyString(), any());
        final var environment = connection.getCompatibilityEnvironment();
        assertThat(environment.parameter("NATIVE_BACKSLASH_SEMANTICS")).isEqualTo(semantics);
        assertThat(environment.signature()).contains("NATIVE_BACKSLASH_SEMANTICS", semantics);
        when(rows.getString(1)).thenReturn("5");
        assertThatThrownBy(connection::getCompatibilityEnvironment).isInstanceOf(SQLException.class).hasMessageContaining("literal length was 5");
        when(rows.next()).thenReturn(false);
        assertThatThrownBy(connection::getCompatibilityEnvironment).isInstanceOf(SQLException.class).hasMessageContaining("Cannot determine");
    }

    @ParameterizedTest
    @CsvSource({ "6,decimal", "7,binary", "2,decimal", "3,decimal" })
    void shouldObserveNativeRealSemanticsWithoutChangingTheDatabase(int jdbcType, String semantics) throws SQLException {
        final var connection = validConnection();
        doReturn("1").when(connection).getIniParameter("NUMBER_MODE");
        final var rows = mock(ResultSet.class);
        final var metadata = mock(ResultSetMetaData.class);
        when(rows.getMetaData()).thenReturn(metadata);
        when(metadata.getColumnType(1)).thenReturn(jdbcType);
        doAnswer(invocation -> {
            assertThat((String) invocation.getArgument(0)).isEqualTo("SELECT CAST(NULL AS REAL) FROM DUAL");
            return ((io.debezium.jdbc.JdbcConnection.ResultSetMapper<?>) invocation.getArgument(1)).apply(rows);
        }).when(connection).queryAndMap(anyString(), any());
        final var environment = connection.getCompatibilityEnvironment();
        assertThat(environment.parameter("NATIVE_REAL_SEMANTICS")).isEqualTo(semantics);
        assertThat(environment.signature()).contains("NATIVE_REAL_SEMANTICS", semantics);
        when(metadata.getColumnType(1)).thenReturn(Types.VARCHAR);
        assertThatThrownBy(connection::getCompatibilityEnvironment).isInstanceOf(SQLException.class).hasMessageContaining("REAL semantics");
    }

    @ParameterizedTest
    @ValueSource(strings = { "0", "4", "7" })
    void shouldIgnoreOracleDateFormatOutsideOracleMode(String mode) throws SQLException {
        final var connection = validConnection();
        doReturn(mode).when(connection).getIniParameter("COMPATIBLE_MODE");
        doReturn("0").when(connection).getIniParameter("FLOAT_MODE");
        doReturn("1").when(connection).getIniParameter("MY_STRICT_TABLES");
        final var initial = connection.getCompatibilityEnvironment();
        final var config = new DamengConnectorConfig(Configuration.create().with("topic.prefix", "dameng").build());
        final var offset = new DamengOffsetContext(config, 100);
        offset.validateCompatibilityEnvironment(initial);
        final var recovered = new DamengOffsetContext.Loader(config).load(offset.getOffset());
        for (var value : java.util.Arrays.asList(null, "", "0", "1", "2", "unknown")) {
            doReturn(value).when(connection).getIniParameter("ORA_DATE_FMT");
            final var observed = connection.getCompatibilityEnvironment();
            assertThat(observed.parameters()).doesNotContainKey("ORA_DATE_FMT");
            assertThat(observed).isEqualTo(initial);
            recovered.validateCompatibilityEnvironment(observed);
        }
        verify(connection, never()).getIniParameter("ORA_DATE_FMT");
    }

    private static DamengConnection validConnection() throws SQLException {
        final var connection = spy(new DamengConnection(JdbcConfiguration.create().build()));
        doReturn(true).when(connection).isArchiveModeEnabled();
        doReturn("2").when(connection).getIniParameter("RLOG_APPEND_LOGIC");
        doReturn("1").when(connection).getIniParameter("RLOG_IGNORE_TABLE_SET");
        doReturn("0").when(connection).getIniParameter("RLOG_LLOG_COMPRESS");
        doReturn("0").when(connection).getIniParameter("COMPATIBLE_MODE");
        doReturn("0").when(connection).getIniParameter("NUMBER_MODE");
        doReturn("0").when(connection).getIniParameter("BACKSLASH_ESCAPE");
        doReturn("0").when(connection).getIniParameter("NESTED_C_STYLE_COMMENT");
        doReturn("BYTE").when(connection).getIniParameter("NLS_LENGTH_SEMANTICS");
        doReturn("0").when(connection).getIniParameter("NVARCHAR_LENGTH_IN_CHAR");
        doReturn(null).when(connection).getIniParameter("IDENTIFIER_MODE");
        doReturn("1").when(connection).getCaseSensitiveFlag();
        return connection;
    }
}
