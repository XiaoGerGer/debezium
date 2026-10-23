/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.List;

import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.dameng.DamengConnection.ArchiveLogFile;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.relational.TableId;

class DamengConnectionTest {
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
        when(metadata.getDatabaseProductVersion()).thenReturn("8.1.2.128");
        when(metadata.getDatabaseMajorVersion()).thenReturn(8);
        when(metadata.getDatabaseMinorVersion()).thenReturn(1);

        assertThat(connection.getDatabaseVersion()).isEqualTo(new DamengDatabaseVersion("DM DBMS", "8.1.2.128", 8, 1));
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

    @Test
    void acceptsOlderServerWithoutCompressionParameter() throws SQLException {
        final var connection = validConnection();
        doReturn(null).when(connection).getIniParameter("RLOG_LLOG_COMPRESS");
        connection.validateLogMiningConfiguration();
    }

    @Test
    void requiresCompleteLogicalRecords() throws SQLException {
        final var connection = validConnection();
        for (String value : List.of("0", "1", "3", "4")) {
            doReturn(value).when(connection).getIniParameter("RLOG_APPEND_LOGIC");
            assertThatThrownBy(connection::validateLogMiningConfiguration).hasMessageContaining("RLOG_APPEND_LOGIC");
        }
    }

    @Test
    void acceptsManualTableLoggingButRejectsUnknownModesAndCompressedInsertLogs() throws SQLException {
        final var connection = validConnection();
        doReturn("0").when(connection).getIniParameter("RLOG_IGNORE_TABLE_SET");
        connection.validateLogMiningConfiguration();
        doReturn("2").when(connection).getIniParameter("RLOG_IGNORE_TABLE_SET");
        assertThatThrownBy(connection::validateLogMiningConfiguration).hasMessageContaining("RLOG_IGNORE_TABLE_SET");
        doReturn("1").when(connection).getIniParameter("RLOG_IGNORE_TABLE_SET");
        doReturn("1").when(connection).getIniParameter("RLOG_LLOG_COMPRESS");
        assertThatThrownBy(connection::validateLogMiningConfiguration).hasMessageContaining("RLOG_LLOG_COMPRESS");
    }

    @Test
    void rejectsIncompatibleDatabaseTypeSemantics() throws SQLException {
        final var connection = validConnection();
        doReturn("2").when(connection).getIniParameter("COMPATIBLE_MODE");
        assertThatThrownBy(connection::validateLogMiningConfiguration).hasMessageContaining("COMPATIBLE_MODE");
    }

    @Test
    void rejectsUnverifiedOrUnknownCharacterLengthSemantics() throws SQLException {
        final var connection = validConnection();
        connection.validateCompatibilityMode();
        doReturn("1").when(connection).getIniParameter("LENGTH_IN_CHAR");
        assertThatThrownBy(connection::validateCompatibilityMode).hasMessageContaining("LENGTH_IN_CHAR=0", "byte length semantics");
        doReturn(null).when(connection).getIniParameter("LENGTH_IN_CHAR");
        assertThatThrownBy(connection::validateCompatibilityMode).hasMessageContaining("LENGTH_IN_CHAR=0");
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
        assertThat(connection.getCompatibilityEnvironment().parameters()).containsEntry("LENGTH_IN_CHAR", "0");
    }

    @Test
    void shouldPropagateParameterQueryFailuresWithoutSubstitutingDefaults() throws SQLException {
        final var connection = validConnection();
        final var failure = new SQLException("permission denied", "28000");
        doThrow(failure).when(connection).getIniParameter("LENGTH_IN_CHAR");
        assertThatThrownBy(connection::getCompatibilityEnvironment).isSameAs(failure);
        doReturn("0").when(connection).getIniParameter("LENGTH_IN_CHAR");
        doReturn("0").when(connection).getIniParameter("BACKSLASH_ESCAPE");
        doReturn("0").when(connection).getIniParameter("NESTED_C_STYLE_COMMENT");
        doReturn(null).when(connection).getIniParameter("NLS_LENGTH_SEMANTICS");
        assertThat(connection.getCompatibilityEnvironment().parameters()).doesNotContainKey("NLS_LENGTH_SEMANTICS");
    }

    @Test
    void rejectsMissingArchiveAtRestartBoundary() throws SQLException {
        final var connection = archiveConnection(List.of(log("a", 101, 200, 11, 0)));
        assertThatThrownBy(() -> connection.archiveLogsForRange(100, 200)).hasMessageContaining("restart LSN");
    }

    @Test
    void rejectsInteriorGapAndMixedDatabaseArchives() throws SQLException {
        final var connection = archiveConnection(List.of(log("a", 1, 100, 11, 0), log("c", 201, 300, 11, 200)));
        assertThatThrownBy(() -> connection.archiveLogsForRange(50, 250)).hasMessageContaining("gap");
        doReturn(List.of(log("a", 1, 300, 11, 0), log("foreign", 101, 200, 22, 100))).when(connection).readArchiveLogFiles();
        assertThatThrownBy(() -> connection.archiveLogsForRange(50, 250)).hasMessageContaining("DB_MAGIC");
    }

    @Test
    void acceptsAdjacentBoundariesAndExplicitPredecessorChain() throws SQLException {
        final var adjacent = archiveConnection(List.of(log("a", 1, 100, 11, 0), log("b", 101, 200, 11, 0)));
        assertThat(adjacent.archiveLogsForRange(50, 200)).hasSize(2);
        final var chain = archiveConnection(List.of(log("a", 1, 100, 11, 0), log("b", 110, 200, 11, 100)));
        assertThat(chain.archiveLogsForRange(50, 200)).hasSize(2);
    }

    @Test
    void rejectsArchiveFromAnotherDatabaseGeneration() throws SQLException {
        final var connection = archiveConnection(List.of(log("a", 1, 200, 22, 0)));
        assertThatThrownBy(() -> connection.archiveLogsForRange(50, 100)).hasMessageContaining("DB_MAGIC");
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

    private static DamengConnection validConnection() throws SQLException {
        final var connection = spy(new DamengConnection(JdbcConfiguration.create().build()));
        doReturn(true).when(connection).isArchiveModeEnabled();
        doReturn("2").when(connection).getIniParameter("RLOG_APPEND_LOGIC");
        doReturn("1").when(connection).getIniParameter("RLOG_IGNORE_TABLE_SET");
        doReturn("0").when(connection).getIniParameter("RLOG_LLOG_COMPRESS");
        doReturn("0").when(connection).getIniParameter("COMPATIBLE_MODE");
        doReturn("0").when(connection).getIniParameter("LENGTH_IN_CHAR");
        doReturn("0").when(connection).getIniParameter("BACKSLASH_ESCAPE");
        doReturn("0").when(connection).getIniParameter("NESTED_C_STYLE_COMMENT");
        doReturn("BYTE").when(connection).getIniParameter("NLS_LENGTH_SEMANTICS");
        doReturn("0").when(connection).getIniParameter("NVARCHAR_LENGTH_IN_CHAR");
        doReturn(null).when(connection).getIniParameter("IDENTIFIER_MODE");
        doReturn("1").when(connection).getCaseSensitiveFlag();
        return connection;
    }
}
