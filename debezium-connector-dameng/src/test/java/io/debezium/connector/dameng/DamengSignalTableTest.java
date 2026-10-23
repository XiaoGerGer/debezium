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
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.TableId;

class DamengSignalTableTest {
    private static final TableId TABLE = new TableId(null, "APP", "DBZ_SIGNAL");
    private DamengConnection connection;
    private ResultSetMetaData columns;

    @BeforeEach
    void before() throws Exception {
        connection = spy(new DamengConnection(JdbcConfiguration.create().build()));
        doReturn("1").when(connection).getCaseSensitiveFlag();
        columns = mock(ResultSetMetaData.class);
        final var result = mock(ResultSet.class);
        when(result.getMetaData()).thenReturn(columns);
        when(columns.getColumnCount()).thenReturn(3);
        final String[] names = { "ID", "TYPE", "DATA" };
        for (int index = 1; index <= names.length; index++) {
            when(columns.getColumnName(index)).thenReturn(names[index - 1]);
            when(columns.getColumnType(index)).thenReturn(Types.VARCHAR);
        }
        doReturn(Set.of(TABLE)).when(connection).readTableNames(eq(null), anyString(), anyString(), any(String[].class));
        doReturn(connection).when(connection).execute(anyString());
        doAnswer(invocation -> {
            invocation.<JdbcConnection.ResultSetConsumer> getArgument(1).accept(result);
            return connection;
        }).when(connection).query(anyString(), any(JdbcConnection.ResultSetConsumer.class));
    }

    @Test
    void shouldReuseMixedCaseSignalColumnsInACaseInsensitiveDatabase() throws Exception {
        doReturn("0").when(connection).getCaseSensitiveFlag();
        when(columns.getColumnName(1)).thenReturn("id");
        when(columns.getColumnName(2)).thenReturn("Type");
        when(columns.getColumnName(3)).thenReturn("dAtA");
        assertThat(connection.ensureSignalTable(new TableId(null, "app", "Dbz_Signal"))).isFalse();
        verify(connection, never()).execute(anyString());
    }

    @Test
    void shouldCreateAMissingSignalTable() throws Exception {
        tables(Set.of());
        assertThat(connection.ensureSignalTable(TABLE)).isTrue();
        verify(connection).execute("CREATE TABLE \"APP\".\"DBZ_SIGNAL\""
                + " (ID VARCHAR(64) PRIMARY KEY, TYPE VARCHAR(64) NOT NULL, DATA VARCHAR(2000))");
        verify(connection).query(eq("SELECT * FROM \"APP\".\"DBZ_SIGNAL\" WHERE 1=0"), any(JdbcConnection.ResultSetConsumer.class));
    }

    @Test
    void shouldReuseAnExistingTableWithoutDdl() throws Exception {
        when(columns.getColumnType(3)).thenReturn(Types.NVARCHAR);
        assertThat(connection.ensureSignalTable(TABLE)).isFalse();
        verify(connection, never()).execute(anyString());
    }

    @Test
    void shouldNotMistakeAPatternMatchForTheConfiguredTable() throws Exception {
        tables(Set.of(new TableId(null, "APP", "DBZXSIGNAL")));
        assertThat(connection.ensureSignalTable(TABLE)).isTrue();
        verify(connection).execute(anyString());
    }

    @Test
    void shouldQuoteSchemaAndTableNamesInDdlAndValidation() throws Exception {
        tables(Set.of());
        final var quoted = new TableId(null, "App\"Schema", "Signal\"Table\"");
        assertThat(connection.ensureSignalTable(quoted)).isTrue();
        verify(connection).execute("CREATE TABLE \"App\"\"Schema\".\"Signal\"\"Table\"\"\""
                + " (ID VARCHAR(64) PRIMARY KEY, TYPE VARCHAR(64) NOT NULL, DATA VARCHAR(2000))");
        verify(connection).query(eq("SELECT * FROM \"App\"\"Schema\".\"Signal\"\"Table\"\"\" WHERE 1=0"),
                any(JdbcConnection.ResultSetConsumer.class));
    }

    @Test
    void shouldAcceptAValidTableCreatedByAnotherTask() throws Exception {
        doReturn(Set.of(), Set.of(TABLE)).when(connection)
                .readTableNames(eq(null), anyString(), anyString(), any(String[].class));
        doThrow(new SQLException("object already exists", "42000")).when(connection).execute(anyString());
        assertThat(connection.ensureSignalTable(TABLE)).isFalse();
    }

    @Test
    void shouldRejectAnIncompatibleTableCreatedByAnotherTask() throws Exception {
        doReturn(Set.of(), Set.of(TABLE)).when(connection)
                .readTableNames(eq(null), anyString(), anyString(), any(String[].class));
        final var failure = new SQLException("object already exists", "42000");
        doThrow(failure).when(connection).execute(anyString());
        when(columns.getColumnCount()).thenReturn(4);
        assertThatThrownBy(() -> connection.ensureSignalTable(TABLE))
                .isInstanceOf(SQLException.class).hasCause(failure)
                .satisfies(error -> assertThat(error.getCause().getSuppressed())
                        .singleElement().satisfies(suppressed -> assertThat(suppressed).hasMessageContaining("exactly three columns")));
    }

    @Test
    void shouldExplainCreationPermissionsAndPreserveTheSqlFailure() throws Exception {
        tables(Set.of());
        final var failure = new SQLException("permission denied", "42000", -551);
        doThrow(failure).when(connection).execute(anyString());
        assertThatThrownBy(() -> connection.ensureSignalTable(TABLE))
                .isInstanceOf(SQLException.class).hasCause(failure)
                .hasMessageContaining("APP.DBZ_SIGNAL", "CREATE TABLE permission", "pre-create", "not visible")
                .satisfies(error -> {
                    assertThat(((SQLException) error).getSQLState()).isEqualTo("42000");
                    assertThat(((SQLException) error).getErrorCode()).isEqualTo(-551);
                });
    }

    @Test
    void shouldNotCreateATableWhenMetadataCannotBeRead() throws Exception {
        final var failure = new SQLException("connection lost", "08006");
        doThrow(failure).when(connection).readTableNames(eq(null), anyString(), anyString(), any(String[].class));
        assertThatThrownBy(() -> connection.ensureSignalTable(TABLE)).isSameAs(failure);
        verify(connection, never()).execute(anyString());
    }

    @Test
    void shouldFailWithoutDdlWhenAnExistingTableCannotBeRead() throws Exception {
        final var failure = new SQLException("SELECT denied", "42000");
        doThrow(failure).when(connection).query(anyString(), any(JdbcConnection.ResultSetConsumer.class));
        assertThatThrownBy(() -> connection.ensureSignalTable(TABLE)).isSameAs(failure);
        verify(connection, never()).execute(anyString());
    }

    @ParameterizedTest
    @ValueSource(ints = { 0, 2, 4 })
    void shouldRejectExtraOrMissingColumnsWithoutAlteringTheTable(int count) throws Exception {
        when(columns.getColumnCount()).thenReturn(count);
        assertThatThrownBy(() -> connection.ensureSignalTable(TABLE))
                .hasMessageContaining("exactly three columns", "Existing tables are not altered");
        verify(connection, never()).execute(anyString());
    }

    @Test
    void shouldRejectColumnsInTheWrongOrder() throws Exception {
        when(columns.getColumnName(1)).thenReturn("TYPE");
        when(columns.getColumnName(2)).thenReturn("ID");
        assertThatThrownBy(() -> connection.ensureSignalTable(TABLE)).hasMessageContaining("column 1 must be named ID");
        verify(connection, never()).execute(anyString());
    }

    @ParameterizedTest
    @ValueSource(ints = { Types.INTEGER, Types.CLOB, Types.CHAR })
    void shouldRejectTypesThatCannotCarryVariableLengthSignalStrings(int type) throws Exception {
        when(columns.getColumnType(3)).thenReturn(type);
        assertThatThrownBy(() -> connection.ensureSignalTable(TABLE)).hasMessageContaining("column 3", "VARCHAR or NVARCHAR");
        verify(connection, never()).execute(anyString());
    }

    private void tables(Set<TableId> tables) throws SQLException {
        doReturn(tables).when(connection).readTableNames(eq(null), anyString(), anyString(), any(String[].class));
    }
}
