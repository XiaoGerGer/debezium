/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.charset.Charset;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import io.debezium.DebeziumException;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.jdbc.JdbcConnection;

class DamengConnectionResultValidationTest {
    @Test
    void shouldRejectMissingOrNullCurrentLsn() throws Exception {
        final var absent = mock(ResultSet.class);
        assertThatThrownBy(connectionWithResult("SELECT CUR_LSN FROM V$RLOG", absent)::currentLsn)
                .isInstanceOf(SQLException.class).hasMessageContaining("current LSN");

        final var nullLsn = mock(ResultSet.class);
        when(nullLsn.next()).thenReturn(true);
        when(nullLsn.wasNull()).thenReturn(true);
        assertThatThrownBy(connectionWithResult("SELECT CUR_LSN FROM V$RLOG", nullLsn)::currentLsn)
                .isInstanceOf(SQLException.class).hasMessageContaining("CUR_LSN is null");
    }

    @Test
    void shouldRejectMissingOrNullDatabaseMagic() throws Exception {
        final var absent = mock(ResultSet.class);
        assertThatThrownBy(connectionWithResult("SELECT DB_MAGIC FROM V$RLOG", absent)::databaseMagic)
                .isInstanceOf(SQLException.class).hasMessageContaining("database identity");

        final var nullMagic = mock(ResultSet.class);
        when(nullMagic.next()).thenReturn(true);
        when(nullMagic.wasNull()).thenReturn(true);
        assertThatThrownBy(connectionWithResult("SELECT DB_MAGIC FROM V$RLOG", nullMagic)::databaseMagic)
                .isInstanceOf(SQLException.class).hasMessageContaining("DB_MAGIC is null");
    }

    @ParameterizedTest
    @CsvSource({ "0,GB18030", "1,UTF-8", "2,EUC-KR" })
    void shouldResolveAndCacheEverySupportedDatabaseCharset(int flag, String expected) throws Exception {
        final var result = mock(ResultSet.class);
        when(result.next()).thenReturn(true);
        when(result.getInt(1)).thenReturn(flag);
        final var connection = connectionWithResult("SELECT SF_GET_UNICODE_FLAG()", result);

        assertThat(connection.getDatabaseCharset()).isEqualTo(Charset.forName(expected));
        assertThat(connection.getDatabaseCharset()).isEqualTo(Charset.forName(expected));
        verify(connection, times(1)).queryAndMap(eq("SELECT SF_GET_UNICODE_FLAG()"), any());
    }

    @Test
    void shouldRejectMissingOrUnknownDatabaseCharsetFlags() throws Exception {
        final var absent = mock(ResultSet.class);
        assertThatThrownBy(connectionWithResult("SELECT SF_GET_UNICODE_FLAG()", absent)::getDatabaseCharset)
                .isInstanceOf(SQLException.class).hasMessageContaining("character set");

        final var unknown = mock(ResultSet.class);
        when(unknown.next()).thenReturn(true);
        when(unknown.getInt(1)).thenReturn(99);
        assertThatThrownBy(connectionWithResult("SELECT SF_GET_UNICODE_FLAG()", unknown)::getDatabaseCharset)
                .isInstanceOf(SQLException.class).hasMessageContaining("flag 99");
    }

    @Test
    void shouldPreserveTheSqlFailureWhenCheckingAStoredRestartPosition() throws Exception {
        final var config = new DamengConnectorConfig(DamengConnectorConfigTest.configuration().build());
        final var connection = spy(new DamengConnection(JdbcConfiguration.create().build()));
        final var failure = new SQLException("V$RLOG temporarily unavailable", "08006");
        doThrow(failure).when(connection).currentLsn();

        assertThatThrownBy(() -> connection.validateLogPosition(null, new DamengOffsetContext(config, 100), config))
                .isInstanceOf(DebeziumException.class).hasCause(failure);
    }

    private DamengConnection connectionWithResult(String sql, ResultSet result) throws SQLException {
        final var connection = spy(new DamengConnection(JdbcConfiguration.create().build()));
        doAnswer(invocation -> invocation.<JdbcConnection.ResultSetMapper<Object>> getArgument(1).apply(result))
                .when(connection).queryAndMap(eq(sql), any());
        return connection;
    }
}
