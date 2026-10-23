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
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.charset.Charset;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.time.LocalDate;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import io.debezium.DebeziumException;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.Column;

class DamengConnectionResultValidationTest {
    @ParameterizedTest
    @ValueSource(strings = { "missing", "null", "zero", "multiple" })
    void shouldRejectUnavailableStandbySourceIdentity(String fault) throws Exception {
        final var result = progressResult(10);
        switch (fault) {
            case "missing" -> when(result.next()).thenReturn(false);
            case "null" -> when(result.wasNull()).thenReturn(true);
            case "zero" -> when(result.getLong(1)).thenReturn(0L);
            case "multiple" -> when(result.next()).thenReturn(true, true, false);
        }
        final var connection = connectionWithResult("SELECT PRIMARY_DB_MAGIC FROM V$RLOG", result);
        doReturn(true).when(connection).isStandbyMining();
        doReturn(20L).when(connection).databaseMagic();
        doNothing().when(connection).validateServerRole();
        assertThatThrownBy(connection::readPrimaryStandbyIdentity).isInstanceOf(SQLException.class).hasMessageContaining("source primary identity")
                .satisfies(error -> assertThat(((SQLException) error).getSQLState()).isEqualTo("08006"));
    }

    @ParameterizedTest
    @CsvSource({ "SLSN,missing", "SLSN,null", "SLSN,negative", "SLSN,multiple", "RPKG_LSN,missing", "RPKG_LSN,null",
            "RPKG_LSN,negative", "RPKG_LSN,multiple" })
    void shouldRejectUnavailableOrAmbiguousStandbyProgress(String field, String fault) throws Exception {
        final var replayable = progressResult(200);
        final var replayed = progressResult(150);
        final var invalid = field.equals("SLSN") ? replayable : replayed;
        switch (fault) {
            case "missing" -> when(invalid.next()).thenReturn(false);
            case "null" -> when(invalid.wasNull()).thenReturn(true);
            case "negative" -> when(invalid.getLong(1)).thenReturn(-1L);
            case "multiple" -> when(invalid.next()).thenReturn(true, true, false);
        }
        final var connection = standbyProgressConnection(replayable, replayed);
        assertThatThrownBy(connection::readStandbyProgress).isInstanceOf(SQLException.class).hasMessageContaining(field)
                .satisfies(error -> assertThat(((SQLException) error).getSQLState()).isEqualTo("08006"));
    }

    @Test
    void shouldUseTheCompletedReplayBoundaryAndRecheckRole() throws Exception {
        final var connection = standbyProgressConnection(progressResult(200), progressResult(150));
        assertThat(connection.readStandbyProgress()).isEqualTo(new DamengConnection.StandbyProgress(200, 150));
        verify(connection, times(2)).validateServerRole();
    }

    private DamengConnection standbyProgressConnection(ResultSet replayable, ResultSet replayed) throws SQLException {
        final var connection = connectionWithResult("SELECT SLSN FROM V$RAPPLY_SYS", replayable);
        doNothing().when(connection).validateServerRole();
        doAnswer(invocation -> invocation.<JdbcConnection.ResultSetMapper<Object>> getArgument(1).apply(replayed))
                .when(connection).queryAndMap(eq("SELECT RPKG_LSN FROM V$RAPPLY_PARALLEL_INFO WHERE DSC_SEQNO=0"), any());
        return connection;
    }

    private ResultSet progressResult(long lsn) throws SQLException {
        final var result = mock(ResultSet.class);
        when(result.next()).thenReturn(true, false);
        when(result.getLong(1)).thenReturn(lsn);
        return result;
    }

    @ParameterizedTest
    @CsvSource({ "-4712-01-01,-4712", "0001-01-01,1", "1582-10-10,1582", "9999-12-31,9999" })
    void shouldReadTheStoredCalendarWithoutTheDriversHybridDateConversion(String stored, int year) throws Exception {
        final var result = mock(ResultSet.class);
        final var connection = new DamengConnection(JdbcConfiguration.create().build());
        final var date = Column.editor().name("D").jdbcType(Types.DATE).create();
        final var timestamp = Column.editor().name("T").jdbcType(Types.TIMESTAMP).create();
        when(result.getString(1)).thenReturn(stored);
        final var parsedDate = (LocalDate) connection.getColumnValue(result, 1, date, null);
        assertThat(parsedDate).isEqualTo(LocalDate.parse(stored));
        assertThat(parsedDate.getYear()).isEqualTo(year);
        when(result.getString(1)).thenReturn(stored + " 12:34:56.123456");
        assertThat(connection.getColumnValue(result, 1, timestamp, null)).isEqualTo(parsedDate.atTime(12, 34, 56, 123456000));
        when(result.getString(1)).thenReturn(null);
        assertThat(connection.getColumnValue(result, 1, date, null)).isNull();
        assertThat(connection.getColumnValue(result, 1, timestamp, null)).isNull();
        assertThat(DamengTemporalValues.offsetTimestamp(null)).isNull();
    }

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
