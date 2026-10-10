/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import io.debezium.connector.dameng.DamengConnection.ArchiveLogFile;
import io.debezium.connector.dameng.logminer.LogMinerSql;
import io.debezium.jdbc.JdbcConfiguration;

class DamengLogMinerSessionTest {
    @Test
    void shouldEndThePreviousMiningSessionBeforeAddingTheNextWindow() throws Exception {
        final var connection = spy(new DamengConnection(JdbcConfiguration.create().build()));
        final var sql = new ArrayList<String>();
        doReturn(List.of(new ArchiveLogFile("first.arc", 1, 100, 11, 0))).when(connection).archiveLogsForRange(anyLong(), anyLong());
        doAnswer(invocation -> {
            sql.add(invocation.getArgument(0, String.class));
            return connection;
        }).when(connection).execute(anyString());

        connection.endLogMining();
        assertThat(sql).isEmpty();

        connection.startLogMining(10, 50, false);
        connection.startLogMining(20, 60, true);
        connection.endLogMining();
        connection.endLogMining();

        assertThat(sql).containsExactly(
                LogMinerSql.addLogFile("first.arc"), LogMinerSql.start(10, 50, false), LogMinerSql.end(),
                LogMinerSql.addLogFile("first.arc"), LogMinerSql.start(20, 60, true), LogMinerSql.end());
    }

    @Test
    void shouldKeepSessionInactiveWhenTheFirstWindowCannotStart() throws Exception {
        final var connection = spy(new DamengConnection(JdbcConfiguration.create().build()));
        final var sql = new ArrayList<String>();
        doReturn(List.of(new ArchiveLogFile("first.arc", 1, 100, 11, 0))).when(connection).archiveLogsForRange(anyLong(), anyLong());
        doAnswer(invocation -> {
            final var statement = invocation.getArgument(0, String.class);
            sql.add(statement);
            if (statement.equals(LogMinerSql.start(10, 50, false))) {
                throw new SQLException("start failed");
            }
            return connection;
        }).when(connection).execute(anyString());

        assertThatThrownBy(() -> connection.startLogMining(10, 50, false)).isInstanceOf(SQLException.class).hasMessage("start failed");
        connection.endLogMining();

        assertThat(sql).containsExactly(LogMinerSql.addLogFile("first.arc"), LogMinerSql.start(10, 50, false));
    }
}
