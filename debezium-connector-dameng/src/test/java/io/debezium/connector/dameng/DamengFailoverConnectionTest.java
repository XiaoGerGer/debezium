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
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.connect.errors.ConnectException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.ArgumentCaptor;

import io.debezium.jdbc.JdbcConnection;

import dm.jdbc.driver.DmDriver;

class DamengFailoverConnectionTest {
    @ParameterizedTest
    @CsvSource({ "20,30,changed while validating", "30,30,has not replayed the current primary-open record" })
    void shouldRetryMixedSourceSamplingAndUnreplayedPrimaryHistory(long initial, long latest, String reason) throws Exception {
        try (var connection = connectionWithNewPrimaryHistory()) {
            doReturn(true).when(connection).isStandbyMining();
            doNothing().when(connection).validateServerRole();
            doReturn(new DamengConnection.StandbyProgress(300, 300)).when(connection).readStandbyProgress();
            final var calls = new AtomicInteger();
            doAnswer(invocation -> {
                final var result = mock(ResultSet.class);
                when(result.next()).thenReturn(true, false);
                when(result.getLong(1)).thenReturn(calls.getAndIncrement() == 0 ? initial : latest);
                return invocation.<JdbcConnection.ResultSetMapper<Object>> getArgument(1).apply(result);
            }).when(connection).queryAndMap(eq("SELECT PRIMARY_DB_MAGIC FROM V$RLOG"), any());
            assertThatThrownBy(connection::readPrimaryStandbyIdentity).isInstanceOf(SQLException.class).hasMessageContaining(reason)
                    .satisfies(error -> assertThat(((SQLException) error).getSQLState()).isEqualTo("08006"));
        }
    }

    @Test
    void shouldWaitForReplicaReplayBeforeResumingAnAcknowledgedOffset() throws Exception {
        final var config = new DamengConnectorConfig(DamengConnectorConfigTest.configuration().with("database.standby.hosts", "replica:5236").build());
        try (var connection = spy(new DamengConnection(config.getJdbcConfig()))) {
            final var history = List.of(new DamengDatabaseIdentity.OpenRecord("open", Instant.EPOCH, 10, 0));
            final var ready = new DamengDatabaseIdentity(1, 10, 200, history, 20);
            final var offset = new DamengOffsetContext(config, 150);
            offset.validatePrimaryStandbyIdentity(ready);
            doReturn(new DamengDatabaseIdentity(1, 10, 120, history, 20)).when(connection).readPrimaryStandbyIdentity();
            assertThatThrownBy(() -> connection.validatePrimaryStandbyIdentity(offset)).isInstanceOf(SQLException.class)
                    .hasMessageContaining("has not replayed through the stored offset")
                    .satisfies(error -> assertThat(((SQLException) error).getSQLState()).isEqualTo("08006"));
            doReturn(ready).when(connection).readPrimaryStandbyIdentity();
            connection.validatePrimaryStandbyIdentity(offset);
            doNothing().when(connection).validateServerRole();
            doReturn(new DamengDatabaseIdentity(1, 10, 200, history, 30)).when(connection).readPrimaryStandbyIdentity();
            assertThatThrownBy(connection::validateUnchangedPrimary).isInstanceOf(SQLException.class).hasMessageContaining("changed while reading");
        }
    }

    @Test
    void shouldRouteCaptureOnlyToStandbyWithoutPassingInternalFlagsToDriver() throws Exception {
        final var config = new DamengConnectorConfig(DamengConnectorConfigTest.configuration()
                .with("database.standby.hosts", "a:5236,b:5237").build());
        final var jdbc = mock(Connection.class);
        final var statement = mock(Statement.class);
        when(jdbc.createStatement()).thenReturn(statement);
        try (var drivers = mockConstruction(DmDriver.class, (driver, context) -> when(driver.connect(anyString(), any())).thenReturn(jdbc));
                var connection = new DamengConnection(config.getJdbcConfig())) {
            assertThat(connection.connection()).isSameAs(jdbc);
            verify(statement).execute("SET TIME ZONE '+00:00'");
            final var properties = ArgumentCaptor.forClass(Properties.class);
            verify(drivers.constructed().get(0)).connect(eq("jdbc:dm://debezium?debezium=(a:5236,b:5237)"), properties.capture());
            assertThat(properties.getValue()).containsEntry("loginMode", "2").containsEntry("loginStatus", "4")
                    .containsEntry("autoReconnect", "0").containsEntry("rwSeparate", "0").doesNotContainKeys("standby.hosts", "failover.hosts");
        }
    }

    @ParameterizedTest
    @CsvSource({ "STANDBY,OPEN,true", "PRIMARY,OPEN,false", "NORMAL,OPEN,false", "STANDBY,MOUNT,false", "STANDBY,SUSPEND,false" })
    void shouldAcceptOnlyOpenStandbyForCapture(String role, String status, boolean accepted) throws Exception {
        try (var connection = connectionWithRole(false, role, status)) {
            doReturn(true).when(connection).isStandbyMining();
            if (accepted) {
                connection.validateServerRole();
            }
            else {
                assertThatThrownBy(connection::validateServerRole).isInstanceOf(SQLException.class).hasMessageContaining("OPEN STANDBY")
                        .satisfies(error -> assertThat(((SQLException) error).getSQLState()).isEqualTo("08006"));
            }
        }
    }

    @Test
    void shouldPassTheControlledServiceGroupAndRoutingPropertiesToTheDriver() throws Exception {
        final var config = new DamengConnectorConfig(DamengConnectorConfigTest.configuration()
                .with("database.failover.hosts", "primary:5236,standby:5237").build());
        final var jdbc = mock(Connection.class);
        final var statement = mock(Statement.class);
        when(jdbc.createStatement()).thenReturn(statement);
        try (var drivers = mockConstruction(DmDriver.class, (driver, context) -> when(driver.connect(anyString(), any())).thenReturn(jdbc));
                var connection = new DamengConnection(config.getJdbcConfig())) {
            assertThat(connection.connection()).isSameAs(jdbc);
            verify(statement).execute("SET TIME ZONE '+00:00'");
            final var properties = ArgumentCaptor.forClass(Properties.class);
            verify(drivers.constructed().get(0)).connect(eq("jdbc:dm://debezium?debezium=(primary:5236,standby:5237)"), properties.capture());
            assertThat(properties.getValue()).doesNotContainKeys("hostname", "host", "port", "failover.hosts")
                    .containsEntry("loginMode", "1").containsEntry("loginStatus", "4")
                    .containsEntry("autoReconnect", "0").containsEntry("rwSeparate", "0")
                    .containsEntry("switchTimes", "1").containsEntry("switchInterval", "200");
        }
    }

    @Test
    void shouldPreserveSingleEndpointUrlsWithoutTransparentReconnection() throws Exception {
        final var config = new DamengConnectorConfig(DamengConnectorConfigTest.configuration().with("database.port", 5327).build());
        final var jdbc = mock(Connection.class);
        final var statement = mock(Statement.class);
        when(jdbc.createStatement()).thenReturn(statement);
        try (var drivers = mockConstruction(DmDriver.class, (driver, context) -> when(driver.connect(anyString(), any())).thenReturn(jdbc));
                var connection = new DamengConnection(config.getJdbcConfig())) {
            assertThat(connection.connection()).isSameAs(jdbc);
            verify(statement).execute("SET TIME ZONE '+00:00'");
            final var properties = ArgumentCaptor.forClass(Properties.class);
            verify(drivers.constructed().get(0)).connect(eq("jdbc:dm://localhost:5327"), properties.capture());
            assertThat(properties.getValue()).containsEntry("autoReconnect", "0").containsEntry("rwSeparate", "0")
                    .doesNotContainKeys("failover.hosts", "loginMode");
        }
    }

    @ParameterizedTest
    @CsvSource({ "false,NORMAL,OPEN", "false,PRIMARY,OPEN", "true,PRIMARY,OPEN" })
    void shouldAcceptOnlyWritableSupportedRoles(boolean failover, String role, String status) throws Exception {
        try (var connection = connectionWithRole(failover, role, status)) {
            connection.validateServerRole();
        }
    }

    @ParameterizedTest
    @CsvSource({ "false,STANDBY,OPEN", "true,STANDBY,OPEN", "true,NORMAL,OPEN", "true,PRIMARY,MOUNT", "false,NORMAL,SUSPEND",
            "true,PRIMARY,''", "false,'',OPEN" })
    void shouldTriggerTaskReconnectWhenTheSessionIsDemotedOrNotOpen(boolean failover, String role, String status) throws Exception {
        try (var connection = connectionWithRole(failover, role, status)) {
            assertThatThrownBy(connection::validateServerRole).isInstanceOf(SQLException.class).hasMessageContaining("MODE$=" + role)
                    .satisfies(error -> assertThat(((SQLException) error).getSQLState()).isEqualTo("08006"));
        }
    }

    @Test
    void shouldRetryWhenPrimaryDemotionRacesWithHistoryRead() throws Exception {
        try (var connection = connectionWithNewPrimaryHistory()) {
            doNothing().doThrow(new SQLException("Former primary was demoted", "08006")).when(connection).validateServerRole();
            final var config = new DamengConnectorConfig(DamengConnectorConfigTest.configuration()
                    .with("database.failover.hosts", "primary:5236,standby:5236").build());
            final var offset = new DamengOffsetContext(config, 150);
            assertThatThrownBy(() -> connection.validatePrimaryStandbyIdentity(offset))
                    .isInstanceOf(SQLException.class)
                    .satisfies(error -> assertThat(((SQLException) error).getSQLState()).isEqualTo("08006"));
        }
    }

    @Test
    void shouldRejectInconsistentHistoryWhenPrimaryIsStable() throws Exception {
        try (var connection = connectionWithNewPrimaryHistory()) {
            doNothing().when(connection).validateServerRole();
            final var config = new DamengConnectorConfig(DamengConnectorConfigTest.configuration()
                    .with("database.failover.hosts", "primary:5236,standby:5236").build());
            final var offset = new DamengOffsetContext(config, 150);
            assertThatThrownBy(() -> connection.validatePrimaryStandbyIdentity(offset))
                    .isInstanceOf(ConnectException.class).hasMessageContaining("latest primary-open record");
        }
    }

    private DamengConnection connectionWithNewPrimaryHistory() throws Exception {
        final var config = new DamengConnectorConfig(DamengConnectorConfigTest.configuration()
                .with("database.failover.hosts", "primary:5236,standby:5236").build());
        final var connection = spy(new DamengConnection(config.getJdbcConfig()));
        doReturn(10L).when(connection).databaseMagic();
        doReturn(300L).when(connection).currentLsn();

        final var permanentMagic = mock(ResultSet.class);
        when(permanentMagic.next()).thenReturn(true);
        when(permanentMagic.getLong(1)).thenReturn(1L);
        doAnswer(invocation -> invocation.<JdbcConnection.ResultSetMapper<Object>> getArgument(1).apply(permanentMagic))
                .when(connection).queryAndMap(eq("SELECT PERMANENT_MAGIC()"), any());

        final var history = mock(ResultSet.class);
        when(history.next()).thenReturn(true, false);
        when(history.getInt(5)).thenReturn(1);
        when(history.getString(1)).thenReturn("new-primary");
        when(history.getTimestamp(2)).thenReturn(Timestamp.from(Instant.ofEpochSecond(2)));
        when(history.getLong(3)).thenReturn(20L);
        when(history.getLong(4)).thenReturn(200L);
        when(history.getLong(6)).thenReturn(10L);
        doAnswer(invocation -> invocation.<JdbcConnection.ResultSetMapper<Object>> getArgument(1).apply(history))
                .when(connection).queryAndMap(eq("SELECT RGUID, OPEN_TIME, CUR_DB_MAGIC, APPLY_LSN_0, N_EP, PRIMARY_DB_MAGIC FROM SYS.SYSOPENHISTORY "
                        + "WHERE OPEN_TIME IS NOT NULL ORDER BY OPEN_TIME"), any());
        return connection;
    }

    private DamengConnection connectionWithRole(boolean failover, String role, String status) throws Exception {
        final var config = new DamengConnectorConfig(DamengConnectorConfigTest.configuration()
                .with("database.failover.hosts", failover ? "primary:5236,standby:5236" : "").build());
        final var connection = spy(new DamengConnection(config.getJdbcConfig()));
        final var result = mock(ResultSet.class);
        when(result.next()).thenReturn(true);
        when(result.getString(1)).thenReturn(role);
        when(result.getString(2)).thenReturn(status);
        doAnswer(invocation -> invocation.<JdbcConnection.ResultSetMapper<Object>> getArgument(1).apply(result))
                .when(connection).queryAndMap(eq("SELECT MODE$, STATUS$ FROM V$INSTANCE"), any());
        return connection;
    }
}
