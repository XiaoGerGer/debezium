/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.SQLException;
import java.sql.SQLRecoverableException;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.LoggerFactory;

import io.debezium.junit.logging.LogInterceptor;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;

class DamengConnectorTaskTest {
    private static final DamengDatabaseVersion TESTED_VERSION = new DamengDatabaseVersion("DM DBMS", "8.1.2.128", 8, 1);
    private final LogInterceptor logs = LogInterceptor.forClass(DamengConnectorTask.class);

    @Test
    void shouldCloseBothReaderConnectionsAndPrimaryWhenStandbyCoordinationCannotStart() throws Exception {
        final var config = DamengConnectorConfigTest.configuration().with("database.standby.hosts", "replica:5236")
                .with("errors.max.retries", 2).build();
        final var task = new DamengConnectorTask();
        try (var connections = mockConstruction(DamengConnection.class, (connection, context) -> {
            when(connection.readPrimaryStandbyIdentity()).thenThrow(new SQLException("primary unavailable", "08006"));
        })) {
            task.preStart(config);
            assertThatThrownBy(() -> task.start(config)).isInstanceOf(RetriableException.class).hasRootCauseMessage("primary unavailable");
            assertThat(connections.constructed()).hasSize(3);
            task.doStop();
            for (DamengConnection connection : connections.constructed()) {
                verify(connection, times(1)).close();
            }
        }
    }

    @AfterEach
    void stopCapturingLogs() {
        logs.stop();
        ((Logger) LoggerFactory.getLogger(DamengConnectorTask.class)).detachAppender(logs);
    }

    @ParameterizedTest
    @CsvSource({ "8.1.2.128, true", "8.1.2.129, false", "8.1.3.140, false", ", false", "'', false" })
    void shouldLogDatabaseVersionAndContinueStartupRegardlessOfTestedStatus(String productVersion, boolean tested) throws SQLException {
        final var config = DamengConnectorConfigTest.configuration().build();
        final var task = new DamengConnectorTask();
        try (var connections = mockConstruction(DamengConnection.class, (connection, context) -> {
            when(connection.getDatabaseVersion()).thenReturn(new DamengDatabaseVersion("DM DBMS", productVersion, 8, 1));
            when(connection.getDatabaseCharset()).thenThrow(new SQLException("subsequent startup validation", "28000"));
        })) {
            task.preStart(config);
            assertThatThrownBy(() -> task.start(config)).isInstanceOf(ConnectException.class)
                    .hasRootCauseMessage("subsequent startup validation");
            verify(connections.constructed().get(0)).getDatabaseCharset();
            assertThat(logs.getLoggingEvents("Connected to DM DBMS database version " + productVersion))
                    .singleElement().satisfies(event -> {
                        assertThat(event.getLevel()).isEqualTo(Level.INFO);
                        assertThat(event.getFormattedMessage()).contains("JDBC major=8, minor=1", "matches tested JDBC version: " + tested);
                    });
            assertThat(logs.containsWarnMessage("has not been tested")).isEqualTo(!tested);
            if (!tested) {
                assertThat(logs.containsWarnMessage("Tested JDBC product versions: [8.1.2.128]. Startup will continue")).isTrue();
            }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "HY000", "0A000", "08S01" })
    void shouldWarnAndContinueStartupWhenJdbcVersionMetadataCannotBeRead(String sqlState) throws SQLException {
        final var config = DamengConnectorConfigTest.configuration().build();
        final var task = new DamengConnectorTask();
        try (var connections = mockConstruction(DamengConnection.class, (connection, context) -> {
            when(connection.getDatabaseVersion()).thenThrow(new SQLException("version metadata unavailable", sqlState));
            when(connection.getDatabaseCharset()).thenThrow(new SQLException("subsequent startup validation", "28000"));
        })) {
            task.preStart(config);
            assertThatThrownBy(() -> task.start(config)).isInstanceOf(ConnectException.class).isNotInstanceOf(RetriableException.class)
                    .hasRootCauseMessage("subsequent startup validation");
            verify(connections.constructed().get(0)).getDatabaseCharset();
            assertThat(task.getErrorHandler().orElseThrow().getRetries()).isZero();
            assertThat(logs.containsWarnMessage("Could not read the Dameng database version through JDBC metadata")).isTrue();
            assertThat(logs.containsWarnMessage("Startup will continue")).isTrue();
            assertThat(logs.containsErrorMessage("version")).isFalse();
        }
    }

    @Test
    void shouldCarryStartupRetryBudgetAcrossAttemptsAndClosePartialConnections() throws Exception {
        final var config = DamengConnectorConfigTest.configuration().with("errors.max.retries", 2).build();
        final var task = new DamengConnectorTask();
        try (var connections = mockConstruction(DamengConnection.class, (connection, context) -> {
            when(connection.getDatabaseVersion()).thenReturn(TESTED_VERSION);
            when(connection.getDatabaseCharset()).thenThrow(new SQLException("database unavailable", "08S01"));
        })) {
            for (int attempt = 1; attempt <= 2; attempt++) {
                task.preStart(config);
                assertThatThrownBy(() -> task.start(config)).isInstanceOf(RetriableException.class);
                assertThat(task.getErrorHandler().orElseThrow().getRetries()).isEqualTo(attempt);
                task.doStop();
            }
            task.preStart(config);
            assertThatThrownBy(() -> task.start(config)).isInstanceOf(ConnectException.class).isNotInstanceOf(RetriableException.class);
            assertThat(task.getErrorHandler().orElseThrow().getRetries()).isEqualTo(2);
            task.doStop();
            assertThat(connections.constructed()).hasSize(6);
            assertThat(logs.countOccurrences("Connected to {} database version")).isEqualTo(3);
            for (DamengConnection connection : connections.constructed()) {
                verify(connection, times(1)).close();
            }
        }
    }

    @Test
    void shouldNotRetryAnInitialConnectionWhenRetriesAreDisabled() {
        final var config = DamengConnectorConfigTest.configuration().with("errors.max.retries", 0).build();
        final var task = new DamengConnectorTask();
        try (var connections = mockConstruction(DamengConnection.class, (connection, context) -> {
            when(connection.getDatabaseVersion()).thenReturn(TESTED_VERSION);
            when(connection.getDatabaseCharset()).thenThrow(new SQLRecoverableException("socket failed"));
        })) {
            task.preStart(config);
            assertThatThrownBy(() -> task.start(config)).isInstanceOf(ConnectException.class).isNotInstanceOf(RetriableException.class);
            assertThat(task.getErrorHandler().orElseThrow().getRetries()).isZero();
        }
    }

    @Test
    void shouldKeepAuthenticationFailuresTerminalWithoutConsumingRetryBudget() {
        final var config = DamengConnectorConfigTest.configuration().with("errors.max.retries", 10).build();
        final var task = new DamengConnectorTask();
        try (var connections = mockConstruction(DamengConnection.class, (connection, context) -> {
            when(connection.getDatabaseVersion()).thenReturn(TESTED_VERSION);
            when(connection.getDatabaseCharset()).thenThrow(new SQLException("invalid password", "28000"));
        })) {
            task.preStart(config);
            assertThatThrownBy(() -> task.start(config)).isInstanceOf(ConnectException.class).isNotInstanceOf(RetriableException.class)
                    .hasRootCauseMessage("invalid password");
            assertThat(task.getErrorHandler().orElseThrow().getRetries()).isZero();
        }
    }

    @Test
    void shouldPreserveTheStartupFailureWhenCleanupAlsoFails() {
        final var config = DamengConnectorConfigTest.configuration().build();
        final var task = new DamengConnectorTask();
        try (var connections = mockConstruction(DamengConnection.class, (connection, context) -> {
            when(connection.getDatabaseVersion()).thenReturn(TESTED_VERSION);
            when(connection.getDatabaseCharset()).thenThrow(new SQLException("invalid password", "28000"));
            doThrow(new SQLException("cleanup socket already closed", "08003")).when(connection).close();
        })) {
            task.preStart(config);
            assertThatThrownBy(() -> task.start(config)).isInstanceOf(ConnectException.class).isNotInstanceOf(RetriableException.class)
                    .hasRootCauseMessage("invalid password")
                    .satisfies(failure -> assertThat(failure.getSuppressed()).hasSize(2));
        }
    }
}
