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
import org.junit.jupiter.api.Test;

class DamengConnectorTaskTest {
    @Test
    void shouldCarryStartupRetryBudgetAcrossAttemptsAndClosePartialConnections() throws Exception {
        final var config = DamengConnectorConfigTest.configuration().with("errors.max.retries", 2).build();
        final var task = new DamengConnectorTask();
        try (var connections = mockConstruction(DamengConnection.class,
                (connection, context) -> when(connection.getDatabaseCharset()).thenThrow(new SQLException("database unavailable", "08S01")))) {
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
            for (DamengConnection connection : connections.constructed()) {
                verify(connection, times(1)).close();
            }
        }
    }

    @Test
    void shouldNotRetryAnInitialConnectionWhenRetriesAreDisabled() {
        final var config = DamengConnectorConfigTest.configuration().with("errors.max.retries", 0).build();
        final var task = new DamengConnectorTask();
        try (var connections = mockConstruction(DamengConnection.class,
                (connection, context) -> when(connection.getDatabaseCharset()).thenThrow(new SQLRecoverableException("socket failed")))) {
            task.preStart(config);
            assertThatThrownBy(() -> task.start(config)).isInstanceOf(ConnectException.class).isNotInstanceOf(RetriableException.class);
            assertThat(task.getErrorHandler().orElseThrow().getRetries()).isZero();
        }
    }

    @Test
    void shouldKeepAuthenticationFailuresTerminalWithoutConsumingRetryBudget() {
        final var config = DamengConnectorConfigTest.configuration().with("errors.max.retries", 10).build();
        final var task = new DamengConnectorTask();
        try (var connections = mockConstruction(DamengConnection.class,
                (connection, context) -> when(connection.getDatabaseCharset()).thenThrow(new SQLException("invalid password", "28000")))) {
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
