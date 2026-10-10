/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.nio.file.AccessDeniedException;
import java.sql.SQLException;
import java.sql.SQLRecoverableException;
import java.sql.SQLTimeoutException;
import java.sql.SQLTransientConnectionException;

import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.relational.history.SchemaHistoryException;

class DamengErrorHandlerTest {
    private final DamengErrorHandler handler = new DamengErrorHandler(
            new DamengConnectorConfig(Configuration.create().with("topic.prefix", "dameng").build()), mock(ChangeEventQueue.class), null);

    @Test
    void shouldRetryNoEligiblePrimaryOnlyForConfiguredFailoverEndpoints() {
        final var noPrimary = new SQLException("Server Mode invalid.", "", 6091);
        assertThat(handler.isRetriable(noPrimary)).isFalse();
        final var config = new DamengConnectorConfig(Configuration.create().with("topic.prefix", "dameng")
                .with("database.failover.hosts", "a:5236,b:5236").build());
        final var failoverHandler = new DamengErrorHandler(config, mock(ChangeEventQueue.class), null);
        assertThat(failoverHandler.isRetriable(noPrimary)).isTrue();
        assertThat(failoverHandler.isRetriable(new SQLException("invalid password", "28000", 6002))).isFalse();
    }

    @Test
    void shouldRetryJdbcConnectionFailuresAndNestedIoFailures() {
        assertThat(handler.isRetriable(new SQLException("communication failure", "08S01"))).isTrue();
        assertThat(handler.isRetriable(new SQLException("connection closed", "08003"))).isTrue();
        assertThat(handler.isRetriable(new SQLRecoverableException())).isTrue();
        assertThat(handler.isRetriable(new SQLTransientConnectionException())).isTrue();
        assertThat(handler.isRetriable(new SQLException("driver exception", new IOException("socket reset")))).isTrue();
    }

    @Test
    void shouldInspectJdbcNextExceptionsWithoutLooping() {
        final var wrapper = new SQLException("batch failed");
        wrapper.setNextException(new SQLException("communication failure", "08S01"));
        assertThat(handler.isRetriable(wrapper)).isTrue();
        final var first = new SQLException("first");
        final var second = new SQLException("second");
        first.initCause(second);
        second.initCause(first);
        assertThat(handler.isRetriable(first)).isFalse();
    }

    @Test
    void shouldNotRetryPermanentErrorsOrRepeatedQueryTimeouts() {
        assertThat(handler.isRetriable(new SQLException("permission denied", "42501"))).isFalse();
        assertThat(handler.isRetriable(new SQLException("invalid password", "28000"))).isFalse();
        assertThat(handler.isRetriable(new SQLTimeoutException("query exceeded its budget"))).isFalse();
        assertThat(handler.isRetriable(new SQLTimeoutException("query exceeded its budget", new IOException("cancelling query")))).isFalse();
        assertThat(handler.isRetriable(new RuntimeException(new DamengSnapshotDeadline.SnapshotTimeoutException(1_000,
                new SQLException("connection closed by deadline", "08003"))))).isFalse();
        assertThat(handler.isRetriable(new IllegalStateException("archive gap"))).isFalse();
        assertThat(handler.isRetriable(new IllegalStateException("buffer limit"))).isFalse();
        assertThat(handler.isRetriable(null)).isFalse();
    }

    @Test
    void shouldNotTreatHistoryStorageFailuresAsNetworkFailures() {
        assertThat(handler.isRetriable(new SchemaHistoryException("Cannot persist DDL", new AccessDeniedException("history.dat")))).isFalse();
        assertThat(handler.isRetriable(new SchemaHistoryException("Cannot persist DDL", new IOException("No space left on device")))).isFalse();
        final var connection = new SQLException("connection closed", "08003");
        connection.initCause(new SchemaHistoryException("Cannot persist DDL", new AccessDeniedException("history.dat")));
        assertThat(handler.isRetriable(connection)).isFalse();
    }

    @Test
    void shouldShareTheRetryBudgetAcrossFailedStartsAndStreamingRestarts() {
        final var config = new DamengConnectorConfig(Configuration.create().with("topic.prefix", "dameng").with("errors.max.retries", 2).build());
        final var first = new DamengErrorHandler(config, mock(ChangeEventQueue.class), null);
        assertThat(first.canRetry(new SQLException("connection refused", "08001"))).isTrue();
        assertThat(first.canRetry(new SQLException("invalid password", "28000"))).isFalse();
        final var replacement = new DamengErrorHandler(config, mock(ChangeEventQueue.class), first);
        assertThat(replacement.canRetry(new SQLException("connection refused", "08001"))).isTrue();
        assertThat(replacement.canRetry(new SQLException("connection refused", "08001"))).isFalse();
        replacement.resetRetries();
        assertThat(replacement.canRetry(new SQLException("connection refused", "08001"))).isTrue();
    }
}
