/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.SQLRecoverableException;
import java.sql.SQLTimeoutException;
import java.sql.SQLTransientConnectionException;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;

import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.dameng.logminer.LogMinerTransactionBuffer;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.relational.history.SchemaHistoryException;

/** Classifies connection failures without retrying invalid schemas or missing archives. */
public final class DamengErrorHandler extends ErrorHandler {
    private final boolean failoverEnabled;

    public DamengErrorHandler(DamengConnectorConfig config, ChangeEventQueue<?> queue, ErrorHandler previous) {
        super(DamengConnector.class, config, queue, previous);
        failoverEnabled = config.requiresDatabaseLineage();
    }

    boolean canRetry(Throwable failure) {
        return isRetriable(failure) && hasMoreRetries();
    }

    @Override
    protected boolean isRetriable(Throwable throwable) {
        if (containsStorageFailure(throwable)) {
            return false;
        }
        final var pending = new ArrayDeque<Throwable>();
        final Set<Throwable> visited = Collections.newSetFromMap(new IdentityHashMap<>());
        if (throwable != null) {
            pending.add(throwable);
        }
        boolean connectionFailure = false;
        while (!pending.isEmpty()) {
            final var failure = pending.removeFirst();
            if (!visited.add(failure)) {
                continue;
            }
            if (LogMinerTransactionBuffer.isStorageFailure(failure)) {
                return false;
            }
            if (failure instanceof DamengSnapshotDeadline.SnapshotTimeoutException) {
                // Closing an over-budget snapshot can cause a nested 08003 error
                // during rollback. Retrying would repeatedly reacquire its locks.
                return false;
            }
            if (failure instanceof SchemaHistoryException) {
                // A history file permission or storage failure can wrap IOException.
                // Reconnecting to DM cannot repair the durable schema history.
                return false;
            }
            if (failure instanceof SQLTimeoutException) {
                // A statement that exceeds its configured budget requires tuning;
                // reconnecting must not turn the timeout into an endless loop.
                return false;
            }
            // Like Oracle, recoverable connection failures restart the task from
            // its acknowledged offset. DM reports communication failures as 08S01.
            if (failure instanceof IOException || failure instanceof SQLRecoverableException
                    || failure instanceof SQLTransientConnectionException) {
                connectionFailure = true;
            }
            if (failure instanceof SQLException sql) {
                // DM JDBC 8.1.3.140 reports no eligible member as 6091, and
                // an in-flight transaction interrupted by manual MOUNT as
                // -6508 / 25000, or a query racing with MOUNT as -510 / 22000.
                // These require rebuilding the role-routed sessions.
                if (failoverEnabled && (sql.getErrorCode() == 6091
                        || (sql.getErrorCode() == -6508 && "25000".equals(sql.getSQLState()))
                        || (sql.getErrorCode() == -510 && "22000".equals(sql.getSQLState())))) {
                    connectionFailure = true;
                }
                if (sql.getSQLState() != null && sql.getSQLState().startsWith("08")) {
                    connectionFailure = true;
                }
                if (sql.getNextException() != null) {
                    pending.add(sql.getNextException());
                }
            }
            if (failure.getCause() != null) {
                pending.add(failure.getCause());
            }
        }
        return connectionFailure;
    }

    private boolean containsStorageFailure(Throwable throwable) {
        final var pending = new ArrayDeque<Throwable>();
        final Set<Throwable> visited = Collections.newSetFromMap(new IdentityHashMap<>());
        if (throwable != null) {
            pending.add(throwable);
        }
        while (!pending.isEmpty()) {
            final var failure = pending.removeFirst();
            if (!visited.add(failure)) {
                continue;
            }
            if (LogMinerTransactionBuffer.isStorageFailure(failure)) {
                return true;
            }
            if (failure.getCause() != null) {
                pending.add(failure.getCause());
            }
            if (failure instanceof SQLException sql && sql.getNextException() != null) {
                pending.add(sql.getNextException());
            }
            // Suppressed cleanup errors may veto a retry, but a cleanup socket
            // failure must never make a primary authentication/schema error retriable.
            Collections.addAll(pending, failure.getSuppressed());
        }
        return false;
    }
}
