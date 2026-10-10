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
import io.debezium.pipeline.ErrorHandler;
import io.debezium.relational.history.SchemaHistoryException;

/** Classifies connection failures without retrying invalid schemas or missing archives. */
public final class DamengErrorHandler extends ErrorHandler {
    private final boolean failoverEnabled;

    public DamengErrorHandler(DamengConnectorConfig config, ChangeEventQueue<?> queue, ErrorHandler previous) {
        super(DamengConnector.class, config, queue, previous);
        failoverEnabled = config.isFailoverEnabled();
    }

    boolean canRetry(Throwable failure) {
        return isRetriable(failure) && hasMoreRetries();
    }

    @Override
    protected boolean isRetriable(Throwable throwable) {
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
                // DM JDBC 8.1.3.140 reports no eligible PRIMARY in a service
                // group as 6091 with an empty SQLState during manual takeover.
                if (failoverEnabled && sql.getErrorCode() == 6091) {
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
}
