/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Bounds the time an initial snapshot may block writes, including downstream backpressure. */
final class DamengSnapshotDeadline implements AutoCloseable {
    private static final Logger LOGGER = LoggerFactory.getLogger(DamengSnapshotDeadline.class);
    private final ScheduledExecutorService timer;
    private final Connection connection;
    private final Thread snapshotThread;
    private boolean closed;
    private boolean expired;

    static final class SnapshotTimeoutException extends ConnectException {
        SnapshotTimeoutException(long timeoutMs, Throwable cause) {
            super("Dameng snapshot exceeded snapshot.max.duration.ms=" + timeoutMs
                    + "; connection abort was requested to release table locks. Increase the budget or reduce the snapshot scope.", cause);
        }
    }

    DamengSnapshotDeadline(Connection connection, long timeoutMs) {
        this.connection = connection;
        snapshotThread = Thread.currentThread();
        timer = Executors.newSingleThreadScheduledExecutor(runnable -> {
            final var thread = new Thread(runnable, "dameng-snapshot-deadline");
            thread.setDaemon(true);
            return thread;
        });
        timer.schedule(this::expire, timeoutMs, TimeUnit.MILLISECONDS);
    }

    private void expire() {
        synchronized (this) {
            if (closed) {
                return;
            }
            expired = true;
            // Mark the snapshot failed before aborting. Its owner cannot proceed
            // to streaming even if the driver takes time to close the socket.
            snapshotThread.interrupt();
        }
        // Interrupt queue waits as well as aborting the database session. Merely
        // stopping row iteration cannot release locks while blocked in JDBC or enqueue.
        try {
            connection.abort(Runnable::run);
        }
        catch (SQLException | RuntimeException e) {
            LOGGER.error("Could not abort the Dameng snapshot connection after its time limit", e);
        }
    }

    synchronized boolean expired() {
        return expired;
    }

    @Override
    public synchronized void close() {
        closed = true;
        timer.shutdownNow();
    }
}
