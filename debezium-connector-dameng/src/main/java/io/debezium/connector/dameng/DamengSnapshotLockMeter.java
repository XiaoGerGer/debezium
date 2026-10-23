/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng;

import java.util.concurrent.TimeUnit;

import io.debezium.annotation.ThreadSafe;
import io.debezium.util.Clock;

/** Measures JDBC lock acquisition and transaction-wide holding time with a monotonic clock. */
@ThreadSafe
final class DamengSnapshotLockMeter {
    static final long RESULT_RETENTION_NANOS = TimeUnit.MINUTES.toNanos(5);
    private final Clock clock;
    private boolean waiting;
    private long waitStarted;
    private long holdStarted;
    private long waitNanos;
    private long maxWaitNanos;
    private int lockedTables;
    private boolean attempted;
    private boolean resultAvailable;
    private long releasedAt;
    private long lastWaitMillis;
    private long lastMaxWaitMillis;
    private long lastHoldMillis;
    private int lastLockedTables;

    DamengSnapshotLockMeter(Clock clock) {
        this.clock = clock;
    }

    synchronized void snapshotStarted() {
        clearCurrent();
        clearResult();
    }

    synchronized void lockWaitStarted() {
        waitStarted = clock.currentTimeInNanos();
        waiting = true;
        attempted = true;
    }

    synchronized long lockWaitFinished(boolean acquired) {
        final long now = clock.currentTimeInNanos();
        final long elapsed = elapsed(now, waitStarted);
        waitNanos += elapsed;
        maxWaitNanos = Math.max(maxWaitNanos, elapsed);
        waiting = false;
        if (acquired && lockedTables++ == 0) {
            holdStarted = now;
        }
        return TimeUnit.NANOSECONDS.toMillis(elapsed);
    }

    synchronized void locksReleased() {
        if (attempted) {
            releasedAt = clock.currentTimeInNanos();
            lastWaitMillis = TimeUnit.NANOSECONDS.toMillis(waitNanos);
            lastMaxWaitMillis = TimeUnit.NANOSECONDS.toMillis(maxWaitNanos);
            lastHoldMillis = lockedTables == 0 ? 0 : TimeUnit.NANOSECONDS.toMillis(elapsed(releasedAt, holdStarted));
            lastLockedTables = lockedTables;
            resultAvailable = true;
        }
        clearCurrent();
    }

    synchronized boolean isWaiting() {
        return waiting;
    }

    synchronized int lockedTableCount() {
        return lockedTables;
    }

    synchronized long waitDurationMillis() {
        return TimeUnit.NANOSECONDS.toMillis(waitNanos + (waiting ? elapsed(clock.currentTimeInNanos(), waitStarted) : 0));
    }

    synchronized long maxWaitDurationMillis() {
        return TimeUnit.NANOSECONDS.toMillis(Math.max(maxWaitNanos, waiting ? elapsed(clock.currentTimeInNanos(), waitStarted) : 0));
    }

    synchronized long holdDurationMillis() {
        return lockedTables == 0 ? 0 : TimeUnit.NANOSECONDS.toMillis(elapsed(clock.currentTimeInNanos(), holdStarted));
    }

    synchronized boolean resultAvailable() {
        if (resultAvailable && elapsed(clock.currentTimeInNanos(), releasedAt) >= RESULT_RETENTION_NANOS) {
            clearResult();
        }
        return resultAvailable;
    }

    synchronized long lastWaitDurationMillis() {
        return resultAvailable() ? lastWaitMillis : 0;
    }

    synchronized long lastMaxWaitDurationMillis() {
        return resultAvailable() ? lastMaxWaitMillis : 0;
    }

    synchronized long lastHoldDurationMillis() {
        return resultAvailable() ? lastHoldMillis : 0;
    }

    synchronized int lastLockedTableCount() {
        return resultAvailable() ? lastLockedTables : 0;
    }

    synchronized void reset() {
        // A JMX reset must not claim that a still-open transaction released its locks.
        // Preserve the live acquisition/holding measurements; forget completed results.
        clearResult();
    }

    private void clearCurrent() {
        waiting = false;
        waitStarted = 0;
        holdStarted = 0;
        waitNanos = 0;
        maxWaitNanos = 0;
        lockedTables = 0;
        attempted = false;
    }

    private void clearResult() {
        resultAvailable = false;
        releasedAt = 0;
        lastWaitMillis = 0;
        lastMaxWaitMillis = 0;
        lastHoldMillis = 0;
        lastLockedTables = 0;
    }

    private static long elapsed(long now, long started) {
        return Math.max(0, now - started);
    }
}
