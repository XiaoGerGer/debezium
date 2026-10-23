/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng;

import java.util.concurrent.TimeUnit;

import io.debezium.annotation.ThreadSafe;
import io.debezium.util.Clock;

/**
 * Tracks incremental snapshot work independently from the initial snapshot.
 * Counters span the task lifetime, or the interval since an explicit JMX reset.
 * Enqueued rows have entered the connector queue and have not necessarily been acknowledged by Kafka.
 */
@ThreadSafe
final class DamengIncrementalSnapshotMeter {
    private final Clock clock;
    private boolean running;
    private boolean paused;
    private String phase = "idle";
    private String currentTable = "";
    private int remainingTables;
    private String chunkId = "";
    private boolean closeWaiting;
    private long closeWaitStarted;
    private long rowsScanned;
    private long rowsEnqueued;
    private long windowsCompleted;
    private long lastQueryDurationMillis;
    private long lastCloseWaitDurationMillis;
    private long lastEmitDurationMillis;

    DamengIncrementalSnapshotMeter(Clock clock) {
        this.clock = clock;
    }

    synchronized void startTable(String table) {
        running = true;
        paused = false;
        currentTable = table;
        phase("preparing");
        chunkId = "";
    }

    synchronized void phase(String phase) {
        this.phase = phase;
        if (!"waiting_close".equals(phase)) {
            closeWaiting = false;
        }
    }

    synchronized void queryCompleted(long rows, long durationNanos) {
        rowsScanned += rows;
        lastQueryDurationMillis = millis(durationNanos);
    }

    synchronized void remainingTables(int count) {
        remainingTables = count;
    }

    synchronized void windowOpened(String chunkId) {
        this.chunkId = chunkId;
        phase("preparing");
    }

    synchronized void windowCloseWritten() {
        closeWaitStarted = clock.currentTimeInNanos();
        closeWaiting = true;
        phase = "waiting_close";
    }

    synchronized boolean windowCloseReceived(String id) {
        if (!closeWaiting || chunkId.isEmpty() || !idEqualsCurrentClose(id)) {
            return false;
        }
        lastCloseWaitDurationMillis = millis(clock.currentTimeInNanos() - closeWaitStarted);
        phase("emitting");
        return true;
    }

    private boolean idEqualsCurrentClose(String id) {
        return (chunkId + "-close").equals(id);
    }

    synchronized void eventEnqueued() {
        rowsEnqueued++;
    }

    synchronized void windowCompleted(long durationNanos) {
        windowsCompleted++;
        lastEmitDurationMillis = millis(durationNanos);
        phase("preparing");
        chunkId = "";
    }

    synchronized void paused() {
        if (running) {
            paused = true;
        }
    }

    synchronized void resumed() {
        paused = false;
    }

    synchronized void finished() {
        running = false;
        paused = false;
        currentTable = "";
        remainingTables = 0;
        chunkId = "";
        phase("idle");
    }

    synchronized void failed() {
        running = false;
        paused = false;
        chunkId = "";
        phase("failed");
    }

    synchronized void reset() {
        // Reset counters without claiming that an active snapshot or watermark wait has ended.
        rowsScanned = 0;
        rowsEnqueued = 0;
        windowsCompleted = 0;
        lastQueryDurationMillis = 0;
        lastCloseWaitDurationMillis = 0;
        lastEmitDurationMillis = 0;
    }

    synchronized boolean isRunning() {
        return running;
    }

    synchronized boolean isPaused() {
        return paused;
    }

    synchronized String phase() {
        return paused ? "paused" : phase;
    }

    synchronized String currentTable() {
        return currentTable;
    }

    synchronized int remainingTables() {
        return remainingTables;
    }

    synchronized long rowsScanned() {
        return rowsScanned;
    }

    synchronized long rowsEnqueued() {
        return rowsEnqueued;
    }

    synchronized long windowsCompleted() {
        return windowsCompleted;
    }

    synchronized long lastQueryDurationMillis() {
        return lastQueryDurationMillis;
    }

    synchronized long currentCloseWaitDurationMillis() {
        return closeWaiting ? millis(clock.currentTimeInNanos() - closeWaitStarted) : 0;
    }

    synchronized long lastCloseWaitDurationMillis() {
        return lastCloseWaitDurationMillis;
    }

    synchronized long lastEmitDurationMillis() {
        return lastEmitDurationMillis;
    }

    private static long millis(long nanos) {
        return TimeUnit.NANOSECONDS.toMillis(Math.max(0, nanos));
    }
}
