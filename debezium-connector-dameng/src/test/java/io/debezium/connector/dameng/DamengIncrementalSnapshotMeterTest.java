/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.util.Clock;

class DamengIncrementalSnapshotMeterTest {
    private final AtomicLong nanos = new AtomicLong();
    private DamengIncrementalSnapshotMeter meter;

    @BeforeEach
    void before() {
        final var clock = mock(Clock.class);
        when(clock.currentTimeInNanos()).thenAnswer(invocation -> nanos.get());
        meter = new DamengIncrementalSnapshotMeter(clock);
    }

    @Test
    void shouldStartIdleAndPreserveTaskCountersAcrossTablesAndSnapshots() {
        assertThat(meter.isRunning()).isFalse();
        assertThat(meter.isPaused()).isFalse();
        assertThat(meter.phase()).isEqualTo("idle");
        assertThat(meter.currentTable()).isEmpty();
        assertThat(meter.remainingTables()).isZero();
        assertThat(meter.currentCloseWaitDurationMillis()).isZero();

        meter.startTable("APP.FIRST_TABLE");
        meter.remainingTables(2);
        assertThat(meter.remainingTables()).isEqualTo(2);
        meter.windowOpened("first");
        meter.phase("reading");
        meter.queryCompleted(2048, TimeUnit.MILLISECONDS.toNanos(12));
        meter.windowCloseWritten();
        advance(25);
        assertThat(meter.windowCloseReceived("first-close")).isTrue();
        meter.eventEnqueued();
        meter.windowCompleted(TimeUnit.MILLISECONDS.toNanos(7));

        assertThat(meter.isRunning()).isTrue();
        assertThat(meter.currentTable()).isEqualTo("APP.FIRST_TABLE");
        assertThat(meter.phase()).isEqualTo("preparing");
        assertThat(meter.rowsScanned()).isEqualTo(2048);
        assertThat(meter.rowsEnqueued()).isEqualTo(1);
        assertThat(meter.windowsCompleted()).isEqualTo(1);
        assertThat(meter.lastQueryDurationMillis()).isEqualTo(12);
        assertThat(meter.lastCloseWaitDurationMillis()).isEqualTo(25);
        assertThat(meter.lastEmitDurationMillis()).isEqualTo(7);

        meter.startTable("APP.SECOND_TABLE");
        meter.remainingTables(1);
        meter.queryCompleted(10, TimeUnit.MILLISECONDS.toNanos(3));
        assertThat(meter.rowsScanned()).isEqualTo(2058);
        assertThat(meter.currentTable()).isEqualTo("APP.SECOND_TABLE");
        assertThat(meter.remainingTables()).isEqualTo(1);
        meter.finished();
        assertThat(meter.isRunning()).isFalse();
        assertThat(meter.currentTable()).isEmpty();
        assertThat(meter.remainingTables()).isZero();
        assertThat(meter.phase()).isEqualTo("idle");
        assertThat(meter.rowsScanned()).isEqualTo(2058);
        meter.startTable("APP.NEW_SNAPSHOT");
        assertThat(meter.rowsScanned()).isEqualTo(2058);
        assertThat(meter.rowsEnqueued()).isEqualTo(1);
        assertThat(meter.windowsCompleted()).isEqualTo(1);
    }

    @Test
    void shouldOnlyAcceptTheCurrentCloseWatermarkOnce() {
        assertThat(meter.windowCloseReceived("-close")).isFalse();
        meter.windowCloseWritten();
        assertThat(meter.windowCloseReceived("-close")).isFalse();
        meter.startTable("APP.CAPTURED");
        meter.windowOpened("current");
        assertThat(meter.windowCloseReceived("current-close")).isFalse();
        meter.windowCloseWritten();
        advance(20);
        assertThat(meter.windowCloseReceived("previous-close")).isFalse();
        assertThat(meter.windowCloseReceived("current-open")).isFalse();
        assertThat(meter.windowCloseReceived(null)).isFalse();
        assertThat(meter.phase()).isEqualTo("waiting_close");
        assertThat(meter.currentCloseWaitDurationMillis()).isEqualTo(20);
        assertThat(meter.windowCloseReceived("current-close")).isTrue();
        assertThat(meter.phase()).isEqualTo("emitting");
        assertThat(meter.currentCloseWaitDurationMillis()).isZero();
        advance(10);
        assertThat(meter.windowCloseReceived("current-close")).isFalse();
        assertThat(meter.lastCloseWaitDurationMillis()).isEqualTo(20);
    }

    @Test
    void shouldPreserveTheUnderlyingPhaseAndElapsedWaitAcrossPause() {
        meter.paused();
        assertThat(meter.isPaused()).isFalse();
        meter.startTable("APP.CAPTURED");
        meter.windowOpened("chunk");
        meter.windowCloseWritten();
        advance(10);
        meter.paused();
        assertThat(meter.isRunning()).isTrue();
        assertThat(meter.isPaused()).isTrue();
        assertThat(meter.phase()).isEqualTo("paused");
        advance(20);
        assertThat(meter.currentCloseWaitDurationMillis()).isEqualTo(30);
        meter.resumed();
        assertThat(meter.isPaused()).isFalse();
        assertThat(meter.phase()).isEqualTo("waiting_close");
        assertThat(meter.windowCloseReceived("chunk-close")).isTrue();
        assertThat(meter.lastCloseWaitDurationMillis()).isEqualTo(30);
        meter.paused();
        assertThat(meter.phase()).isEqualTo("paused");
        meter.resumed();
        assertThat(meter.phase()).isEqualTo("emitting");
        meter.paused();
        meter.finished();
        assertThat(meter.isPaused()).isFalse();
    }

    @Test
    void shouldKeepTheFailingTableButStopLiveWaitMeasurements() {
        meter.startTable("APP.FAILING");
        meter.remainingTables(3);
        meter.windowOpened("chunk");
        meter.windowCloseWritten();
        advance(10);
        meter.paused();
        meter.failed();
        advance(20);
        assertThat(meter.isRunning()).isFalse();
        assertThat(meter.isPaused()).isFalse();
        assertThat(meter.phase()).isEqualTo("failed");
        assertThat(meter.currentTable()).isEqualTo("APP.FAILING");
        assertThat(meter.remainingTables()).isEqualTo(3);
        assertThat(meter.currentCloseWaitDurationMillis()).isZero();
        assertThat(meter.windowCloseReceived("chunk-close")).isFalse();
    }

    @Test
    void shouldDiscardOldWaitWhenThePhaseOrWindowChanges() {
        meter.startTable("APP.CAPTURED");
        meter.windowOpened("first");
        meter.windowCloseWritten();
        advance(10);
        meter.phase("reading");
        assertThat(meter.currentCloseWaitDurationMillis()).isZero();
        assertThat(meter.windowCloseReceived("first-close")).isFalse();
        meter.windowCloseWritten();
        meter.windowOpened("second");
        assertThat(meter.currentCloseWaitDurationMillis()).isZero();
        assertThat(meter.windowCloseReceived("first-close")).isFalse();
        meter.windowCloseWritten();
        meter.startTable("APP.NEXT");
        assertThat(meter.currentCloseWaitDurationMillis()).isZero();
        assertThat(meter.windowCloseReceived("second-close")).isFalse();
    }

    @Test
    void shouldUseElapsedTimeWithANegativeMonotonicOriginAndClampNegativeDurations() {
        nanos.set(-TimeUnit.SECONDS.toNanos(1));
        meter.startTable("APP.CAPTURED");
        meter.windowOpened("chunk");
        meter.windowCloseWritten();
        advance(15);
        assertThat(meter.currentCloseWaitDurationMillis()).isEqualTo(15);
        assertThat(meter.windowCloseReceived("chunk-close")).isTrue();
        assertThat(meter.lastCloseWaitDurationMillis()).isEqualTo(15);
        meter.queryCompleted(1, -1);
        meter.windowCompleted(-1);
        assertThat(meter.lastQueryDurationMillis()).isZero();
        assertThat(meter.lastEmitDurationMillis()).isZero();
    }

    @Test
    void shouldClearCountersAndCompletedMeasurementsButPreserveActiveStateOnReset() {
        meter.startTable("APP.CAPTURED");
        meter.remainingTables(2);
        meter.windowOpened("first");
        meter.queryCompleted(10, TimeUnit.MILLISECONDS.toNanos(5));
        meter.eventEnqueued();
        meter.windowCloseWritten();
        advance(10);
        meter.windowCloseReceived("first-close");
        meter.windowCompleted(TimeUnit.MILLISECONDS.toNanos(8));
        meter.windowOpened("second");
        meter.windowCloseWritten();
        advance(20);
        meter.paused();
        meter.reset();

        assertThat(meter.rowsScanned()).isZero();
        assertThat(meter.rowsEnqueued()).isZero();
        assertThat(meter.windowsCompleted()).isZero();
        assertThat(meter.lastQueryDurationMillis()).isZero();
        assertThat(meter.lastCloseWaitDurationMillis()).isZero();
        assertThat(meter.lastEmitDurationMillis()).isZero();
        assertThat(meter.isRunning()).isTrue();
        assertThat(meter.isPaused()).isTrue();
        assertThat(meter.currentTable()).isEqualTo("APP.CAPTURED");
        assertThat(meter.remainingTables()).isEqualTo(2);
        assertThat(meter.phase()).isEqualTo("paused");
        assertThat(meter.currentCloseWaitDurationMillis()).isEqualTo(20);
        meter.resumed();
        advance(10);
        assertThat(meter.windowCloseReceived("second-close")).isTrue();
        assertThat(meter.lastCloseWaitDurationMillis()).isEqualTo(30);
    }

    private void advance(long millis) {
        nanos.addAndGet(TimeUnit.MILLISECONDS.toNanos(millis));
    }
}
