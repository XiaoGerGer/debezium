/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.lang.management.ManagementFactory;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import javax.management.ObjectName;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.connector.base.ChangeEventQueueMetrics;
import io.debezium.util.Clock;

class DamengSnapshotMetricsTest {
    private final AtomicLong nanos = new AtomicLong();
    private DamengSnapshotChangeEventSourceMetrics metrics;
    private DamengSnapshotLockMeter locks;
    private DamengTaskContext taskContext;
    private ChangeEventQueueMetrics queue;
    private DamengEventMetadataProvider metadata;
    private DamengPartition partition;
    private String server;

    @BeforeEach
    void before() {
        server = "dameng-snapshot-metrics-" + UUID.randomUUID();
        final var raw = DamengConnectorConfigTest.configuration().with("topic.prefix", server).build();
        taskContext = spy(new DamengTaskContext(raw, new DamengConnectorConfig(raw)));
        final var clock = mock(Clock.class);
        when(clock.currentTimeInNanos()).thenAnswer(invocation -> nanos.get());
        when(clock.currentTimeInMillis()).thenAnswer(invocation -> TimeUnit.NANOSECONDS.toMillis(nanos.get()));
        when(taskContext.getClock()).thenReturn(clock);
        queue = mock(ChangeEventQueueMetrics.class);
        metadata = new DamengEventMetadataProvider();
        metrics = new DamengSnapshotChangeEventSourceMetrics(taskContext, queue, metadata);
        locks = metrics.lockMeter();
        partition = new DamengPartition(server);
        metrics.snapshotStarted(partition);
    }

    @Test
    void shouldMeasurePendingAcquisitionWithoutClaimingALockWasAcquired() {
        locks.lockWaitStarted();
        advance(75);
        assertThat(metrics.getSnapshotLockWaiting()).isTrue();
        assertThat(metrics.getSnapshotLocksHeld()).isFalse();
        assertThat(metrics.getLockedTableCount()).isZero();
        assertThat(metrics.getSnapshotLockWaitDurationMillis()).isEqualTo(75);
        assertThat(metrics.getSnapshotMaxLockWaitDurationMillis()).isEqualTo(75);
        assertThat(metrics.getSnapshotLockHoldDurationMillis()).isZero();
    }

    @Test
    void shouldMeasureAllAcquisitionsAndHoldFromTheFirstSuccessfulAcquisition() {
        locks.lockWaitStarted();
        advance(20);
        assertThat(locks.lockWaitFinished(true)).isEqualTo(20);
        advance(30);
        locks.lockWaitStarted();
        advance(50);
        locks.lockWaitFinished(true);
        advance(100);

        assertThat(metrics.getSnapshotLockWaiting()).isFalse();
        assertThat(metrics.getSnapshotLocksHeld()).isTrue();
        assertThat(metrics.getLockedTableCount()).isEqualTo(2);
        assertThat(metrics.getSnapshotLockWaitDurationMillis()).isEqualTo(70);
        assertThat(metrics.getSnapshotMaxLockWaitDurationMillis()).isEqualTo(50);
        assertThat(metrics.getSnapshotLockHoldDurationMillis()).isEqualTo(180);
        locks.locksReleased();
        metrics.snapshotCompleted(partition);

        assertThat(metrics.getSnapshotCompleted()).isTrue();
        assertThat(metrics.getSnapshotLocksHeld()).isFalse();
        assertThat(metrics.getLockedTableCount()).isZero();
        assertThat(metrics.getSnapshotLockHoldDurationMillis()).isZero();
        assertThat(metrics.getSnapshotLockWaitDurationMillis()).isZero();
        assertThat(metrics.getSnapshotMaxLockWaitDurationMillis()).isZero();
        assertThat(metrics.getLastSnapshotLockStatisticsAvailable()).isTrue();
        assertThat(metrics.getLastSnapshotLockedTableCount()).isEqualTo(2);
        assertThat(metrics.getLastSnapshotLockWaitDurationMillis()).isEqualTo(70);
        assertThat(metrics.getLastSnapshotMaxLockWaitDurationMillis()).isEqualTo(50);
        assertThat(metrics.getLastSnapshotLockHoldDurationMillis()).isEqualTo(180);
    }

    @Test
    void shouldCountFailedAcquisitionTimeButNotAnUnacquiredLock() {
        locks.lockWaitStarted();
        advance(25);
        locks.lockWaitFinished(true);
        locks.lockWaitStarted();
        advance(1000);
        locks.lockWaitFinished(false);
        advance(5);
        locks.locksReleased();
        metrics.snapshotAborted(partition);
        assertThat(metrics.getSnapshotAborted()).isTrue();
        assertThat(metrics.getLastSnapshotLockedTableCount()).isEqualTo(1);
        assertThat(metrics.getLastSnapshotLockWaitDurationMillis()).isEqualTo(1025);
        assertThat(metrics.getLastSnapshotMaxLockWaitDurationMillis()).isEqualTo(1000);
        assertThat(metrics.getLastSnapshotLockHoldDurationMillis()).isEqualTo(1005);
    }

    @Test
    void shouldReportAFirstLockFailureWithoutInventingHoldingTime() {
        locks.lockWaitStarted();
        advance(1000);
        locks.lockWaitFinished(false);
        locks.locksReleased();
        assertThat(metrics.getSnapshotLockWaiting()).isFalse();
        assertThat(metrics.getLastSnapshotLockStatisticsAvailable()).isTrue();
        assertThat(metrics.getLastSnapshotLockWaitDurationMillis()).isEqualTo(1000);
        assertThat(metrics.getLastSnapshotLockHoldDurationMillis()).isZero();
        assertThat(metrics.getLastSnapshotLockedTableCount()).isZero();
    }

    @Test
    void shouldExpireCompletedStatisticsAtFiveMinutes() {
        locks.lockWaitStarted();
        advance(10);
        locks.lockWaitFinished(true);
        advance(40);
        locks.locksReleased();
        advance(TimeUnit.MINUTES.toMillis(5) - 1);
        assertThat(metrics.getLastSnapshotLockStatisticsAvailable()).isTrue();
        assertThat(metrics.getLastSnapshotLockHoldDurationMillis()).isEqualTo(40);
        advance(1);
        assertThat(metrics.getLastSnapshotLockHoldDurationMillis()).isZero();
        assertThat(metrics.getLastSnapshotLockStatisticsAvailable()).isFalse();
        assertThat(metrics.getLastSnapshotLockWaitDurationMillis()).isZero();
        assertThat(metrics.getLastSnapshotMaxLockWaitDurationMillis()).isZero();
        assertThat(metrics.getLastSnapshotLockedTableCount()).isZero();
    }

    @Test
    void shouldKeepLiveLocksAcrossJmxResetAndAbortedCallbacksUntilRelease() {
        locks.lockWaitStarted();
        advance(10);
        locks.lockWaitFinished(true);
        locks.lockWaitStarted();
        advance(20);
        metrics.reset();
        metrics.snapshotAborted(partition);
        assertThat(metrics.getSnapshotLocksHeld()).isTrue();
        assertThat(metrics.getSnapshotLockWaiting()).isTrue();
        assertThat(metrics.getLockedTableCount()).isEqualTo(1);
        assertThat(metrics.getSnapshotLockHoldDurationMillis()).isEqualTo(20);
        assertThat(metrics.getSnapshotLockWaitDurationMillis()).isEqualTo(30);
        locks.lockWaitFinished(false);
        locks.locksReleased();
        assertThat(metrics.getSnapshotLocksHeld()).isFalse();
        metrics.reset();
        assertThat(metrics.getLastSnapshotLockStatisticsAvailable()).isFalse();
    }

    @Test
    void shouldClearThePreviousResultWhenANewSnapshotStarts() {
        locks.lockWaitStarted();
        advance(10);
        locks.lockWaitFinished(true);
        locks.locksReleased();
        metrics.snapshotStarted(partition);
        assertThat(metrics.getSnapshotRunning()).isTrue();
        assertThat(metrics.getLastSnapshotLockStatisticsAvailable()).isFalse();
        assertThat(metrics.getSnapshotLocksHeld()).isFalse();
    }

    @Test
    void shouldNotInventAResultForSnapshotsThatAcquireNoLocks() {
        locks.locksReleased();
        assertThat(metrics.getLastSnapshotLockStatisticsAvailable()).isFalse();
        assertThat(metrics.getLastSnapshotLockHoldDurationMillis()).isZero();
    }

    @Test
    void shouldUseMonotonicElapsedTimeEvenWhenItsOriginIsNegative() {
        nanos.set(-TimeUnit.SECONDS.toNanos(1));
        locks.lockWaitStarted();
        advance(10);
        locks.lockWaitFinished(true);
        advance(40);
        locks.locksReleased();
        assertThat(metrics.getLastSnapshotLockHoldDurationMillis()).isEqualTo(40);
        assertThat(metrics.getLastSnapshotLockWaitDurationMillis()).isEqualTo(10);
    }

    @Test
    void shouldNotResetIncrementalCountersWhenTheCommonSnapshotStarts() {
        final var incremental = metrics.incrementalMeter();
        incremental.startTable("APP.CAPTURED");
        incremental.remainingTables(2);
        incremental.queryCompleted(2048, TimeUnit.MILLISECONDS.toNanos(5));
        incremental.eventEnqueued();
        metrics.snapshotStarted(partition);
        assertThat(metrics.getIncrementalSnapshotRunning()).isTrue();
        assertThat(metrics.getIncrementalSnapshotCurrentTable()).isEqualTo("APP.CAPTURED");
        assertThat(metrics.getIncrementalSnapshotRemainingTableCount()).isEqualTo(2);
        assertThat(metrics.getIncrementalSnapshotRowsScanned()).isEqualTo(2048);
        assertThat(metrics.getIncrementalSnapshotRowsEnqueued()).isEqualTo(1);
    }

    @Test
    void shouldExposeIncrementalAttributesThroughTheRegisteredSnapshotMBean() throws Exception {
        final var name = new ObjectName("debezium.dameng:type=connector-metrics,context=snapshot,server=" + server);
        final var mbeans = ManagementFactory.getPlatformMBeanServer();
        final var incremental = metrics.incrementalMeter();
        incremental.startTable("APP.CAPTURED");
        incremental.remainingTables(3);
        incremental.windowOpened("chunk");
        incremental.phase("reading");
        incremental.queryCompleted(2048, TimeUnit.MILLISECONDS.toNanos(7));
        incremental.windowCloseWritten();
        metrics.register();
        try {
            advance(50);
            assertThat(mbeans.getAttribute(name, "IncrementalSnapshotRunning")).isEqualTo(true);
            assertThat(mbeans.getAttribute(name, "IncrementalSnapshotPaused")).isEqualTo(false);
            assertThat(mbeans.getAttribute(name, "IncrementalSnapshotPhase")).isEqualTo("waiting_close");
            assertThat(mbeans.getAttribute(name, "IncrementalSnapshotCurrentTable")).isEqualTo("APP.CAPTURED");
            assertThat(mbeans.getAttribute(name, "IncrementalSnapshotRemainingTableCount")).isEqualTo(3);
            assertThat(mbeans.getAttribute(name, "IncrementalSnapshotRowsScanned")).isEqualTo(2048L);
            assertThat(mbeans.getAttribute(name, "IncrementalSnapshotLastQueryDurationMillis")).isEqualTo(7L);
            assertThat(mbeans.getAttribute(name, "IncrementalSnapshotCurrentCloseWaitDurationMillis")).isEqualTo(50L);
            incremental.windowCloseReceived("chunk-close");
            incremental.eventEnqueued();
            incremental.windowCompleted(TimeUnit.MILLISECONDS.toNanos(9));
            assertThat(mbeans.getAttribute(name, "IncrementalSnapshotLastCloseWaitDurationMillis")).isEqualTo(50L);
            assertThat(mbeans.getAttribute(name, "IncrementalSnapshotRowsEnqueued")).isEqualTo(1L);
            assertThat(mbeans.getAttribute(name, "IncrementalSnapshotWindowsCompleted")).isEqualTo(1L);
            assertThat(mbeans.getAttribute(name, "IncrementalSnapshotLastEmitDurationMillis")).isEqualTo(9L);
            incremental.paused();
            assertThat(mbeans.getAttribute(name, "IncrementalSnapshotPaused")).isEqualTo(true);
            assertThat(mbeans.getAttribute(name, "IncrementalSnapshotPhase")).isEqualTo("paused");
            mbeans.invoke(name, "reset", new Object[0], new String[0]);
            assertThat(mbeans.getAttribute(name, "IncrementalSnapshotRowsScanned")).isEqualTo(0L);
            assertThat(mbeans.getAttribute(name, "IncrementalSnapshotRowsEnqueued")).isEqualTo(0L);
            assertThat(mbeans.getAttribute(name, "IncrementalSnapshotWindowsCompleted")).isEqualTo(0L);
            assertThat(mbeans.getAttribute(name, "IncrementalSnapshotRunning")).isEqualTo(true);
            assertThat(mbeans.getAttribute(name, "IncrementalSnapshotRemainingTableCount")).isEqualTo(3);
            incremental.finished();
            assertThat(mbeans.getAttribute(name, "IncrementalSnapshotRunning")).isEqualTo(false);
            assertThat(mbeans.getAttribute(name, "IncrementalSnapshotCurrentTable")).isEqualTo("");
            assertThat(mbeans.getAttribute(name, "IncrementalSnapshotRemainingTableCount")).isEqualTo(0);
        }
        finally {
            metrics.unregister();
        }
    }

    @Test
    void shouldExposeLockAndCommonSnapshotAttributesThroughTheCoordinatorFactory() throws Exception {
        final var factory = new DamengChangeEventSourceMetricsFactory(
                new DamengStreamingChangeEventSourceMetrics(taskContext, queue, metadata, List::of));
        final var bean = (DamengSnapshotChangeEventSourceMetrics) factory.getSnapshotMetrics(taskContext, queue, metadata);
        final var name = new ObjectName("debezium.dameng:type=connector-metrics,context=snapshot,server=" + server);
        final var mbeans = ManagementFactory.getPlatformMBeanServer();
        bean.snapshotStarted(partition);
        bean.lockMeter().lockWaitStarted();
        advance(15);
        bean.lockMeter().lockWaitFinished(true);
        bean.register();
        try {
            assertThat(mbeans.getAttribute(name, "SnapshotRunning")).isEqualTo(true);
            assertThat(mbeans.getAttribute(name, "SnapshotLocksHeld")).isEqualTo(true);
            assertThat(mbeans.getAttribute(name, "LockedTableCount")).isEqualTo(1);
            assertThat(mbeans.getAttribute(name, "SnapshotLockWaitDurationMillis")).isEqualTo(15L);
            advance(50);
            bean.lockMeter().locksReleased();
            assertThat(mbeans.getAttribute(name, "SnapshotLocksHeld")).isEqualTo(false);
            assertThat(mbeans.getAttribute(name, "LastSnapshotLockHoldDurationMillis")).isEqualTo(50L);
            mbeans.invoke(name, "reset", new Object[0], new String[0]);
            assertThat(mbeans.getAttribute(name, "LastSnapshotLockStatisticsAvailable")).isEqualTo(false);
        }
        finally {
            bean.unregister();
        }
        assertThat(mbeans.isRegistered(name)).isFalse();
    }

    private void advance(long millis) {
        nanos.addAndGet(TimeUnit.MILLISECONDS.toNanos(millis));
    }
}
