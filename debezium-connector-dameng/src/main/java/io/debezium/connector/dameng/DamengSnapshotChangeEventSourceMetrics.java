/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng;

import io.debezium.annotation.ThreadSafe;
import io.debezium.connector.base.ChangeEventQueueMetrics;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.pipeline.metrics.DefaultSnapshotChangeEventSourceMetrics;
import io.debezium.pipeline.source.spi.EventMetadataProvider;

/** Extends the common snapshot MBean; the coordinator owns registration and cleanup. */
@ThreadSafe
public final class DamengSnapshotChangeEventSourceMetrics extends DefaultSnapshotChangeEventSourceMetrics<DamengPartition>
        implements DamengSnapshotChangeEventSourceMetricsMXBean {
    private final DamengSnapshotLockMeter locks;
    private final DamengIncrementalSnapshotMeter incremental;

    public DamengSnapshotChangeEventSourceMetrics(CdcSourceTaskContext taskContext, ChangeEventQueueMetrics queue,
                                                  EventMetadataProvider metadataProvider) {
        super(taskContext, queue, metadataProvider);
        locks = new DamengSnapshotLockMeter(taskContext.getClock());
        incremental = new DamengIncrementalSnapshotMeter(taskContext.getClock());
    }

    DamengSnapshotLockMeter lockMeter() {
        return locks;
    }

    DamengIncrementalSnapshotMeter incrementalMeter() {
        return incremental;
    }

    @Override
    public boolean getIncrementalSnapshotRunning() {
        return incremental.isRunning();
    }

    @Override
    public boolean getIncrementalSnapshotPaused() {
        return incremental.isPaused();
    }

    @Override
    public String getIncrementalSnapshotPhase() {
        return incremental.phase();
    }

    @Override
    public String getIncrementalSnapshotCurrentTable() {
        return incremental.currentTable();
    }

    @Override
    public int getIncrementalSnapshotRemainingTableCount() {
        return incremental.remainingTables();
    }

    @Override
    public long getIncrementalSnapshotRowsScanned() {
        return incremental.rowsScanned();
    }

    @Override
    public long getIncrementalSnapshotRowsEnqueued() {
        return incremental.rowsEnqueued();
    }

    @Override
    public long getIncrementalSnapshotWindowsCompleted() {
        return incremental.windowsCompleted();
    }

    @Override
    public long getIncrementalSnapshotLastQueryDurationMillis() {
        return incremental.lastQueryDurationMillis();
    }

    @Override
    public long getIncrementalSnapshotCurrentCloseWaitDurationMillis() {
        return incremental.currentCloseWaitDurationMillis();
    }

    @Override
    public long getIncrementalSnapshotLastCloseWaitDurationMillis() {
        return incremental.lastCloseWaitDurationMillis();
    }

    @Override
    public long getIncrementalSnapshotLastEmitDurationMillis() {
        return incremental.lastEmitDurationMillis();
    }

    @Override
    public void snapshotStarted(DamengPartition partition) {
        super.snapshotStarted(partition);
        locks.snapshotStarted();
    }

    @Override
    public boolean getSnapshotLockWaiting() {
        return locks.isWaiting();
    }

    @Override
    public boolean getSnapshotLocksHeld() {
        return locks.lockedTableCount() > 0;
    }

    @Override
    public int getLockedTableCount() {
        return locks.lockedTableCount();
    }

    @Override
    public long getSnapshotLockWaitDurationMillis() {
        return locks.waitDurationMillis();
    }

    @Override
    public long getSnapshotMaxLockWaitDurationMillis() {
        return locks.maxWaitDurationMillis();
    }

    @Override
    public long getSnapshotLockHoldDurationMillis() {
        return locks.holdDurationMillis();
    }

    @Override
    public boolean getLastSnapshotLockStatisticsAvailable() {
        return locks.resultAvailable();
    }

    @Override
    public long getLastSnapshotLockWaitDurationMillis() {
        return locks.lastWaitDurationMillis();
    }

    @Override
    public long getLastSnapshotMaxLockWaitDurationMillis() {
        return locks.lastMaxWaitDurationMillis();
    }

    @Override
    public long getLastSnapshotLockHoldDurationMillis() {
        return locks.lastHoldDurationMillis();
    }

    @Override
    public int getLastSnapshotLockedTableCount() {
        return locks.lastLockedTableCount();
    }

    @Override
    public void reset() {
        super.reset();
        locks.reset();
        incremental.reset();
    }
}
