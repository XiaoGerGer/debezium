/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng;

import io.debezium.pipeline.metrics.SnapshotChangeEventSourceMetricsMXBean;

/** Initial snapshot locks and incremental snapshot progress, with separate lifecycle state. */
public interface DamengSnapshotChangeEventSourceMetricsMXBean extends SnapshotChangeEventSourceMetricsMXBean {
    boolean getIncrementalSnapshotRunning();

    boolean getIncrementalSnapshotPaused();

    String getIncrementalSnapshotPhase();

    String getIncrementalSnapshotCurrentTable();

    /** Tables still scheduled in the current incremental snapshot, including the current table. */
    int getIncrementalSnapshotRemainingTableCount();

    /** Rows read into incremental snapshot windows since task start or an explicit metrics reset. */
    long getIncrementalSnapshotRowsScanned();

    /** Snapshot events enqueued, after deduplication; this does not measure Kafka acknowledgements. */
    long getIncrementalSnapshotRowsEnqueued();

    long getIncrementalSnapshotWindowsCompleted();

    long getIncrementalSnapshotLastQueryDurationMillis();

    long getIncrementalSnapshotCurrentCloseWaitDurationMillis();

    long getIncrementalSnapshotLastCloseWaitDurationMillis();

    long getIncrementalSnapshotLastEmitDurationMillis();

    boolean getSnapshotLockWaiting();

    boolean getSnapshotLocksHeld();

    int getLockedTableCount();

    /** Cumulative LOCK TABLE execution time, including the currently pending acquisition. */
    long getSnapshotLockWaitDurationMillis();

    long getSnapshotMaxLockWaitDurationMillis();

    /** Elapsed time since the first successful acquisition, zero after confirmed rollback. */
    long getSnapshotLockHoldDurationMillis();

    boolean getLastSnapshotLockStatisticsAvailable();

    long getLastSnapshotLockWaitDurationMillis();

    long getLastSnapshotMaxLockWaitDurationMillis();

    long getLastSnapshotLockHoldDurationMillis();

    int getLastSnapshotLockedTableCount();
}
