/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng;

import io.debezium.pipeline.metrics.StreamingChangeEventSourceMetricsMXBean;

/** Dameng archive mining and cumulative input measurements. */
public interface DamengStreamingChangeEventSourceMetricsMXBean extends StreamingChangeEventSourceMetricsMXBean {
    String getRecoveryNonce();

    long getRecoveryBaselineLsn();

    /** Latest planned forward LSN span (archive selection or fixed span), not a row count. */
    long getCurrentMiningBatchSize();

    long getLastWindowLsnSpan();

    /** Soft whole-file budget; zero for fixed LSN windows. */
    long getMiningWindowByteBudget();

    /** Whole LEN of files selected at planning time, not actual I/O or resident memory. */
    long getSelectedArchiveFileBytes();

    long getSelectedArchiveFileCount();

    /** Queue-full condition waits since metrics initialization/reset; includes all queue writers. */
    long getQueueEnqueueWaitDurationMillis();

    /** Cumulative phase durations for successfully completed iterations. Dispatch includes queue waits. */
    long getForwardMiningDurationMillis();

    long getReconstructionDurationMillis();

    long getDispatchDurationMillis();

    long getLastForwardMiningDurationMillis();

    long getLastReconstructionDurationMillis();

    long getLastDispatchDurationMillis();

    long getTransactionBufferMemoryBytes();

    long getTransactionBufferDiskBytes();

    long getTransactionBufferRecords();

    long getSpilledTransactions();

    /** Cumulative estimated input bytes for the current or most recent session; telemetry only, not live heap. */
    long getWindowBufferEstimatedBytes();

    long getPeakWindowBufferEstimatedBytes();

    /** Input rows, including markers, discarded rows and SQL continuation pieces. */
    long getWindowBufferRecords();

    long getPeakWindowBufferRecords();

    /** Open transactions observed in the most recent buffer sample. */
    long getOpenTransactions();

    long getOldestReplayLsn();

    long getScannedUpperLsn();

    long getLatestArchivedLsn();

    long getStandbyReplayableLsn();

    long getStandbyReplayedLsn();

    long getStandbyMiningUpperLsn();

    /** Resident metadata or single SQL resource limit failures, not cumulative input limits. */
    long getBufferLimitFailures();

    long getLastMiningDurationMillis();

    long getMaxMiningDurationMillis();

    /** Mining session attempts, including boundary-triggered reconstruction. */
    long getMiningSessions();

    /** Mining session attempts used to reconstruct an earlier transaction's history. */
    long getReconstructionSessions();

    /** Raw rows fetched, including CSF pieces and repeated history. */
    long getMinedRows();

    /** Raw rows fetched during boundary-triggered reconstruction. */
    long getReconstructedRows();
}
