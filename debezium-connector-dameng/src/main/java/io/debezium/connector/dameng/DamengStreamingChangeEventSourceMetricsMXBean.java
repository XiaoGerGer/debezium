/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng;

import io.debezium.pipeline.metrics.StreamingChangeEventSourceMetricsMXBean;

/** Dameng archive mining and cumulative window reservation measurements. */
public interface DamengStreamingChangeEventSourceMetricsMXBean extends StreamingChangeEventSourceMetricsMXBean {
    /** Estimated byte reservation for the current or most recently observed window, not live JVM heap. */
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

    long getBufferLimitFailures();

    long getLastMiningDurationMillis();

    long getMaxMiningDurationMillis();
}
