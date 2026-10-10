/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import io.debezium.annotation.ThreadSafe;
import io.debezium.connector.base.ChangeEventQueueMetrics;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.pipeline.metrics.CapturedTablesSupplier;
import io.debezium.pipeline.metrics.DefaultStreamingChangeEventSourceMetrics;
import io.debezium.pipeline.source.spi.EventMetadataProvider;

/** Extends the common streaming MBean; the coordinator owns registration and cleanup. */
@ThreadSafe
public final class DamengStreamingChangeEventSourceMetrics extends DefaultStreamingChangeEventSourceMetrics<DamengPartition>
        implements DamengStreamingChangeEventSourceMetricsMXBean {
    private final AtomicLong windowBufferEstimatedBytes = new AtomicLong();
    private final AtomicLong peakWindowBufferEstimatedBytes = new AtomicLong();
    private final AtomicLong windowBufferRecords = new AtomicLong();
    private final AtomicLong peakWindowBufferRecords = new AtomicLong();
    private final AtomicLong openTransactions = new AtomicLong();
    private final AtomicLong oldestReplayLsn = new AtomicLong();
    private final AtomicLong scannedUpperLsn = new AtomicLong();
    private final AtomicLong latestArchivedLsn = new AtomicLong();
    private final AtomicLong bufferLimitFailures = new AtomicLong();
    private final AtomicLong lastMiningDurationMillis = new AtomicLong();
    private final AtomicLong maxMiningDurationMillis = new AtomicLong();

    public DamengStreamingChangeEventSourceMetrics(CdcSourceTaskContext taskContext, ChangeEventQueueMetrics changeEventQueueMetrics,
                                                   EventMetadataProvider metadataProvider, CapturedTablesSupplier capturedTablesSupplier) {
        super(taskContext, changeEventQueueMetrics, metadataProvider, capturedTablesSupplier);
    }

    public void onWindowStarted(long replayLsn, long scannedLsn, long archivedLsn) {
        oldestReplayLsn.set(replayLsn);
        scannedUpperLsn.set(scannedLsn);
        latestArchivedLsn.set(archivedLsn);
        windowBufferEstimatedBytes.set(0);
        windowBufferRecords.set(0);
    }

    public void onBufferUpdated(long estimatedBytes, long records, long openTransactionCount) {
        onBufferUsageUpdated(estimatedBytes, records);
        openTransactions.set(openTransactionCount);
    }

    public void onBufferUsageUpdated(long estimatedBytes, long records) {
        windowBufferEstimatedBytes.set(estimatedBytes);
        peakWindowBufferEstimatedBytes.accumulateAndGet(estimatedBytes, Math::max);
        windowBufferRecords.set(records);
        peakWindowBufferRecords.accumulateAndGet(records, Math::max);
    }

    public void onWindowCompleted(long replayLsn, long scannedLsn, long durationNanos) {
        oldestReplayLsn.set(replayLsn);
        scannedUpperLsn.set(scannedLsn);
        onMiningFinished(durationNanos);
    }

    public void onMiningFinished(long durationNanos) {
        final long durationMillis = TimeUnit.NANOSECONDS.toMillis(Math.max(0, durationNanos));
        lastMiningDurationMillis.set(durationMillis);
        maxMiningDurationMillis.accumulateAndGet(durationMillis, Math::max);
    }

    public void onBufferLimitExceeded() {
        bufferLimitFailures.incrementAndGet();
    }

    public void setLatestArchivedLsn(long archivedLsn) {
        latestArchivedLsn.set(archivedLsn);
    }

    public void setOldestReplayLsn(long replayLsn) {
        oldestReplayLsn.set(replayLsn);
    }

    @Override
    public long getWindowBufferEstimatedBytes() {
        return windowBufferEstimatedBytes.get();
    }

    @Override
    public long getPeakWindowBufferEstimatedBytes() {
        return peakWindowBufferEstimatedBytes.get();
    }

    @Override
    public long getWindowBufferRecords() {
        return windowBufferRecords.get();
    }

    @Override
    public long getPeakWindowBufferRecords() {
        return peakWindowBufferRecords.get();
    }

    @Override
    public long getOpenTransactions() {
        return openTransactions.get();
    }

    @Override
    public long getOldestReplayLsn() {
        return oldestReplayLsn.get();
    }

    @Override
    public long getScannedUpperLsn() {
        return scannedUpperLsn.get();
    }

    @Override
    public long getLatestArchivedLsn() {
        return latestArchivedLsn.get();
    }

    @Override
    public long getBufferLimitFailures() {
        return bufferLimitFailures.get();
    }

    @Override
    public long getLastMiningDurationMillis() {
        return lastMiningDurationMillis.get();
    }

    @Override
    public long getMaxMiningDurationMillis() {
        return maxMiningDurationMillis.get();
    }

    @Override
    public void reset() {
        super.reset();
        windowBufferEstimatedBytes.set(0);
        peakWindowBufferEstimatedBytes.set(0);
        windowBufferRecords.set(0);
        peakWindowBufferRecords.set(0);
        openTransactions.set(0);
        oldestReplayLsn.set(0);
        scannedUpperLsn.set(0);
        latestArchivedLsn.set(0);
        bufferLimitFailures.set(0);
        lastMiningDurationMillis.set(0);
        maxMiningDurationMillis.set(0);
    }
}
