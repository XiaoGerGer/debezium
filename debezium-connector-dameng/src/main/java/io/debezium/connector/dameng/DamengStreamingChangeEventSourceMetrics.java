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
import io.debezium.connector.dameng.logminer.LogMinerArchiveWindow;
import io.debezium.pipeline.metrics.CapturedTablesSupplier;
import io.debezium.pipeline.metrics.DefaultStreamingChangeEventSourceMetrics;
import io.debezium.pipeline.source.spi.EventMetadataProvider;

/** Extends the common streaming MBean; the coordinator owns registration and cleanup. */
@ThreadSafe
public final class DamengStreamingChangeEventSourceMetrics extends DefaultStreamingChangeEventSourceMetrics<DamengPartition>
        implements DamengStreamingChangeEventSourceMetricsMXBean {
    private final ChangeEventQueueMetrics queueMetrics;
    private volatile String recoveryNonce = "";
    private volatile long recoveryBaselineLsn;

    public void onRecoveryBaseline(DamengOffsetContext offset) {
        recoveryNonce = offset.getRecoveryNonce() == null ? "" : offset.getRecoveryNonce();
        recoveryBaselineLsn = offset.getRecoveryBaselineLsn();
    }

    @Override
    public String getRecoveryNonce() {
        return recoveryNonce;
    }

    @Override
    public long getRecoveryBaselineLsn() {
        return recoveryBaselineLsn;
    }

    private final AtomicLong queueWaitBaseline = new AtomicLong();
    private final AtomicLong currentMiningBatchSize = new AtomicLong();
    private final AtomicLong miningWindowByteBudget = new AtomicLong();
    private final AtomicLong selectedArchiveFileBytes = new AtomicLong();
    private final AtomicLong selectedArchiveFileCount = new AtomicLong();
    private final AtomicLong lastWindowLsnSpan = new AtomicLong();
    private final AtomicLong forwardMiningNanos = new AtomicLong();
    private final AtomicLong reconstructionNanos = new AtomicLong();
    private final AtomicLong dispatchNanos = new AtomicLong();
    private final AtomicLong lastForwardMiningNanos = new AtomicLong();
    private final AtomicLong lastReconstructionNanos = new AtomicLong();
    private final AtomicLong lastDispatchNanos = new AtomicLong();
    private final AtomicLong windowBufferEstimatedBytes = new AtomicLong();
    private final AtomicLong peakWindowBufferEstimatedBytes = new AtomicLong();
    private final AtomicLong windowBufferRecords = new AtomicLong();
    private final AtomicLong peakWindowBufferRecords = new AtomicLong();
    private final AtomicLong openTransactions = new AtomicLong();
    private final AtomicLong oldestReplayLsn = new AtomicLong();
    private final AtomicLong scannedUpperLsn = new AtomicLong();
    private final AtomicLong latestArchivedLsn = new AtomicLong();
    private final AtomicLong standbyReplayableLsn = new AtomicLong();
    private final AtomicLong standbyReplayedLsn = new AtomicLong();
    private final AtomicLong standbyMiningUpperLsn = new AtomicLong();

    public void onStandbyProgress(DamengConnection.StandbyProgress progress, long upperLsn) {
        standbyReplayableLsn.set(progress.replayableLsn());
        standbyReplayedLsn.set(progress.replayedLsn());
        standbyMiningUpperLsn.set(upperLsn);
    }

    @Override
    public long getStandbyReplayableLsn() {
        return standbyReplayableLsn.get();
    }

    @Override
    public long getStandbyReplayedLsn() {
        return standbyReplayedLsn.get();
    }

    @Override
    public long getStandbyMiningUpperLsn() {
        return standbyMiningUpperLsn.get();
    }

    private final AtomicLong bufferLimitFailures = new AtomicLong();
    private final AtomicLong lastMiningDurationMillis = new AtomicLong();
    private final AtomicLong maxMiningDurationMillis = new AtomicLong();
    private final AtomicLong miningSessions = new AtomicLong();
    private final AtomicLong reconstructionSessions = new AtomicLong();
    private final AtomicLong minedRows = new AtomicLong();
    private final AtomicLong reconstructedRows = new AtomicLong();
    private final AtomicLong transactionBufferMemoryBytes = new AtomicLong();
    private final AtomicLong transactionBufferDiskBytes = new AtomicLong();
    private final AtomicLong transactionBufferRecords = new AtomicLong();
    private final AtomicLong spilledTransactions = new AtomicLong();

    public void onTransactionBufferUpdated(long memoryBytes, long diskBytes, long records, long spills, long open) {
        transactionBufferMemoryBytes.set(memoryBytes);
        transactionBufferDiskBytes.set(diskBytes);
        transactionBufferRecords.set(records);
        spilledTransactions.set(spills);
        openTransactions.set(open);
    }

    @Override
    public long getTransactionBufferMemoryBytes() {
        return transactionBufferMemoryBytes.get();
    }

    @Override
    public long getTransactionBufferDiskBytes() {
        return transactionBufferDiskBytes.get();
    }

    @Override
    public long getTransactionBufferRecords() {
        return transactionBufferRecords.get();
    }

    @Override
    public long getSpilledTransactions() {
        return spilledTransactions.get();
    }

    public DamengStreamingChangeEventSourceMetrics(CdcSourceTaskContext taskContext, ChangeEventQueueMetrics changeEventQueueMetrics,
                                                   EventMetadataProvider metadataProvider, CapturedTablesSupplier capturedTablesSupplier) {
        super(taskContext, changeEventQueueMetrics, metadataProvider, capturedTablesSupplier);
        queueMetrics = changeEventQueueMetrics;
        queueWaitBaseline.set(queueMetrics.enqueueWaitNanos());
    }

    public void onMiningWindowConfigured(long fixedSpan, long budgetBytes) {
        currentMiningBatchSize.set(fixedSpan);
        miningWindowByteBudget.set(budgetBytes);
    }

    public void onArchiveWindowSelected(LogMinerArchiveWindow window, long span, long budgetBytes) {
        miningWindowByteBudget.set(budgetBytes);
        selectedArchiveFileBytes.set(window.fileBytes());
        selectedArchiveFileCount.set(window.fileCount());
        currentMiningBatchSize.set(span);
    }

    @Override
    public long getMiningWindowByteBudget() {
        return miningWindowByteBudget.get();
    }

    @Override
    public long getSelectedArchiveFileBytes() {
        return selectedArchiveFileBytes.get();
    }

    @Override
    public long getSelectedArchiveFileCount() {
        return selectedArchiveFileCount.get();
    }

    public void onIterationCompleted(long span, long forward, long reconstruction, long dispatch) {
        lastWindowLsnSpan.set(span);
        lastForwardMiningNanos.set(forward);
        lastReconstructionNanos.set(reconstruction);
        lastDispatchNanos.set(dispatch);
        forwardMiningNanos.addAndGet(forward);
        reconstructionNanos.addAndGet(reconstruction);
        dispatchNanos.addAndGet(dispatch);
    }

    @Override
    public long getCurrentMiningBatchSize() {
        return currentMiningBatchSize.get();
    }

    @Override
    public long getLastWindowLsnSpan() {
        return lastWindowLsnSpan.get();
    }

    @Override
    public long getQueueEnqueueWaitDurationMillis() {
        return TimeUnit.NANOSECONDS.toMillis(Math.max(0, queueMetrics.enqueueWaitNanos() - queueWaitBaseline.get()));
    }

    @Override
    public long getForwardMiningDurationMillis() {
        return TimeUnit.NANOSECONDS.toMillis(forwardMiningNanos.get());
    }

    @Override
    public long getReconstructionDurationMillis() {
        return TimeUnit.NANOSECONDS.toMillis(reconstructionNanos.get());
    }

    @Override
    public long getDispatchDurationMillis() {
        return TimeUnit.NANOSECONDS.toMillis(dispatchNanos.get());
    }

    @Override
    public long getLastForwardMiningDurationMillis() {
        return TimeUnit.NANOSECONDS.toMillis(lastForwardMiningNanos.get());
    }

    @Override
    public long getLastReconstructionDurationMillis() {
        return TimeUnit.NANOSECONDS.toMillis(lastReconstructionNanos.get());
    }

    @Override
    public long getLastDispatchDurationMillis() {
        return TimeUnit.NANOSECONDS.toMillis(lastDispatchNanos.get());
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

    public void onMiningSessionStarted(boolean reconstruction) {
        miningSessions.incrementAndGet();
        if (reconstruction) {
            reconstructionSessions.incrementAndGet();
        }
    }

    public void onMiningRowsRead(long rows, boolean reconstruction) {
        minedRows.addAndGet(rows);
        if (reconstruction) {
            reconstructedRows.addAndGet(rows);
        }
    }

    @Override
    public long getMiningSessions() {
        return miningSessions.get();
    }

    @Override
    public long getReconstructionSessions() {
        return reconstructionSessions.get();
    }

    @Override
    public long getMinedRows() {
        return minedRows.get();
    }

    @Override
    public long getReconstructedRows() {
        return reconstructedRows.get();
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
        queueWaitBaseline.set(queueMetrics.enqueueWaitNanos());
        lastWindowLsnSpan.set(0);
        selectedArchiveFileBytes.set(0);
        selectedArchiveFileCount.set(0);
        forwardMiningNanos.set(0);
        reconstructionNanos.set(0);
        dispatchNanos.set(0);
        lastForwardMiningNanos.set(0);
        lastReconstructionNanos.set(0);
        lastDispatchNanos.set(0);
        // Retain the configured budget and planned span across a JMX reset.
        windowBufferEstimatedBytes.set(0);
        peakWindowBufferEstimatedBytes.set(0);
        windowBufferRecords.set(0);
        peakWindowBufferRecords.set(0);
        openTransactions.set(0);
        oldestReplayLsn.set(0);
        scannedUpperLsn.set(0);
        latestArchivedLsn.set(0);
        standbyReplayableLsn.set(0);
        standbyReplayedLsn.set(0);
        standbyMiningUpperLsn.set(0);
        bufferLimitFailures.set(0);
        lastMiningDurationMillis.set(0);
        maxMiningDurationMillis.set(0);
        miningSessions.set(0);
        reconstructionSessions.set(0);
        minedRows.set(0);
        reconstructedRows.set(0);
        transactionBufferMemoryBytes.set(0);
        transactionBufferDiskBytes.set(0);
        transactionBufferRecords.set(0);
        spilledTransactions.set(0);
    }
}
