/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.management.ManagementFactory;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import javax.management.ObjectName;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.connector.base.ChangeEventQueueMetrics;
import io.debezium.connector.dameng.logminer.LogMinerArchiveWindow;

class DamengStreamingMetricsTest {
    @Test
    void shouldExposeArchiveBudgetSeparatelyFromLsnAndHeapMetrics() throws Exception {
        assertThat(metrics.getMiningWindowByteBudget()).isZero();
        metrics.onMiningWindowConfigured(0, 64);
        assertThat(metrics.getMiningWindowByteBudget()).isEqualTo(64);
        assertThat(metrics.getCurrentMiningBatchSize()).isZero();
        metrics.onArchiveWindowSelected(new LogMinerArchiveWindow(200, 128, 2), 110, 64);
        final var objectName = new ObjectName("debezium.dameng:type=connector-metrics,context=streaming,server=" + server);
        metrics.register();
        try {
            final var mbeans = ManagementFactory.getPlatformMBeanServer();
            assertThat(mbeans.getAttribute(objectName, "MiningWindowByteBudget")).isEqualTo(64L);
            assertThat(mbeans.getAttribute(objectName, "SelectedArchiveFileBytes")).isEqualTo(128L);
            assertThat(mbeans.getAttribute(objectName, "SelectedArchiveFileCount")).isEqualTo(2L);
            assertThat(metrics.getCurrentMiningBatchSize()).isEqualTo(110);
            metrics.reset();
            assertThat(metrics.getMiningWindowByteBudget()).isEqualTo(64);
            assertThat(metrics.getSelectedArchiveFileBytes()).isZero();
            assertThat(metrics.getSelectedArchiveFileCount()).isZero();
        }
        finally {
            metrics.unregister();
        }
    }

    @Test
    void shouldExposeRecoveryBaselineWithoutResettingItWithCounters() throws Exception {
        final var offset = new DamengOffsetContext(new DamengConnectorConfig(DamengConnectorConfigTest.configuration().build()), 100);
        metrics.onRecoveryBaseline(offset);
        assertThat(metrics.getRecoveryNonce()).isEmpty();
        assertThat(metrics.getRecoveryBaselineLsn()).isZero();
        offset.setRecoveryBaseline("incident-1", 100);
        metrics.onRecoveryBaseline(offset);
        final var objectName = new ObjectName("debezium.dameng:type=connector-metrics,context=streaming,server=" + server);
        metrics.register();
        try {
            final var mbeans = ManagementFactory.getPlatformMBeanServer();
            assertThat(mbeans.getAttribute(objectName, "RecoveryNonce")).isEqualTo("incident-1");
            assertThat(mbeans.getAttribute(objectName, "RecoveryBaselineLsn")).isEqualTo(100L);
            metrics.reset();
            assertThat(metrics.getRecoveryNonce()).isEqualTo("incident-1");
            assertThat(metrics.getRecoveryBaselineLsn()).isEqualTo(100);
        }
        finally {
            metrics.unregister();
        }
    }

    private DamengTaskContext taskContext;
    private DamengStreamingChangeEventSourceMetrics metrics;
    private ChangeEventQueueMetrics queue;
    private DamengEventMetadataProvider metadata;
    private String server;

    @BeforeEach
    void before() {
        server = "dameng-metrics-" + UUID.randomUUID();
        final var rawConfig = DamengConnectorConfigTest.configuration().with("topic.prefix", server).build();
        final var config = new DamengConnectorConfig(rawConfig);
        taskContext = new DamengTaskContext(rawConfig, config);
        queue = mock(ChangeEventQueueMetrics.class);
        metadata = new DamengEventMetadataProvider();
        metrics = new DamengStreamingChangeEventSourceMetrics(taskContext, queue, metadata, List::of);
    }

    @Test
    void shouldSeparatePhaseDurationsAndRetainFixedSpanAfterMetricReset() throws Exception {
        metrics.onMiningWindowConfigured(1000, 0);
        assertThat(metrics.getMiningWindowByteBudget()).isZero();
        when(queue.enqueueWaitNanos()).thenReturn(20_000_000L);
        metrics.onIterationCompleted(123, 1_500_000, 2_500_000, 30_000_000);
        metrics.onIterationCompleted(456, 1_500_000, 0, 40_000_000);
        assertThat(metrics.getForwardMiningDurationMillis()).isEqualTo(3);
        assertThat(metrics.getReconstructionDurationMillis()).isEqualTo(2);
        assertThat(metrics.getDispatchDurationMillis()).isEqualTo(70);
        assertThat(metrics.getLastForwardMiningDurationMillis()).isEqualTo(1);
        assertThat(metrics.getLastReconstructionDurationMillis()).isZero();
        assertThat(metrics.getLastDispatchDurationMillis()).isEqualTo(40);
        assertThat(metrics.getQueueEnqueueWaitDurationMillis()).isEqualTo(20);
        assertThat(metrics.getLastWindowLsnSpan()).isEqualTo(456);
        final var objectName = new ObjectName("debezium.dameng:type=connector-metrics,context=streaming,server=" + server);
        metrics.register();
        try {
            final var server = ManagementFactory.getPlatformMBeanServer();
            assertThat(server.getMBeanInfo(objectName).getAttributes()).extracting(javax.management.MBeanAttributeInfo::getName)
                    .doesNotContain("WindowIncreases", "WindowDecreases", "WindowAdjustmentReason");
            assertThat(server.getAttribute(objectName, "CurrentMiningBatchSize")).isEqualTo(1000L);
            assertThat(server.getAttribute(objectName, "QueueEnqueueWaitDurationMillis")).isEqualTo(20L);
            metrics.reset();
            assertThat(metrics.getQueueEnqueueWaitDurationMillis()).isZero();
            assertThat(metrics.getForwardMiningDurationMillis()).isZero();
            assertThat(metrics.getReconstructionDurationMillis()).isZero();
            assertThat(metrics.getDispatchDurationMillis()).isZero();
            assertThat(metrics.getLastForwardMiningDurationMillis()).isZero();
            assertThat(metrics.getLastDispatchDurationMillis()).isZero();
            assertThat(metrics.getLastWindowLsnSpan()).isZero();
            assertThat(metrics.getCurrentMiningBatchSize()).isEqualTo(1000);
        }
        finally {
            metrics.unregister();
        }
        when(queue.enqueueWaitNanos()).thenReturn(25_000_000L);
        assertThat(metrics.getQueueEnqueueWaitDurationMillis()).isEqualTo(5);
    }

    @Test
    void shouldRetainWindowPeaksAndMeasureProgressWithoutInventingHeapUsage() {
        metrics.onWindowStarted(10, 20, 30);
        metrics.onBufferUpdated(4096, 100, 2);
        metrics.onWindowCompleted(12, 25, TimeUnit.MILLISECONDS.toNanos(17));
        metrics.onWindowStarted(12, 25, 40);
        metrics.onBufferUpdated(1024, 40, 1);
        metrics.onWindowCompleted(12, 35, TimeUnit.MILLISECONDS.toNanos(5));
        metrics.setLatestArchivedLsn(45);
        metrics.onBufferLimitExceeded();
        metrics.onMiningSessionStarted(false);
        metrics.onMiningRowsRead(40, false);
        metrics.onMiningSessionStarted(true);
        metrics.onMiningRowsRead(100, true);

        assertThat(metrics.getWindowBufferEstimatedBytes()).isEqualTo(1024);
        assertThat(metrics.getPeakWindowBufferEstimatedBytes()).isEqualTo(4096);
        assertThat(metrics.getWindowBufferRecords()).isEqualTo(40);
        assertThat(metrics.getPeakWindowBufferRecords()).isEqualTo(100);
        assertThat(metrics.getOpenTransactions()).isEqualTo(1);
        assertThat(metrics.getOldestReplayLsn()).isEqualTo(12);
        assertThat(metrics.getScannedUpperLsn()).isEqualTo(35);
        assertThat(metrics.getLatestArchivedLsn()).isEqualTo(45);
        assertThat(metrics.getLastMiningDurationMillis()).isEqualTo(5);
        assertThat(metrics.getMaxMiningDurationMillis()).isEqualTo(17);
        assertThat(metrics.getBufferLimitFailures()).isEqualTo(1);
        assertThat(metrics.getMiningSessions()).isEqualTo(2);
        assertThat(metrics.getReconstructionSessions()).isEqualTo(1);
        assertThat(metrics.getMinedRows()).isEqualTo(140);
        assertThat(metrics.getReconstructedRows()).isEqualTo(100);
    }

    @Test
    void shouldExposeCustomAndCommonJmxAttributesAndUnregisterForRestart() throws Exception {
        final var objectName = new ObjectName("debezium.dameng:type=connector-metrics,context=streaming,server=" + server);
        final var mbeanServer = ManagementFactory.getPlatformMBeanServer();
        metrics.onBufferUpdated(1234, 12, 1);
        metrics.onWindowStarted(2, 3, 5);
        metrics.onBufferLimitExceeded();
        metrics.onMiningSessionStarted(true);
        metrics.onMiningRowsRead(100, true);
        metrics.register();
        try {
            metrics.onTransactionBufferUpdated(1024, 2048, 30, 4, 2);
            assertThat(mbeanServer.getAttribute(objectName, "TransactionBufferMemoryBytes")).isEqualTo(1024L);
            assertThat(mbeanServer.getAttribute(objectName, "TransactionBufferDiskBytes")).isEqualTo(2048L);
            assertThat(mbeanServer.getAttribute(objectName, "TransactionBufferRecords")).isEqualTo(30L);
            assertThat(mbeanServer.getAttribute(objectName, "SpilledTransactions")).isEqualTo(4L);
            assertThat(mbeanServer.isRegistered(objectName)).isTrue();
            assertThat(mbeanServer.getAttribute(objectName, "PeakWindowBufferEstimatedBytes")).isEqualTo(1234L);
            assertThat(mbeanServer.getAttribute(objectName, "BufferLimitFailures")).isEqualTo(1L);
            assertThat(mbeanServer.getAttribute(objectName, "LatestArchivedLsn")).isEqualTo(5L);
            assertThat(mbeanServer.getAttribute(objectName, "Connected")).isEqualTo(false);
            assertThat(mbeanServer.getAttribute(objectName, "MiningSessions")).isEqualTo(1L);
            assertThat(mbeanServer.getAttribute(objectName, "ReconstructionSessions")).isEqualTo(1L);
            assertThat(mbeanServer.getAttribute(objectName, "MinedRows")).isEqualTo(100L);
            assertThat(mbeanServer.getAttribute(objectName, "ReconstructedRows")).isEqualTo(100L);
            mbeanServer.invoke(objectName, "reset", new Object[0], new String[0]);
            assertThat(mbeanServer.getAttribute(objectName, "PeakWindowBufferEstimatedBytes")).isEqualTo(0L);
            assertThat(mbeanServer.getAttribute(objectName, "BufferLimitFailures")).isEqualTo(0L);
            assertThat(mbeanServer.getAttribute(objectName, "MiningSessions")).isEqualTo(0L);
            assertThat(mbeanServer.getAttribute(objectName, "ReconstructionSessions")).isEqualTo(0L);
            assertThat(mbeanServer.getAttribute(objectName, "MinedRows")).isEqualTo(0L);
            assertThat(mbeanServer.getAttribute(objectName, "ReconstructedRows")).isEqualTo(0L);
            assertThat(mbeanServer.getAttribute(objectName, "TransactionBufferMemoryBytes")).isEqualTo(0L);
            assertThat(mbeanServer.getAttribute(objectName, "TransactionBufferDiskBytes")).isEqualTo(0L);
            assertThat(mbeanServer.getAttribute(objectName, "TransactionBufferRecords")).isEqualTo(0L);
            assertThat(mbeanServer.getAttribute(objectName, "SpilledTransactions")).isEqualTo(0L);
        }
        finally {
            metrics.unregister();
        }
        assertThat(mbeanServer.isRegistered(objectName)).isFalse();

        final var restarted = new DamengStreamingChangeEventSourceMetrics(taskContext, queue, metadata, List::of);
        restarted.register();
        try {
            assertThat(mbeanServer.isRegistered(objectName)).isTrue();
            assertThat(mbeanServer.getAttribute(objectName, "BufferLimitFailures")).isEqualTo(0L);
        }
        finally {
            restarted.unregister();
        }
        assertThat(mbeanServer.isRegistered(objectName)).isFalse();
    }

    @Test
    void shouldShareTheSourceMetricsWithTheCoordinatorFactory() {
        final var factory = new DamengChangeEventSourceMetricsFactory(metrics);

        assertThat(factory.getStreamingMetrics(taskContext, queue, metadata, List::of)).isSameAs(metrics);
        assertThat(factory.getSnapshotMetrics(taskContext, queue, metadata)).isNotNull();
    }
}
