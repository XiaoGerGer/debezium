/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.lang.management.ManagementFactory;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import javax.management.ObjectName;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.connector.base.ChangeEventQueueMetrics;

class DamengStreamingMetricsTest {
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
    void shouldRetainWindowPeaksAndMeasureProgressWithoutInventingHeapUsage() {
        metrics.onWindowStarted(10, 20, 30);
        metrics.onBufferUpdated(4096, 100, 2);
        metrics.onWindowCompleted(12, 25, TimeUnit.MILLISECONDS.toNanos(17));
        metrics.onWindowStarted(12, 25, 40);
        metrics.onBufferUpdated(1024, 40, 1);
        metrics.onWindowCompleted(12, 35, TimeUnit.MILLISECONDS.toNanos(5));
        metrics.setLatestArchivedLsn(45);
        metrics.onBufferLimitExceeded();

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
    }

    @Test
    void shouldExposeCustomAndCommonJmxAttributesAndUnregisterForRestart() throws Exception {
        final var objectName = new ObjectName("debezium.dameng:type=connector-metrics,context=streaming,server=" + server);
        final var mbeanServer = ManagementFactory.getPlatformMBeanServer();
        metrics.onBufferUpdated(1234, 12, 1);
        metrics.onWindowStarted(2, 3, 5);
        metrics.onBufferLimitExceeded();
        metrics.register();
        try {
            assertThat(mbeanServer.isRegistered(objectName)).isTrue();
            assertThat(mbeanServer.getAttribute(objectName, "PeakWindowBufferEstimatedBytes")).isEqualTo(1234L);
            assertThat(mbeanServer.getAttribute(objectName, "BufferLimitFailures")).isEqualTo(1L);
            assertThat(mbeanServer.getAttribute(objectName, "LatestArchivedLsn")).isEqualTo(5L);
            assertThat(mbeanServer.getAttribute(objectName, "Connected")).isEqualTo(false);
            mbeanServer.invoke(objectName, "reset", new Object[0], new String[0]);
            assertThat(mbeanServer.getAttribute(objectName, "PeakWindowBufferEstimatedBytes")).isEqualTo(0L);
            assertThat(mbeanServer.getAttribute(objectName, "BufferLimitFailures")).isEqualTo(0L);
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
