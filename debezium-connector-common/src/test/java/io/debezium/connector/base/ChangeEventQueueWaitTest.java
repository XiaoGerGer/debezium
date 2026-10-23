/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.base;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;

import org.awaitility.Awaitility;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import io.debezium.pipeline.Sizeable;
import io.debezium.util.LoggingContext;

class ChangeEventQueueWaitTest {
    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void shouldCountOnlyCapacityWaitsIncludingInterruptedWaits(boolean byteLimit) throws Exception {
        final var queue = new ChangeEventQueue.Builder<Sizeable>().maxQueueSize(10).maxBatchSize(1)
                .maxQueueSizeInBytes(byteLimit ? 1 : 0).pollInterval(Duration.ofMillis(50))
                .loggingContextSupplier(() -> LoggingContext.forConnector("test", "wait", "queue")).build();
        final Sizeable event = () -> 1L;
        final int capacity = byteLimit ? 1 : 10;
        for (int i = 0; i < capacity; i++) {
            queue.enqueue(event);
        }
        assertThat(queue.enqueueWaitNanos()).isZero();
        final var interrupted = new AtomicBoolean();
        final var writer = new Thread(() -> {
            try {
                queue.enqueue(event);
            }
            catch (InterruptedException expected) {
                interrupted.set(true);
            }
        });
        writer.start();
        try {
            Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> queue.enqueueWaitNanos() > 0);
            final long beforeInterrupt = queue.enqueueWaitNanos();
            writer.interrupt();
            writer.join(5000);
            assertThat(writer.isAlive()).isFalse();
            assertThat(interrupted).isTrue();
            assertThat(queue.enqueueWaitNanos()).isGreaterThanOrEqualTo(beforeInterrupt);
            final long beforePoll = queue.enqueueWaitNanos();
            assertThat(queue.poll()).hasSize(1);
            queue.enqueue(event);
            assertThat(queue.enqueueWaitNanos()).isEqualTo(beforePoll);
        }
        finally {
            writer.interrupt();
            writer.join(5000);
            queue.close();
        }
    }
}
