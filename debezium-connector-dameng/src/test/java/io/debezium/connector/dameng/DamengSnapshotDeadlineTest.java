/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import java.sql.Connection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;

class DamengSnapshotDeadlineTest {
    @Test
    void shouldInterruptTheSnapshotOwnerAndAbortItsConnectionAtTheDeadline() throws Exception {
        final var connection = mock(Connection.class);
        final var started = new CountDownLatch(1);
        final var finished = new CountDownLatch(1);
        final var aborted = new CountDownLatch(1);
        final var interrupted = new AtomicBoolean();
        final var deadlineRef = new AtomicReference<DamengSnapshotDeadline>();
        final var failure = new AtomicReference<Throwable>();
        doAnswer(invocation -> {
            aborted.countDown();
            return null;
        }).when(connection).abort(any());
        final var owner = new Thread(() -> {
            try (var deadline = new DamengSnapshotDeadline(connection, 50)) {
                deadlineRef.set(deadline);
                started.countDown();
                try {
                    new CountDownLatch(1).await();
                }
                catch (InterruptedException e) {
                    interrupted.set(true);
                }
            }
            catch (Throwable e) {
                failure.set(e);
            }
            finally {
                finished.countDown();
            }
        }, "dameng-snapshot-test-owner");

        owner.start();
        try {
            assertThat(started.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(finished.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(aborted.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(failure.get()).isNull();
            assertThat(interrupted.get()).isTrue();
            assertThat(deadlineRef.get().expired()).isTrue();
            verify(connection).abort(any());
        }
        finally {
            owner.interrupt();
            owner.join(TimeUnit.SECONDS.toMillis(5));
        }
    }

    @Test
    void shouldLeaveTheConnectionOpenAfterTheSnapshotFinishesWithinItsBudget() throws Exception {
        final var connection = mock(Connection.class);
        final var deadline = new DamengSnapshotDeadline(connection, 30);
        deadline.close();
        deadline.close();

        Thread.sleep(150);

        assertThat(deadline.expired()).isFalse();
        verify(connection, never()).abort(any());
    }
}
