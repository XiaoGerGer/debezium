/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.connector.dameng.compatibility.DamengCompatibilityEnvironment;
import io.debezium.connector.dameng.compatibility.DamengCompatibilityMode;

class DamengStandbySupportTest {
    private final DamengConnection primary = mock(DamengConnection.class);
    private final DamengConnection reader = mock(DamengConnection.class);
    private final DamengDatabaseIdentity primaryIdentity = identity(1, 10, 10, "open");
    private DamengStandbySupport support;

    @BeforeEach
    void before() throws Exception {
        final var config = new DamengConnectorConfig(DamengConnectorConfigTest.configuration()
                .with("database.standby.hosts", "replica:5236").with("database.standby.replay.timeout.ms", 1).build());
        support = new DamengStandbySupport(config, primary);
        when(primary.readPrimaryStandbyIdentity()).thenReturn(primaryIdentity);
        when(reader.readPrimaryStandbyIdentity()).thenReturn(identity(1, 10, 20, "open"));
        when(primary.currentLsn()).thenReturn(100L);
        when(reader.readStandbyProgress()).thenReturn(new DamengConnection.StandbyProgress(150, 100));
        final var environment = new DamengCompatibilityEnvironment(new DamengCompatibilityMode("0"), Map.of());
        when(primary.getCompatibilityEnvironment()).thenReturn(environment);
        when(reader.getCompatibilityEnvironment()).thenReturn(environment);
        when(primary.getDatabaseCharset()).thenReturn(StandardCharsets.UTF_8);
        when(reader.getDatabaseCharset()).thenReturn(StandardCharsets.UTF_8);
    }

    @Test
    void shouldKeepPrimaryLocksWhileResettingTheReplicaReadTransactionAtTheBarrier() throws Exception {
        support.initialize(reader);
        assertThat(support.primary()).isSameAs(primary);
        assertThat(support.snapshotBoundary(reader)).isEqualTo(100);
        verify(reader).commit();
        verify(primary, never()).commit();
        assertThat(new DamengConnection.StandbyProgress(50, 60).safeLsn()).isEqualTo(50);
        assertThat(new DamengConnection.StandbyProgress(60, 40).safeLsn()).isEqualTo(40);
    }

    @Test
    void shouldRejectForeignReplicaBeforeWaitingForItsUnrelatedLsn() throws Exception {
        when(reader.readPrimaryStandbyIdentity()).thenReturn(identity(2, 10, 20, "open"));
        assertThatThrownBy(() -> support.initialize(reader)).hasMessageContaining("different database families");
        verify(reader, never()).readStandbyProgress();
    }

    @Test
    void shouldRejectPrimaryUsedAsReplica() throws Exception {
        when(reader.readPrimaryStandbyIdentity()).thenReturn(primaryIdentity);
        assertThatThrownBy(() -> support.initialize(reader)).hasMessageContaining("different physical members");
    }

    @Test
    void shouldRetryWhenReplicaHasNotReplayedThePrimaryOpenRecord() throws Exception {
        when(reader.readPrimaryStandbyIdentity()).thenReturn(identity(1, 10, 20, "old-open"));
        assertThatThrownBy(() -> support.initialize(reader)).isInstanceOf(SQLException.class).hasMessageContaining("current primary-open history");
    }

    @Test
    void shouldRejectDifferentCharsetAndCompatibilitySettings() throws Exception {
        when(reader.getDatabaseCharset()).thenReturn(StandardCharsets.US_ASCII);
        assertThatThrownBy(() -> support.initialize(reader)).hasMessageContaining("different compatibility settings or character sets");
        when(reader.getDatabaseCharset()).thenReturn(StandardCharsets.UTF_8);
        when(reader.getCompatibilityEnvironment()).thenReturn(new DamengCompatibilityEnvironment(new DamengCompatibilityMode("0"), Map.of("CASE_SENSITIVE", "0")));
        assertThatThrownBy(() -> support.initialize(reader)).hasMessageContaining("different compatibility settings or character sets");
    }

    @Test
    void shouldWaitForCompletedReplayAndFailOnTimeoutWithoutCommitting() throws Exception {
        when(reader.readStandbyProgress()).thenReturn(new DamengConnection.StandbyProgress(200, 90));
        assertThatThrownBy(() -> support.initialize(reader)).isInstanceOf(SQLException.class).hasMessageContaining("did not reach primary LSN 100");
        verify(reader, never()).commit();
        verify(primary, never()).commit();
    }

    @Test
    void shouldAbortTheBarrierIfThePrimaryChanges() throws Exception {
        support.initialize(reader);
        when(primary.readPrimaryStandbyIdentity()).thenReturn(identity(1, 20, 20, "promoted"));
        assertThatThrownBy(() -> support.snapshotBoundary(reader)).isInstanceOf(SQLException.class).hasMessageContaining("primary changed");
        verify(reader, never()).commit();
    }

    @Test
    void shouldPreserveInterruptionDuringReplayWait() throws Exception {
        support.initialize(reader);
        when(reader.readStandbyProgress()).thenReturn(new DamengConnection.StandbyProgress(200, 90));
        Thread.currentThread().interrupt();
        try {
            assertThatThrownBy(() -> support.awaitReplay(reader, 100)).isInstanceOf(SQLException.class).hasMessageContaining("Interrupted");
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
        }
        finally {
            Thread.interrupted();
        }
    }

    private static DamengDatabaseIdentity identity(long family, long source, long node, String guid) {
        return new DamengDatabaseIdentity(family, source, 200,
                List.of(new DamengDatabaseIdentity.OpenRecord(guid, Instant.EPOCH, source, 0)), node);
    }
}
