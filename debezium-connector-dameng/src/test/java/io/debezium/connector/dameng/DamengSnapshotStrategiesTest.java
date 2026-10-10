/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.debezium.connector.dameng.snapshot.NeverSnapshotter;
import io.debezium.connector.dameng.snapshot.SelectAllSnapshotQuery;
import io.debezium.connector.dameng.snapshot.SharedSnapshotLock;

class DamengSnapshotStrategiesTest {
    @Test
    void shouldLockTheNamedTableInShareModeForTheInitialSnapshot() {
        final var lock = new SharedSnapshotLock();
        lock.configure(Map.of());

        assertThat(lock.name()).isEqualTo("shared");
        assertThat(lock.tableLockingStatement(Duration.ofSeconds(5), "\"APP\".\"EVENTS\""))
                .contains("LOCK TABLE \"APP\".\"EVENTS\" IN SHARE MODE");
    }

    @Test
    void shouldSelectOnlyTheRequestedColumnsFromTheNamedTable() {
        final var query = new SelectAllSnapshotQuery();
        query.configure(Map.of());

        assertThat(query.name()).isEqualTo("select_all");
        assertThat(query.snapshotQuery("\"APP\".\"EVENTS\"", List.of("\"ID\"", "\"VALUE\"")))
                .contains("SELECT \"ID\", \"VALUE\" FROM \"APP\".\"EVENTS\"");
    }

    @Test
    void shouldKeepTheLegacyNeverModeAvailableForSchemaOnlySnapshots() {
        final var snapshotter = new NeverSnapshotter();

        assertThat(snapshotter.name()).isEqualTo("never");
        assertThat(snapshotter.shouldSnapshotData(false, false)).isFalse();
        assertThat(snapshotter.shouldStream()).isTrue();
    }
}
