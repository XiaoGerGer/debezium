/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;

import org.junit.jupiter.api.Test;

import io.debezium.relational.history.HistoryRecord;
import io.debezium.relational.history.HistoryRecordComparator;

class DamengHistoryRecordComparatorTest {
    private final HistoryRecordComparator comparator = new DamengConnectorConfig(DamengConnectorConfigTest.configuration().build())
            .getHistoryRecordComparator();

    @Test
    void shouldRestoreAllDdlAtCompletedCommitDespiteResetSerial() {
        assertThat(comparator.isAtOrBefore(record(Map.of("lsn", 10L, "commit_lsn", 100L, "event_serial_no", 7L, "mining_transaction_id", "TX1")),
                record(Map.of("lsn", 20L, "commit_lsn", 100L, "event_serial_no", 0L)))).isTrue();
    }

    @Test
    void shouldRestoreOnlyPublishedDdlWithinIncompleteTransaction() {
        final var desired = record(Map.of("lsn", 10L, "commit_lsn", 100L, "event_serial_no", 3L, "mining_transaction_id", "TX1"));
        assertThat(comparator.isAtOrBefore(record(Map.of("commit_lsn", 100L, "event_serial_no", 3L, "mining_transaction_id", "TX1")), desired)).isTrue();
        assertThat(comparator.isAtOrBefore(record(Map.of("commit_lsn", 100L, "event_serial_no", 4L, "mining_transaction_id", "TX1")), desired)).isFalse();
    }

    @Test
    void shouldOrderTransactionsSharingCommitLsnBeforeComparingSerials() {
        final var desired = record(Map.of("commit_lsn", 100L, "event_serial_no", 3L, "mining_transaction_id", "TX2"));
        assertThat(comparator.isAtOrBefore(record(Map.of("commit_lsn", 100L, "event_serial_no", 9L, "mining_transaction_id", "TX1")), desired)).isTrue();
        assertThat(comparator.isAtOrBefore(record(Map.of("commit_lsn", 100L, "event_serial_no", 1L, "mining_transaction_id", "TX3")), desired)).isFalse();
    }

    @Test
    void shouldRestoreCompletedTransactionButExcludeNextTransactionSharingCommitLsn() {
        final var desired = record(Map.of("commit_lsn", 100L, "event_serial_no", Long.MAX_VALUE, "mining_transaction_id", "TX1"));
        assertThat(comparator.isAtOrBefore(record(Map.of("commit_lsn", 100L, "event_serial_no", 7L, "mining_transaction_id", "TX1")), desired)).isTrue();
        assertThat(comparator.isAtOrBefore(record(Map.of("commit_lsn", 100L, "event_serial_no", 1L, "mining_transaction_id", "TX2")), desired)).isFalse();
    }

    @Test
    void shouldCompareMiningIdentityIndependentlyOfTransactionMetadata() {
        final var desired = record(Map.of("commit_lsn", 100L, "event_serial_no", 3L,
                "mining_transaction_id", "TX2", "transaction_id", "META1"));
        assertThat(comparator.isAtOrBefore(record(Map.of("commit_lsn", 100L, "event_serial_no", 9L,
                "mining_transaction_id", "TX1", "transaction_id", "META3")), desired)).isTrue();
        assertThat(comparator.isAtOrBefore(record(Map.of("commit_lsn", 100L, "event_serial_no", 1L,
                "mining_transaction_id", "TX3", "transaction_id", "META0")), desired)).isFalse();
    }

    @Test
    void shouldRecoverLegacyHistoryAgainstNewMiningPosition() {
        final var desired = record(Map.of("commit_lsn", 100L, "event_serial_no", 3L, "mining_transaction_id", "TX2"));
        assertThat(comparator.isAtOrBefore(record(Map.of("commit_lsn", 100L, "event_serial_no", 9L, "transaction_id", "TX1")), desired)).isTrue();
        assertThat(comparator.isAtOrBefore(record(Map.of("commit_lsn", 100L, "event_serial_no", 1L, "transaction_id", "TX3")), desired)).isFalse();
    }

    @Test
    void shouldCompareCommittedPositionInsteadOfSafeReplayPosition() {
        final var desired = record(Map.of("lsn", 10L, "commit_lsn", 100L));
        assertThat(comparator.isAtOrBefore(record(Map.of("lsn", 50L, "commit_lsn", 90L)), desired)).isTrue();
        assertThat(comparator.isAtOrBefore(record(Map.of("lsn", 1L, "commit_lsn", 101L)), desired)).isFalse();
    }

    @Test
    void shouldFallBackToSnapshotLsn() {
        assertThat(comparator.isAtOrBefore(record(Map.of("lsn", 100L)), record(Map.of("lsn", 100L)))).isTrue();
        assertThat(comparator.isAtOrBefore(record(Map.of("lsn", 101L)), record(Map.of("lsn", 100L)))).isFalse();
    }

    private static HistoryRecord record(Map<String, ?> position) {
        return new HistoryRecord(Map.of("server", "dameng"), position, "DAMENG", "APP", null, null, null);
    }
}
