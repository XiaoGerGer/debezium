/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng.logminer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import io.debezium.connector.dameng.logminer.LogMinerBatch.LogMinerRow;

class LogMinerBatchTest {
    private static final Instant TIMESTAMP = Instant.parse("2026-09-24T01:02:03Z");

    @Test
    void shouldKeepOpenTransactionPinnedAfterAnotherCommit() {
        final var batch = new LogMinerBatch();
        batch.add(row("open", 12, 10, 1, 0, "INSERT open", "r1", 0));
        batch.add(row("committed", 22, 20, 1, 0, "INSERT committed", "r2", 0));
        batch.add(row("committed", 30, 20, 7, 0, null, "r3", 0));
        batch.finish();

        assertThat(batch.committedTransactions()).extracting(LogMinerBatch.Transaction::id).containsExactly("committed");
        assertThat(batch.restartLsn(40)).isEqualTo(10);
    }

    @Test
    void shouldNotCommitStartOrPreCommitMarkers() {
        final var batch = new LogMinerBatch();
        batch.add(row("tx", 10, 10, 6, 0, null, "r1", 0));
        batch.add(row("tx", 11, 10, 1, 0, "INSERT", "r2", 0));
        batch.add(row("tx", 12, 10, 15, 0, null, "r3", 0));

        assertThat(batch.committedTransactions()).isEmpty();
        assertThat(batch.restartLsn(20)).isEqualTo(10);
    }

    @Test
    void shouldReleaseFullyRolledBackTransaction() {
        final var batch = new LogMinerBatch();
        batch.add(row("tx", 10, 10, 1, 1, "INSERT rolled back", "r1", 0));
        batch.add(row("tx", 15, 10, 36, 0, null, "r2", 0));

        assertThat(batch.committedTransactions()).isEmpty();
        assertThat(batch.restartLsn(20)).isEqualTo(20);
    }

    @Test
    void shouldKeepSavepointTransactionPinnedUntilItCommits() {
        final var batch = new LogMinerBatch();
        batch.add(row("tx", 10, 10, 1, 0, "INSERT keep", "r1", 0));
        batch.add(row("tx", 11, 10, 3, 1, "UPDATE discard", "r2", 0));
        batch.add(row("tx", 12, 10, 36, 0, null, "r3", 0));

        assertThat(batch.committedTransactions()).isEmpty();
        assertThat(batch.restartLsn(20)).isEqualTo(10);

        batch.add(row("tx", 21, 10, 7, 0, null, "r4", 0));
        assertThat(batch.committedTransactions()).singleElement()
                .satisfies(tx -> assertThat(tx.records()).extracting(LogMinerRow::redo).containsExactly("INSERT keep"));
        assertThat(batch.restartLsn(30)).isEqualTo(30);
    }

    @Test
    void shouldNotDiscardDmlForSequenceAllocationWithSameTransactionId() {
        final var batch = new LogMinerBatch();
        batch.add(row("tx", 10, 10, 1, 0, "INSERT", "r1", 0));
        batch.add(row("tx", 11, 10, 37, 0, "SEQ MODIFY", "r2", 0));
        batch.add(row("tx", 12, 10, 7, 0, null, "r3", 0));

        assertThat(batch.committedTransactions()).singleElement()
                .satisfies(tx -> assertThat(tx.records()).extracting(LogMinerRow::redo).containsExactly("INSERT"));
    }

    @Test
    void shouldIgnoreSequenceAllocationWithoutATransactionId() {
        final var batch = new LogMinerBatch();
        batch.add(row(null, 10, 10, 37, 0, "SEQ MODIFY", "r1", 0));

        assertThat(batch.getBufferedRecordCount()).isEqualTo(1);
        assertThat(batch.getOpenTransactionCount()).isZero();
        assertThat(batch.committedTransactions()).isEmpty();
        assertThat(batch.restartLsn(20)).isEqualTo(20);
    }

    @ParameterizedTest
    @ValueSource(ints = { 7, 38 })
    void shouldRecognizeCommitAndXaCommit(int operation) {
        final var batch = new LogMinerBatch();
        batch.add(row("tx", 10, 10, 1, 0, "INSERT", "r1", 0));
        batch.add(row("tx", 20, 10, operation, 0, null, "r2", 0));

        assertThat(batch.committedTransactions()).singleElement().satisfies(tx -> {
            assertThat(tx.commitLsn()).isEqualTo(20);
            assertThat(tx.startLsn()).isEqualTo(10);
        });
    }

    @Test
    void shouldSortTransactionsByCommitPositionAndPreserveStatementOrder() {
        final var batch = new LogMinerBatch();
        batch.add(row("later", 10, 10, 1, 0, "INSERT later", "r1", 0));
        batch.add(row("earlier", 20, 20, 1, 0, "INSERT earlier", "r2", 0));
        batch.add(row("later", 11, 10, 3, 0, "UPDATE later", "r3", 0));
        batch.add(row("later", 40, 10, 7, 0, null, "r4", 0));
        batch.add(row("earlier", 30, 20, 7, 0, null, "r5", 0));

        assertThat(batch.committedTransactions()).extracting(LogMinerBatch.Transaction::id).containsExactly("earlier", "later");
        assertThat(batch.committedTransactions().get(1).records()).extracting(LogMinerRow::redo)
                .containsExactly("INSERT later", "UPDATE later");
    }

    @Test
    void shouldDeterministicallyOrderTransactionsSharingCommitPosition() {
        final var batch = new LogMinerBatch();
        batch.add(row("B", 11, 10, 1, 0, "B", "r1", 0));
        batch.add(row("A", 12, 10, 1, 0, "A", "r2", 0));
        batch.add(row("B", 20, 10, 7, 0, null, "r3", 0));
        batch.add(row("A", 20, 10, 7, 0, null, "r4", 0));

        assertThat(batch.committedTransactions()).extracting(LogMinerBatch.Transaction::id).containsExactly("A", "B");
    }

    @Test
    void shouldConcatenateSqlAcrossSeveralContinuationRows() {
        final var batch = new LogMinerBatch();
        final String first = "INSERT INTO \"APP\".\"EVENTS\"(\"NAME\") VALUES('" + "a".repeat(3950);
        final String second = "达梦''引号".repeat(900);
        final String third = "end')";
        batch.add(row("tx", 10, 10, 1, 0, first, "r1", 1));
        batch.add(row("tx", 10, 10, 1, 0, second, "r1", 1));
        batch.add(row("tx", 10, 10, 1, 0, third, "r1", 0));
        batch.add(row("tx", 20, 10, 7, 0, null, "r2", 0));

        assertThat(batch.committedTransactions()).singleElement().satisfies(tx -> {
            assertThat(tx.records()).hasSize(1);
            assertThat(tx.records().get(0).redo()).isEqualTo(first + second + third);
            assertThat(tx.records().get(0).timestamp()).isEqualTo(TIMESTAMP);
            assertThat(tx.records().get(0).continuation()).isZero();
        });
    }

    @Test
    void shouldPreserveTheFirstContinuationPieceMetadataForHistoricalTableLookup() {
        final var batch = new LogMinerBatch();
        final var first = new LogMinerRow(10, 8, 20, "tx", 1, 0, "APP", "T", "APP", "INSERT INTO ",
                "r1", 1, 1, TIMESTAMP, 42);
        final var last = new LogMinerRow(10, 0, 0, "tx", 1, 0, null, null, null, "T VALUES(1)",
                "r1", 2, 0, TIMESTAMP, 0);
        batch.add(first);
        batch.add(last);
        batch.add(row("tx", 20, 8, 7, 0, null, "r2", 0));

        assertThat(batch.committedTransactions()).singleElement().satisfies(tx -> {
            assertThat(tx.startLsn()).isEqualTo(8);
            assertThat(tx.records()).singleElement().satisfies(record -> {
                assertThat(record.redo()).isEqualTo("INSERT INTO T VALUES(1)");
                assertThat(record.objectId()).isEqualTo(42);
                assertThat(record.owner()).isEqualTo("APP");
            });
        });
    }

    @Test
    void shouldNotPublishOrAdvancePastAnIncompleteContinuation() {
        final var batch = new LogMinerBatch();
        batch.add(row("tx", 10, 10, 1, 0, "INSERT incomplete", "r1", 1));

        assertThatThrownBy(batch::finish).isInstanceOf(IllegalStateException.class).hasMessageContaining("Incomplete");
        assertThatThrownBy(batch::committedTransactions).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> batch.restartLsn(20)).isInstanceOf(IllegalStateException.class);
    }

    @ParameterizedTest
    @ValueSource(strings = { "transaction", "lsn", "recordSet", "operation" })
    void shouldRejectInterleavedContinuation(String mismatch) {
        final var batch = new LogMinerBatch();
        batch.add(row("tx", 10, 10, 1, 0, "INSERT incomplete", "r1", 1));
        final var second = row(mismatch.equals("transaction") ? "other" : "tx", mismatch.equals("lsn") ? 11 : 10,
                10, mismatch.equals("operation") ? 3 : 1, 0, "tail", mismatch.equals("recordSet") ? "r2" : "r1", 0);

        assertThatThrownBy(() -> batch.add(second)).isInstanceOf(IllegalStateException.class).hasMessageContaining("continuation");
    }

    @Test
    void shouldKeepUnsupportedRecordsVisibleForTheStreamingFailurePolicy() {
        final var batch = new LogMinerBatch();
        batch.add(row("tx", 10, 10, 255, 0, "unsupported", "r1", 0));
        batch.add(row("tx", 20, 10, 7, 0, null, "r2", 0));

        assertThat(batch.committedTransactions()).singleElement()
                .satisfies(tx -> assertThat(tx.records()).extracting(LogMinerRow::operation).containsExactly(255));
    }

    @Test
    void shouldRejectRowsWithoutTransactionIdentity() {
        assertThatThrownBy(() -> new LogMinerBatch().add(row(null, 10, 10, 1, 0, "INSERT", "r1", 0)))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("transaction id");
    }

    @Test
    void shouldPinTransactionThatContainsOnlyAStartRecord() {
        final var batch = new LogMinerBatch();
        batch.add(row("tx", 10, 0, 6, 0, null, "r1", 0));

        assertThat(batch.restartLsn(20)).isEqualTo(10);
    }

    @Test
    void shouldStopTheEntireWindowBeforeExceedingItsRecordBudget() {
        final var batch = new LogMinerBatch(Long.MAX_VALUE, 3);
        batch.add(row("committed", 10, 10, 1, 0, "INSERT committed", "r1", 0));
        batch.add(row("committed", 11, 10, 7, 0, null, "r2", 0));
        batch.add(row("open", 12, 12, 1, 0, "INSERT open", "r3", 0));
        assertThat(batch.getBufferedRecordCount()).isEqualTo(3);

        assertThatThrownBy(() -> batch.add(row("open", 13, 12, 1, 0, "INSERT overflow", "r4", 0)))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("log.mining.buffer.max.records=3")
                .hasMessageContaining("LSN 13").hasMessageContaining("Retain the required archive logs");
        assertThat(batch.getBufferedRecordCount()).isEqualTo(3);
        assertCapacityFailure(batch);
    }

    @Test
    void shouldCountMarkersAndDiscardedRowsAcrossDifferentTransactions() {
        final var batch = new LogMinerBatch(Long.MAX_VALUE, 3);
        batch.add(row("first", 10, 10, 6, 0, null, "r1", 0));
        batch.add(row("second", 11, 11, 1, 1, "INSERT discarded", "r2", 0));
        batch.add(row("sequence", 12, 12, 37, 0, "SEQ MODIFY", "r3", 0));

        assertThatThrownBy(() -> batch.add(row("third", 13, 13, 6, 0, null, "r4", 0)))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("buffer limit");
        assertCapacityFailure(batch);
    }

    @Test
    void shouldApplyTheByteBudgetAcrossAllTransactionsWithoutPublishingAPartialWindow() {
        final var first = row("first", 10, 10, 1, 0, "x".repeat(1_000), "r1", 0);
        final var second = row("second", 11, 11, 1, 0, "y".repeat(1_000), "r2", 0);
        final var probe = new LogMinerBatch();
        probe.add(first);
        final long firstBytes = probe.getEstimatedBufferBytes();
        probe.add(second);
        final var batch = new LogMinerBatch(probe.getEstimatedBufferBytes() - 1, Long.MAX_VALUE);
        batch.add(first);

        assertThatThrownBy(() -> batch.add(second)).isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("log.mining.buffer.max.bytes");
        assertThat(batch.getEstimatedBufferBytes()).isEqualTo(firstBytes);
        assertCapacityFailure(batch);
    }

    @Test
    void shouldAllowAnExactlySizedWindowAndExposeReadOnlyRecordsWithoutCopying() {
        final var insert = row("tx", 10, 10, 1, 0, "INSERT", "r1", 0);
        final var commit = row("tx", 11, 10, 7, 0, null, "r2", 0);
        final var probe = new LogMinerBatch();
        probe.add(insert);
        probe.add(commit);
        final var batch = new LogMinerBatch(probe.getEstimatedBufferBytes(), 2);
        batch.add(insert);
        batch.add(commit);
        final var transaction = batch.committedTransactions().get(0);

        assertThat(transaction.records()).containsExactly(insert).isSameAs(transaction.records());
        assertThatThrownBy(() -> transaction.records().add(commit)).isInstanceOf(UnsupportedOperationException.class);
        assertThat(batch.restartLsn(11)).isEqualTo(11);
    }

    @ParameterizedTest
    @ValueSource(ints = { 0, 1 })
    void shouldReserveContinuationAssemblyBeforeGrowingTheSqlBuffer(int finalContinuation) {
        final var first = row("tx", 10, 10, 1, 0, "a".repeat(1_000), "r1", 1);
        final var second = row("tx", 10, 10, 1, 0, "达梦".repeat(500), "r1", finalContinuation);
        final var probe = new LogMinerBatch();
        probe.add(first);
        final long firstBytes = probe.getEstimatedBufferBytes();
        probe.add(second);
        final var batch = new LogMinerBatch(probe.getEstimatedBufferBytes() - 1, 10);
        batch.add(first);

        assertThatThrownBy(() -> batch.add(second)).isInstanceOf(IllegalStateException.class).hasMessageContaining("buffer limit");
        assertThat(batch.getEstimatedBufferBytes()).isEqualTo(firstBytes);
        assertThat(batch.getBufferedRecordCount()).isEqualTo(1);
        assertCapacityFailure(batch);
    }

    @Test
    void shouldCountContinuationPiecesEvenWhenTheyFormOnlyOneStatement() {
        final var batch = new LogMinerBatch(Long.MAX_VALUE, 2);
        batch.add(row("tx", 10, 10, 1, 0, "first", "r1", 1));
        batch.add(row("tx", 10, 10, 1, 0, "middle", "r1", 1));

        assertThatThrownBy(() -> batch.add(row("tx", 10, 10, 1, 0, "last", "r1", 0)))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("buffer limit");
        assertCapacityFailure(batch);
    }

    @ParameterizedTest
    @ValueSource(longs = { 0, -1, Long.MIN_VALUE })
    void shouldRejectUnboundedOrNegativeBufferLimits(long limit) {
        assertThatThrownBy(() -> new LogMinerBatch(limit, 1)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new LogMinerBatch(1, limit)).isInstanceOf(IllegalArgumentException.class);
    }

    private void assertCapacityFailure(LogMinerBatch batch) {
        assertThatThrownBy(batch::finish).isInstanceOf(IllegalStateException.class).hasMessageContaining("buffer limit");
        assertThatThrownBy(batch::committedTransactions).isInstanceOf(IllegalStateException.class).hasMessageContaining("buffer limit");
        assertThatThrownBy(() -> batch.restartLsn(100)).isInstanceOf(IllegalStateException.class).hasMessageContaining("buffer limit");
        assertThatThrownBy(() -> batch.add(row("tx", 100, 100, 7, 0, null, "r100", 0)))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("buffer limit");
    }

    private static LogMinerRow row(String transaction, long lsn, long start, int operation, int rollback,
                                   String sql, String recordSet, int continuation) {
        return new LogMinerRow(lsn, start, 0, transaction, operation, rollback, "APP", "EVENTS", "APP", sql,
                recordSet, 1, continuation, TIMESTAMP);
    }
}
