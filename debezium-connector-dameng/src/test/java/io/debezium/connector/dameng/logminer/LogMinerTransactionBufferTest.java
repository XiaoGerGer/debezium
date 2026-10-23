/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng.logminer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class LogMinerTransactionBufferTest {
    @TempDir
    Path directory;

    @Test
    void shouldTreatOnlyCommitMarkersAsTransactionCompletion() {
        try (var buffer = buffer(4096)) {
            buffer.add(row("open", 10, 10, 6, 0, null));
            buffer.add(row("open", 11, 10, 1, 0, "INSERT open"));
            buffer.add(row("open", 12, 10, 15, 0, null));
            buffer.add(row("xa", 20, 20, 1, 0, "INSERT xa"));
            buffer.add(row("xa", 21, 20, 38, 0, null));

            assertThat(buffer.committedTransactions()).extracting(LogMinerTransactionBuffer.Transaction::id).containsExactly("xa");
            assertThat(buffer.openTransactions()).isEqualTo(1);
            assertThat(buffer.completeWindow(30)).isEqualTo(10);
        }
    }

    @Test
    void shouldIgnoreSequenceAllocationWithoutTransactionIdentity() {
        try (var buffer = buffer(4096)) {
            buffer.add(new LogMinerRow(10, 10, 0, null, 37, 0, null, null, null, "sequence change", "r", 0, 0, Instant.EPOCH));
            buffer.add(row("tx", 11, 10, 1, 0, "INSERT"));
            buffer.add(row("tx", 12, 10, 7, 0, null));

            final var committed = buffer.committedTransactions();
            assertThat(committed).hasSize(1);
            assertThat(read(committed.get(0))).extracting(LogMinerRow::redo).containsExactly("INSERT");
        }
    }

    @Test
    void shouldRejectMissingTransactionIdentityAndKeepUnsupportedOperationsVisible() {
        try (var missingIdentity = buffer(4096)) {
            assertThatThrownBy(() -> missingIdentity.add(row(null, 10, 10, 1, 0, "INSERT")))
                    .isInstanceOf(IllegalStateException.class).hasMessageContaining("transaction id");
        }
        try (var unsupported = buffer(4096)) {
            unsupported.add(row("tx", 10, 10, 255, 0, "unsupported"));
            unsupported.add(row("tx", 11, 10, 7, 0, null));
            assertThat(read(unsupported.committedTransactions().get(0))).extracting(LogMinerRow::operation).containsExactly(255);
        }
    }

    @Test
    void shouldReconstructOnlyPostRecoverySuffixOfAnExistingTransaction() {
        try (var buffer = new LogMinerTransactionBuffer(512, 1_000_000, 100, directory, 1_000_000, 100)) {
            assertThat(buffer.completeWindow(110)).isEqualTo(110);
            buffer.beginWindow();
            buffer.add(row("old", 115, 0, 1, 0, "keep"));
            buffer.add(row("old", 116, 0, 1, 0, "rollback"));
            assertThat(buffer.completeWindow(120)).isEqualTo(115);
            buffer.beginWindow();
            buffer.add(row("old", 125, 0, 7, 0, null));
            assertThat(buffer.requiresReconstruction()).isTrue();
            buffer.resetForReconstruction();
            buffer.add(row("old", 115, 50, 1, 0, "keep"));
            buffer.add(row("old", 116, 50, 1, 1, "rollback"));
            buffer.add(row("old", 125, 50, 7, 0, null));
            assertThat(read(buffer.committedTransactions().get(0))).extracting(LogMinerRow::redo).containsExactly("keep");
            buffer.release(buffer.committedTransactions().get(0));
            assertThat(buffer.completeWindow(130)).isEqualTo(130);
        }
    }

    @Test
    void shouldDrainFullRollbackOfARecoverySuffixAndIgnoreUnmatchedTerminalMarkers() {
        try (var buffer = new LogMinerTransactionBuffer(512, 1_000_000, 100, directory, 1_000_000, 100)) {
            buffer.add(row("old", 115, 50, 1, 1, "rollback"));
            buffer.add(row("old", 120, 50, 36, 1, null));
            assertThat(buffer.completeWindow(125)).isEqualTo(125);
            buffer.beginWindow();
            buffer.add(row("unknown", 126, 0, 7, 0, null));
            buffer.add(row("unknown", 127, 0, 38, 0, null));
            buffer.add(row("unknown", 128, 0, 36, 1, null));
            assertThat(buffer.completeWindow(130)).isEqualTo(130);
            assertThat(buffer.memoryBytes()).isZero();
            buffer.add(row("unobserved-at-cut", 135, 0, 1, 0, "post-cut"));
            buffer.add(row("unobserved-at-cut", 136, 0, 7, 0, null));
            assertThat(read(buffer.committedTransactions().get(0))).extracting(LogMinerRow::redo).containsExactly("post-cut");
        }
    }

    @ParameterizedTest
    @ValueSource(longs = { 768, 4096, 1048576 })
    void shouldPreserveConcurrentTransactionsAndReleaseOnlyCompletedHistory(long memory) throws Exception {
        try (var buffer = buffer(memory)) {
            buffer.add(row("long", 11, 10, 1, 0, "first"));
            assertThat(buffer.completeWindow(20)).isEqualTo(10);
            buffer.beginWindow();
            buffer.add(row("short", 22, 21, 1, 0, "short"));
            buffer.add(row("long", 23, 0, 3, 0, "later"));
            buffer.add(row("short", 24, 21, 7, 0, null));
            buffer.finishWindow();
            assertThat(read(buffer.committedTransactions().get(0))).extracting(LogMinerRow::redo).containsExactly("short");
            buffer.release(buffer.committedTransactions().get(0));
            assertThat(buffer.completeWindow(30)).isEqualTo(10);
            buffer.beginWindow();
            buffer.add(row("long", 35, 0, 7, 0, null));
            buffer.finishWindow();
            assertThat(buffer.requiresReconstruction()).isTrue();
            assertThatThrownBy(buffer::committedTransactions).hasMessageContaining("must be reconstructed");
            buffer.resetForReconstruction();
            buffer.beginWindow();
            buffer.add(row("long", 11, 10, 1, 0, "first"));
            buffer.add(row("long", 23, 10, 3, 0, "later"));
            buffer.add(row("long", 35, 10, 7, 0, null));
            assertThat(read(buffer.committedTransactions().get(0))).extracting(LogMinerRow::redo).containsExactly("first", "later");
            buffer.release(buffer.committedTransactions().get(0));
            assertThat(buffer.completeWindow(40)).isEqualTo(40);
            assertThat(buffer.memoryBytes()).isZero();
            assertThat(buffer.diskBytes()).isZero();
            assertThat(buffer.bufferedRecords()).isZero();
        }
        assertEmptyDirectory();
    }

    @Test
    void shouldSpillAgainstTheAggregateMemoryThresholdAndRetainUnicodeAndLongSql() throws Exception {
        final String sql = "中文\uD800\n'\\".repeat(20000);
        try (var buffer = buffer(2048)) {
            buffer.add(row("a", 11, 10, 1, 0, "small"));
            buffer.add(row("b", 13, 12, 1, 0, "small"));
            buffer.add(row("a", 14, 10, 3, 0, sql));
            buffer.add(row("b", 16, 12, 7, 0, null));
            buffer.add(row("a", 17, 10, 7, 0, null));
            buffer.finishWindow();
            assertThat(buffer.memoryBytes()).isLessThanOrEqualTo(2048);
            assertThat(buffer.spilledTransactions()).isPositive();
            assertThat(buffer.committedTransactions()).extracting(LogMinerTransactionBuffer.Transaction::id).containsExactly("b", "a");
            assertThat(read(buffer.committedTransactions().get(1))).containsExactly(row("a", 11, 10, 1, 0, "small"), row("a", 14, 10, 3, 0, sql));
        }
        assertEmptyDirectory();
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void shouldDiscardFullRollbackWithOrWithoutAnEarlierWindow(boolean earlierWindow) throws Exception {
        try (var buffer = buffer(512)) {
            buffer.add(row("a", 10, 10, 6, earlierWindow ? 0 : 1, null));
            buffer.add(row("a", 11, 10, 1, earlierWindow ? 0 : 1, "discard"));
            if (earlierWindow) {
                assertThat(buffer.completeWindow(20)).isEqualTo(10);
                buffer.beginWindow();
            }
            buffer.add(row("a", 25, earlierWindow ? 0 : 10, 36, 1, null));
            buffer.finishWindow();
            if (earlierWindow) {
                assertThat(buffer.requiresReconstruction()).isTrue();
                buffer.resetForReconstruction();
                buffer.beginWindow();
                buffer.add(row("a", 10, 10, 6, 1, null));
                buffer.add(row("a", 11, 10, 1, 1, "discard"));
                buffer.add(row("a", 25, 10, 36, 1, null));
            }
            assertThat(buffer.committedTransactions()).isEmpty();
            assertThat(buffer.completeWindow(30)).isEqualTo(30);
            assertThat(buffer.diskBytes()).isZero();
        }
        assertEmptyDirectory();
    }

    @Test
    void shouldReplaceSpilledProvisionalImagesWithTheFinalRollbackView() {
        try (var buffer = buffer(512)) {
            buffer.add(row("a", 11, 10, 1, 0, "keep"));
            buffer.add(row("a", 12, 10, 36, 0, null));
            buffer.add(row("a", 13, 10, 1, 0, "discard"));
            buffer.completeWindow(20);
            buffer.beginWindow();
            buffer.add(row("a", 21, 0, 36, 0, null));
            buffer.add(row("a", 25, 0, 7, 0, null));
            assertThat(buffer.requiresReconstruction()).isTrue();
            assertThatThrownBy(buffer::committedTransactions).hasMessageContaining("must be reconstructed");
            assertThatThrownBy(() -> buffer.completeWindow(30)).hasMessageContaining("must be reconstructed");
            buffer.resetForReconstruction();
            assertThat(buffer.diskBytes()).isZero();
            assertThat(buffer.bufferedRecords()).isZero();
            buffer.beginWindow();
            buffer.add(row("a", 11, 10, 1, 0, "keep"));
            buffer.add(row("a", 12, 10, 36, 0, null));
            buffer.add(row("a", 13, 10, 1, 1, "discard"));
            buffer.add(row("a", 21, 10, 36, 0, null));
            buffer.add(row("a", 25, 10, 7, 0, null));
            assertThat(read(buffer.committedTransactions().get(0))).extracting(LogMinerRow::redo).containsExactly("keep");
        }
    }

    @Test
    void shouldRetainAnEmptyTransactionAfterPartialRollbackUntilItActuallyEnds() {
        try (var buffer = buffer(512)) {
            buffer.add(row("a", 10, 10, 6, 1, null));
            buffer.add(row("a", 11, 10, 1, 1, "rolled back"));
            buffer.add(row("a", 12, 10, 36, 0, null));
            assertThat(buffer.completeWindow(20)).isEqualTo(10);
            assertThat(buffer.openTransactions()).isEqualTo(1);
            buffer.beginWindow();
            buffer.add(row("a", 21, 0, 1, 0, "later"));
            assertThat(buffer.requiresReconstruction()).isFalse();
            assertThat(buffer.completeWindow(30)).isEqualTo(10);
        }
    }

    @Test
    void shouldRejectDiskQuotaAndCleanOnlyItsOwnFiles() throws Exception {
        final var unrelated = Files.writeString(directory.resolve("unrelated"), "keep");
        try (var buffer = new LogMinerTransactionBuffer(512, 1048576, 10000, directory, 32)) {
            assertThatThrownBy(() -> buffer.add(row("a", 11, 10, 1, 0, "value")))
                    .isInstanceOf(LogMinerSpillStore.StorageException.class).hasMessageContaining("spill.max.bytes");
        }
        assertThat(Files.readString(unrelated)).isEqualTo("keep");
        try (var files = Files.list(directory)) {
            assertThat(files.toList()).containsExactly(unrelated);
        }
    }

    @Test
    void shouldFailWhenSpillPathIsNotADirectory() throws Exception {
        final var invalid = Files.writeString(directory.resolve("file"), "not a directory");
        try (var buffer = new LogMinerTransactionBuffer(512, 1048576, 10000, invalid, 1048576)) {
            assertThatThrownBy(() -> buffer.add(row("a", 11, 10, 1, 0, "value")))
                    .isInstanceOf(LogMinerSpillStore.StorageException.class).hasMessageContaining("spill.path");
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "truncate", "checksum", "length" })
    void shouldRejectDamagedTransactionFiles(String damage) throws Exception {
        try (var buffer = buffer(512)) {
            buffer.add(row("a", 11, 10, 1, 0, "value"));
            buffer.add(row("a", 15, 10, 7, 0, null));
            buffer.finishWindow();
            final Path file;
            try (var files = Files.walk(directory)) {
                file = files.filter(Files::isRegularFile).findFirst().orElseThrow();
            }
            final var bytes = Files.readAllBytes(file);
            if (damage.equals("truncate")) {
                Files.write(file, new byte[0], StandardOpenOption.TRUNCATE_EXISTING);
            }
            else {
                bytes[damage.equals("length") ? 0 : bytes.length - 1] ^= 127;
                Files.write(file, bytes, StandardOpenOption.TRUNCATE_EXISTING);
            }
            assertThatThrownBy(() -> read(buffer.committedTransactions().get(0)))
                    .isInstanceOf(LogMinerSpillStore.StorageException.class);
        }
        assertEmptyDirectory();
    }

    @Test
    void shouldBoundMetadataEvenAfterAllRowsHaveSpilled() {
        try (var buffer = buffer(512)) {
            buffer.add(row("a", 11, 10, 1, 0, "value"));
            assertThatThrownBy(() -> buffer.add(row("b", 13, 12, 1, 0, "value"))).hasMessageContaining("metadata exceeds");
        }
    }

    @Test
    void shouldReassembleContinuationsBeforeSpillingAndRejectMissingPieces() {
        try (var buffer = buffer(512)) {
            final var first = row("a", 11, 10, 1, 0, "part one");
            buffer.add(new LogMinerRow(first.lsn(), first.startLsn(), 0, first.transactionId(), 1, 0, null, null, null,
                    first.redo(), first.recordSetId(), 0, 1, first.timestamp(), 7));
            assertThatThrownBy(buffer::finishWindow).hasMessageContaining("Incomplete");
            buffer.add(piece(1, 0, "part two"));
            buffer.add(row("a", 15, 10, 7, 0, null));
            buffer.finishWindow();
            assertThat(read(buffer.committedTransactions().get(0))).extracting(LogMinerRow::redo).containsExactly("part onepart two");
        }
    }

    @Test
    void shouldKeepCumulativeBytesAndRowsAsTelemetryBeyondTheFormerDefaults() {
        try (var buffer = buffer(512)) {
            final var ignored = row(null, 10, 10, 37, 0, "x".repeat(512));
            for (int i = 0; i < 1_000_001; i++) {
                buffer.add(ignored);
            }
            buffer.add(row("a", 11, 10, 1, 0, "keep"));
            buffer.add(row("a", 12, 10, 7, 0, null));
            assertThat(buffer.windowBytes()).isGreaterThan(512L * 1024 * 1024);
            assertThat(buffer.windowRecords()).isGreaterThan(1_000_000);
            assertThat(buffer.isCapacityExceeded()).isFalse();
            assertThat(read(buffer.committedTransactions().get(0))).extracting(LogMinerRow::redo).containsExactly("keep");
            buffer.release(buffer.committedTransactions().get(0));
            assertThat(buffer.completeWindow(20)).isEqualTo(20);
            buffer.beginWindow();
            assertThat(buffer.windowBytes()).isZero();
            assertThat(buffer.windowRecords()).isZero();
        }
    }

    @Test
    void shouldSpillManyRowsWithoutConfusingSqlAndResidentTransactionLimitsWithCumulativeWork() throws Exception {
        try (var buffer = new LogMinerTransactionBuffer(512, 8192, 1, directory, 4 * 1024 * 1024)) {
            final var payload = "中".repeat(4096);
            for (int i = 0; i < 256; i++) {
                buffer.add(row("a", 11 + i, 10, 1, 0, payload));
            }
            buffer.add(row("a", 300, 10, 7, 0, null));
            assertThat(buffer.memoryBytes()).isLessThanOrEqualTo(512);
            assertThat(buffer.diskBytes()).isGreaterThan(2 * 1024 * 1024);
            assertThat(buffer.windowBytes()).isGreaterThan(2 * 1024 * 1024);
            final var tx = buffer.committedTransactions().get(0);
            try (var cursor = tx.open()) {
                for (int i = 0; i < 256; i++) {
                    final var record = cursor.next();
                    assertThat(record.lsn()).isEqualTo(11 + i);
                    assertThat(record.redo()).isEqualTo(payload);
                }
                assertThat(cursor.next()).isNull();
            }
            buffer.release(tx);
            assertThat(buffer.completeWindow(300)).isEqualTo(300);
            assertThat(buffer.diskBytes()).isZero();
            // A released transaction no longer consumes the metadata count limit.
            buffer.beginWindow();
            buffer.add(row("b", 301, 301, 1, 0, "next"));
            assertThat(buffer.openTransactions()).isEqualTo(1);
        }
        assertEmptyDirectory();
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void shouldBoundOneSqlBeforeConcatenationAndPreventReuseAfterCapacityFailure(boolean continuation) {
        try (var buffer = new LogMinerTransactionBuffer(512, 8, 2, directory, 1048576)) {
            final var first = row("a", 11, 10, 1, 0, "中😀");
            if (continuation) {
                buffer.add(new LogMinerRow(first.lsn(), first.startLsn(), 0, first.transactionId(), 1, 0, null, null, null,
                        first.redo(), first.recordSetId(), 0, 1, first.timestamp(), 7));
            }
            assertThatThrownBy(() -> buffer.add(piece(continuation ? 1 : 0, 0, continuation ? "ab" : "中😀ab")))
                    .isInstanceOf(IllegalStateException.class).hasMessageContaining("log.mining.buffer.max.sql.bytes=8");
            assertThat(buffer.isCapacityExceeded()).isTrue();
            assertThatThrownBy(buffer::committedTransactions).hasMessageContaining("max.sql.bytes");
            assertThatThrownBy(() -> buffer.completeWindow(20)).hasMessageContaining("max.sql.bytes");
            assertThatThrownBy(buffer::resetForReconstruction).hasMessageContaining("max.sql.bytes");
            assertThatThrownBy(() -> buffer.add(row("a", 12, 10, 7, 0, null))).hasMessageContaining("max.sql.bytes");
        }
    }

    @Test
    void shouldAllowExactlyTheSqlLimitAndPreserveFirstPieceMetadataWhenSpilling() {
        try (var buffer = new LogMinerTransactionBuffer(512, 8, 2, directory, 1048576)) {
            final var first = new LogMinerRow(11, 10, 0, "a", 1, 0, "APP", "T", "writer", "中", "r", 0, 1, Instant.EPOCH,
                    7, 0, 0, 123, 45);
            buffer.add(first);
            buffer.add(new LogMinerRow(11, 0, 0, "a", 1, 0, null, null, null, "😀a", "r", 1, 0, Instant.EPOCH, 0));
            buffer.add(row("a", 12, 10, 7, 0, null));
            assertThat(buffer.diskBytes()).isPositive();
            assertThat(read(buffer.committedTransactions().get(0))).containsExactly(first.withRedo("中😀a"));
        }
    }

    @Test
    void shouldAssembleThreePiecesBeforePublishingTheTransaction() {
        try (var buffer = buffer(512)) {
            buffer.add(piece(0, 1, "first"));
            buffer.add(piece(1, 1, "middle"));
            buffer.add(piece(2, 0, "last"));
            buffer.add(row("a", 12, 10, 7, 0, null));
            assertThat(read(buffer.committedTransactions().get(0))).extracting(LogMinerRow::redo).containsExactly("firstmiddlelast");
        }
    }

    @ParameterizedTest
    @ValueSource(ints = { -1, 0, 2, Integer.MAX_VALUE })
    void shouldRejectMissingRepeatedAndOutOfOrderContinuationSequences(int sequence) {
        try (var buffer = buffer(512)) {
            buffer.add(piece(0, 1, "first"));
            assertThatThrownBy(() -> buffer.add(piece(sequence, 0, "last"))).hasMessageContaining("SSN");
            assertThatThrownBy(buffer::committedTransactions).hasMessageContaining("SSN");
            assertThatThrownBy(() -> buffer.completeWindow(20)).hasMessageContaining("SSN");
            assertThatThrownBy(buffer::resetForReconstruction).hasMessageContaining("SSN");
        }
    }

    @ParameterizedTest
    @ValueSource(ints = { -1, 1, 2 })
    void shouldRejectAnOrphanSqlSuffix(int sequence) {
        try (var buffer = buffer(512)) {
            assertThatThrownBy(() -> buffer.add(piece(sequence, 0, "suffix"))).hasMessageContaining("SSN");
        }
    }

    @ParameterizedTest
    @org.junit.jupiter.params.provider.NullAndEmptySource
    void shouldRejectMissingSqlInAnyContinuationPiece(String sql) {
        try (var buffer = buffer(512)) {
            assertThatThrownBy(() -> buffer.add(piece(0, 1, sql))).hasMessageContaining("SQL_REDO");
        }
        try (var buffer = buffer(512)) {
            buffer.add(piece(0, 1, "first"));
            assertThatThrownBy(() -> buffer.add(piece(1, 0, sql))).hasMessageContaining("SQL_REDO");
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "CSF", "ROLL_BACK", "RS_ID", "XID", "LSN", "OPERATION", "OBJECT", "VERSION", "ROLLBACK_MISMATCH" })
    void shouldRejectInvalidFlagsAndConflictingContinuationIdentity(String damage) {
        try (var buffer = buffer(512)) {
            final var first = new LogMinerRow(11, 10, 0, "a", 1, 0, null, null, null, "first", "r", 0, 1, Instant.EPOCH,
                    7, 0, 0, 123, 45);
            buffer.add(first);
            final var next = new LogMinerRow(damage.equals("LSN") ? 12 : 11, 10, 0, damage.equals("XID") ? "b" : "a",
                    damage.equals("OPERATION") ? 3 : 1, damage.equals("ROLL_BACK") ? 2 : damage.equals("ROLLBACK_MISMATCH") ? 1 : 0,
                    null, null, null, "last", damage.equals("RS_ID") ? null : "r", 1, damage.equals("CSF") ? 2 : 0, Instant.EPOCH,
                    damage.equals("OBJECT") ? 8 : 7, damage.equals("VERSION") ? 2 : 0, 0, 123, 45);
            assertThatThrownBy(() -> buffer.add(next)).isInstanceOf(IllegalStateException.class).hasMessageContaining("LSN");
            assertThatThrownBy(buffer::committedTransactions).isInstanceOf(IllegalStateException.class);
        }
    }

    private static LogMinerRow piece(int sequence, int continuation, String sql) {
        return new LogMinerRow(11, 10, 0, "a", 1, 0, null, null, null, sql, "r", sequence, continuation, Instant.EPOCH, 7);
    }

    @Test
    void shouldBoundResidentTransactionEntriesEvenWhenTheirRowsAreDiscarded() {
        try (var buffer = new LogMinerTransactionBuffer(1048576, 8192, 1, directory, 1048576)) {
            buffer.add(row("a", 11, 10, 1, 1, "discarded"));
            assertThatThrownBy(() -> buffer.add(row("b", 13, 12, 1, 1, "discarded")))
                    .hasMessageContaining("max.transactions=1");
            assertThat(buffer.isCapacityExceeded()).isTrue();
            assertThatThrownBy(() -> buffer.completeWindow(20)).hasMessageContaining("max.transactions=1");
        }
    }

    @ParameterizedTest
    @ValueSource(ints = { 0, 1, 2, 3 })
    void shouldRejectNonpositiveResourceLimits(int index) {
        final long[] limits = { 512, 8192, 10, 1048576 };
        limits[index] = 0;
        assertThatThrownBy(() -> new LogMinerTransactionBuffer(limits[0], limits[1], limits[2], directory, limits[3]))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private LogMinerTransactionBuffer buffer(long memory) {
        return new LogMinerTransactionBuffer(memory, 4 * 1024 * 1024, 10000, directory, 8 * 1024 * 1024);
    }

    private List<LogMinerRow> read(LogMinerTransactionBuffer.Transaction transaction) {
        final List<LogMinerRow> rows = new ArrayList<>();
        try (var cursor = transaction.open()) {
            for (var row = cursor.next(); row != null; row = cursor.next()) {
                rows.add(row);
            }
        }
        return rows;
    }

    private void assertEmptyDirectory() throws Exception {
        try (var files = Files.list(directory)) {
            assertThat(files.toList()).isEmpty();
        }
    }

    private static LogMinerRow row(String xid, long lsn, long start, int operation, int rollback, String sql) {
        return new LogMinerRow(lsn, start, 0, xid, operation, rollback, null, null, null, sql, operation == 36 && rollback == 1 ? "-1" : "r", 0, 0,
                Instant.parse("2026-10-16T01:02:03.123456789Z"), 7);
    }
}
