/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import io.debezium.connector.dameng.DamengConnection.ArchiveLogFile;
import io.debezium.connector.dameng.DamengDatabaseIdentity.OpenRecord;

class DamengArchiveLogTest {
    private static final long DATABASE_MAGIC = 42;
    private DamengConnection connection;

    @BeforeEach
    void before() throws Exception {
        connection = mock(DamengConnection.class);
        when(connection.archiveLogsForRange(anyLong(), anyLong())).thenCallRealMethod();
        when(connection.databaseMagic()).thenReturn(DATABASE_MAGIC);
    }

    @Test
    void shouldSelectOnlyRequiredArchivesInMiningOrder() throws Exception {
        final var first = file("first", 100, 200, 99);
        final var second = file("second", 200, 300, 200);
        when(connection.readArchiveLogFiles()).thenReturn(List.of(second, file("older", 1, 99, 0), file("newer", 301, 400, 300), first));

        assertThat(connection.archiveLogsForRange(150, 250)).containsExactly(first, second);
    }

    @Test
    void shouldAcceptSharedInclusiveArchiveBoundary() throws Exception {
        final var first = file("first", 100, 200, 99);
        final var second = file("second", 200, 300, 200);
        when(connection.readArchiveLogFiles()).thenReturn(List.of(first, second));

        assertThat(connection.archiveLogsForRange(200, 200)).containsExactly(first, second);
    }

    @Test
    void shouldAcceptContiguousArchivePositions() throws Exception {
        final var first = file("first", 100, 200, 99);
        final var second = file("second", 201, 300, 200);
        when(connection.readArchiveLogFiles()).thenReturn(List.of(first, second));

        assertThat(connection.archiveLogsForRange(150, 250)).containsExactly(first, second);
    }

    @Test
    void shouldAcceptNumericLsnGapWhenArchivePredecessorProvesContinuity() throws Exception {
        final var first = file("first", 100, 200, 99);
        final var second = file("second", 250, 300, 200);
        when(connection.readArchiveLogFiles()).thenReturn(List.of(first, second));

        assertThat(connection.archiveLogsForRange(150, 300)).containsExactly(first, second);
    }

    @Test
    void shouldAdvanceAcrossAFileBoundaryWithANumericGapButRequireTheInclusiveRestartRow() throws Exception {
        final var next = file("next", 250, 300, 200);
        when(connection.readArchiveLogFiles()).thenReturn(List.of(next));
        assertThat(connection.archiveLogsForRange(201, 300)).containsExactly(next);
        assertThatThrownBy(() -> connection.archiveLogsForRange(200, 300)).hasMessageContaining("restart LSN");
        assertThatThrownBy(() -> connection.archiveLogsForRange(202, 300)).hasMessageContaining("restart LSN");
        assertThatThrownBy(() -> connection.archiveLogsForRange(0, 300)).hasMessageContaining("restart LSN");
        when(connection.readArchiveLogFiles()).thenReturn(List.of(file("unknown-predecessor", 250, 300, 0)));
        assertThatThrownBy(() -> connection.archiveLogsForRange(1, 300)).hasMessageContaining("restart LSN");
    }

    @Test
    void shouldNotMoveCoverageBackwardForOverlappingArchiveCopies() throws Exception {
        final var first = file("first", 100, 250, 99);
        final var overlap = file("overlap", 150, 200, 149);
        final var third = file("third", 250, 350, 250);
        when(connection.readArchiveLogFiles()).thenReturn(List.of(third, overlap, first));

        assertThat(connection.archiveLogsForRange(100, 350)).containsExactly(first, overlap, third);
    }

    @Test
    void shouldRejectAnInvertedMiningWindow() {
        assertThatThrownBy(() -> connection.archiveLogsForRange(200, 100))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("end LSN");
    }

    @Test
    void shouldRejectAnEmptyArchiveHistory() throws Exception {
        when(connection.readArchiveLogFiles()).thenReturn(List.of());

        assertThatThrownBy(() -> connection.archiveLogsForRange(100, 200))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("No archived log");
    }

    @Test
    void shouldRejectArchiveHistoryThatNoLongerContainsRestartPosition() throws Exception {
        when(connection.readArchiveLogFiles()).thenReturn(List.of(file("remaining", 150, 300, 149)));

        assertThatThrownBy(() -> connection.archiveLogsForRange(100, 200))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("restart LSN 100");
    }

    @Test
    void shouldRejectAnArchiveGap() throws Exception {
        when(connection.readArchiveLogFiles()).thenReturn(List.of(file("first", 100, 200, 99), file("third", 300, 400, 299)));

        assertThatThrownBy(() -> connection.archiveLogsForRange(150, 350))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("gap");
    }

    @Test
    void shouldRejectMissingPredecessorDespiteAdjacentNumericPositions() throws Exception {
        when(connection.readArchiveLogFiles()).thenReturn(List.of(file("first", 100, 200, 99), file("second", 201, 300, 250)));

        assertThatThrownBy(() -> connection.archiveLogsForRange(150, 250))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("missing predecessor");
    }

    @Test
    void shouldRejectAnArchiveWindowThatEndsBeforeTheRequestedPosition() throws Exception {
        when(connection.readArchiveLogFiles()).thenReturn(List.of(file("first", 100, 200, 99)));

        assertThatThrownBy(() -> connection.archiveLogsForRange(100, 250))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("before requested 250");
    }

    @ParameterizedTest
    @ValueSource(longs = { 0, 99 })
    void shouldRejectArchivesFromAnUnknownOrDifferentDatabase(long magic) throws Exception {
        when(connection.readArchiveLogFiles()).thenReturn(List.of(new ArchiveLogFile("foreign", 100, 200, magic, 99)));

        assertThatThrownBy(() -> connection.archiveLogsForRange(100, 200))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("DB_MAGIC mismatch");
    }

    @Test
    void shouldRejectAnUnknownCurrentDatabaseIdentity() throws Exception {
        when(connection.databaseMagic()).thenReturn(0L);
        when(connection.readArchiveLogFiles()).thenReturn(List.of(file("first", 100, 200, 99)));

        assertThatThrownBy(() -> connection.archiveLogsForRange(100, 200))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("DB_MAGIC is unavailable");
    }

    @Test
    void shouldAcceptTheVerifiedArchiveChainThroughSwitchoverRestartAndSwitchback() throws Exception {
        // Boundaries mirror a real A -> B -> B restart -> A DM8 data-guard
        // run. A writes every local file, including B's replicated redo.
        when(connection.primaryStandbyIdentity()).thenReturn(new DamengDatabaseIdentity(7, DATABASE_MAGIC, 44_454, List.of(
                open("a-normal", 1, DATABASE_MAGIC, 21_569, DATABASE_MAGIC),
                open("a-primary", 2, DATABASE_MAGIC, 38_806, DATABASE_MAGIC),
                open("b-primary", 3, 20, 40_167, DATABASE_MAGIC),
                open("b-restart", 4, 20, 41_805, 20),
                open("a-return", 5, DATABASE_MAGIC, 43_085, 20))));
        final var files = List.of(
                haFile("a-1", 38_807, 40_152, DATABASE_MAGIC),
                haFile("a-2", 40_152, 40_167, DATABASE_MAGIC),
                haFile("b-1", 40_168, 41_531, 20),
                haFile("b-2", 41_531, 41_805, 20),
                haFile("b-3", 41_806, 43_063, 20),
                haFile("b-4", 43_063, 43_085, 20),
                haFile("a-3", 43_086, 44_454, DATABASE_MAGIC));
        when(connection.readArchiveLogFiles()).thenReturn(files);

        assertThat(connection.archiveLogsForRange(40_140, 44_000)).containsExactlyElementsOf(files);
    }

    @Test
    void shouldRejectOverlappingArchivesAfterReturningToTheSamePrimary() throws Exception {
        useSwitchedBackIdentity();
        // Both files have the currently valid source magic and LSN range,
        // but one could be a former primary's unreconciled divergent tail.
        when(connection.readArchiveLogFiles()).thenReturn(List.of(
                haFile("old-a-tail", 301, 380, DATABASE_MAGIC),
                haFile("new-a-log", 350, 450, DATABASE_MAGIC)));

        assertThatThrownBy(() -> connection.archiveLogsForRange(360, 400))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("Overlapping").hasMessageContaining("divergent");
    }

    @Test
    void shouldRejectFullyContainedArchiveCopiesInPrimaryStandbyMode() throws Exception {
        useSwitchedBackIdentity();
        when(connection.readArchiveLogFiles()).thenReturn(List.of(
                haFile("primary", 301, 450, DATABASE_MAGIC),
                haFile("uncertain-copy", 350, 380, DATABASE_MAGIC)));

        assertThatThrownBy(() -> connection.archiveLogsForRange(360, 370))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("Overlapping");
    }

    @Test
    void shouldNotClipADivergentArchiveTailToTheRequestedMiningWindow() throws Exception {
        useSwitchedBackIdentity();
        when(connection.readArchiveLogFiles()).thenReturn(List.of(haFile("old-a-tail", 100, 250, DATABASE_MAGIC)));

        assertThatThrownBy(() -> connection.archiveLogsForRange(150, 190))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("verified primary history");
    }

    @Test
    void shouldNotClipAnEarlierPrimaryRunIntoTheCurrentRunWithTheSameMagic() throws Exception {
        useSwitchedBackIdentity();
        when(connection.readArchiveLogFiles()).thenReturn(List.of(haFile("old-a-tail", 100, 400, DATABASE_MAGIC)));

        assertThatThrownBy(() -> connection.archiveLogsForRange(350, 380))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("verified primary history");
    }

    private void useSwitchedBackIdentity() {
        when(connection.primaryStandbyIdentity()).thenReturn(new DamengDatabaseIdentity(7, DATABASE_MAGIC, 500, List.of(
                open("a", 1, DATABASE_MAGIC, 100, DATABASE_MAGIC),
                open("b", 2, 20, 200, DATABASE_MAGIC),
                open("a-return", 3, DATABASE_MAGIC, 300, 20))));
    }

    private static OpenRecord open(String guid, long time, long magic, long appliedLsn, long previousMagic) {
        return new OpenRecord(guid, Instant.ofEpochSecond(time), magic, appliedLsn, previousMagic);
    }

    private static ArchiveLogFile haFile(String path, long first, long last, long sourceMagic) {
        return new ArchiveLogFile(path, first, last, DATABASE_MAGIC, 0, 7, sourceMagic, 0);
    }

    private static ArchiveLogFile file(String path, long first, long last, long previous) {
        return new ArchiveLogFile(path, first, last, DATABASE_MAGIC, previous);
    }
}
