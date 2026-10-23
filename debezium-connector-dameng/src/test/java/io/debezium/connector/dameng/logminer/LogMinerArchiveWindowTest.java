/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng.logminer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import io.debezium.connector.dameng.DamengConnection.ArchiveLogFile;

class LogMinerArchiveWindowTest {
    @Test
    void shouldSortAndBudgetWholeFilesFromTheForwardPosition() {
        final var files = List.of(file("next", 201, 300, 64), file("old", 1, 99, 1024),
                file("second", 151, 200, 40), file("first", 100, 150, 30));
        assertThat(LogMinerArchiveWindow.select(files, 120, 119, 300, 64))
                .isEqualTo(new LogMinerArchiveWindow(200, 70, 2));
    }

    @Test
    void shouldReadAnOversizedFileWithoutWaitingOrSplittingItsLsnRange() {
        assertThat(LogMinerArchiveWindow.select(List.of(file("large", 100, 1_000_000, 1024)), 101, 100, 900_000, 64))
                .isEqualTo(new LogMinerArchiveWindow(900_000, 1024, 1));
    }

    @Test
    void shouldReadAvailableTailImmediatelyEvenBelowBudget() {
        assertThat(LogMinerArchiveWindow.select(List.of(file("tail", 100, 150, 4)), 101, 100, 150, 64))
                .isEqualTo(new LogMinerArchiveWindow(150, 4, 1));
    }

    @Test
    void shouldRespectStandbySafetyInsideAFileAndIgnoreLaterFiles() {
        final var files = List.of(file("first", 100, 200, 64), file("unsafe", 201, 300, 0));
        assertThat(LogMinerArchiveWindow.select(files, 101, 100, 175, 1024))
                .isEqualTo(new LogMinerArchiveWindow(175, 64, 1));
    }

    @Test
    void shouldAccountForSharedBoundaryFilesWithoutExtendingTheCappedEnd() {
        final var files = List.of(file("first", 100, 200, 64), file("shared", 200, 300, 64), file("future", 301, 400, 64));
        assertThat(LogMinerArchiveWindow.select(files, 100, 100, 400, 64))
                .isEqualTo(new LogMinerArchiveWindow(200, 128, 2));
        assertThat(LogMinerArchiveWindow.select(files, 201, 200, 400, 64))
                .isEqualTo(new LogMinerArchiveWindow(300, 64, 1));
    }

    @Test
    void shouldAdvancePastARestartExactlyAtAFileEnd() {
        final var files = List.of(file("previous", 100, 200, 64), file("next", 201, 300, 64));
        assertThat(LogMinerArchiveWindow.select(files, 200, 200, 300, 64))
                .isEqualTo(new LogMinerArchiveWindow(300, 128, 2));
    }

    @Test
    void shouldAllowTheSameFileToContinueGrowingWithoutGrowingTheBudget() {
        assertThat(LogMinerArchiveWindow.select(List.of(file("active", 100, 200, 64)), 101, 100, 200, 64).endLsn()).isEqualTo(200);
        assertThat(LogMinerArchiveWindow.select(List.of(file("active", 100, 240, 64)), 201, 200, 240, 64))
                .isEqualTo(new LogMinerArchiveWindow(240, 64, 1));
    }

    @Test
    void shouldPlanThroughNumericFileGapsForTheConnectionToValidatePredecessors() {
        assertThat(LogMinerArchiveWindow.select(List.of(file("next", 250, 300, 64)), 201, 200, 300, 64))
                .isEqualTo(new LogMinerArchiveWindow(300, 64, 1));
    }

    @Test
    void shouldHandleSpansLargerThanIntegerMaxAndSaturateByteAccounting() {
        final var files = List.of(file("a", 1, Long.MAX_VALUE - 1, Long.MAX_VALUE - 1),
                file("b", Long.MAX_VALUE - 1, Long.MAX_VALUE, 20));
        assertThat(LogMinerArchiveWindow.select(files, 1, 0, Long.MAX_VALUE, Long.MAX_VALUE))
                .isEqualTo(new LogMinerArchiveWindow(Long.MAX_VALUE, Long.MAX_VALUE, 2));
    }

    @Test
    void shouldIgnoreInvalidSizesOutsideTheChosenRange() {
        final var files = List.of(file("old", 1, 99, 0), file("chosen", 100, 200, 64), file("later", 201, 300, 0));
        assertThat(LogMinerArchiveWindow.select(files, 101, 100, 300, 64).endLsn()).isEqualTo(200);
    }

    @ParameterizedTest
    @ValueSource(longs = { 0, -1 })
    void shouldRejectUnknownFileSizesInsteadOfInventingAByteEstimate(long size) {
        assertThatThrownBy(() -> LogMinerArchiveWindow.select(List.of(file("bad", 100, 200, size)), 101, 100, 200, 64))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("LEN");
    }

    @ParameterizedTest
    @CsvSource({ "101,100,200,0", "101,100,200,-1", "-1,0,200,64", "1,-1,200,64", "100,100,100,64", "201,100,200,64" })
    void shouldRejectInvalidPlanningInputs(long start, long scanned, long upper, long budget) {
        assertThatThrownBy(() -> LogMinerArchiveWindow.select(List.of(), start, scanned, upper, budget))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void shouldRejectMissingForwardArchives() {
        assertThatThrownBy(() -> LogMinerArchiveWindow.select(List.of(), 101, 100, 200, 64))
                .hasMessageContaining("No Dameng archive");
        assertThatThrownBy(() -> LogMinerArchiveWindow.select(List.of(file("old", 1, 100, 64)), 100, 100, 200, 64))
                .hasMessageContaining("No Dameng archive");
    }

    private static ArchiveLogFile file(String path, long first, long last, long bytes) {
        return new ArchiveLogFile(path, first, last, 42, first - 1, 0, 0, 0, bytes);
    }
}
