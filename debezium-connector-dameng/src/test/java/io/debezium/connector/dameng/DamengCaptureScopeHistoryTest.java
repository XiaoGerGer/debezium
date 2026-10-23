/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

import java.nio.file.Path;
import java.sql.Types;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.debezium.relational.Attribute;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.relational.history.SchemaHistoryListener;
import io.debezium.relational.history.TableChanges;
import io.debezium.storage.file.history.FileSchemaHistory;

class DamengCaptureScopeHistoryTest {
    private static final DamengPartition PARTITION = new DamengPartition("dameng");
    private static final Table A = table("A", 101);
    private static final Table B_FIRST = table("B", 102);
    private static final Table B_RECREATED = table("B", 202);
    @TempDir
    Path directory;

    @Test
    void shouldRecoverInterleavedTableEpochsFromARealHistoryFileAtEveryCommitFrontier() throws Exception {
        final Path file = directory.resolve("interleaved-history.json");
        final var original = config(file, "APP\\.A");
        final var expanded = config(file, "APP\\.(A|B)");
        final var a70 = withColumn(A, "A_70");
        final var a170 = withColumn(a70, "A_170");
        final var b120 = withColumn(B_FIRST, "B_120");
        final var b150 = withColumn(b120, "OLD_EPOCH_150");
        final var b220 = withColumn(B_RECREATED, "NEW_EPOCH_220");
        DamengOffsetContext checkpoint;
        final var writer = history(expanded);
        try {
            record(writer, original, 10, new TableChanges().create(A));
            final var first = new DamengCaptureScopeChange(expanded, PARTITION, writer);
            checkpoint = first.resolve(new DamengOffsetContext.Loader(expanded, true).load(offset(original, 40).getOffset()));
            first.prepare(checkpoint, List.of(A.id(), B_FIRST.id()), preparation(100, B_FIRST));
            // S1 is appended before CDC catches up with the old table's DDL.
            record(writer, expanded, 70, new TableChanges().alter(a70));
            record(writer, expanded, 120, new TableChanges().alter(b120));
            record(writer, expanded, 150, new TableChanges().alter(b150));
            final var narrowing = new DamengCaptureScopeChange(original, PARTITION, writer);
            checkpoint = narrowing.resolve(new DamengOffsetContext.Loader(original, true).load(checkpoint.getOffset()));
            narrowing.prepare(checkpoint, List.of(A.id(), B_FIRST.id()), preparation(160, B_FIRST));
            final var readding = new DamengCaptureScopeChange(expanded, PARTITION, writer);
            checkpoint = readding.resolve(new DamengOffsetContext.Loader(expanded, true).load(checkpoint.getOffset()));
            readding.prepare(checkpoint, List.of(A.id(), B_RECREATED.id()), preparation(200, B_RECREATED));
            // S2 is also persisted early. Its floor blocks replayed old B DDL
            // before the streaming path can append another history record.
            assertThat(readding.excludes(B_FIRST.id(), 102, 150)).isTrue();
            assertThat(readding.excludes(null, 102, 100)).isTrue();
            assertThat(readding.excludes(A.id(), 101, 170)).isFalse();
            record(writer, expanded, 170, new TableChanges().alter(a170));
            record(writer, expanded, 220, new TableChanges().alter(b220));
        }
        finally {
            writer.stop();
        }

        final var reader = history(expanded);
        try {
            final long[] frontiers = { 40, 70, 99, 100, 119, 120, 150, 170, 199, 200, 219, 220 };
            for (long frontier : frontiers) {
                final var desired = new DamengOffsetContext.Loader(expanded, true).load(checkpoint.getOffset());
                desired.setCommitLsn(frontier);
                final var change = new DamengCaptureScopeChange(expanded, PARTITION, reader);
                change.resolve(desired);
                final var recovered = new Tables();
                reader.recover(Map.of(PARTITION.getSourcePartition(), desired.getOffset()), recovered, null);
                assertThat(recovered.forTable(A.id())).as("A at commit %s", frontier)
                        .isEqualTo(frontier < 70 ? A : frontier < 170 ? a70 : a170);
                final Table expectedB = frontier < 100 ? null
                        : frontier < 120 ? B_FIRST
                                : frontier < 150 ? b120
                                        : frontier < 200 ? b150
                                                : frontier < 220 ? B_RECREATED : b220;
                assertThat(recovered.forTable(B_FIRST.id())).as("B at commit %s", frontier).isEqualTo(expectedB);
                assertThat(desired.getLsn()).as("replay LSN at commit %s", frontier).isEqualTo(10);
            }
        }
        finally {
            reader.stop();
        }
    }

    @Test
    void shouldRecoverAnEmptyBaselinePredicateAndAllowOnlyNewTableCreationsAfterItsBoundary() throws Exception {
        final Path file = directory.resolve("empty-baseline-history.json");
        final var original = config(file, "APP\\.A");
        final var expanded = config(file, "APP\\.(A|B)");
        DamengOffsetContext checkpoint;
        final var writer = history(expanded);
        try {
            record(writer, original, 10, new TableChanges().create(A));
            final var change = new DamengCaptureScopeChange(expanded, PARTITION, writer);
            checkpoint = change.resolve(new DamengOffsetContext.Loader(expanded, true).load(offset(original, 40).getOffset()));
            change.prepare(checkpoint, List.of(A.id()), preparation(100, B_FIRST));
            assertThat(change.pendingTables()).isEmpty();
            // No JDBC-visible B exists at S. Its old CREATE is rejected using
            // the persisted filter transition rather than a table baseline.
            assertThat(change.excludes(B_FIRST.id(), 102, 90)).isTrue();
            assertThat(change.excludes(B_RECREATED.id(), 202, 110)).isFalse();
            record(writer, expanded, 110, new TableChanges().create(B_RECREATED));
        }
        finally {
            writer.stop();
        }

        final var reader = history(expanded);
        try {
            final var change = new DamengCaptureScopeChange(expanded, PARTITION, reader);
            final var resumed = change.resolve(new DamengOffsetContext.Loader(expanded, true).load(checkpoint.getOffset()));
            assertThat(change.excludes(B_FIRST.id(), 102, 90)).isTrue();
            assertThat(change.excludes(B_FIRST.id(), 102, 100)).isTrue();
            assertThat(change.excludes(B_RECREATED.id(), 202, 110)).isFalse();
            for (long frontier : new long[]{ 90, 100, 109, 110 }) {
                resumed.setCommitLsn(frontier);
                final var recovered = new Tables();
                reader.recover(Map.of(PARTITION.getSourcePartition(), resumed.getOffset()), recovered, null);
                assertThat(recovered.forTable(A.id())).as("A at commit %s", frontier).isEqualTo(A);
                assertThat(recovered.forTable(B_FIRST.id())).as("B at commit %s", frontier)
                        .isEqualTo(frontier < 110 ? null : B_RECREATED);
            }
        }
        finally {
            reader.stop();
        }
    }

    private static DamengConnectorConfig config(Path file, String include) {
        return new DamengConnectorConfig(DamengConnectorConfigTest.configuration()
                .with("table.include.list", include)
                .with("schema.history.internal", FileSchemaHistory.class.getName())
                .with(FileSchemaHistory.FILE_PATH, file.toString()).build());
    }

    private static FileSchemaHistory history(DamengConnectorConfig config) {
        final var history = new FileSchemaHistory();
        history.configure(config.getConfig(), config.getHistoryRecordComparator(), SchemaHistoryListener.NOOP, false);
        history.start();
        return history;
    }

    private static DamengOffsetContext offset(DamengConnectorConfig config, long commit) {
        final var offset = new DamengOffsetContext(config, 10);
        offset.setCommitLsn(commit);
        offset.setChangeLsn(commit);
        return offset;
    }

    private static void record(FileSchemaHistory history, DamengConnectorConfig config, long commit, TableChanges changes) {
        history.record(PARTITION.getSourcePartition(), offset(config, commit).getOffset(), "DAMENG", "APP", null, changes, Instant.EPOCH);
    }

    private static DamengCaptureScopePreparation preparation(long boundary, Table table) throws Exception {
        final var preparation = mock(DamengCaptureScopePreparation.class);
        doAnswer(invocation -> {
            final var baseline = new DamengCaptureScopePreparation.Baseline(boundary, List.of(table), 7, null);
            final Consumer<DamengCaptureScopePreparation.Baseline> persist = invocation.getArgument(1);
            persist.accept(baseline);
            return baseline;
        }).when(preparation).prepare(anyCollection(), any());
        doAnswer(invocation -> {
            final var baseline = new DamengCaptureScopePreparation.Baseline(boundary, List.of(), 7, null);
            final Consumer<DamengCaptureScopePreparation.Baseline> persist = invocation.getArgument(0);
            persist.accept(baseline);
            return baseline;
        }).when(preparation).prepareBoundary(any());
        return preparation;
    }

    private static Table withColumn(Table table, String name) {
        return table.edit().addAttributes(table.attributes())
                .addColumn(Column.editor().name(name).position(table.columns().size() + 1).jdbcType(Types.INTEGER).type("INT").create()).create();
    }

    private static Table table(String name, long identity) {
        return Table.editor().tableId(new TableId(null, "APP", name))
                .addColumn(Column.editor().name("ID").position(1).jdbcType(Types.INTEGER).type("INT").create())
                .addAttribute(Attribute.editor().name("dameng.object.id").value(Long.toString(identity)).create())
                .setPrimaryKeyNames("ID").create();
    }
}
