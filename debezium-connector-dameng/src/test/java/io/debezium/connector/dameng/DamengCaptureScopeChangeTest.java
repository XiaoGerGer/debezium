/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.sql.Types;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import org.apache.kafka.connect.errors.ConnectException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import io.debezium.document.DocumentReader;
import io.debezium.document.DocumentWriter;
import io.debezium.relational.Attribute;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.relational.history.HistoryRecord;
import io.debezium.relational.history.SchemaHistory;
import io.debezium.relational.history.SchemaHistoryException;
import io.debezium.relational.history.TableChanges;

class DamengCaptureScopeChangeTest {
    private static final DamengPartition PARTITION = new DamengPartition("dameng");
    private static final Table FIRST = table("A", 101);
    private static final Table ADDED = table("B", 102);
    private static final String CHANGE_KEY = "dameng_capture_scope_change";
    private static final String BOUNDARY_KEY = "capture_scope_boundary_lsn";
    private static final String RESUME_KEY = "capture_scope_resume_offset";
    private static final String PREVIOUS_MANIFEST_KEY = "capture_scope_previous_manifest";
    private final SchemaHistory history = mock(SchemaHistory.class);
    private final DamengCaptureScopePreparation preparation = mock(DamengCaptureScopePreparation.class);
    private final List<HistoryRecord> records = new ArrayList<>();
    private final DamengConnectorConfig original = config("APP\\.A");
    private final DamengConnectorConfig expanded = config("APP\\.(A|B)");
    private boolean failAfterWrite;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void before() throws Exception {
        when(history.exists()).thenAnswer(invocation -> !records.isEmpty());
        doAnswer(invocation -> {
            final Consumer<HistoryRecord> consumer = invocation.getArgument(0);
            records.forEach(consumer);
            return null;
        }).when(history).readRecords(any());
        doAnswer(invocation -> {
            final HistoryRecord record = new HistoryRecord(invocation.getArgument(0), invocation.getArgument(1),
                    invocation.getArgument(2), invocation.getArgument(3), invocation.getArgument(4), invocation.getArgument(5), invocation.getArgument(6));
            records.add(roundTrip(record));
            if (failAfterWrite) {
                throw new SchemaHistoryException("history acknowledgement lost");
            }
            return null;
        }).when(history).record(anyMap(), anyMap(), any(), any(), any(), any(), any());
        doAnswer(invocation -> {
            final var baseline = new DamengCaptureScopePreparation.Baseline(100, List.of(ADDED), 7, null);
            final Consumer<DamengCaptureScopePreparation.Baseline> persist = invocation.getArgument(1);
            persist.accept(baseline);
            return baseline;
        }).when(preparation).prepare(anyCollection(), any());
        doAnswer(invocation -> {
            final var baseline = new DamengCaptureScopePreparation.Baseline(100, List.of(), 7, null);
            final Consumer<DamengCaptureScopePreparation.Baseline> persist = invocation.getArgument(0);
            persist.accept(baseline);
            return baseline;
        }).when(preparation).prepareBoundary(any());
    }

    @Test
    void shouldPersistTheCompleteBoundaryWithoutAdvancingTheExistingReplayOrCommitPosition() throws Exception {
        final var previous = checkpoint(original);
        final var offset = startup(expanded, previous);
        final var change = change(expanded);
        assertThat(change.resolve(offset)).isSameAs(offset);

        change.prepare(offset, List.of(FIRST.id(), ADDED.id()), preparation);

        assertThat(records).hasSize(1);
        assertPositionUnchanged(offset);
        assertThat(offset.getCaptureScopeGeneration()).isNotBlank();
        assertThat(offset.getCaptureScopeManifest()).isEqualTo(new DamengOffsetContext(expanded, 1).getCaptureScopeManifest());
        final var position = records.get(0).document().getDocument(HistoryRecord.Fields.POSITION);
        assertThat(position.getString(CHANGE_KEY)).isEqualTo("v1");
        assertThat(position.getLong(BOUNDARY_KEY)).isEqualTo(100);
        assertThat(position.getLong(SourceInfo.COMMIT_LSN_KEY)).isEqualTo(100);
        assertThat(position.getString(DamengOffsetContext.MINING_TRANSACTION_ID_KEY)).isNull();
        assertThat(position.getString(PREVIOUS_MANIFEST_KEY)).isEqualTo(previous.get(DamengOffsetContext.CAPTURE_SCOPE_MANIFEST_KEY));
        final var resume = DocumentReader.defaultReader().read(position.getString(RESUME_KEY));
        assertThat(resume.getLong(SourceInfo.LSN_KEY)).isEqualTo(10);
        assertThat(resume.getLong(SourceInfo.COMMIT_LSN_KEY)).isEqualTo(80);
        assertThat(resume.getLong(SourceInfo.EVENT_SERIAL_NO_KEY)).isEqualTo(3);
        assertThat(resume.getString(DamengOffsetContext.MINING_TRANSACTION_ID_KEY)).isEqualTo("existing-tx");
        assertThat(records.get(0).document().getArray(HistoryRecord.Fields.TABLE_CHANGES).size()).isEqualTo(1);
        assertThat(change.excludes(ADDED.id(), 102, 99)).isTrue();
        assertThat(change.excludes(ADDED.id(), 102, 100)).isTrue();
        assertThat(change.excludes(ADDED.id(), 102, 101)).isFalse();
        assertThat(change.excludes(FIRST.id(), 101, 1)).isFalse();
    }

    @Test
    void shouldPreserveBeginOnlyCheckpointOrdinalAcrossScopeExpansionAndRecovery() throws Exception {
        final var checkpoint = startup(original, checkpoint(original));
        checkpoint.setEventSerialNo(-1);
        final var expandedChange = change(expanded);

        expandedChange.prepare(checkpoint, List.of(FIRST.id(), ADDED.id()), preparation);

        final var descriptor = records.get(0).document().getDocument(HistoryRecord.Fields.POSITION);
        final var savedResume = DocumentReader.defaultReader().read(descriptor.getString(RESUME_KEY));
        assertThat(savedResume.getLong(SourceInfo.EVENT_SERIAL_NO_KEY)).isEqualTo(-1);

        final var recovered = change(expanded).resolve(startup(expanded, checkpoint(original)));
        assertThat(recovered.getEventSerialNo()).isEqualTo(-1);
        assertThat(recovered.getTransactionId()).isEqualTo("existing-tx");
        assertThat(recovered.getLsn()).isEqualTo(10);
        assertThat(recovered.getCommitLsn()).isEqualTo(80);
    }

    @Test
    void shouldReuseTheAcknowledgedBoundaryWhenConnectStillHasTheOldScopeCheckpoint() throws Exception {
        final var previous = checkpoint(original);
        final var prepared = prepare(previous);
        final var generation = prepared.getCaptureScopeGeneration();
        final var restart = change(expanded);

        final var resumed = restart.resolve(startup(expanded, previous));
        restart.prepare(resumed, List.of(FIRST.id(), ADDED.id()), preparation);

        assertPositionUnchanged(resumed);
        assertThat(resumed.getCaptureScopeGeneration()).isEqualTo(generation);
        assertThat(records).hasSize(1);
        verify(preparation).prepare(anyCollection(), any());
        assertThat(restart.excludes(ADDED.id(), 102, 100)).isTrue();
        assertThat(restart.excludes(ADDED.id(), 102, 101)).isFalse();
    }

    @Test
    void shouldPreferTheNewGenerationConnectCheckpointOverThePersistedResumeOffset() throws Exception {
        final var prepared = prepare(checkpoint(original));
        prepared.setLsn(12);
        prepared.setCommitLsn(140);
        prepared.setChangeLsn(130);
        prepared.setTransactionId("newer-tx");
        prepared.setEventSerialNo(9);

        final var resumed = change(expanded).resolve(startup(expanded, prepared.getOffset()));

        assertThat(resumed.getLsn()).isEqualTo(12);
        assertThat(resumed.getCommitLsn()).isEqualTo(140);
        assertThat(resumed.getChangeLsn()).isEqualTo(130);
        assertThat(resumed.getTransactionId()).isEqualTo("newer-tx");
        assertThat(resumed.getEventSerialNo()).isEqualTo(9);
        assertThat(resumed.getCaptureScopeGeneration()).isEqualTo(prepared.getCaptureScopeGeneration());
    }

    @Test
    void shouldRecoverAnAcknowledgementLostAfterHistoryStoredTheBoundary() throws Exception {
        final var previous = checkpoint(original);
        failAfterWrite = true;
        assertThatThrownBy(() -> prepare(previous)).isInstanceOf(SchemaHistoryException.class);
        assertThat(records).hasSize(1);
        failAfterWrite = false;

        final var restarted = change(expanded);
        final var resumed = restarted.resolve(startup(expanded, previous));
        restarted.prepare(resumed, List.of(FIRST.id(), ADDED.id()), preparation);

        assertPositionUnchanged(resumed);
        assertThat(resumed.getCaptureScopeGeneration()).isEqualTo(records.get(0).document().getDocument(HistoryRecord.Fields.POSITION)
                .getString(DamengOffsetContext.CAPTURE_SCOPE_GENERATION_KEY));
        assertThat(records).hasSize(1);
        verify(preparation).prepare(anyCollection(), any());
    }

    @Test
    void shouldRejectACheckpointWhoseScopeGenerationHasLostItsHistoryDescriptor() throws Exception {
        final var prepared = prepare(checkpoint(original));
        records.clear();

        assertThatThrownBy(() -> change(expanded).resolve(startup(expanded, prepared.getOffset())))
                .isInstanceOf(ConnectException.class);
    }

    @Test
    void shouldRejectAnUnknownGenerationInsteadOfSilentlyReplacingItFromHistory() throws Exception {
        final var prepared = prepare(checkpoint(original));
        final Map<String, Object> damaged = new HashMap<>(prepared.getOffset());
        damaged.put(DamengOffsetContext.CAPTURE_SCOPE_GENERATION_KEY, "00000000-0000-0000-0000-000000000001");

        assertThatThrownBy(() -> change(expanded).resolve(startup(expanded, damaged)))
                .isInstanceOf(ConnectException.class);
    }

    @Test
    void shouldNotCreateABoundaryOrAcquireLocksWhenTheScopeHasNotChanged() throws Exception {
        final var offset = new DamengOffsetContext(original, 10);
        final var change = change(original);

        assertThat(change.resolve(offset)).isSameAs(offset);
        change.prepare(offset, List.of(FIRST.id(), ADDED.id()), preparation);

        assertThat(records).isEmpty();
        verifyNoInteractions(preparation);
        verify(history, never()).record(anyMap(), anyMap(), any(), any(), any(), any(), any());
    }

    @Test
    void shouldKeepTheOrdinaryLoaderStrictWhileStartupPreservesTheOriginalScope() {
        final var previous = checkpoint(original);
        assertThatThrownBy(() -> new DamengOffsetContext.Loader(expanded).load(previous)).isInstanceOf(ConnectException.class);
        final var resumed = startup(expanded, previous);
        assertThat(resumed.getOffset().get(DamengOffsetContext.CAPTURE_SCOPE_KEY)).isEqualTo(previous.get(DamengOffsetContext.CAPTURE_SCOPE_KEY));
        assertThat(resumed.getCaptureScopeManifest()).isEqualTo(previous.get(DamengOffsetContext.CAPTURE_SCOPE_MANIFEST_KEY));
        assertPositionUnchanged(resumed);
    }

    @Test
    void shouldRejectSignalTableChangesBeforePreparingOrPersistingANewBaseline() throws Exception {
        final var changed = new DamengConnectorConfig(expanded.getConfig().edit().with("signal.data.collection", "APP.SIGNAL").build());
        final var offset = startup(changed, checkpoint(original));
        final var change = change(changed);
        final var resolved = change.resolve(offset);

        assertThatThrownBy(() -> change.prepare(resolved, List.of(FIRST.id(), ADDED.id()), preparation))
                .isInstanceOf(ConnectException.class);
        assertThat(records).isEmpty();
        verifyNoInteractions(preparation);
    }

    @Test
    void shouldIgnoreOtherSourceAndOrdinaryDdlRecordsWhenFindingTheScopeDescriptor() throws Exception {
        final var prepared = prepare(checkpoint(original));
        final var foreign = roundTrip(records.get(0));
        foreign.document().getDocument(HistoryRecord.Fields.SOURCE).setString("server", "other-source");
        records.add(foreign);
        records.add(new HistoryRecord(PARTITION.getSourcePartition(), Map.of("lsn", 150L), "DAMENG", "APP",
                "ALTER TABLE APP.A ADD DATA INT", new TableChanges(), Instant.EPOCH));

        assertThat(change(expanded).resolve(startup(expanded, checkpoint(original))).getCaptureScopeGeneration())
                .isEqualTo(prepared.getCaptureScopeGeneration());
    }

    @Test
    void shouldInstallTheBaselineOnlyAfterItsBoundaryAndKeepTheHistoricalFloorAfterActivation() throws Exception {
        final var change = change(expanded);
        final var offset = change.resolve(startup(expanded, checkpoint(original)));
        change.prepare(offset, List.of(FIRST.id(), ADDED.id()), preparation);
        final var schema = mock(DamengDatabaseSchema.class);
        assertThat(change.pendingTables()).containsExactly(ADDED.id());
        assertThat(change.isPending(ADDED.id())).isTrue();

        change.beforeRow(schema, 99);
        change.beforeRow(schema, 100);
        change.scannedThrough(schema, 99);
        verifyNoInteractions(schema);
        assertThat(change.isPending(ADDED.id())).isTrue();

        change.beforeRow(schema, 101);
        change.beforeRow(schema, 102);
        change.scannedThrough(schema, 200);

        verify(schema).registerRecoveryTables(List.of(ADDED));
        assertThat(change.pendingTables()).isEmpty();
        assertThat(change.isPending(ADDED.id())).isFalse();
        assertThat(change.excludes(ADDED.id(), 102, 100)).isTrue();
        assertThat(change.excludes(ADDED.id(), 102, 101)).isFalse();
        assertPositionUnchanged(offset);
    }

    @Test
    void shouldActivateOnTheScannedFrontierEvenIfThereIsNoLaterTableRow() throws Exception {
        final var change = change(expanded);
        final var offset = change.resolve(startup(expanded, checkpoint(original)));
        change.prepare(offset, List.of(FIRST.id(), ADDED.id()), preparation);
        final var schema = mock(DamengDatabaseSchema.class);

        change.scannedThrough(schema, 100);
        change.beforeRow(schema, 101);

        verify(schema).registerRecoveryTables(List.of(ADDED));
        assertThat(change.isPending(ADDED.id())).isFalse();
        assertPositionUnchanged(offset);
    }

    @Test
    void shouldFilterHistoricalRowsByObjectIdWhenTheTableNameIsNotAvailable() throws Exception {
        final var change = change(expanded);
        final var offset = change.resolve(startup(expanded, checkpoint(original)));
        change.prepare(offset, List.of(FIRST.id(), ADDED.id()), preparation);

        assertThat(change.excludes(null, 102, 99)).isTrue();
        assertThat(change.excludes(null, 102, 100)).isTrue();
        assertThat(change.excludes(null, 102, 101)).isFalse();
        assertThat(change.excludes(null, 101, 99)).isFalse();
        assertThat(change.excludes(FIRST.id(), 101, 99)).isFalse();
    }

    @Test
    void shouldKeepTheBaselinePendingWhenRecoveryHasNotReachedItsCommitFrontier() throws Exception {
        final var prepared = prepare(checkpoint(original));
        final var change = change(expanded);
        final var offset = change.resolve(startup(expanded, prepared.getOffset()));
        final var schema = mock(DamengDatabaseSchema.class);

        change.recovered(schema, offset);
        change.beforeRow(schema, 100);

        assertThat(change.isPending(ADDED.id())).isTrue();
        verifyNoInteractions(schema);
        change.scannedThrough(schema, 100);
        verify(schema).registerRecoveryTables(List.of(ADDED));
    }

    @Test
    void shouldNotOverwriteLaterRecoveredAlterationsWhenReplayLsnIsStillBeforeTheBoundary() throws Exception {
        final var prepared = prepare(checkpoint(original));
        prepared.setCommitLsn(140);
        final var change = change(expanded);
        final var offset = change.resolve(startup(expanded, prepared.getOffset()));
        final var schema = mock(DamengDatabaseSchema.class);

        change.recovered(schema, offset);
        change.beforeRow(schema, 101);
        change.scannedThrough(schema, 200);

        assertThat(offset.getLsn()).isEqualTo(10);
        assertThat(change.isPending(ADDED.id())).isFalse();
        assertThat(change.excludes(ADDED.id(), 102, 100)).isTrue();
        verifyNoInteractions(schema);
    }

    @Test
    void shouldAllowAnExactDuplicateHistoryAcknowledgementWithoutRepeatingPreparation() throws Exception {
        final var previous = checkpoint(original);
        final var prepared = prepare(previous);
        records.add(roundTrip(records.get(0)));

        final var resumed = change(expanded).resolve(startup(expanded, previous));

        assertThat(resumed.getCaptureScopeGeneration()).isEqualTo(prepared.getCaptureScopeGeneration());
        assertPositionUnchanged(resumed);
        verify(preparation).prepare(anyCollection(), any());
    }

    @Test
    void shouldRejectConflictingCopiesOfTheSameGeneration() throws Exception {
        final var previous = checkpoint(original);
        prepare(previous);
        final var damaged = roundTrip(records.get(0));
        damaged.document().getDocument(HistoryRecord.Fields.POSITION).setNumber(BOUNDARY_KEY, 101);
        records.add(damaged);

        assertThatThrownBy(() -> change(expanded).resolve(startup(expanded, previous)))
                .isInstanceOf(ConnectException.class).hasMessageContaining("generation");
    }

    @Test
    void shouldRejectADescriptorWhoseResumeOffsetHasAnotherGeneration() throws Exception {
        final var previous = checkpoint(original);
        prepare(previous);
        final var position = records.get(0).document().getDocument(HistoryRecord.Fields.POSITION);
        final var resume = DocumentReader.defaultReader().read(position.getString(RESUME_KEY));
        resume.setString(DamengOffsetContext.CAPTURE_SCOPE_GENERATION_KEY, "other-generation");
        position.setString(RESUME_KEY, DocumentWriter.defaultWriter().write(resume));

        assertThatThrownBy(() -> change(expanded).resolve(startup(expanded, previous)))
                .isInstanceOf(ConnectException.class).hasMessageContaining("resume offset");
    }

    @Test
    void shouldPersistPureNarrowingWithoutLockingOrAdvancingGlobalCdc() throws Exception {
        final var offset = startup(original, checkpoint(expanded));
        final var snapshot = offset.getIncrementalSnapshotContext();
        snapshot.addDataCollectionNamesToSnapshot("request", List.of("APP.B", "APP.A"), List.of(), "");
        snapshot.maximumKey(new Object[]{ 100 });
        snapshot.pauseSnapshot();
        final var change = change(original);

        change.prepare(offset, List.of(FIRST.id(), ADDED.id()), preparation);

        verify(preparation).prepareBoundary(any());
        verify(preparation, never()).prepare(anyCollection(), any());
        assertThat(records).hasSize(1);
        assertThat(records.get(0).document().getArray(HistoryRecord.Fields.TABLE_CHANGES).size()).isZero();
        assertThat(records.get(0).document().getDocument(HistoryRecord.Fields.POSITION).getLong(BOUNDARY_KEY)).isEqualTo(100);
        assertPositionUnchanged(offset);
        assertThat(snapshot.getDataCollections()).extracting(collection -> collection.getId()).containsExactly(FIRST.id());
        assertThat(snapshot.maximumKey()).isEmpty();
        assertThat(snapshot.isSnapshotPaused()).isTrue();
        final var recovered = change(original).resolve(startup(original, checkpoint(expanded)));
        assertPositionUnchanged(recovered);
        assertThat(recovered.getIncrementalSnapshotContext().getDataCollections()).extracting(collection -> collection.getId())
                .containsExactly(FIRST.id());
    }

    @Test
    void shouldRecoverTheLatestGenerationInAnExpansionAndNarrowingChain() throws Exception {
        final var first = prepare(checkpoint(original));
        final var narrowed = startup(original, first.getOffset());
        final var narrowing = change(original);
        narrowing.resolve(narrowed);
        narrowing.prepare(narrowed, List.of(FIRST.id(), ADDED.id()), preparation);
        assertThat(records).hasSize(2);
        assertThat(narrowed.getCaptureScopeGeneration()).isNotEqualTo(first.getCaptureScopeGeneration());

        final var recovered = change(original).resolve(startup(original, checkpoint(original)));

        assertThat(recovered.getCaptureScopeGeneration()).isEqualTo(narrowed.getCaptureScopeGeneration());
        assertThat(recovered.getCaptureScopeManifest()).isEqualTo(new DamengOffsetContext(original, 1).getCaptureScopeManifest());
        assertPositionUnchanged(recovered);
        verify(preparation).prepare(anyCollection(), any());
    }

    @Test
    void shouldRejectAGenerationChainWhosePreviousManifestIsNotThePreviousGeneration() throws Exception {
        final var first = prepare(checkpoint(original));
        final var narrowing = change(original);
        final var narrowed = narrowing.resolve(startup(original, first.getOffset()));
        narrowing.prepare(narrowed, List.of(FIRST.id(), ADDED.id()), preparation);
        records.get(1).document().getDocument(HistoryRecord.Fields.POSITION)
                .setString(PREVIOUS_MANIFEST_KEY, new DamengOffsetContext(original, 1).getCaptureScopeManifest());

        assertThatThrownBy(() -> change(original).resolve(startup(original, narrowed.getOffset())))
                .isInstanceOf(ConnectException.class).hasMessageContaining("chain");
    }

    @Test
    void shouldRejectADecreasingBoundaryAcrossOtherwiseValidScopeGenerations() throws Exception {
        final var first = prepare(checkpoint(original));
        final var narrowing = change(original);
        final var narrowed = narrowing.resolve(startup(original, first.getOffset()));
        narrowing.prepare(narrowed, List.of(FIRST.id(), ADDED.id()), preparation);
        final var position = records.get(1).document().getDocument(HistoryRecord.Fields.POSITION);
        position.setNumber(BOUNDARY_KEY, 99);
        position.setNumber(SourceInfo.COMMIT_LSN_KEY, 99);

        assertThatThrownBy(() -> change(original).resolve(startup(original, narrowed.getOffset())))
                .isInstanceOf(ConnectException.class);
        assertPositionUnchanged(narrowed);
    }

    @Test
    void shouldStartANewTableEpochWhenAPreviouslyRemovedTableIsAddedAgain() throws Exception {
        final var first = prepare(checkpoint(original));
        final var narrowing = change(original);
        final var narrowed = narrowing.resolve(startup(original, first.getOffset()));
        narrowing.prepare(narrowed, List.of(FIRST.id(), ADDED.id()), preparation);
        final var recreated = table("B", 202);
        doAnswer(invocation -> {
            final var baseline = new DamengCaptureScopePreparation.Baseline(200, List.of(recreated), 7, null);
            final Consumer<DamengCaptureScopePreparation.Baseline> persist = invocation.getArgument(1);
            persist.accept(baseline);
            return baseline;
        }).when(preparation).prepare(anyCollection(), any());
        final var change = change(expanded);
        final var offset = change.resolve(startup(expanded, narrowed.getOffset()));

        change.prepare(offset, List.of(FIRST.id(), recreated.id()), preparation);
        final var schema = mock(DamengDatabaseSchema.class);
        change.scannedThrough(schema, 199);
        assertThat(change.isPending(recreated.id())).isTrue();
        verifyNoInteractions(schema);
        change.scannedThrough(schema, 200);

        verify(schema).registerRecoveryTables(List.of(recreated));
        assertThat(records).hasSize(3);
        assertThat(change.excludes(recreated.id(), 202, 200)).isTrue();
        assertThat(change.excludes(null, 202, 200)).isTrue();
        assertThat(change.excludes(recreated.id(), 202, 201)).isFalse();
        assertThat(change.excludes(null, 102, 100)).isTrue();
        assertThat(change.excludes(null, 102, 101)).isFalse();
        assertThat(change.excludes(FIRST.id(), 102, 90)).isFalse();
        final var restarted = change(expanded);
        restarted.resolve(startup(expanded, offset.getOffset()));
        assertThat(restarted.excludes(null, 102, 100)).isTrue();
        assertThat(restarted.excludes(null, 202, 200)).isTrue();
        assertThat(restarted.excludes(FIRST.id(), 102, 90)).isFalse();
        assertPositionUnchanged(offset);
    }

    @Test
    void shouldNotExposeRemovedPendingTablesToIncrementalSnapshotRequests() throws Exception {
        final var first = prepare(checkpoint(original));
        final var narrowing = change(original);
        final var offset = narrowing.resolve(startup(original, first.getOffset()));
        narrowing.prepare(offset, List.of(FIRST.id(), ADDED.id()), preparation);

        assertThat(narrowing.isPending(ADDED.id())).isFalse();
        assertThat(narrowing.pendingTables()).isEmpty();
        assertThat(narrowing.excludes(ADDED.id(), 102, 100)).isTrue();
        assertThat(narrowing.excludes(null, 102, 100)).isTrue();
    }

    @Test
    void shouldRejectAHistoricalRenameIdentityConflictBeforePersistingTheDescriptor() throws Exception {
        final var renamed = table("B", 101);
        doAnswer(invocation -> {
            final var baseline = new DamengCaptureScopePreparation.Baseline(100, List.of(renamed), 7, null);
            final Consumer<DamengCaptureScopePreparation.Baseline> persist = invocation.getArgument(1);
            persist.accept(baseline);
            return baseline;
        }).when(preparation).prepare(anyCollection(), any());
        final var schema = mock(DamengDatabaseSchema.class);
        final var tables = new Tables();
        tables.overwriteTable(FIRST);
        when(schema.getTables()).thenReturn(tables);
        final var change = change(expanded);
        final var offset = change.resolve(startup(expanded, checkpoint(original)));
        final var manifest = offset.getCaptureScopeManifest();

        assertThatThrownBy(() -> change.prepare(offset, List.of(FIRST.id(), renamed.id()), preparation, schema))
                .isInstanceOf(ConnectException.class).hasMessageContaining("APP.B").hasMessageContaining("APP.A").hasMessageContaining("rename");

        assertThat(records).isEmpty();
        assertThat(offset.getCaptureScopeGeneration()).isNull();
        assertThat(offset.getCaptureScopeManifest()).isEqualTo(manifest);
        assertPositionUnchanged(offset);
    }

    @Test
    void shouldAllowAReaddedTableToReplaceItsOwnHistoricalObjectIdentity() throws Exception {
        final var schema = mock(DamengDatabaseSchema.class);
        final var tables = new Tables();
        tables.overwriteTable(FIRST);
        tables.overwriteTable(ADDED);
        when(schema.getTables()).thenReturn(tables);
        final var change = change(expanded);
        final var offset = change.resolve(startup(expanded, checkpoint(original)));

        change.prepare(offset, List.of(FIRST.id(), ADDED.id()), preparation, schema);

        assertThat(records).hasSize(1);
        assertThat(change.isPending(ADDED.id())).isTrue();
        assertPositionUnchanged(offset);
    }

    @Test
    void shouldPreserveThePredicateBoundaryWhenTheExpandedScopeHasNoVisibleAddedTable() throws Exception {
        final var change = change(expanded);
        final var offset = change.resolve(startup(expanded, checkpoint(original)));

        change.prepare(offset, List.of(FIRST.id()), preparation);

        verify(preparation).prepareBoundary(any());
        verify(preparation, never()).prepare(anyCollection(), any());
        assertThat(records).hasSize(1);
        assertThat(records.get(0).document().getArray(HistoryRecord.Fields.TABLE_CHANGES).size()).isZero();
        assertThat(change.excludes(ADDED.id(), 0, 99)).isTrue();
        assertThat(change.excludes(ADDED.id(), 0, 100)).isTrue();
        assertThat(change.excludes(ADDED.id(), 0, 101)).isFalse();
        assertThat(change.excludes(FIRST.id(), 101, 99)).isFalse();
        assertThat(change.pendingTables()).isEmpty();
        assertPositionUnchanged(offset);

        final var restarted = change(expanded);
        final var resumed = restarted.resolve(startup(expanded, checkpoint(original)));

        assertThat(restarted.excludes(ADDED.id(), 0, 99)).isTrue();
        assertThat(restarted.excludes(ADDED.id(), 0, 100)).isTrue();
        assertThat(restarted.excludes(ADDED.id(), 0, 101)).isFalse();
        assertThat(restarted.excludes(FIRST.id(), 101, 99)).isFalse();
        assertThat(resumed.getCaptureScopeGeneration()).isEqualTo(offset.getCaptureScopeGeneration());
        assertPositionUnchanged(resumed);
    }

    @Test
    void shouldRejectAnInvisibleNewlyIncludedHistoricalTableBeforePreparingTheBoundary() throws Exception {
        final var schema = mock(DamengDatabaseSchema.class);
        final var tables = new Tables();
        tables.overwriteTable(FIRST);
        tables.overwriteTable(ADDED);
        when(schema.getTables()).thenReturn(tables);
        final var change = change(expanded);
        final var offset = change.resolve(startup(expanded, checkpoint(original)));
        final var previous = offset.getCaptureScopeManifest();

        assertThatThrownBy(() -> change.prepare(offset, List.of(FIRST.id()), preparation, schema))
                .isInstanceOf(ConnectException.class).hasMessageContaining("APP.B").hasMessageContaining("not currently visible")
                .hasMessageContaining("SELECT");

        verifyNoInteractions(preparation);
        assertThat(records).isEmpty();
        assertThat(offset.getCaptureScopeGeneration()).isNull();
        assertThat(offset.getCaptureScopeManifest()).isEqualTo(previous);
        assertPositionUnchanged(offset);
    }

    @Test
    void shouldRejectAChangedLegacyScopeWithoutAManifestBeforeAcquiringLocks() throws Exception {
        final Map<String, Object> legacy = new HashMap<>(checkpoint(original));
        legacy.remove(DamengOffsetContext.CAPTURE_SCOPE_MANIFEST_KEY);
        final var change = change(expanded);
        final var offset = change.resolve(startup(expanded, legacy));

        assertThatThrownBy(() -> change.prepare(offset, List.of(FIRST.id(), ADDED.id()), preparation))
                .isInstanceOf(ConnectException.class).hasMessageContaining("manifest");

        verifyNoInteractions(preparation);
        assertThat(records).isEmpty();
        assertPositionUnchanged(offset);
    }

    @Test
    void shouldAllowAnUnchangedLegacyScopeToAcquireItsManifestWithoutAChangeBoundary() throws Exception {
        final Map<String, Object> legacy = new HashMap<>(checkpoint(original));
        legacy.remove(DamengOffsetContext.CAPTURE_SCOPE_MANIFEST_KEY);
        final var change = change(original);
        final var offset = change.resolve(startup(original, legacy));

        change.prepare(offset, List.of(FIRST.id()), preparation);

        assertThat(offset.getCaptureScopeManifest()).isEqualTo(new DamengOffsetContext(original, 1).getCaptureScopeManifest());
        verifyNoInteractions(preparation);
        assertThat(records).isEmpty();
        assertPositionUnchanged(offset);
    }

    @Test
    void shouldRejectAScopeChangeDuringAnIncompleteInitialSnapshot() throws Exception {
        final var previous = new DamengOffsetContext(original, 10);
        previous.preSnapshotStart(false);
        final var change = change(expanded);
        final var offset = change.resolve(startup(expanded, previous.getOffset()));

        assertThatThrownBy(() -> change.prepare(offset, List.of(FIRST.id(), ADDED.id()), preparation))
                .isInstanceOf(ConnectException.class).hasMessageContaining("initial snapshot");

        verifyNoInteractions(preparation);
        assertThat(records).isEmpty();
        assertThat(offset.isInitialSnapshotRunning()).isTrue();
        assertThat(offset.getLsn()).isEqualTo(10);
    }

    @Test
    void shouldRejectADescriptorWithoutThePhysicalObjectIdentity() throws Exception {
        final var previous = checkpoint(original);
        prepare(previous);
        records.get(0).document().getArray(HistoryRecord.Fields.TABLE_CHANGES).get(0).asDocument()
                .getDocument("table").setArray("attributes", io.debezium.document.Array.create());

        assertThatThrownBy(() -> change(expanded).resolve(startup(expanded, previous)))
                .isInstanceOf(ConnectException.class);
    }

    @ParameterizedTest(name = "invalid descriptor: {0}")
    @ValueSource(strings = { "unsupported-version", "missing-generation", "blank-generation", "missing-tables", "missing-boundary", "negative-boundary",
            "zero-boundary", "missing-previous", "invalid-previous-json", "missing-resume", "invalid-resume-json", "invalid-current-manifest",
            "commit-boundary-mismatch", "nonzero-ordinal", "mining-transaction", "wrong-scope-signature" })
    void shouldRejectMalformedScopeDescriptorsEvenWhenConnectAlreadyHasTheSameGeneration(String damage) throws Exception {
        final var prepared = prepare(checkpoint(original));
        final var record = records.get(0).document();
        final var position = record.getDocument(HistoryRecord.Fields.POSITION);
        switch (damage) {
            case "unsupported-version" -> position.setString(CHANGE_KEY, "v99");
            case "missing-generation" -> position.remove(DamengOffsetContext.CAPTURE_SCOPE_GENERATION_KEY);
            case "blank-generation" -> position.setString(DamengOffsetContext.CAPTURE_SCOPE_GENERATION_KEY, " ");
            case "missing-tables" -> record.remove(HistoryRecord.Fields.TABLE_CHANGES);
            case "missing-boundary" -> position.remove(BOUNDARY_KEY);
            case "negative-boundary" -> position.setNumber(BOUNDARY_KEY, -1);
            case "zero-boundary" -> position.setNumber(BOUNDARY_KEY, 0);
            case "missing-previous" -> position.remove(PREVIOUS_MANIFEST_KEY);
            case "invalid-previous-json" -> position.setString(PREVIOUS_MANIFEST_KEY, "{");
            case "missing-resume" -> position.remove(RESUME_KEY);
            case "invalid-resume-json" -> position.setString(RESUME_KEY, "{");
            case "invalid-current-manifest" -> position.setString(DamengOffsetContext.CAPTURE_SCOPE_MANIFEST_KEY, "{}");
            case "commit-boundary-mismatch" -> position.setNumber(SourceInfo.COMMIT_LSN_KEY, 99);
            case "nonzero-ordinal" -> position.setNumber(SourceInfo.EVENT_SERIAL_NO_KEY, 1);
            case "mining-transaction" -> position.setString(DamengOffsetContext.MINING_TRANSACTION_ID_KEY, "unexpected-tx");
            case "wrong-scope-signature" -> position.setString(DamengOffsetContext.CAPTURE_SCOPE_KEY, DamengCaptureScope.signature(original));
            default -> throw new IllegalArgumentException(damage);
        }

        assertThatThrownBy(() -> change(expanded).resolve(startup(expanded, prepared.getOffset())))
                .isInstanceOf(ConnectException.class);
        assertPositionUnchanged(prepared);
    }

    @ParameterizedTest(name = "invalid resume offset: {0}")
    @ValueSource(strings = { "missing-lsn", "negative-lsn", "fractional-lsn", "nonnumeric-lsn", "negative-commit", "negative-change", "negative-ordinal",
            "wrong-scope-signature", "different-recovery-nonce" })
    void shouldValidateTheDurableResumeOffsetEvenWhenANewerSameGenerationCheckpointExists(String damage) throws Exception {
        final var prepared = prepare(checkpoint(original));
        final var position = records.get(0).document().getDocument(HistoryRecord.Fields.POSITION);
        final var resume = DocumentReader.defaultReader().read(position.getString(RESUME_KEY));
        switch (damage) {
            case "missing-lsn" -> resume.remove(SourceInfo.LSN_KEY);
            case "negative-lsn" -> resume.setNumber(SourceInfo.LSN_KEY, -1);
            case "fractional-lsn" -> resume.setNumber(SourceInfo.LSN_KEY, 10.5);
            case "nonnumeric-lsn" -> resume.setString(SourceInfo.LSN_KEY, "not-an-lsn");
            case "negative-commit" -> resume.setNumber(SourceInfo.COMMIT_LSN_KEY, -1);
            case "negative-change" -> resume.setNumber(SourceInfo.CHANGE_LSN_KEY, -1);
            case "negative-ordinal" -> resume.setNumber(SourceInfo.EVENT_SERIAL_NO_KEY, -2);
            case "wrong-scope-signature" -> resume.setString(DamengOffsetContext.CAPTURE_SCOPE_KEY, DamengCaptureScope.signature(original));
            case "different-recovery-nonce" -> {
                resume.setString(DamengOffsetContext.RECOVERY_NONCE_KEY, "another-recovery");
                resume.setNumber(DamengOffsetContext.RECOVERY_BASELINE_LSN_KEY, 5);
            }
            default -> throw new IllegalArgumentException(damage);
        }
        position.setString(RESUME_KEY, DocumentWriter.defaultWriter().write(resume));
        prepared.setCommitLsn(140);

        assertThatThrownBy(() -> change(expanded).resolve(startup(expanded, prepared.getOffset())))
                .isInstanceOf(ConnectException.class);
        assertThat(prepared.getLsn()).isEqualTo(10);
        assertThat(prepared.getCommitLsn()).isEqualTo(140);
    }

    @ParameterizedTest(name = "invalid table baseline: {0}")
    @ValueSource(strings = { "alter", "drop", "empty-columns", "already-included", "outside-scope", "zero-identity", "negative-identity",
            "invalid-identity", "duplicate-identity", "duplicate-table-different-identity" })
    void shouldRejectUnsafeTableBaselinesBeforeActivatingAnyTable(String damage) throws Exception {
        final var prepared = prepare(checkpoint(original));
        final var changes = new TableChanges();
        switch (damage) {
            case "alter" -> changes.alter(ADDED);
            case "drop" -> changes.drop(ADDED.id());
            case "empty-columns" -> changes.create(Table.editor().tableId(ADDED.id())
                    .addAttribute(Attribute.editor().name("dameng.object.id").value("102").create()).create());
            case "already-included" -> changes.create(FIRST);
            case "outside-scope" -> changes.create(table("C", 103));
            case "zero-identity" -> changes.create(table("B", 0));
            case "negative-identity" -> changes.create(table("B", -1));
            case "invalid-identity" -> changes.create(ADDED.edit().addAttributes(ADDED.attributes())
                    .addAttribute(Attribute.editor().name("dameng.object.id").value("invalid").create()).create());
            case "duplicate-identity" -> changes.create(ADDED).create(ADDED);
            case "duplicate-table-different-identity" -> changes.create(ADDED).create(table("B", 202));
            default -> throw new IllegalArgumentException(damage);
        }
        records.get(0).document().setArray(HistoryRecord.Fields.TABLE_CHANGES, new io.debezium.relational.history.JsonTableChangeSerializer().serialize(changes));
        final var change = change(expanded);

        assertThatThrownBy(() -> change.resolve(startup(expanded, prepared.getOffset())))
                .isInstanceOf(ConnectException.class);
    }

    @Test
    void shouldIgnoreDescriptorsFromAnEarlierExplicitRecoveryGeneration() throws Exception {
        prepare(checkpoint(original));
        final var recovery = new DamengOffsetContext(expanded, 500);
        recovery.setRecoveryBaseline("fresh-recovery", 500);
        final var change = change(expanded);

        assertThat(change.resolve(recovery)).isSameAs(recovery);
        assertThat(change.pendingTables()).isEmpty();
        assertThat(change.excludes(ADDED.id(), 102, 100)).isFalse();
        assertThat(change.excludes(null, 102, 100)).isFalse();
    }

    @Test
    void shouldRejectACheckpointWhoseManifestDisagreesWithItsKnownGeneration() throws Exception {
        final var prepared = prepare(checkpoint(original));
        final var wrongScope = new DamengOffsetContext(original, 10);
        wrongScope.setCaptureScope(wrongScope.getCaptureScopeManifest(), prepared.getCaptureScopeGeneration());

        assertThatThrownBy(() -> change(expanded).resolve(wrongScope))
                .isInstanceOf(ConnectException.class).hasMessageContaining("manifest");
    }

    private DamengOffsetContext prepare(Map<String, ?> checkpoint) throws Exception {
        final var change = change(expanded);
        final var offset = change.resolve(startup(expanded, checkpoint));
        change.prepare(offset, List.of(FIRST.id(), ADDED.id()), preparation);
        return offset;
    }

    private DamengCaptureScopeChange change(DamengConnectorConfig config) {
        return new DamengCaptureScopeChange(config, PARTITION, history);
    }

    private static Map<String, ?> checkpoint(DamengConnectorConfig config) {
        final var offset = new DamengOffsetContext(config, 10);
        offset.setCommitLsn(80);
        offset.setChangeLsn(70);
        offset.setTransactionId("existing-tx");
        offset.setEventSerialNo(3);
        return offset.getOffset();
    }

    private static DamengOffsetContext startup(DamengConnectorConfig config, Map<String, ?> checkpoint) {
        return new DamengOffsetContext.Loader(config, true).load(checkpoint);
    }

    private static void assertPositionUnchanged(DamengOffsetContext offset) {
        assertThat(offset.getLsn()).isEqualTo(10);
        assertThat(offset.getCommitLsn()).isEqualTo(80);
        assertThat(offset.getChangeLsn()).isEqualTo(70);
        assertThat(offset.getTransactionId()).isEqualTo("existing-tx");
        assertThat(offset.getEventSerialNo()).isEqualTo(3);
    }

    private static DamengConnectorConfig config(String tables) {
        return new DamengConnectorConfig(DamengConnectorConfigTest.configuration().with("table.include.list", tables).build());
    }

    private static Table table(String name, long objectId) {
        return Table.editor().tableId(new TableId(null, "APP", name))
                .addColumn(Column.editor().name("ID").position(1).jdbcType(Types.INTEGER).type("INT").create())
                .addAttribute(Attribute.editor().name("dameng.object.id").value(Long.toString(objectId)).create())
                .setPrimaryKeyNames("ID").create();
    }

    private static HistoryRecord roundTrip(HistoryRecord record) throws java.io.IOException {
        return new HistoryRecord(DocumentReader.defaultReader().read(DocumentWriter.defaultWriter().write(record.document())));
    }
}
