/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import org.apache.kafka.connect.errors.ConnectException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import io.debezium.document.DocumentReader;
import io.debezium.document.DocumentWriter;
import io.debezium.pipeline.spi.Offsets;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.relational.history.AbstractSchemaHistory;
import io.debezium.relational.history.HistoryRecord;
import io.debezium.relational.history.SchemaHistoryException;
import io.debezium.relational.history.SchemaHistoryListener;
import io.debezium.relational.history.TableChanges;

class DamengRecoveryTest {
    private static final DamengPartition PARTITION = new DamengPartition("dameng");
    private static final Table TABLE = table("T");
    private final History history = new History();

    @Test
    void shouldKeepNormalOffsetValidationUnlessExplicitlyRequested() throws Exception {
        final var config = config("");
        final var recovery = recovery(config);
        assertThat(recovery.resolve(null)).isNull();
        assertThat(recovery.isRequested()).isFalse();
        final var offset = new DamengOffsetContext(config, 10);
        assertThat(recovery(config).resolve(offset.getOffset()).getOffset()).isEqualTo(offset.getOffset());
        assertThatThrownBy(() -> recovery(config).resolve(Map.of("lsn", 10))).hasMessageContaining("capture scope");
        final var explicit = recovery(config("incident-1"));
        assertThat(explicit.resolve(Map.of("lsn", 10))).isNull();
        assertThat(explicit.isRequested()).isTrue();
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void shouldResumePersistedBaselineEvenWithoutANewConnectCheckpoint(boolean removedNonce) throws Exception {
        final var config = config("incident-1");
        final var original = persist(config, 100);
        final var restartConfig = removedNonce ? config("") : config;
        final var restart = recovery(restartConfig);
        final var offset = restart.resolve(Map.of("lsn", 1));
        assertThat(restart.isRequested()).isFalse();
        assertThat(offset.getLsn()).isEqualTo(100);
        assertThat(offset.getRecoveryNonce()).isEqualTo("incident-1");
        assertThat(offset.getIncrementalSnapshotContext().snapshotRunning()).isFalse();
        assertThat(offset.getOffset()).isEqualTo(original.getOffset());
        assertThat(recovery(restartConfig).resolve(null).getLsn()).isEqualTo(100);
    }

    @Test
    void shouldPreferSameGenerationCheckpointAndNotRequeueAutomaticSnapshot() throws Exception {
        final var config = config("incident-1");
        final var offset = persist(config, 100);
        offset.setLsn(120);
        offset.setCommitLsn(125);
        final var auto = new DamengConnectorConfig(config.getConfig().edit().with("signal.data.collection", "APP.SIGNAL")
                .with("recovery.incremental.snapshot.auto.start", true).build());
        // Use the same capture scope; only the auto-start flag changes.
        final var sameScope = new DamengConnectorConfig(auto.getConfig().edit().with("recovery.incremental.snapshot.auto.start", false).build());
        history.records.clear();
        final var baseline = persist(sameScope, 100);
        baseline.setLsn(offset.getLsn());
        baseline.setCommitLsn(offset.getCommitLsn());
        final var result = recovery(auto).resolve(baseline.getOffset());
        assertThat(result.getLsn()).isEqualTo(120);
        assertThat(result.getCommitLsn()).isEqualTo(125);
        assertThat(result.getIncrementalSnapshotContext().snapshotRunning()).isFalse();
    }

    @Test
    void shouldRejectReusedNonceAndAllowNewGeneration() throws Exception {
        final var first = persist(config("first"), 100);
        assertThat(recovery(config("second")).resolve(first.getOffset())).isNull();
        final var second = persist(config("second"), 200);
        assertThatThrownBy(() -> recovery(config("first")).resolve(second.getOffset())).hasMessageContaining("already used");
        assertThat(recovery(config("third")).resolve(second.getOffset())).isNull();
    }

    @Test
    void shouldRequireMatchingDurableBaselineButPermitExplicitReplacementOfLostHistory() throws Exception {
        final var offset = persist(config("first"), 100);
        final Map<String, Object> damaged = new HashMap<>(offset.getOffset());
        damaged.put(DamengOffsetContext.RECOVERY_BASELINE_LSN_KEY, 99L);
        assertThatThrownBy(() -> recovery(config("first")).resolve(damaged)).hasMessageContaining("does not match");
        history.records.clear();
        assertThatThrownBy(() -> recovery(config("")).resolve(offset.getOffset())).hasMessageContaining("baseline is missing");
        assertThatThrownBy(() -> recovery(config("first")).resolve(offset.getOffset())).hasMessageContaining("baseline is missing");
        assertThat(recovery(config("new")).resolve(offset.getOffset())).isNull();
    }

    @Test
    void shouldNotApplyOldGenerationTablesEvenAtEqualLsn() throws Exception {
        final var config = config("first");
        history.configure(config.getConfig(), config.getHistoryRecordComparator(), SchemaHistoryListener.NOOP, false);
        history.record(PARTITION.getSourcePartition(), Map.of("lsn", 100), "DAMENG", null, null,
                new TableChanges().create(table("DROPPED")), null);
        final var offset = persist(config, 100);
        final var recovered = new Tables();
        history.recover(Offsets.of(PARTITION, offset), recovered, null);
        assertThat(recovered.tableIds()).containsExactly(TABLE.id());
    }

    @Test
    void shouldIgnoreOtherSourceRecordsAndNormalSchemaChangesWhenFindingBaseline() throws Exception {
        final var config = config("first");
        final var offset = persist(config, 100);
        final Map<String, Object> foreign = new HashMap<>(offset.getOffset());
        foreign.put(DamengRecovery.BASELINE_KEY, "v1");
        history.record(Map.of("server", "another"), foreign, null, null, null, new TableChanges(), null);
        history.record(PARTITION.getSourcePartition(), offset.getOffset(), null, null);
        assertThat(recovery(config).resolve(null).getLsn()).isEqualTo(100);
    }

    @ParameterizedTest
    @ValueSource(strings = { "version", "nonce", "tables" })
    void shouldRejectMalformedOrAmbiguousBaseline(String fault) throws Exception {
        persist(config("first"), 100);
        final var record = history.records.get(0);
        switch (fault) {
            case "version" -> record.document().getDocument("position").setString(DamengRecovery.BASELINE_KEY, "v2");
            case "nonce" -> record.document().getDocument("position").remove(DamengOffsetContext.RECOVERY_NONCE_KEY);
            case "tables" -> record.document().remove("tableChanges");
            default -> throw new AssertionError(fault);
        }
        assertThatThrownBy(() -> recovery(config("first")).resolve(null)).isInstanceOf(ConnectException.class);
    }

    @Test
    void shouldAcceptIdenticalProducerRetriesButRejectConflictingOrReorderedBaselines() throws Exception {
        persist(config("first"), 100);
        final var first = history.records.get(0);
        history.records.add(first);
        assertThat(recovery(config("first")).resolve(null).getLsn()).isEqualTo(100);
        persist(config("second"), 200);
        history.records.add(first);
        assertThatThrownBy(() -> recovery(config("second")).resolve(null)).hasMessageContaining("Conflicting");
        history.records.clear();
        persist(config("first"), 100);
        persist(config("first"), 101);
        assertThatThrownBy(() -> recovery(config("first")).resolve(null)).hasMessageContaining("Conflicting");
    }

    @Test
    void shouldRecoverAfterHistoryAcknowledgementIsLostWithoutSelectingANewerLsn() throws Exception {
        final var config = config("first");
        history.failAfterWrite = true;
        assertThatThrownBy(() -> persist(config, 100)).isInstanceOf(SchemaHistoryException.class);
        history.failAfterWrite = false;
        assertThat(recovery(config).resolve(Map.of("lsn", 1)).getLsn()).isEqualTo(100);
    }

    @Test
    void shouldSeedAutomaticSnapshotOnceAndPersistItsProgress() throws Exception {
        final var config = new DamengConnectorConfig(DamengConnectorConfigTest.configuration()
                .with("recovery.restart.nonce", "first").with("signal.data.collection", "APP.SIGNAL")
                .with("recovery.incremental.snapshot.auto.start", true).build());
        final var recovery = recovery(config);
        final var offset = new DamengOffsetContext(config, 100);
        recovery.persist(offset, List.of(TABLE, table("SIGNAL")));
        assertThat(offset.getIncrementalSnapshotContext().snapshotRunning()).isTrue();
        assertThat(offset.getIncrementalSnapshotContext().currentDataCollectionId().getId()).isEqualTo(TABLE.id());
        final var reloaded = recovery(config).resolve(null);
        assertThat(reloaded.getIncrementalSnapshotContext().snapshotRunning()).isTrue();
        reloaded.getIncrementalSnapshotContext().nextDataCollection();
        assertThat(reloaded.getIncrementalSnapshotContext().snapshotRunning()).isFalse();
        assertThat(recovery(config).resolve(reloaded.getOffset()).getIncrementalSnapshotContext().snapshotRunning()).isFalse();
    }

    @Test
    void shouldRejectAutomaticBackfillWithoutKeysBeforePersistingAnything() {
        final var config = new DamengConnectorConfig(DamengConnectorConfigTest.configuration()
                .with("recovery.restart.nonce", "first").with("signal.data.collection", "APP.SIGNAL")
                .with("recovery.incremental.snapshot.auto.start", true).build());
        final var noKey = TABLE.edit().setPrimaryKeyNames(List.of()).create();
        assertThatThrownBy(() -> recovery(config).persist(new DamengOffsetContext(config, 100), List.of(noKey)))
                .hasMessageContaining("requires primary keys");
        assertThat(history.records).isEmpty();
    }

    private DamengOffsetContext persist(DamengConnectorConfig config, long lsn) {
        final var offset = new DamengOffsetContext(config, lsn);
        recovery(config).persist(offset, List.of(TABLE));
        return offset;
    }

    private DamengRecovery recovery(DamengConnectorConfig config) {
        return new DamengRecovery(config, PARTITION, history);
    }

    private static DamengConnectorConfig config(String nonce) {
        return new DamengConnectorConfig(DamengConnectorConfigTest.configuration().with("recovery.restart.nonce", nonce).build());
    }

    private static Table table(String name) {
        return Table.editor().tableId(new TableId(null, "APP", name))
                .addColumn(Column.editor().name("ID").jdbcType(Types.INTEGER).type("INT").position(1).create())
                .setPrimaryKeyNames("ID").create();
    }

    private static final class History extends AbstractSchemaHistory {
        private final List<HistoryRecord> records = new ArrayList<>();
        private boolean failAfterWrite;

        private History() {
            final var config = config("");
            configure(config.getConfig(), config.getHistoryRecordComparator(), SchemaHistoryListener.NOOP, false);
        }

        @Override
        protected void storeRecord(HistoryRecord record) {
            try {
                // Model a storage round trip, including JSON numeric conversions.
                records.add(new HistoryRecord(DocumentReader.defaultReader().read(DocumentWriter.defaultWriter().write(record.document()))));
            }
            catch (java.io.IOException e) {
                throw new AssertionError(e);
            }
            if (failAfterWrite) {
                throw new SchemaHistoryException("acknowledgement lost");
            }
        }

        @Override
        protected void recoverRecords(Consumer<HistoryRecord> consumer) {
            records.forEach(consumer);
        }

        @Override
        public boolean exists() {
            return !records.isEmpty();
        }

        @Override
        public boolean storageExists() {
            return true;
        }
    }
}
