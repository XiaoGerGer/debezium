/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.errors.ConnectException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import io.debezium.pipeline.txmetadata.TransactionContext;
import io.debezium.relational.TableId;

class DamengOffsetContextTest {
    private final DamengConnectorConfig config = new DamengConnectorConfig(DamengConnectorConfigTest.configuration().build());

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void shouldLoadNumericAndStringOffsets(boolean strings) {
        final var offset = loadPosition(Map.of(
                "lsn", strings ? "100" : 100L,
                "change_lsn", strings ? "150" : 150L,
                "commit_lsn", strings ? "200" : 200L,
                "event_serial_no", strings ? "3" : 3L,
                "mining_transaction_id", "ABC123"));

        assertThat(offset.getLsn()).isEqualTo(100);
        assertThat(offset.getChangeLsn()).isEqualTo(150);
        assertThat(offset.getCommitLsn()).isEqualTo(200);
        assertThat(offset.getEventSerialNo()).isEqualTo(3);
        assertThat(offset.getTransactionId()).isEqualTo("ABC123");
    }

    @Test
    void shouldRoundTripRestartPosition() {
        final var offset = new DamengOffsetContext(config, 100);
        offset.setChangeLsn(150);
        offset.setCommitLsn(200);
        offset.setEventSerialNo(7);
        offset.setTransactionId("TX1");
        offset.validateDatabaseMagic(123456789L);

        final var restored = new DamengOffsetContext.Loader(config).load(offset.getOffset());

        assertThat(restored.getOffset()).isEqualTo(offset.getOffset());
        assertThat(restored.getDatabaseMagic()).isEqualTo(123456789L);
    }

    @Test
    void shouldAdvanceMiningTransactionAfterRestartWithoutTransactionMetadata() {
        final var initial = new DamengOffsetContext(config, 100);
        initial.setCommitLsn(120);
        initial.setTransactionId("TX1");
        initial.setEventSerialNo(1);
        final var restored = new DamengOffsetContext.Loader(config).load(initial.getOffset());

        restored.setCommitLsn(140);
        restored.setTransactionId("TX2");
        restored.setEventSerialNo(2);
        assertThat(restored.getOffset().get("mining_transaction_id")).isEqualTo("TX2");
        assertThat(restored.getOffset()).doesNotContainKey(TransactionContext.OFFSET_TRANSACTION_ID);
        assertThat(restored.getTransactionContext().isTransactionInProgress()).isFalse();
        final var restartedAgain = new DamengOffsetContext.Loader(config).load(restored.getOffset());
        assertThat(restartedAgain.getCommitLsn()).isEqualTo(140);
        assertThat(restartedAgain.getTransactionId()).isEqualTo("TX2");
        assertThat(restartedAgain.getEventSerialNo()).isEqualTo(2);
    }

    @Test
    void shouldLoadLegacyMiningTransactionWithoutCreatingMetadataTransaction() {
        final var restored = loadPosition(Map.of("lsn", 100L, "transaction_id", "TX1"));
        assertThat(restored.getTransactionId()).isEqualTo("TX1");
        assertThat(restored.getTransactionContext().isTransactionInProgress()).isFalse();
        restored.setTransactionId("TX2");
        assertThat(restored.getOffset().get("mining_transaction_id")).isEqualTo("TX2");
        assertThat(restored.getOffset()).doesNotContainKey("transaction_id");
    }

    @Test
    void shouldKeepMiningPositionIndependentAcrossMetadataTransactionChanges() {
        final var metadataConfig = new DamengConnectorConfig(DamengConnectorConfigTest.configuration()
                .with("provide.transaction.metadata", true).build());
        var offset = new DamengOffsetContext(metadataConfig, 100);
        for (int transaction = 1; transaction <= 3; transaction++) {
            final String id = "TX" + transaction;
            offset.setTransactionId(id);
            offset.setCommitLsn(100 + transaction);
            offset.setEventSerialNo(0);
            offset.getTransactionContext().beginTransaction(id);
            offset.getTransactionContext().event(new TableId(null, "APP", "EVENTS"));
            assertThat(offset.getOffset().get("mining_transaction_id")).isEqualTo(id);
            assertThat(offset.getOffset().get(TransactionContext.OFFSET_TRANSACTION_ID)).isEqualTo(id);

            offset = new DamengOffsetContext.Loader(metadataConfig).load(offset.getOffset());
            assertThat(offset.getTransactionId()).isEqualTo(id);
            assertThat(offset.getTransactionContext().getTransactionId()).isEqualTo(id);
            assertThat(offset.getTransactionContext().getTotalEventCount()).isEqualTo(1L);
            offset.getTransactionContext().endTransaction();
            offset.setEventSerialNo(Long.MAX_VALUE);
            assertThat(offset.getOffset().get("mining_transaction_id")).isEqualTo(id);
            assertThat(offset.getOffset()).doesNotContainKey(TransactionContext.OFFSET_TRANSACTION_ID);
        }
    }

    @Test
    void shouldRestoreMiningAndMetadataPositionsIndependentlyWithinTransaction() {
        final var metadataConfig = new DamengConnectorConfig(DamengConnectorConfigTest.configuration()
                .with("provide.transaction.metadata", true).build());
        final var offset = new DamengOffsetContext(metadataConfig, 100);
        offset.setTransactionId("MINING_TX");
        offset.setCommitLsn(200);
        offset.setChangeLsn(180);
        offset.setEventSerialNo(3);
        offset.getTransactionContext().beginTransaction("METADATA_TX");
        final TableId tableId = new TableId(null, "APP", "EVENTS");
        offset.getTransactionContext().event(tableId);
        offset.getTransactionContext().event(tableId);

        final var restored = new DamengOffsetContext.Loader(metadataConfig).load(offset.getOffset());
        assertThat(restored.getTransactionId()).isEqualTo("MINING_TX");
        assertThat(restored.getCommitLsn()).isEqualTo(200);
        assertThat(restored.getChangeLsn()).isEqualTo(180);
        assertThat(restored.getEventSerialNo()).isEqualTo(3);
        assertThat(restored.getTransactionContext().getTransactionId()).isEqualTo("METADATA_TX");
        assertThat(restored.getTransactionContext().getTotalEventCount()).isEqualTo(2);
        assertThat(restored.getTransactionContext().event(tableId)).isEqualTo(3);
        assertThat(restored.getOffset().get("mining_transaction_id")).isEqualTo("MINING_TX");
        assertThat(restored.getOffset().get("transaction_id")).isEqualTo("METADATA_TX");
    }

    @Test
    void shouldRejectOffsetFromRecreatedDatabase() {
        final var restored = loadPosition(Map.of("lsn", 100L, "database_magic", "12345"));
        restored.validateDatabaseMagic(12345L);
        assertThatThrownBy(() -> restored.validateDatabaseMagic(54321L)).isInstanceOf(ConnectException.class)
                .hasMessageContaining("12345").hasMessageContaining("54321");
        assertThat(restored.getDatabaseMagic()).isEqualTo(12345L);
    }

    @Test
    void shouldBindLegacyOffsetsToCurrentDatabase() {
        final var restored = loadPosition(Map.of("lsn", 100L));
        assertThat(restored.getDatabaseMagic()).isNull();
        restored.validateDatabaseMagic(12345L);
        assertThat(restored.getOffset().get("database_magic")).isEqualTo(12345L);
    }

    @Test
    void shouldRestoreIncrementalSnapshotProgressWithSchemaQualifiedTableIds() {
        final var offset = new DamengOffsetContext(config, 100);
        final var context = offset.getIncrementalSnapshotContext();
        context.addDataCollectionNamesToSnapshot("request-1", List.of("APP.CUSTOMERS"), List.of(), "");
        context.maximumKey(new Object[]{ 100 });
        context.sendEvent(new Object[]{ 25 });

        final var restored = new DamengOffsetContext.Loader(config).load(offset.getOffset());
        assertThat(restored.getIncrementalSnapshotContext().snapshotRunning()).isTrue();
        assertThat(restored.getIncrementalSnapshotContext().currentDataCollectionId().getId()).isEqualTo(new TableId(null, "APP", "CUSTOMERS"));
        assertThat(restored.getIncrementalSnapshotContext().maximumKey()).hasValueSatisfying(key -> assertThat(key).containsExactly(100));
        assertThat(restored.getIncrementalSnapshotContext().chunkEndPosititon()).containsExactly(25);
    }

    @Test
    void shouldLoadMinimalOffset() {
        final var offset = loadPosition(Map.of("lsn", 100L));

        assertThat(offset.getLsn()).isEqualTo(100);
        assertThat(offset.getChangeLsn()).isEqualTo(100);
        assertThat(offset.getCommitLsn()).isEqualTo(100);
        assertThat(offset.getEventSerialNo()).isZero();
        assertThat(offset.getTransactionId()).isNull();
    }

    @Test
    void shouldKeepReplayPositionIndependentFromLastDeliveredChange() {
        final var offset = new DamengOffsetContext(config, 100);
        offset.setChangeLsn(180);
        offset.setCommitLsn(200);
        offset.setTransactionId("COMMITTED_TX");
        offset.setEventSerialNo(3);
        offset.event(new TableId(null, "APP", "EVENTS"), Instant.parse("2026-09-24T01:02:03Z"));

        final var restored = new DamengOffsetContext.Loader(config).load(offset.getOffset());

        assertThat(restored.getLsn()).as("oldest open transaction needed after restart").isEqualTo(100);
        assertThat(restored.getChangeLsn()).isEqualTo(180);
        assertThat(restored.getCommitLsn()).isEqualTo(200);
        assertThat(restored.getTransactionId()).isEqualTo("COMMITTED_TX");
        assertThat(restored.getEventSerialNo()).isEqualTo(3);
        assertThat(offset.getSourceInfo().getInt64("lsn")).as("source metadata preserves the replay position").isEqualTo(100);
        assertThat(offset.getSourceInfo().getInt64("change_lsn")).as("source metadata identifies this change").isEqualTo(180);
    }

    @Test
    void shouldRoundTripIncompleteSnapshot() {
        final var offset = new DamengOffsetContext(config, 100);
        offset.preSnapshotStart(false);

        final var restored = new DamengOffsetContext.Loader(config).load(offset.getOffset());

        assertThat(restored.isInitialSnapshotRunning()).isTrue();
    }

    @Test
    void shouldRoundTripCompletedSnapshot() {
        final var offset = new DamengOffsetContext(config, 100);
        offset.preSnapshotStart(false);
        offset.preSnapshotCompletion();
        offset.postSnapshotCompletion();

        final var restored = new DamengOffsetContext.Loader(config).load(offset.getOffset());

        assertThat(restored.isInitialSnapshotRunning()).isFalse();
    }

    @Test
    void shouldIncludeDamengSourceIdentityAndPreciseTimestamp() {
        final var offset = new DamengOffsetContext(config, 100);
        offset.setCommitLsn(200);
        offset.setEventSerialNo(3);
        offset.setTransactionId("TX1");
        offset.event(new TableId("DAMENG", "APP", "EVENTS"), Instant.parse("2026-09-24T01:02:03.123456Z"));
        final var source = offset.getSourceInfo();

        assertThat(offset.getSourceInfoSchema()).isEqualTo(source.schema());
        assertThat(offset.getSourceInfoObject().struct()).isEqualTo(source);
        assertThat(offset.getSourceInfoObject().getTableId()).isEqualTo(new TableId("DAMENG", "APP", "EVENTS"));
        assertThat(source.schema().name()).isEqualTo("io.debezium.connector.dameng.Source");
        assertThat(source.getString("connector")).isEqualTo("dameng");
        assertThat(source.getString("version")).isEqualTo(Module.version());
        assertThat(source.getString("db")).isEqualTo("DAMENG");
        assertThat(source.getString("schema")).isEqualTo("APP");
        assertThat(source.getString("table")).isEqualTo("EVENTS");
        assertThat(source.getInt64("lsn")).isEqualTo(100L);
        assertThat(source.getInt64("ts_us")).isEqualTo(Instant.parse("2026-09-24T01:02:03.123456Z").toEpochMilli() * 1_000 + 456);
    }

    private DamengOffsetContext loadPosition(Map<String, Object> values) {
        final Map<String, Object> position = new HashMap<>(values);
        position.put(DamengOffsetContext.CAPTURE_SCOPE_KEY, DamengCaptureScope.signature(config));
        return new DamengOffsetContext.Loader(config).load(position);
    }

}
