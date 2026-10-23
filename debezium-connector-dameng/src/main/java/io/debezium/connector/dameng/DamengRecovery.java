/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng;

import java.time.Instant;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.document.Document;
import io.debezium.relational.Table;
import io.debezium.relational.history.HistoryRecord;
import io.debezium.relational.history.SchemaHistory;
import io.debezium.relational.history.TableChanges;

/**
 * A recovery baseline is one acknowledged history record containing both the
 * offset and the complete schema. No event may acknowledge the new generation
 * before this record. This closes
 * the crash gap before Connect has committed the first new source offset.
 */
final class DamengRecovery {
    static final String BASELINE_KEY = "dameng_recovery_baseline";
    private static final Logger LOGGER = LoggerFactory.getLogger(DamengRecovery.class);
    private final DamengConnectorConfig config;
    private final DamengPartition partition;
    private final SchemaHistory history;
    private final Set<String> usedNonces = new HashSet<>();
    private Map<String, Object> baseline;
    private HistoryRecord baselineRecord;
    private boolean requested;

    DamengRecovery(DamengConnectorConfig config, DamengPartition partition, SchemaHistory history) {
        this.config = config;
        this.partition = partition;
        this.history = history;
    }

    DamengOffsetContext resolve(Map<String, ?> stored) throws InterruptedException {
        return resolve(stored, false);
    }

    DamengOffsetContext resolve(Map<String, ?> stored, boolean allowScopeChange) throws InterruptedException {
        if (history.exists()) {
            history.readRecords(this::inspect);
        }
        final String storedNonce = stored == null ? null : (String) stored.get(DamengOffsetContext.RECOVERY_NONCE_KEY);
        final String currentNonce = baseline == null ? storedNonce : (String) baseline.get(DamengOffsetContext.RECOVERY_NONCE_KEY);
        final String desired = config.getRecoveryRestartNonce();
        if (!desired.isEmpty() && !desired.equals(currentNonce)) {
            if (usedNonces.contains(desired) || desired.equals(storedNonce)) {
                throw new ConnectException("Dameng recovery.restart.nonce was already used by an older recovery; supply a new value");
            }
            requested = true;
            LOGGER.warn("Dameng explicit recovery requested: nonce={}, previous LSN={}. A new schema/LSN baseline will replace the replay position; "
                    + "skipped changes and deletes will not be recovered. Automatic incremental snapshot={}", desired,
                    stored == null ? null : stored.get(SourceInfo.LSN_KEY), config.isRecoveryIncrementalSnapshotAutoStart());
            return null;
        }
        if (currentNonce != null && baseline == null) {
            throw new ConnectException("Dameng recovery baseline is missing from schema history; restore the history or explicitly request a new nonce");
        }
        final var loader = new DamengOffsetContext.Loader(config, allowScopeChange);
        if (baseline != null && !Objects.equals(storedNonce, currentNonce)) {
            LOGGER.warn("Resuming durable Dameng recovery baseline nonce={} at LSN={} before its first Connect offset checkpoint",
                    currentNonce, baseline.get(SourceInfo.LSN_KEY));
            return loader.load(baseline);
        }
        if (baseline != null && !Objects.equals(String.valueOf(stored.get(DamengOffsetContext.RECOVERY_BASELINE_LSN_KEY)),
                String.valueOf(baseline.get(DamengOffsetContext.RECOVERY_BASELINE_LSN_KEY)))) {
            throw new ConnectException("Dameng stored offset does not match its durable recovery baseline");
        }
        return stored == null ? null : loader.load(stored);
    }

    private void inspect(HistoryRecord record) {
        final Document source = record.document().getDocument(HistoryRecord.Fields.SOURCE);
        final var expectedSource = Document.create();
        partition.getSourcePartition().forEach(expectedSource::setString);
        if (!expectedSource.equals(source)) {
            return;
        }
        final Document position = record.document().getDocument(HistoryRecord.Fields.POSITION);
        if (position == null || !position.has(BASELINE_KEY)) {
            return;
        }
        if (!"v1".equals(position.getString(BASELINE_KEY)) || record.document().getArray(HistoryRecord.Fields.TABLE_CHANGES) == null) {
            throw new ConnectException("Invalid or unsupported Dameng recovery baseline in schema history");
        }
        final String nonce = position.getString(DamengOffsetContext.RECOVERY_NONCE_KEY);
        if (nonce == null || nonce.isBlank()) {
            throw new ConnectException("Missing or repeated Dameng recovery nonce in schema history");
        }
        if (!usedNonces.add(nonce)) {
            // A history producer can retry a write whose acknowledgement was lost.
            // Only an identical baseline in the current generation is idempotent;
            // an old generation or a changed schema/position must never replace it.
            if (baselineRecord != null && position.equals(baselineRecord.document().getDocument(HistoryRecord.Fields.POSITION))
                    && record.document().getArray(HistoryRecord.Fields.TABLE_CHANGES)
                            .equals(baselineRecord.document().getArray(HistoryRecord.Fields.TABLE_CHANGES))) {
                return;
            }
            throw new ConnectException("Conflicting or repeated Dameng recovery nonce in schema history");
        }
        baselineRecord = record;
        baseline = new HashMap<>();
        for (Document.Field field : position) {
            baseline.put(field.getName().toString(), field.getValue().asObject());
        }
    }

    boolean isRequested() {
        return requested;
    }

    void persist(DamengOffsetContext offset, Collection<Table> tables) {
        offset.setRecoveryBaseline(config.getRecoveryRestartNonce(), offset.getLsn());
        offset.preSnapshotCompletion();
        offset.postSnapshotCompletion();
        final var changes = new TableChanges();
        for (Table table : tables) {
            changes.create(table);
        }
        if (config.isRecoveryIncrementalSnapshotAutoStart()) {
            final List<Table> businessTables = tables.stream()
                    .filter(table -> config.getTableFilters().dataCollectionFilter().isIncluded(table.id()))
                    .filter(table -> !config.getSignalingDataCollectionTableIds().contains(table.id())).toList();
            if (businessTables.stream().anyMatch(table -> table.primaryKeyColumnNames().isEmpty())) {
                throw new ConnectException("Dameng automatic recovery incremental snapshot requires primary keys on all captured business tables");
            }
            offset.getIncrementalSnapshotContext().addDataCollectionNamesToSnapshot("recovery-" + config.getRecoveryRestartNonce(),
                    businessTables.stream().map(table -> table.id().toDoubleQuotedString()).toList(), List.of(), "");
        }
        final Map<String, Object> position = new HashMap<>(offset.getOffset());
        position.put(BASELINE_KEY, "v1");
        // A single record is deliberate: partial table lists must never activate a
        // baseline. Oversized records fail before CDC; configure history limits accordingly.
        history.record(partition.getSourcePartition(), position, config.getDatabaseName(), null, null, changes, Instant.now());
        LOGGER.warn("Dameng recovery baseline persisted: nonce={}, LSN={}, schema tables={}, automatic incremental snapshot={}. "
                + "CDC will resume from this baseline; current-row snapshots do not recover skipped deletes",
                offset.getRecoveryNonce(), offset.getLsn(), tables.size(), config.isRecoveryIncrementalSnapshotAutoStart());
    }
}
