/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.regex.Pattern;

import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.document.Document;
import io.debezium.document.DocumentReader;
import io.debezium.document.DocumentWriter;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.history.HistoryRecord;
import io.debezium.relational.history.JsonTableChangeSerializer;
import io.debezium.relational.history.SchemaHistory;
import io.debezium.relational.history.TableChanges;
import io.debezium.util.Strings;

/** Durable per-table cuts, independent of the connector's global replay position. */
final class DamengCaptureScopeChange {
    static final String CHANGE_KEY = "dameng_capture_scope_change";
    static final String PREVIOUS_MANIFEST_KEY = "capture_scope_previous_manifest";
    static final String RESUME_OFFSET_KEY = "capture_scope_resume_offset";
    static final String BOUNDARY_LSN_KEY = "capture_scope_boundary_lsn";
    private static final Logger LOGGER = LoggerFactory.getLogger(DamengCaptureScopeChange.class);
    private final DamengConnectorConfig config;
    private final DamengPartition partition;
    private final SchemaHistory history;
    private final Map<String, HistoryRecord> generations = new LinkedHashMap<>();
    private final Map<TableId, Activation> activations = new LinkedHashMap<>();
    private final Map<Long, Long> identityFloors = new HashMap<>();
    private final List<ScopeBoundary> scopeBoundaries = new ArrayList<>();
    private HistoryRecord latest;
    private String recoveryNonce;

    private record ScopeBoundary(long lsn, DamengConnectorConfig before, DamengConnectorConfig after) {
        boolean added(TableId id) {
            return after.getTableFilters().dataCollectionFilter().isIncluded(id)
                    && !before.getTableFilters().dataCollectionFilter().isIncluded(id);
        }
    }

    private static final class Activation {
        private final long boundary;
        private final Table table;
        private boolean installed;

        private Activation(long boundary, Table table) {
            this.boundary = boundary;
            this.table = table;
        }
    }

    DamengCaptureScopeChange(DamengConnectorConfig config, DamengPartition partition, SchemaHistory history) {
        this.config = config;
        this.partition = partition;
        this.history = history;
    }

    DamengOffsetContext resolve(DamengOffsetContext offset) throws InterruptedException {
        recoveryNonce = offset == null ? null : offset.getRecoveryNonce();
        if (history.exists()) {
            history.readRecords(this::inspect);
        }
        final String generation = offset == null ? null : offset.getCaptureScopeGeneration();
        if (generation != null && !generations.containsKey(generation)) {
            throw new ConnectException("Dameng capture scope generation is missing from schema history; restore its durable descriptor");
        }
        if (latest == null) {
            return offset;
        }
        final var position = latest.document().getDocument(HistoryRecord.Fields.POSITION);
        if (!Objects.equals(generation, position.getString(DamengOffsetContext.CAPTURE_SCOPE_GENERATION_KEY))) {
            offset = new DamengOffsetContext.Loader(config, true).load(readOffset(position.getString(RESUME_OFFSET_KEY)));
            LOGGER.info("Resuming durable Dameng capture scope generation {} without moving replay LSN {}",
                    offset.getCaptureScopeGeneration(), offset.getLsn());
        }
        if (!Objects.equals(offset.getCaptureScopeManifest(), position.getString(DamengOffsetContext.CAPTURE_SCOPE_MANIFEST_KEY))) {
            throw new ConnectException("Dameng capture scope manifest does not match its durable generation");
        }
        return offset;
    }

    private void inspect(HistoryRecord record) {
        final var source = Document.create();
        partition.getSourcePartition().forEach(source::set);
        final var position = record.document().getDocument(HistoryRecord.Fields.POSITION);
        if (!source.equals(record.document().getDocument(HistoryRecord.Fields.SOURCE)) || position == null
                || !position.has(CHANGE_KEY) || !Objects.equals(recoveryNonce, position.getString(DamengOffsetContext.RECOVERY_NONCE_KEY))) {
            return;
        }
        final String generation = position.getString(DamengOffsetContext.CAPTURE_SCOPE_GENERATION_KEY);
        if (!"v1".equals(position.getString(CHANGE_KEY)) || generation == null || generation.isBlank()
                || record.document().getArray(HistoryRecord.Fields.TABLE_CHANGES) == null || position.getLong(BOUNDARY_LSN_KEY, -1L) < 0
                || position.getString(PREVIOUS_MANIFEST_KEY) == null || position.getString(RESUME_OFFSET_KEY) == null) {
            throw new ConnectException("Invalid or unsupported Dameng capture scope descriptor");
        }
        final var previous = generations.get(generation);
        if (previous != null) {
            if (latest == previous && previous.document().equals(record.document())) {
                return;
            }
            throw new ConnectException("Conflicting or repeated Dameng capture scope generation in schema history");
        }
        final String manifest = position.getString(DamengOffsetContext.CAPTURE_SCOPE_MANIFEST_KEY);
        DamengCaptureScope.validateChange(position.getString(PREVIOUS_MANIFEST_KEY), manifest);
        if (latest != null && !Objects.equals(latest.document().getDocument(HistoryRecord.Fields.POSITION)
                .getString(DamengOffsetContext.CAPTURE_SCOPE_MANIFEST_KEY), position.getString(PREVIOUS_MANIFEST_KEY))) {
            throw new ConnectException("Broken Dameng capture scope generation chain in schema history");
        }
        final var resume = readOffset(position.getString(RESUME_OFFSET_KEY));
        if (!Objects.equals(generation, resume.get(DamengOffsetContext.CAPTURE_SCOPE_GENERATION_KEY))
                || !Objects.equals(manifest, resume.get(DamengOffsetContext.CAPTURE_SCOPE_MANIFEST_KEY))) {
            throw new ConnectException("Dameng capture scope descriptor has a conflicting resume offset");
        }
        final long boundary = position.getLong(BOUNDARY_LSN_KEY);
        validateResume(position, resume, boundary);
        final var oldConfig = DamengCaptureScope.previousConfig(config, position.getString(PREVIOUS_MANIFEST_KEY));
        final var newConfig = DamengCaptureScope.previousConfig(config, manifest);
        if (boundary <= 0) {
            throw new ConnectException("Dameng capture scope descriptor requires a positive LSN boundary");
        }
        if (latest != null && boundary < latest.document().getDocument(HistoryRecord.Fields.POSITION).getLong(BOUNDARY_LSN_KEY)) {
            throw new ConnectException("Dameng capture scope LSN boundary moved backwards in schema history");
        }
        final Map<Long, TableId> identities = new HashMap<>();
        final var tableIds = new HashSet<TableId>();
        for (var change : new JsonTableChangeSerializer().deserialize(record.document().getArray(HistoryRecord.Fields.TABLE_CHANGES), false)) {
            if (boundary <= 0 || change.getType() != TableChanges.TableChangeType.CREATE || change.getTable() == null
                    || change.getTable().columns().isEmpty() || !newConfig.getTableFilters().dataCollectionFilter().isIncluded(change.getId())
                    || oldConfig.getTableFilters().dataCollectionFilter().isIncluded(change.getId())) {
                throw new ConnectException("Dameng capture scope descriptor must contain complete CREATE table baselines");
            }
            if (!tableIds.add(change.getId())) {
                throw new ConnectException("Duplicate Dameng table in capture scope descriptor: " + change.getId());
            }
            final long identity = objectIdentity(change.getTable());
            if (identities.put(identity, change.getId()) != null) {
                throw new ConnectException("Duplicate Dameng object identity in capture scope descriptor");
            }
            activations.put(change.getId(), new Activation(boundary, change.getTable()));
            identityFloors.merge(identity, boundary, Math::max);
        }
        generations.put(generation, record);
        scopeBoundaries.add(new ScopeBoundary(boundary, oldConfig, newConfig));
        latest = record;
    }

    void prepare(DamengOffsetContext offset, Collection<TableId> visibleTables, DamengCaptureScopePreparation preparation)
            throws SQLException, InterruptedException {
        prepare(offset, visibleTables, preparation, null);
    }

    void prepare(DamengOffsetContext offset, Collection<TableId> visibleTables, DamengCaptureScopePreparation preparation, DamengDatabaseSchema schema)
            throws SQLException, InterruptedException {
        if (offset == null) {
            return;
        }
        final String desired = DamengCaptureScope.manifest(config);
        final String previous = offset.getCaptureScopeManifest();
        if (Objects.equals(previous, desired)) {
            return;
        }
        if (previous == null) {
            throw new ConnectException("Stored Dameng offset has no capture scope manifest; restart once with the previous capture filters before changing scope");
        }
        if (offset.isInitialSnapshotRunning()) {
            throw new ConnectException("Dameng capture scope cannot change while the initial snapshot is incomplete; finish the initial snapshot first");
        }
        DamengCaptureScope.validateChange(previous, desired);
        final var oldConfig = DamengCaptureScope.previousConfig(config, previous);
        warnUnmatchedIncludeRules(visibleTables);
        final var added = visibleTables.stream().filter(config.getTableFilters().dataCollectionFilter()::isIncluded)
                .filter(id -> !oldConfig.getTableFilters().dataCollectionFilter().isIncluded(id)).sorted().toList();
        if (schema != null) {
            for (TableId id : schema.getTables().tableIds()) {
                if (config.getTableFilters().dataCollectionFilter().isIncluded(id)
                        && !oldConfig.getTableFilters().dataCollectionFilter().isIncluded(id) && !visibleTables.contains(id)) {
                    throw new ConnectException("Newly included Dameng table " + id
                            + " has historical metadata but is not currently visible through JDBC; restore metadata visibility and SELECT permissions before expanding capture");
                }
            }
        }
        final String generation = UUID.randomUUID().toString();
        final var candidate = new DamengOffsetContext.Loader(config, true).load(offset.getOffset());
        candidate.acceptCaptureScope(config, generation);
        final var removedSnapshots = DamengIncrementalSnapshotChangeEventSource.pruneExcludedSnapshotTables(config, candidate);
        if (added.isEmpty()) {
            preparation.prepareBoundary(baseline -> {
                validateBaselineIdentity(candidate, baseline);
                persist(candidate, previous, baseline.lsn(), List.of());
            });
        }
        else {
            preparation.prepare(added, baseline -> {
                validateBaselineIdentity(candidate, baseline);
                if (schema != null) {
                    for (Table table : baseline.tables()) {
                        for (TableId id : schema.getTables().tableIds()) {
                            if (!id.equals(table.id()) && objectIdentity(schema.getTables().forTable(id)) == objectIdentity(table)) {
                                throw new ConnectException("Added Dameng table " + table.id() + " shares its historical object identity with " + id
                                        + "; let the previous capture configuration process the rename before changing scope");
                            }
                        }
                    }
                }
                persist(candidate, previous, baseline.lsn(), baseline.tables());
            });
        }
        offset.acceptCaptureScope(config, generation);
        DamengIncrementalSnapshotChangeEventSource.pruneExcludedSnapshotTables(config, offset);
        LOGGER.info("Dameng capture scope generation {} persisted: added tables={}, cancelled incremental snapshot tables={}; replay LSN remains {}",
                generation, added, removedSnapshots, offset.getLsn());
    }

    private void persist(DamengOffsetContext offset, String previousManifest, long boundary, Collection<Table> tables) {
        final var position = descriptorPosition(offset.getOffset(), previousManifest, boundary);
        final var changes = new TableChanges();
        tables.forEach(changes::create);
        final var timestamp = Instant.now();
        history.record(partition.getSourcePartition(), position, config.getDatabaseName(), null, null, changes, timestamp);
        inspect(new HistoryRecord(partition.getSourcePartition(), position, config.getDatabaseName(), null, null, changes, timestamp));
    }

    private static Map<String, Object> descriptorPosition(Map<String, ?> resume, String previousManifest, long boundary) {
        final Map<String, Object> position = new HashMap<>(resume);
        position.put(CHANGE_KEY, "v1");
        position.put(PREVIOUS_MANIFEST_KEY, previousManifest);
        position.put(RESUME_OFFSET_KEY, writeOffset(resume));
        position.put(BOUNDARY_LSN_KEY, boundary);
        // Only the descriptor's schema position uses S. Its resume offset retains
        // the original commit, ordinal and replay LSN, including long transactions.
        position.put(SourceInfo.COMMIT_LSN_KEY, boundary);
        position.remove(DamengOffsetContext.MINING_TRANSACTION_ID_KEY);
        position.remove(SourceInfo.TRANSACTION_ID_KEY);
        position.put(SourceInfo.EVENT_SERIAL_NO_KEY, 0L);
        return position;
    }

    private void validateResume(Document position, Map<String, Object> resume, long boundary) {
        for (String field : List.of(SourceInfo.LSN_KEY, SourceInfo.CHANGE_LSN_KEY, SourceInfo.COMMIT_LSN_KEY, SourceInfo.EVENT_SERIAL_NO_KEY)) {
            final Object value = resume.get(field);
            final long minimum = field.equals(SourceInfo.EVENT_SERIAL_NO_KEY) ? -1 : 0;
            if (!(value instanceof Number number) || number.longValue() < minimum
                    || new BigDecimal(number.toString()).compareTo(BigDecimal.valueOf(number.longValue())) != 0
                    || !field.equals(SourceInfo.EVENT_SERIAL_NO_KEY) && number.longValue() > boundary) {
                throw new ConnectException("Invalid Dameng capture scope resume offset field " + field);
            }
        }
        // Validate the saved offset even when Connect already has a newer checkpoint
        // in the same generation. It remains the crash-recovery fallback.
        new DamengOffsetContext.Loader(config, true).load(resume);
        final var expected = Document.create();
        descriptorPosition(resume, position.getString(PREVIOUS_MANIFEST_KEY), boundary).forEach(expected::set);
        expected.setString(RESUME_OFFSET_KEY, position.getString(RESUME_OFFSET_KEY));
        if (!expected.equals(position)) {
            throw new ConnectException("Dameng capture scope descriptor position conflicts with its resume offset or LSN boundary");
        }
    }

    void recovered(DamengDatabaseSchema schema, DamengOffsetContext offset) {
        // History recovery already applied cuts through the committed frontier.
        // Never overwrite a later recovered ALTER with its original JDBC baseline.
        activations.values().forEach(value -> value.installed = offset != null && offset.getCommitLsn() >= value.boundary);
    }

    void beforeRow(DamengDatabaseSchema schema, long lsn) {
        install(schema, lsn, false);
    }

    void scannedThrough(DamengDatabaseSchema schema, long lsn) {
        install(schema, lsn, true);
    }

    private void install(DamengDatabaseSchema schema, long lsn, boolean inclusive) {
        final List<Table> ready = new ArrayList<>();
        for (var activation : activations.values()) {
            if (!activation.installed && (lsn > activation.boundary || inclusive && lsn == activation.boundary)) {
                ready.add(activation.table);
            }
        }
        if (!ready.isEmpty()) {
            schema.registerRecoveryTables(ready);
            ready.forEach(table -> {
                activations.get(table.id()).installed = true;
                LOGGER.info("Dameng added table {} activated at LSN {}; existing rows require a separately requested incremental snapshot",
                        table.id(), activations.get(table.id()).boundary);
            });
        }
    }

    boolean excludes(TableId table, long objectId, long lsn) {
        if (table != null) {
            return scopeBoundaries.stream().anyMatch(boundary -> lsn <= boundary.lsn() && boundary.added(table));
        }
        return identityFloors.containsKey(objectId) && lsn <= identityFloors.get(objectId);
    }

    boolean isPending(TableId table) {
        return config.getTableFilters().dataCollectionFilter().isIncluded(table) && activations.containsKey(table) && !activations.get(table).installed;
    }

    Collection<TableId> pendingTables() {
        return activations.keySet().stream().filter(this::isPending).toList();
    }

    private static String writeOffset(Map<String, ?> offset) {
        final var document = Document.create();
        offset.forEach(document::set);
        try {
            return DocumentWriter.defaultWriter().write(document);
        }
        catch (IOException e) {
            throw new ConnectException("Could not serialize Dameng capture scope resume offset", e);
        }
    }

    private static long objectIdentity(Table table) {
        final var attribute = table.attributeWithName("dameng.object.id");
        try {
            if (attribute != null && Long.parseLong(attribute.value()) > 0) {
                return Long.parseLong(attribute.value());
            }
        }
        catch (NumberFormatException e) {
            throw new ConnectException("Invalid Dameng object identity in capture scope descriptor for " + table.id(), e);
        }
        throw new ConnectException("Missing Dameng object identity in capture scope descriptor for " + table.id());
    }

    private static void validateBaselineIdentity(DamengOffsetContext offset, DamengCaptureScopePreparation.Baseline baseline) {
        if (baseline.primaryStandbyIdentity() != null) {
            offset.validatePrimaryStandbyIdentity(baseline.primaryStandbyIdentity());
        }
        else {
            offset.validateDatabaseMagic(baseline.databaseMagic());
        }
    }

    private void warnUnmatchedIncludeRules(Collection<TableId> visibleTables) {
        for (Pattern pattern : Strings.setOfRegex(config.tableIncludeList(), Pattern.CASE_INSENSITIVE)) {
            if (visibleTables.stream().noneMatch(table -> pattern.matcher(config.getTableIdMapper().toString(table)).matches())) {
                LOGGER.warn("Dameng table.include.list rule '{}' did not select any table visible to database user '{}'. "
                        + "Check table existence, identifier case, metadata/SELECT privileges, and database/schema filters. "
                        + "A regex that also matches another visible table cannot reveal inaccessible table names from dictionary-free redo.",
                        pattern.pattern(), config.getJdbcConfig().getString("user"));
            }
        }
    }

    private static Map<String, Object> readOffset(String json) {
        try {
            final Map<String, Object> result = new HashMap<>();
            for (Document.Field field : DocumentReader.defaultReader().read(json)) {
                result.put(field.getName().toString(), field.getValue().asObject());
            }
            return result;
        }
        catch (IOException | RuntimeException e) {
            throw new ConnectException("Invalid Dameng capture scope resume offset", e);
        }
    }
}
