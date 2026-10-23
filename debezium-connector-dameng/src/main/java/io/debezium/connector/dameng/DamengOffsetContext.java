/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;

import io.debezium.connector.AbstractSourceInfo;
import io.debezium.connector.SnapshotRecord;
import io.debezium.connector.SnapshotType;
import io.debezium.connector.dameng.compatibility.DamengCompatibilityEnvironment;
import io.debezium.pipeline.CommonOffsetContext;
import io.debezium.pipeline.source.snapshot.incremental.IncrementalSnapshotContext;
import io.debezium.pipeline.source.snapshot.incremental.SignalBasedIncrementalSnapshotContext;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.txmetadata.TransactionContext;
import io.debezium.relational.TableId;
import io.debezium.spi.schema.DataCollectionId;

public final class DamengOffsetContext extends CommonOffsetContext<SourceInfo> {
    public static final String DATABASE_MAGIC_KEY = "database_magic";
    public static final String MINING_TRANSACTION_ID_KEY = "mining_transaction_id";
    public static final String CAPTURE_SCOPE_KEY = "capture_scope";
    public static final String CAPTURE_SCOPE_MANIFEST_KEY = "capture_scope_manifest";
    public static final String CAPTURE_SCOPE_GENERATION_KEY = "capture_scope_generation";
    public static final String PERMANENT_MAGIC_KEY = "permanent_magic";
    public static final String PRIMARY_OPEN_GUID_KEY = "primary_open_guid";
    public static final String COMPATIBILITY_SIGNATURE_KEY = "compatibility_signature";
    public static final String RECOVERY_NONCE_KEY = "recovery_nonce";
    public static final String RECOVERY_BASELINE_LSN_KEY = "recovery_baseline_lsn";
    private final Schema sourceInfoSchema;
    private String captureScope;
    private String captureScopeManifest;
    private String captureScopeGeneration;
    private final TransactionContext transactionContext;
    private IncrementalSnapshotContext<TableId> incrementalSnapshotContext = new SignalBasedIncrementalSnapshotContext<>(false);
    private Long databaseMagic;
    private Long permanentMagic;
    private String primaryOpenGuid;
    private String compatibilitySignature;
    private boolean restored;
    private String recoveryNonce;
    private long recoveryBaselineLsn;

    public DamengOffsetContext(DamengConnectorConfig config, long lsn, long changeLsn, long commitLsn, String transactionId,
                               long eventSerialNo, SnapshotType snapshot, boolean snapshotCompleted,
                               TransactionContext transactionContext) {
        super(new SourceInfo(config), snapshotCompleted);
        captureScope = DamengCaptureScope.signature(config);
        captureScopeManifest = DamengCaptureScope.manifest(config);
        sourceInfo.setLsn(lsn);
        sourceInfo.setChangeLsn(changeLsn);
        sourceInfo.setCommitLsn(commitLsn);
        sourceInfo.setTransactionId(transactionId);
        sourceInfo.setEventSerialNo(eventSerialNo);
        sourceInfoSchema = sourceInfo.schema();
        this.transactionContext = transactionContext;
        if (snapshotCompleted) {
            postSnapshotCompletion();
        }
        else if (snapshot != null) {
            setSnapshot(snapshot);
            sourceInfo.setSnapshot(SnapshotRecord.TRUE);
        }
    }

    public DamengOffsetContext(DamengConnectorConfig config, long lsn) {
        this(config, lsn, lsn, lsn, null, 0, null, false, new TransactionContext());
    }

    @Override
    public Map<String, ?> getOffset() {
        Map<String, Object> result = new HashMap<>();
        result.put(CAPTURE_SCOPE_KEY, captureScope);
        result.put(CAPTURE_SCOPE_MANIFEST_KEY, captureScopeManifest);
        if (captureScopeGeneration != null) {
            result.put(CAPTURE_SCOPE_GENERATION_KEY, captureScopeGeneration);
        }
        if (recoveryNonce != null) {
            result.put(RECOVERY_NONCE_KEY, recoveryNonce);
            result.put(RECOVERY_BASELINE_LSN_KEY, recoveryBaselineLsn);
        }
        if (compatibilitySignature != null) {
            result.put(COMPATIBILITY_SIGNATURE_KEY, compatibilitySignature);
        }
        getSnapshot().ifPresent(s -> result.put(AbstractSourceInfo.SNAPSHOT_KEY, s.toString()));
        result.put(SNAPSHOT_COMPLETED_KEY, snapshotCompleted);
        result.put(SourceInfo.LSN_KEY, sourceInfo.getLsn());
        result.put(SourceInfo.CHANGE_LSN_KEY, sourceInfo.getChangeLsn());
        result.put(SourceInfo.COMMIT_LSN_KEY, sourceInfo.getCommitLsn());
        result.put(SourceInfo.EVENT_SERIAL_NO_KEY, sourceInfo.getEventSerialNo());
        if (databaseMagic != null) {
            result.put(DATABASE_MAGIC_KEY, databaseMagic);
        }
        if (permanentMagic != null) {
            result.put(PERMANENT_MAGIC_KEY, permanentMagic);
            result.put(PRIMARY_OPEN_GUID_KEY, primaryOpenGuid);
        }
        if (sourceInfo.getTransactionId() != null) {
            // TransactionContext owns transaction_id for optional event metadata.
            // The mining resume position must remain independent of that context.
            result.put(MINING_TRANSACTION_ID_KEY, sourceInfo.getTransactionId());
        }
        return incrementalSnapshotContext.store(transactionContext.store(result));
    }

    @Override
    public Schema getSourceInfoSchema() {
        return sourceInfoSchema;
    }

    public long getLsn() {
        return sourceInfo.getLsn();
    }

    public void setLsn(long lsn) {
        sourceInfo.setLsn(lsn);
    }

    public Long getDatabaseMagic() {
        return databaseMagic;
    }

    String getCaptureScopeManifest() {
        return captureScopeManifest;
    }

    String getCaptureScopeGeneration() {
        return captureScopeGeneration;
    }

    void setCaptureScope(String manifest, String generation) {
        captureScopeManifest = manifest;
        captureScopeGeneration = generation;
    }

    void acceptCaptureScope(DamengConnectorConfig config, String generation) {
        captureScope = DamengCaptureScope.signature(config);
        setCaptureScope(DamengCaptureScope.manifest(config), generation);
    }

    public String getRecoveryNonce() {
        return recoveryNonce;
    }

    public long getRecoveryBaselineLsn() {
        return recoveryBaselineLsn;
    }

    public void setRecoveryBaseline(String nonce, long baselineLsn) {
        if (nonce == null || nonce.isBlank() || baselineLsn <= 0) {
            throw new ConnectException("Invalid Dameng recovery baseline");
        }
        recoveryNonce = nonce;
        recoveryBaselineLsn = baselineLsn;
    }

    /** Bind fresh positions, but never infer historical parsing rules from today's server settings. */
    public void validateCompatibilityEnvironment(DamengCompatibilityEnvironment environment) {
        if (restored && compatibilitySignature == null) {
            throw new ConnectException("Stored Dameng offset has no compatibility signature; historical lexical and identifier settings cannot be verified. "
                    + "Use a new connector identity and fresh schema history with an initial snapshot before upgrading this offset");
        }
        final var current = environment.signature();
        if (compatibilitySignature != null && !compatibilitySignature.equals(current)) {
            throw new ConnectException("Dameng compatibility settings differ from the stored offset. Restore the original parameters only if no affected history "
                    + "was generated under the changed settings; otherwise use a new connector identity and fresh schema history with an initial snapshot. "
                    + "Parameters must match on all primary/standby members; old redo cannot be reinterpreted safely");
        }
        compatibilitySignature = current;
    }

    /** Binds legacy offsets once and rejects positions belonging to another database incarnation. */
    public void validateDatabaseMagic(long currentDatabaseMagic) {
        if (databaseMagic != null && databaseMagic != currentDatabaseMagic) {
            throw new ConnectException("Stored Dameng offset belongs to database magic " + databaseMagic
                    + " but the connected database has magic " + currentDatabaseMagic
                    + "; use a new connector identity and snapshot after recreating or replacing the database");
        }
        databaseMagic = currentDatabaseMagic;
    }

    public void validatePrimaryStandbyIdentity(DamengDatabaseIdentity identity) {
        if (databaseMagic != null) {
            if (permanentMagic == null || primaryOpenGuid == null) {
                throw new ConnectException("Stored Dameng offset has no primary/standby lineage anchor. "
                        + "Use a new connector identity and fresh snapshot/history before enabling database.failover.hosts");
            }
            identity.validateResume(permanentMagic, databaseMagic, primaryOpenGuid,
                    Math.max(getLsn(), Math.max(getChangeLsn(), getCommitLsn())));
        }
        permanentMagic = identity.permanentMagic();
        databaseMagic = identity.databaseMagic();
        primaryOpenGuid = identity.openGuid();
    }

    public long getChangeLsn() {
        return sourceInfo.getChangeLsn();
    }

    public void setChangeLsn(long lsn) {
        sourceInfo.setChangeLsn(lsn);
    }

    public long getCommitLsn() {
        return sourceInfo.getCommitLsn();
    }

    public void setCommitLsn(long lsn) {
        sourceInfo.setCommitLsn(lsn);
    }

    public long getEventSerialNo() {
        return sourceInfo.getEventSerialNo();
    }

    public void setEventSerialNo(long value) {
        sourceInfo.setEventSerialNo(value);
    }

    public String getTransactionId() {
        return sourceInfo.getTransactionId();
    }

    public void setTransactionId(String value) {
        sourceInfo.setTransactionId(value);
    }

    public SourceInfo getSourceInfoObject() {
        return sourceInfo;
    }

    @Override
    public void event(DataCollectionId collectionId, Instant timestamp) {
        sourceInfo.setSourceTime(timestamp);
        sourceInfo.setTableId((io.debezium.relational.TableId) collectionId);
    }

    @Override
    public TransactionContext getTransactionContext() {
        return transactionContext;
    }

    @Override
    public IncrementalSnapshotContext<TableId> getIncrementalSnapshotContext() {
        return incrementalSnapshotContext;
    }

    public static final class Loader implements OffsetContext.Loader<DamengOffsetContext> {
        private final DamengConnectorConfig config;
        private final boolean allowScopeChange;

        public Loader(DamengConnectorConfig config) {
            this(config, false);
        }

        Loader(DamengConnectorConfig config, boolean allowScopeChange) {
            this.config = config;
            this.allowScopeChange = allowScopeChange;
        }

        @Override
        public DamengOffsetContext load(Map<String, ?> offset) {
            final Object captureScope = offset.get(CAPTURE_SCOPE_KEY);
            if (captureScope == null) {
                throw new ConnectException("Stored Dameng offset has no capture scope signature; its historical table coverage cannot be verified. "
                        + "Use a new connector identity and fresh schema history with an initial snapshot before upgrading this offset");
            }
            if (!(captureScope instanceof String) || !((String) captureScope).matches("v1:[0-9a-f]{64}")) {
                throw new ConnectException("Invalid Dameng capture scope signature in stored offset");
            }
            final Object storedManifest = offset.get(CAPTURE_SCOPE_MANIFEST_KEY);
            if (storedManifest != null && (!(storedManifest instanceof String)
                    || !DamengCaptureScope.signature(DamengCaptureScope.previousConfig(config, (String) storedManifest)).equals(captureScope))) {
                throw new ConnectException("Dameng capture scope manifest does not match its signature");
            }
            if (!DamengCaptureScope.signature(config).equals(captureScope) && !allowScopeChange) {
                throw new ConnectException("Dameng capture scope differs from the stored offset. Changes to database, schema or table include/exclude filters, "
                        + "table.ignore.builtin or signal.data.collection require a new connector identity and fresh schema history with an initial snapshot; "
                        + "reusing the previous offset could silently omit changes without historical table definitions");
            }
            long lsn = number(offset.get(SourceInfo.LSN_KEY));
            long commitLsn = offset.containsKey(SourceInfo.COMMIT_LSN_KEY) ? number(offset.get(SourceInfo.COMMIT_LSN_KEY)) : lsn;
            long serial = offset.containsKey(SourceInfo.EVENT_SERIAL_NO_KEY) ? number(offset.get(SourceInfo.EVENT_SERIAL_NO_KEY)) : 0;
            Object tx = offset.containsKey(MINING_TRANSACTION_ID_KEY) ? offset.get(MINING_TRANSACTION_ID_KEY) : offset.get(SourceInfo.TRANSACTION_ID_KEY);
            long changeLsn = offset.containsKey(SourceInfo.CHANGE_LSN_KEY) ? number(offset.get(SourceInfo.CHANGE_LSN_KEY)) : lsn;
            final var context = new DamengOffsetContext(config, lsn, changeLsn, commitLsn, tx == null ? null : tx.toString(), serial,
                    loadSnapshot(offset).orElse(null), loadSnapshotCompleted(offset),
                    config.shouldProvideTransactionMetadata() ? TransactionContext.load(offset) : new TransactionContext());
            context.captureScope = captureScope.toString();
            final Object manifest = offset.get(CAPTURE_SCOPE_MANIFEST_KEY);
            context.captureScopeManifest = manifest == null
                    ? (DamengCaptureScope.signature(config).equals(captureScope) ? DamengCaptureScope.manifest(config) : null)
                    : (String) manifest;
            final Object generation = offset.get(CAPTURE_SCOPE_GENERATION_KEY);
            if (generation != null && (!(generation instanceof String) || ((String) generation).isBlank())) {
                throw new ConnectException("Invalid Dameng capture scope generation in stored offset");
            }
            context.captureScopeGeneration = (String) generation;
            if (offset.get(DATABASE_MAGIC_KEY) != null) {
                context.databaseMagic = number(offset.get(DATABASE_MAGIC_KEY));
            }
            if (offset.get(PERMANENT_MAGIC_KEY) != null) {
                context.permanentMagic = number(offset.get(PERMANENT_MAGIC_KEY));
                context.primaryOpenGuid = (String) offset.get(PRIMARY_OPEN_GUID_KEY);
            }
            context.incrementalSnapshotContext = SignalBasedIncrementalSnapshotContext.load(offset, false);
            context.restored = true;
            final var signature = offset.get(COMPATIBILITY_SIGNATURE_KEY);
            if (signature != null && (!(signature instanceof String) || !((String) signature).startsWith("v1:"))) {
                throw new ConnectException("Invalid or unsupported Dameng compatibility signature in stored offset");
            }
            context.compatibilitySignature = (String) signature;
            if (offset.get(RECOVERY_NONCE_KEY) != null) {
                context.setRecoveryBaseline((String) offset.get(RECOVERY_NONCE_KEY), number(offset.get(RECOVERY_BASELINE_LSN_KEY)));
                if (lsn < context.recoveryBaselineLsn || commitLsn < context.recoveryBaselineLsn) {
                    throw new ConnectException("Stored Dameng offset precedes its recovery baseline");
                }
            }
            return context;
        }

        private static long number(Object value) {
            return value instanceof Number ? ((Number) value).longValue() : Long.parseLong(String.valueOf(value));
        }
    }
}
