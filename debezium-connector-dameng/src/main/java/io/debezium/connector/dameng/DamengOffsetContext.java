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
    public static final String PERMANENT_MAGIC_KEY = "permanent_magic";
    public static final String PRIMARY_OPEN_GUID_KEY = "primary_open_guid";
    private final Schema sourceInfoSchema;
    private final String captureScope;
    private final TransactionContext transactionContext;
    private IncrementalSnapshotContext<TableId> incrementalSnapshotContext = new SignalBasedIncrementalSnapshotContext<>(false);
    private Long databaseMagic;
    private Long permanentMagic;
    private String primaryOpenGuid;

    public DamengOffsetContext(DamengConnectorConfig config, long lsn, long changeLsn, long commitLsn, String transactionId,
                               long eventSerialNo, SnapshotType snapshot, boolean snapshotCompleted,
                               TransactionContext transactionContext) {
        super(new SourceInfo(config), snapshotCompleted);
        captureScope = DamengCaptureScope.signature(config);
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

        public Loader(DamengConnectorConfig config) {
            this.config = config;
        }

        @Override
        public DamengOffsetContext load(Map<String, ?> offset) {
            final Object captureScope = offset.get(CAPTURE_SCOPE_KEY);
            if (captureScope == null) {
                throw new ConnectException("Stored Dameng offset has no capture scope signature; its historical table coverage cannot be verified. "
                        + "Use a new connector identity and fresh schema history with an initial snapshot before upgrading this offset");
            }
            if (!DamengCaptureScope.signature(config).equals(captureScope)) {
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
            if (offset.get(DATABASE_MAGIC_KEY) != null) {
                context.databaseMagic = number(offset.get(DATABASE_MAGIC_KEY));
            }
            if (offset.get(PERMANENT_MAGIC_KEY) != null) {
                context.permanentMagic = number(offset.get(PERMANENT_MAGIC_KEY));
                context.primaryOpenGuid = (String) offset.get(PRIMARY_OPEN_GUID_KEY);
            }
            context.incrementalSnapshotContext = SignalBasedIncrementalSnapshotContext.load(offset, false);
            return context;
        }

        private static long number(Object value) {
            return value instanceof Number ? ((Number) value).longValue() : Long.parseLong(String.valueOf(value));
        }
    }
}
