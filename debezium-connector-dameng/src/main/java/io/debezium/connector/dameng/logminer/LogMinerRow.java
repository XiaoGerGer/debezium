/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng.logminer;

import java.time.Instant;

/** One decoded row returned by Dameng LogMiner. */
public record LogMinerRow(long lsn, long startLsn, long commitLsn, String transactionId, int operation, int rollback,
        String owner, String tableName, String username, String redo, String recordSetId, int sequence, int continuation,
        Instant timestamp, long objectId, long objectVersion, long archiveSequence, long blockNumber, int byteOffset) {

    public LogMinerRow(long lsn, long startLsn, long commitLsn, String transactionId, int operation,
                       int rollback, String owner, String tableName, String username, String redo,
                       String recordSetId, int sequence, int continuation, Instant timestamp, long objectId) {
        this(lsn, startLsn, commitLsn, transactionId, operation, rollback, owner, tableName, username, redo,
                recordSetId, sequence, continuation, timestamp, objectId, -1, -1, -1, -1);
    }

    public LogMinerRow(long lsn, long startLsn, long commitLsn, String transactionId, int operation,
                       int rollback, String owner, String tableName, String username, String redo,
                       String recordSetId, int sequence, int continuation, Instant timestamp) {
        this(lsn, startLsn, commitLsn, transactionId, operation, rollback, owner, tableName, username, redo,
                recordSetId, sequence, continuation, timestamp, 0);
    }

    public LogMinerRow withRedo(String sql) {
        return new LogMinerRow(lsn, startLsn, commitLsn, transactionId, operation, rollback, owner, tableName, username,
                sql, recordSetId, sequence, 0, timestamp, objectId, objectVersion, archiveSequence, blockNumber, byteOffset);
    }

    /** Numeric coordinates only; never include row values in error diagnostics. */
    public String position() {
        return "LSN " + lsn + " (XID=" + transactionId + ", RS_ID=" + recordSetId + ", SSN=" + sequence
                + ", DATA_OBJ#=" + objectId + ", DATA_OBJV#=" + objectVersion + ", RBASQN=" + archiveSequence
                + ", RBABLK=" + blockNumber + ", RBABYTE=" + byteOffset + ")";
    }
}
