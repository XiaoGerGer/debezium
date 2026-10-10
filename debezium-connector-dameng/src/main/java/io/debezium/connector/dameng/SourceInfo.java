/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng;

import java.time.Instant;

import io.debezium.annotation.NotThreadSafe;
import io.debezium.connector.common.BaseSourceInfo;
import io.debezium.relational.TableId;

@NotThreadSafe
public final class SourceInfo extends BaseSourceInfo {
    public static final String LSN_KEY = "lsn";
    public static final String CHANGE_LSN_KEY = "change_lsn";
    public static final String COMMIT_LSN_KEY = "commit_lsn";
    public static final String EVENT_SERIAL_NO_KEY = "event_serial_no";
    public static final String TRANSACTION_ID_KEY = "transaction_id";

    private long lsn;
    private final String databaseName;
    private long commitLsn;
    private long changeLsn;
    private long eventSerialNo;
    private String transactionId;
    private Instant timestamp;
    private TableId tableId;

    SourceInfo(DamengConnectorConfig config) {
        super(config);
        databaseName = config.getDatabaseName();
    }

    public long getLsn() {
        return lsn;
    }

    public void setLsn(long value) {
        lsn = value;
    }

    public long getCommitLsn() {
        return commitLsn;
    }

    public void setCommitLsn(long value) {
        commitLsn = value;
    }

    public long getChangeLsn() {
        return changeLsn;
    }

    public void setChangeLsn(long value) {
        changeLsn = value;
    }

    public long getEventSerialNo() {
        return eventSerialNo;
    }

    public void setEventSerialNo(long value) {
        eventSerialNo = value;
    }

    public String getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(String value) {
        transactionId = value;
    }

    public void setSourceTime(Instant value) {
        timestamp = value;
    }

    public TableId getTableId() {
        return tableId;
    }

    public void setTableId(TableId value) {
        tableId = value;
    }

    @Override
    protected Instant timestamp() {
        return timestamp;
    }

    @Override
    protected String database() {
        return databaseName;
    }
}
