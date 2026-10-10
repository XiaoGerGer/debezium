/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng;

import java.sql.Types;
import java.util.BitSet;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.ConnectHeaders;

import io.debezium.connector.dameng.logminer.parser.LogMinerDmlParser.DmlEntry;
import io.debezium.connector.dameng.logminer.parser.LogMinerDmlParser.SqlExpression;
import io.debezium.data.Envelope.Operation;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.relational.RelationalChangeRecordEmitter;
import io.debezium.relational.Table;
import io.debezium.relational.TableSchema;
import io.debezium.util.Clock;

public final class DamengChangeRecordEmitter extends RelationalChangeRecordEmitter<DamengPartition> {
    private final DmlEntry entry;
    private final Table table;

    public DamengChangeRecordEmitter(DamengPartition partition, OffsetContext offset, DmlEntry entry, Clock clock,
                                     DamengConnectorConfig config, Table table) {
        super(partition, offset, clock, config);
        this.entry = entry;
        this.table = table;
    }

    @Override
    public Operation getOperation() {
        return entry.operation();
    }

    @Override
    protected Object[] getOldColumnValues() {
        return entry.operation() == Operation.UPDATE || entry.operation() == Operation.DELETE
                ? markUnavailableLobValues(entry.oldValues(), entry.oldPresent())
                : null;
    }

    @Override
    protected Object[] getNewColumnValues() {
        return entry.operation() == Operation.CREATE || entry.operation() == Operation.READ || entry.operation() == Operation.UPDATE
                ? markUnavailableLobValues(entry.newValues(), entry.newPresent())
                : null;
    }

    @Override
    protected void emitTruncateRecord(Receiver<DamengPartition> receiver, TableSchema schema) throws InterruptedException {
        final Struct envelope = schema.getEnvelopeSchema().truncate(getOffset().getSourceInfo(), getClock().currentTimeAsInstant());
        receiver.changeRecord(getPartition(), schema, Operation.TRUNCATE, null, envelope, getOffset(), null);
    }

    @Override
    protected void emitUpdateAsPrimaryKeyChangeRecord(Receiver<DamengPartition> receiver, TableSchema schema, Struct oldKey,
                                                      Struct newKey, Struct oldValue, Struct newValue)
            throws InterruptedException {
        final DamengOffsetContext offset = (DamengOffsetContext) getOffset();
        final long serial = offset.getEventSerialNo();
        final ConnectHeaders deleteHeaders = new ConnectHeaders();
        deleteHeaders.add(PK_UPDATE_NEWKEY_FIELD, newKey, schema.keySchema());
        try {
            // The row is not complete until its CREATE is emitted. A restart after
            // only DELETE (or its tombstone) must replay this primary key change.
            offset.setEventSerialNo(serial - 1);
            final Struct envelope = schema.getEnvelopeSchema().delete(oldValue, offset.getSourceInfo(), getClock().currentTimeAsInstant());
            receiver.changeRecord(getPartition(), schema, Operation.DELETE, oldKey, envelope, offset, deleteHeaders);
        }
        finally {
            offset.setEventSerialNo(serial);
        }
        final ConnectHeaders createHeaders = new ConnectHeaders();
        createHeaders.add(PK_UPDATE_OLDKEY_FIELD, oldKey, schema.keySchema());
        final Struct envelope = schema.getEnvelopeSchema().create(newValue, offset.getSourceInfo(), getClock().currentTimeAsInstant());
        receiver.changeRecord(getPartition(), schema, Operation.CREATE, newKey, envelope, offset, createHeaders);
    }

    private Object[] markUnavailableLobValues(Object[] values, BitSet present) {
        final Object[] result = values.clone();
        for (var column : table.columns()) {
            final int position = column.position() - 1;
            if (!present.get(position)) {
                switch (column.jdbcType()) {
                    case Types.CLOB:
                    case Types.NCLOB:
                        result[position] = new SqlExpression("OUT_CLOB");
                        break;
                    case Types.BLOB:
                        result[position] = new SqlExpression("OUT_BLOB");
                        break;
                    default:
                }
            }
        }
        return result;
    }
}
