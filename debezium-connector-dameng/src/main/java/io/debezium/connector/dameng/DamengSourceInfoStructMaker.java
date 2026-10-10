/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.connector.AbstractSourceInfoStructMaker;
import io.debezium.relational.TableId;

public final class DamengSourceInfoStructMaker extends AbstractSourceInfoStructMaker<SourceInfo> {
    private Schema schema;

    @Override
    public void init(String connector, String version, CommonConnectorConfig config) {
        super.init(connector, version, config);
        schema = commonSchemaBuilder().name("io.debezium.connector.dameng.Source")
                .field(SourceInfo.SCHEMA_NAME_KEY, Schema.STRING_SCHEMA)
                .field(SourceInfo.TABLE_NAME_KEY, Schema.STRING_SCHEMA)
                .field(SourceInfo.LSN_KEY, Schema.INT64_SCHEMA)
                .field(SourceInfo.CHANGE_LSN_KEY, Schema.INT64_SCHEMA)
                .field(SourceInfo.COMMIT_LSN_KEY, Schema.INT64_SCHEMA)
                .field(SourceInfo.EVENT_SERIAL_NO_KEY, Schema.INT64_SCHEMA)
                .field(SourceInfo.TRANSACTION_ID_KEY, Schema.OPTIONAL_STRING_SCHEMA)
                .build();
    }

    @Override
    public Schema schema() {
        return schema;
    }

    @Override
    public Struct struct(SourceInfo sourceInfo) {
        TableId table = sourceInfo.getTableId();
        return commonStruct(sourceInfo)
                .put(SourceInfo.SCHEMA_NAME_KEY, table == null ? "" : table.schema())
                .put(SourceInfo.TABLE_NAME_KEY, table == null ? "" : table.table())
                .put(SourceInfo.LSN_KEY, sourceInfo.getLsn())
                .put(SourceInfo.CHANGE_LSN_KEY, sourceInfo.getChangeLsn())
                .put(SourceInfo.COMMIT_LSN_KEY, sourceInfo.getCommitLsn())
                .put(SourceInfo.EVENT_SERIAL_NO_KEY, sourceInfo.getEventSerialNo())
                .put(SourceInfo.TRANSACTION_ID_KEY, sourceInfo.getTransactionId());
    }
}
