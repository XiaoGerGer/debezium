/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng;

import java.time.Instant;
import java.util.Map;

import org.apache.kafka.connect.data.Struct;

import io.debezium.data.Envelope;
import io.debezium.pipeline.source.spi.EventMetadataProvider;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.spi.schema.DataCollectionId;
import io.debezium.util.Collect;

public final class DamengEventMetadataProvider implements EventMetadataProvider {
    @Override
    public Instant getEventTimestamp(DataCollectionId source, OffsetContext offset, Object key, Struct value) {
        if (value == null) {
            return null;
        }
        Struct sourceInfo = value.getStruct(Envelope.FieldName.SOURCE);
        Long timestamp = sourceInfo.getInt64(io.debezium.connector.AbstractSourceInfo.TIMESTAMP_KEY);
        return timestamp == null ? null : Instant.ofEpochMilli(timestamp);
    }

    @Override
    public Map<String, String> getEventSourcePosition(DataCollectionId source, OffsetContext offset, Object key, Struct value) {
        if (value == null) {
            return null;
        }
        Struct sourceInfo = value.getStruct(Envelope.FieldName.SOURCE);
        return Collect.hashMapOf(SourceInfo.LSN_KEY, String.valueOf(sourceInfo.getInt64(SourceInfo.LSN_KEY)),
                SourceInfo.CHANGE_LSN_KEY, String.valueOf(sourceInfo.getInt64(SourceInfo.CHANGE_LSN_KEY)),
                SourceInfo.COMMIT_LSN_KEY, String.valueOf(sourceInfo.getInt64(SourceInfo.COMMIT_LSN_KEY)));
    }

    @Override
    public String getTransactionId(DataCollectionId source, OffsetContext offset, Object key, Struct value) {
        if (value == null) {
            return null;
        }
        return value.getStruct(Envelope.FieldName.SOURCE).getString(SourceInfo.TRANSACTION_ID_KEY);
    }
}
