/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.Map;

import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.Test;

import io.debezium.data.Envelope;
import io.debezium.relational.TableId;

class DamengEventMetadataProviderTest {
    private final DamengEventMetadataProvider metadata = new DamengEventMetadataProvider();

    @Test
    void shouldReadEventTimeAndRestartPositionsFromTheEmittedSource() {
        final var config = new DamengConnectorConfig(DamengConnectorConfigTest.configuration().build());
        final var offset = new DamengOffsetContext(config, 100);
        final var table = new TableId(null, "APP", "EVENTS");
        final var eventTime = Instant.parse("2026-09-24T01:02:03.456Z");
        offset.setChangeLsn(125);
        offset.setCommitLsn(150);
        offset.setTransactionId("TX1");
        offset.event(table, eventTime);
        final var source = offset.getSourceInfo();
        final var valueSchema = SchemaBuilder.struct().field(Envelope.FieldName.SOURCE, source.schema()).build();
        final var value = new Struct(valueSchema).put(Envelope.FieldName.SOURCE, source);

        assertThat(metadata.getEventTimestamp(table, offset, null, value)).isEqualTo(eventTime);
        assertThat(metadata.getEventSourcePosition(table, offset, null, value)).isEqualTo(Map.of(
                SourceInfo.LSN_KEY, "100", SourceInfo.CHANGE_LSN_KEY, "125", SourceInfo.COMMIT_LSN_KEY, "150"));
        assertThat(metadata.getTransactionId(table, offset, null, value)).isEqualTo("TX1");
    }

    @Test
    void shouldReturnNoMetadataForADeletedRecordTombstone() {
        assertThat(metadata.getEventTimestamp(null, null, null, null)).isNull();
        assertThat(metadata.getEventSourcePosition(null, null, null, null)).isNull();
        assertThat(metadata.getTransactionId(null, null, null, null)).isNull();
    }
}
