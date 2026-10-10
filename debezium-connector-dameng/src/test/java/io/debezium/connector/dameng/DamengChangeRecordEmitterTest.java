/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.sql.Types;
import java.time.Instant;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.connector.dameng.logminer.parser.LogMinerDmlParser.DmlEntry;
import io.debezium.data.Envelope.Operation;
import io.debezium.relational.Column;
import io.debezium.relational.CustomConverterRegistry;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.history.MemorySchemaHistory;
import io.debezium.schema.SchemaChangeEvent;
import io.debezium.util.Clock;

class DamengChangeRecordEmitterTest {
    private final DamengConnectorConfig config = new DamengConnectorConfig(DamengConnectorConfigTest.configuration()
            .with("schema.history.internal", MemorySchemaHistory.class.getName()).build());
    private final DamengPartition partition = new DamengPartition(config.getLogicalName());
    private final DamengOffsetContext offset = new DamengOffsetContext(config, 10);
    private final Table table = Table.editor().tableId(new TableId(null, "APP", "KEYS"))
            .addColumn(Column.editor().name("ID").position(1).jdbcType(Types.INTEGER).type("INTEGER").optional(false).create())
            .setPrimaryKeyNames("ID").create();

    @Test
    void shouldEmitTruncateWithNullKey() throws Exception {
        try (var schema = schema()) {
            final var emitter = emitter(new DmlEntry(Operation.TRUNCATE, null, null, new BitSet(), new BitSet()));
            final List<Operation> operations = new ArrayList<>();
            emitter.emitChangeRecords(schema.schemaFor(table.id()), (partition, collection, operation, key, value, offset, headers) -> {
                operations.add(operation);
                assertThat(key).isNull();
                assertThat(value.getString("op")).isEqualTo("t");
            });
            assertThat(operations).containsExactly(Operation.TRUNCATE);
        }
    }

    @Test
    void shouldCompletePrimaryKeyChangeOffsetOnlyAfterCreate() throws Exception {
        try (var schema = schema()) {
            offset.setEventSerialNo(0);
            final List<Long> serials = new ArrayList<>();
            final List<Operation> operations = new ArrayList<>();
            emitter(primaryKeyChange()).emitChangeRecords(schema.schemaFor(table.id()), (partition, collection, operation, key, value, context, headers) -> {
                operations.add(operation);
                serials.add((Long) context.getOffset().get("event_serial_no"));
                assertThat(value.getStruct("source").getInt64("event_serial_no")).isEqualTo(serials.get(serials.size() - 1));
                assertThat(headers).isNotEmpty();
            });
            assertThat(operations).containsExactly(Operation.DELETE, Operation.CREATE);
            assertThat(serials).containsExactly(-1L, 0L);
            assertThat(offset.getEventSerialNo()).isZero();
        }
    }

    @Test
    void shouldRetainReplayableOffsetWhenInterruptedAfterDelete() throws Exception {
        try (var schema = schema()) {
            offset.setEventSerialNo(3);
            final AtomicReference<Map<String, ?>> acknowledged = new AtomicReference<>();
            assertThatThrownBy(() -> emitter(primaryKeyChange()).emitChangeRecords(schema.schemaFor(table.id()),
                    (partition, collection, operation, key, value, context, headers) -> {
                        acknowledged.set(context.getOffset());
                        throw new InterruptedException("Stopped after delete");
                    })).isInstanceOf(InterruptedException.class);
            final var restored = new DamengOffsetContext.Loader(config).load(acknowledged.get());
            assertThat(restored.getEventSerialNo()).isEqualTo(2);
            assertThat(offset.getEventSerialNo()).isEqualTo(3);
        }
    }

    private DamengDatabaseSchema schema() {
        offset.event(table.id(), Instant.parse("2026-09-24T00:00:00Z"));
        final var schema = new DamengDatabaseSchema(config, new DamengValueConverters(config), config.schemaNameAdjuster(),
                config.getTopicNamingStrategy(CommonConnectorConfig.TOPIC_NAMING_STRATEGY), new CustomConverterRegistry(List.of()),
                new DamengTaskContext(config.getConfig(), config));
        schema.applySchemaChange(SchemaChangeEvent.ofSnapshotCreate(partition, offset, "DAMENG", table));
        return schema;
    }

    private DamengChangeRecordEmitter emitter(DmlEntry entry) {
        return new DamengChangeRecordEmitter(partition, offset, entry, Clock.system(), config, table);
    }

    private static DmlEntry primaryKeyChange() {
        final BitSet present = new BitSet();
        present.set(0);
        return new DmlEntry(Operation.UPDATE, new Object[]{ 1 }, new Object[]{ 2 }, present, present);
    }
}
