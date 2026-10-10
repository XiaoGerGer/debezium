/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.sql.Types;
import java.util.List;

import org.apache.kafka.connect.errors.ConnectException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.pipeline.spi.Offsets;
import io.debezium.relational.Attribute;
import io.debezium.relational.Column;
import io.debezium.relational.CustomConverterRegistry;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.relational.history.MemorySchemaHistory;
import io.debezium.schema.SchemaChangeEvent;
import io.debezium.schema.SchemaChangeEvent.SchemaChangeEventType;

class DamengDatabaseSchemaTest {
    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = { "0", "-1", "invalid" })
    void shouldRejectMissingOrInvalidHistoricalObjectIdentity(String identity) throws Exception {
        final var configuration = DamengConnectorConfigTest.configuration()
                .with("schema.history.internal", MemorySchemaHistory.class.getName()).build();
        final var config = new DamengConnectorConfig(configuration);
        final var editor = table("CAPTURED").edit();
        if (identity != null) {
            editor.addAttribute(Attribute.editor().name("dameng.object.id").value(identity).create());
        }
        final var table = editor.create();
        try (var schema = new DamengDatabaseSchema(config, new DamengValueConverters(config), config.schemaNameAdjuster(),
                config.getTopicNamingStrategy(CommonConnectorConfig.TOPIC_NAMING_STRATEGY), new CustomConverterRegistry(List.of()),
                new DamengTaskContext(configuration, config))) {
            final var partition = new DamengPartition(config.getLogicalName());
            final var offset = new DamengOffsetContext(config, 10);
            schema.applySchemaChange(SchemaChangeEvent.ofSnapshotCreate(partition, offset, "DAMENG", table));
            assertThatThrownBy(() -> schema.recover(Offsets.of(partition, offset))).isInstanceOf(ConnectException.class)
                    .hasMessageContaining("APP.CAPTURED").hasMessageContaining("snapshot");
        }
    }

    @Test
    void shouldRemoveRenamedAndDroppedTablesFromMetadataAndSchemas() throws Exception {
        final var configuration = DamengConnectorConfigTest.configuration()
                .with("schema.history.internal", MemorySchemaHistory.class.getName()).build();
        final var config = new DamengConnectorConfig(configuration);
        final var partition = new DamengPartition(config.getLogicalName());
        final var offset = new DamengOffsetContext(config, 10);
        final var original = table("ORIGINAL");
        final var renamed = original.edit().addAttributes(original.attributes()).tableId(new TableId(null, "APP", "RENAMED")).create();
        try (var schema = new DamengDatabaseSchema(config, new DamengValueConverters(config), config.schemaNameAdjuster(),
                config.getTopicNamingStrategy(CommonConnectorConfig.TOPIC_NAMING_STRATEGY), new CustomConverterRegistry(List.of()),
                new DamengTaskContext(configuration, config))) {
            schema.applySchemaChange(SchemaChangeEvent.ofSnapshotCreate(partition, offset, "DAMENG", original));
            schema.validateObjectIdentities();
            assertThat(schema.schemaFor(original.id())).isNotNull();
            offset.setCommitLsn(20);
            offset.setEventSerialNo(2);
            offset.setTransactionId("TX1");
            schema.applySchemaChange(SchemaChangeEvent.ofRename(partition, offset, "DAMENG", "APP",
                    "ALTER TABLE APP.ORIGINAL RENAME TO RENAMED", renamed, original.id()));

            assertThat(schema.getTables().forTable(original.id())).isNull();
            assertThat(schema.schemaFor(original.id())).isNull();
            assertThat(schema.getTables().forTable(renamed.id())).isEqualTo(renamed);
            assertThat(schema.schemaFor(renamed.id())).isNotNull();

            offset.setTransactionId(null);
            offset.setEventSerialNo(0);
            final var recovered = new Tables();
            schema.getSchemaHistory().recover(Offsets.of(partition, offset), recovered, null);
            assertThat(recovered.forTable(original.id())).isNull();
            assertThat(recovered.forTable(renamed.id())).isEqualTo(renamed);
            assertThat(recovered.forTable(renamed.id()).attributeWithName("dameng.object.id").asLong()).isEqualTo(12345L);

            offset.setCommitLsn(25);
            final var truncated = renamed.edit().addAttributes(renamed.attributes()).addAttribute(Attribute.editor().name("dameng.object.id").value("67890").create())
                    .create();
            schema.applySchemaChange(SchemaChangeEvent.of(SchemaChangeEventType.TRUNCATE, partition, offset, "DAMENG", "APP",
                    "TRUNCATE TABLE APP.RENAMED", truncated, false));
            assertThat(schema.getTables().forTable(renamed.id()).attributeWithName("dameng.object.id").asLong()).isEqualTo(67890L);
            final var afterTruncate = new Tables();
            schema.getSchemaHistory().recover(Offsets.of(partition, offset), afterTruncate, null);
            assertThat(afterTruncate.forTable(renamed.id()).attributeWithName("dameng.object.id").asLong()).isEqualTo(67890L);

            offset.setCommitLsn(30);
            schema.applySchemaChange(SchemaChangeEvent.of(SchemaChangeEventType.DROP, partition, offset, "DAMENG", "APP",
                    "DROP TABLE APP.RENAMED", renamed, false));
            assertThat(schema.getTables().forTable(renamed.id())).isNull();
            assertThat(schema.schemaFor(renamed.id())).isNull();

            final var afterDrop = new Tables();
            schema.getSchemaHistory().recover(Offsets.of(partition, offset), afterDrop, null);
            assertThat(afterDrop.tableIds()).isEmpty();
        }
    }

    private static Table table(String name) {
        return Table.editor().tableId(new TableId(null, "APP", name))
                .addAttribute(Attribute.editor().name("dameng.object.id").value("12345").create())
                .addColumn(Column.editor().name("ID").position(1).jdbcType(Types.INTEGER).type("INTEGER").optional(false).create())
                .setPrimaryKeyNames("ID").create();
    }
}
