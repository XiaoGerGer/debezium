/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.Types;
import java.util.BitSet;

import javax.sql.rowset.serial.SerialBlob;
import javax.sql.rowset.serial.SerialClob;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.errors.DataException;
import org.junit.jupiter.api.Test;

import io.debezium.connector.dameng.logminer.parser.LogMinerDmlParser.DmlEntry;
import io.debezium.connector.dameng.logminer.parser.LogMinerDmlParser.SqlExpression;
import io.debezium.data.Envelope.Operation;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;

class DamengLobValueConvertersTest {
    private final DamengConnectorConfig config = new DamengConnectorConfig(DamengConnectorConfigTest.configuration()
            .with("unavailable.value.placeholder", "missing:未获取").build());
    private final DamengValueConverters converters = new DamengValueConverters(config);

    @Test
    void shouldReadJdbcSnapshotLobContents() throws Exception {
        assertThat(convert(Types.CLOB, new SerialClob("snapshot 文本".toCharArray()))).isEqualTo("snapshot 文本");
        assertThat(convert(Types.BLOB, new SerialBlob(new byte[]{ 0, 1, (byte) 255 })))
                .isEqualTo(ByteBuffer.wrap(new byte[]{ 0, 1, (byte) 255 }));
    }

    @Test
    void shouldDistinguishNullTextAndUnavailableLobMarkers() {
        assertThat(convert(Types.CLOB, null)).isNull();
        assertThat(convert(Types.CLOB, "OUT_CLOB")).isEqualTo("OUT_CLOB");
        assertThat(convert(Types.CLOB, new SqlExpression("OUT_CLOB"))).isEqualTo("missing:未获取");
        assertThat(convert(Types.NCLOB, new SqlExpression("OUT_CLOB"))).isEqualTo("missing:未获取");
        assertThat(convert(Types.BLOB, new SqlExpression("OUT_BLOB")))
                .isEqualTo(ByteBuffer.wrap("missing:未获取".getBytes(StandardCharsets.UTF_8)));
        assertThatThrownBy(() -> convert(Types.INTEGER, new SqlExpression("OUT_CLOB"))).isInstanceOf(DataException.class);
    }

    @Test
    void shouldMarkMissingImagesWithoutReplacingExplicitNulls() {
        final var table = Table.editor().tableId(new TableId(null, "APP", "LOBS"))
                .addColumn(column("ID", Types.INTEGER, 1)).addColumn(column("TEXT_VALUE", Types.CLOB, 2))
                .addColumn(column("BINARY_VALUE", Types.BLOB, 3)).create();
        final BitSet oldPresent = new BitSet();
        oldPresent.set(0);
        final BitSet newPresent = new BitSet();
        newPresent.set(0, 3);
        final Object[] oldValues = { 1, null, null };
        final var entry = new DmlEntry(Operation.UPDATE, oldValues, new Object[]{ 1, null, null }, oldPresent, newPresent);
        final var emitter = new DamengChangeRecordEmitter(new DamengPartition("test"), new DamengOffsetContext(config, 1), entry,
                Clock.system(), config, table);

        assertThat(emitter.getOldColumnValues()).containsExactly(1, new SqlExpression("OUT_CLOB"), new SqlExpression("OUT_BLOB"));
        assertThat(emitter.getNewColumnValues()).containsExactly(1, null, null);
        assertThat(oldValues).containsExactly(1, null, null);
    }

    @Test
    void shouldMarkMissingAfterImages() {
        final var table = Table.editor().tableId(new TableId(null, "APP", "LOBS"))
                .addColumn(column("ID", Types.INTEGER, 1)).addColumn(column("TEXT_VALUE", Types.CLOB, 2)).create();
        final BitSet present = new BitSet();
        present.set(0);
        final var entry = new DmlEntry(Operation.UPDATE, new Object[]{ 1, null }, new Object[]{ 1, null }, present, present);
        final var emitter = new DamengChangeRecordEmitter(new DamengPartition("test"), new DamengOffsetContext(config, 1), entry,
                Clock.system(), config, table);
        assertThat(emitter.getNewColumnValues()).containsExactly(1, new SqlExpression("OUT_CLOB"));
    }

    @Test
    void shouldRejectUnmappedTypesInsteadOfSilentlyOmittingTheColumn() {
        assertThatThrownBy(() -> converters.schemaBuilder(column("UNMAPPED", Types.OTHER, 1)))
                .isInstanceOf(DataException.class).hasMessageContaining("UNMAPPED").hasMessageContaining("exclude");
    }

    private Object convert(int jdbcType, Object value) {
        final var column = column("VALUE", jdbcType, 1);
        final var field = new Field(column.name(), 0, converters.schemaBuilder(column).optional().build());
        return converters.converter(column, field).convert(value);
    }

    private static Column column(String name, int jdbcType, int position) {
        return Column.editor().name(name).position(position).jdbcType(jdbcType).type("TYPE").length(1000).optional(true).create();
    }
}
