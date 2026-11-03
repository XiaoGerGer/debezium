/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng;

import io.debezium.relational.Attribute;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;

/** Table kinds retained in schema history, independently of the current dictionary. */
public enum DamengTableType {
    ROW,
    PARTITION_ROOT,
    EXTERNAL,
    TEMPORARY;

    public static final String ATTRIBUTE_NAME = "dameng.table.kind";

    public static boolean isExternal(Table table) {
        return type(table) == EXTERNAL;
    }

    public static boolean isTemporary(Table table) {
        return type(table) == TEMPORARY;
    }

    public static boolean isNonRow(Table table) {
        final var type = type(table);
        return type == EXTERNAL || type == TEMPORARY;
    }

    private static DamengTableType type(Table table) {
        if (table == null) {
            return ROW;
        }
        final var kind = table.attributeWithName(ATTRIBUTE_NAME);
        // Histories written before table-kind tracking contain only supported row tables.
        return kind == null ? ROW : valueOf(kind.value());
    }

    /** External tables need identity and lifecycle history, not a captured column schema. */
    public static Table externalTable(TableId id) {
        return marker(id, EXTERNAL);
    }

    public static Table externalTable(TableId id, long objectId) {
        return new Identity(EXTERNAL, objectId).table(id);
    }

    public static Table temporaryTable(TableId id) {
        return marker(id, TEMPORARY);
    }

    public static Table temporaryTable(TableId id, long objectId) {
        return new Identity(TEMPORARY, objectId).table(id);
    }

    private static Table marker(TableId id, DamengTableType type) {
        return Table.editor().tableId(id)
                .addAttribute(Attribute.editor().name(ATTRIBUTE_NAME).value(type.name()).create()).create();
    }

    /** Dictionary identity compared across a snapshot boundary, including the table kind. */
    public record Identity(DamengTableType type, long objectId) {
        public Table table(TableId id) {
            if (objectId <= 0) {
                throw new IllegalArgumentException("Invalid Dameng table object identity for " + id);
            }
            final var table = marker(id, type);
            return table.edit().addAttributes(table.attributes())
                    .addAttribute(Attribute.editor().name("dameng.object.id").value(Long.toString(objectId)).create()).create();
        }
    }
}
