/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng;

import java.util.Optional;

import io.debezium.relational.TableId;

/** Automatically named storage tables excluded by the connector's built-in filter. */
public enum DamengInternalTableType {
    MATERIALIZED_VIEW("MTAB$_"),
    MATERIALIZED_VIEW_LOG("MLOG$_"),
    BITMAP_JOIN_INDEX("BMJ$_");

    private final String prefix;

    DamengInternalTableType(String prefix) {
        this.prefix = prefix;
    }

    /** Classifies the table name independently of its schema and the capture configuration. */
    public static Optional<DamengInternalTableType> fromTableId(TableId tableId) {
        final var name = tableId.table();
        if (name != null) {
            for (final var type : values()) {
                if (name.regionMatches(true, 0, type.prefix, 0, type.prefix.length())) {
                    return Optional.of(type);
                }
            }
        }
        return Optional.empty();
    }
}
