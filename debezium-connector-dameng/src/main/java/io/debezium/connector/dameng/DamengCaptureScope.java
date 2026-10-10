/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.List;

import io.debezium.config.Field;
import io.debezium.relational.RelationalDatabaseConnectorConfig;

/** Identifies the capture configuration whose historical table definitions were snapshotted. */
final class DamengCaptureScope {
    private static final List<Field> FILTER_FIELDS = List.of(
            RelationalDatabaseConnectorConfig.DATABASE_INCLUDE_LIST,
            RelationalDatabaseConnectorConfig.DATABASE_EXCLUDE_LIST,
            RelationalDatabaseConnectorConfig.SCHEMA_INCLUDE_LIST,
            RelationalDatabaseConnectorConfig.SCHEMA_EXCLUDE_LIST,
            RelationalDatabaseConnectorConfig.TABLE_INCLUDE_LIST,
            RelationalDatabaseConnectorConfig.TABLE_EXCLUDE_LIST,
            RelationalDatabaseConnectorConfig.SIGNAL_DATA_COLLECTION);

    private DamengCaptureScope() {
    }

    static String signature(DamengConnectorConfig config) {
        final var manifest = new StringBuilder();
        for (Field field : FILTER_FIELDS) {
            append(manifest, field.name());
            append(manifest, config.getConfig().getString(field));
        }
        append(manifest, RelationalDatabaseConnectorConfig.TABLE_IGNORE_BUILTIN.name());
        append(manifest, Boolean.toString(config.getConfig().getBoolean(RelationalDatabaseConnectorConfig.TABLE_IGNORE_BUILTIN)));
        try {
            return "v1:" + HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(manifest.toString().getBytes(StandardCharsets.UTF_8)));
        }
        catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is required to identify the Dameng capture scope", e);
        }
    }

    private static void append(StringBuilder manifest, String value) {
        manifest.append(value == null ? -1 : value.length()).append(':');
        if (value != null) {
            manifest.append(value);
        }
    }
}
