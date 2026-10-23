/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.List;
import java.util.Objects;

import org.apache.kafka.connect.errors.ConnectException;

import io.debezium.config.Field;
import io.debezium.document.Document;
import io.debezium.document.DocumentReader;
import io.debezium.document.DocumentWriter;
import io.debezium.document.Value;
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

    static String manifest(DamengConnectorConfig config) {
        final var document = Document.create();
        for (Field field : FILTER_FIELDS) {
            document.setString(field.name(), config.getConfig().getString(field));
        }
        document.setBoolean(RelationalDatabaseConnectorConfig.TABLE_IGNORE_BUILTIN.name(),
                config.getConfig().getBoolean(RelationalDatabaseConnectorConfig.TABLE_IGNORE_BUILTIN));
        document.setString("database.dbname", config.getDatabaseName());
        try {
            return DocumentWriter.defaultWriter().write(document);
        }
        catch (IOException e) {
            throw new ConnectException("Could not serialize Dameng capture scope manifest", e);
        }
    }

    static Document readManifest(String manifest) {
        try {
            final var document = DocumentReader.defaultReader().read(manifest);
            for (Field field : FILTER_FIELDS) {
                if (!document.has(field.name()) || (!Value.isNull(document.get(field.name())) && !document.get(field.name()).isString())) {
                    throw new ConnectException("Incomplete Dameng capture scope manifest: " + field.name());
                }
            }
            if (!document.has("database.dbname") || (!Value.isNull(document.get("database.dbname")) && !document.get("database.dbname").isString())
                    || !document.has(RelationalDatabaseConnectorConfig.TABLE_IGNORE_BUILTIN.name())
                    || !document.get(RelationalDatabaseConnectorConfig.TABLE_IGNORE_BUILTIN.name()).isBoolean()) {
                throw new ConnectException("Incomplete Dameng capture scope manifest");
            }
            return document;
        }
        catch (IOException | RuntimeException e) {
            throw new ConnectException("Invalid Dameng capture scope manifest", e);
        }
    }

    static DamengConnectorConfig previousConfig(DamengConnectorConfig current, String manifest) {
        final var document = readManifest(manifest);
        final var builder = current.getConfig().edit();
        for (Field field : FILTER_FIELDS) {
            final String value = document.getString(field.name());
            if (value == null) {
                builder.without(field.name());
            }
            else {
                builder.with(field, value);
            }
        }
        builder.with(RelationalDatabaseConnectorConfig.TABLE_IGNORE_BUILTIN,
                document.getBoolean(RelationalDatabaseConnectorConfig.TABLE_IGNORE_BUILTIN.name()));
        final var previous = new DamengConnectorConfig(builder.build());
        previous.configureIdentifierCase(current.hasCaseInsensitiveIdentifiers());
        return previous;
    }

    static void validateChange(String before, String after) {
        final var oldScope = readManifest(before);
        final var newScope = readManifest(after);
        for (String field : List.of("database.dbname", "database.include.list", "database.exclude.list",
                "table.ignore.builtin", "signal.data.collection")) {
            if (!Objects.equals(oldScope.get(field), newScope.get(field))) {
                throw new ConnectException("Dameng capture scope change cannot modify " + field
                        + "; use a new connector identity and fresh schema history");
            }
        }
    }

    private static void append(StringBuilder manifest, String value) {
        manifest.append(value == null ? -1 : value.length()).append(':');
        if (value != null) {
            manifest.append(value);
        }
    }
}
