/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.ConfigDefinition;
import io.debezium.config.Configuration;
import io.debezium.config.ConfigurationNames;
import io.debezium.config.EnumeratedValue;
import io.debezium.config.Field;
import io.debezium.connector.AbstractSourceInfo;
import io.debezium.connector.SourceInfoStructMaker;
import io.debezium.connector.dameng.logminer.LogMinerBatch;
import io.debezium.document.Document;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.relational.ColumnFilterMode;
import io.debezium.relational.HistorizedRelationalDatabaseConnectorConfig;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables.TableFilter;
import io.debezium.relational.history.HistoryRecordComparator;

public final class DamengConnectorConfig extends HistorizedRelationalDatabaseConnectorConfig {
    public static final int DEFAULT_PORT = 5236;
    public static final Field PORT = RelationalDatabaseConnectorConfig.PORT.withDefault(DEFAULT_PORT);
    public static final Field HOSTNAME = RelationalDatabaseConnectorConfig.HOSTNAME.optional().withNoValidation()
            .withValidation(DamengConnectorConfig::validateDamengHostname)
            .withDescription("Resolvable hostname or IP address of the database server. Required unless database.failover.hosts is set.");
    public static final Field FAILOVER_HOSTS = Field.create("database.failover.hosts")
            .withDisplayName("Primary failover hosts").withType(Type.STRING).withDefault("")
            .withWidth(Width.LONG).withImportance(Importance.HIGH)
            .withValidation(DamengConnectorConfig::validateFailoverHosts)
            .withDescription("Optional comma-separated host:port endpoints belonging to one primary/standby database. "
                    + "Use [IPv6]:port for IPv6 addresses. Overrides database.hostname and database.port. "
                    + "The connector connects only to an OPEN PRIMARY and owns reconnects so offsets and schema history are restored together.");
    public static final Field DATABASE_NAME = RelationalDatabaseConnectorConfig.DATABASE_NAME
            .withDefault("DAMENG");
    public static final Field SNAPSHOT_MODE = Field.create(CommonConnectorConfig.SNAPSHOT_MODE_PROPERTY_NAME)
            .withDisplayName("Snapshot mode").withEnum(SnapshotMode.class, SnapshotMode.INITIAL)
            .withWidth(Width.SHORT).withImportance(Importance.LOW)
            .withDescription("Snapshot mode: initial, initial_only, no_data, or the legacy no_data alias never.");
    public static final Field SNAPSHOT_LOCKING_MODE = Field.create(CommonConnectorConfig.SNAPSHOT_LOCKING_MODE_PROPERTY_NAME)
            .withDisplayName("Snapshot locking mode").withEnum(SnapshotLockingMode.class, SnapshotLockingMode.SHARED)
            .withWidth(Width.SHORT).withImportance(Importance.LOW)
            .withDescription("Keep shared locks on captured tables until the complete schema and data snapshot finishes.");
    public static final Field CONNECTOR_ADAPTER = Field.create(ConfigurationNames.DATABASE_CONFIG_PREFIX + "connection.adapter")
            .withDisplayName("Connector adapter").withEnum(ConnectorAdapter.class, ConnectorAdapter.LOG_MINER)
            .withWidth(Width.SHORT).withImportance(Importance.HIGH)
            .withDescription("Dameng change capture adapter. Dameng currently supports LogMiner.");
    public static final Field SOURCE_INFO_STRUCT_MAKER = CommonConnectorConfig.SOURCE_INFO_STRUCT_MAKER
            .withDefault(DamengSourceInfoStructMaker.class.getName());
    public static final Field QUERY_FETCH_SIZE = CommonConnectorConfig.QUERY_FETCH_SIZE.withDefault(2_000);
    public static final Field QUERY_TIMEOUT_MS = RelationalDatabaseConnectorConfig.QUERY_TIMEOUT_MS
            .withValidation(Field::isNonNegativeInteger);
    public static final Field SOCKET_TIMEOUT_MS = Field.create("database.socket.timeout.ms")
            .withDisplayName("Socket timeout").withType(Type.INT).withDefault(600_000)
            .withWidth(Width.MEDIUM).withImportance(Importance.HIGH)
            .withValidation(Field::isPositiveInteger)
            .withDescription("Maximum wait in milliseconds for Dameng JDBC network communication. "
                    + "Must be positive and is passed to the driver's socketTimeout property; use this setting instead of database.socketTimeout.");
    public static final Field SNAPSHOT_MAX_DURATION_MS = Field.create("snapshot.max.duration.ms")
            .withDisplayName("Maximum initial snapshot duration").withType(Type.LONG).withDefault(600_000L)
            .withWidth(Width.MEDIUM).withImportance(Importance.HIGH)
            .withValidation(Field::isPositiveLong)
            .withDescription("Maximum total initial snapshot duration in milliseconds, including schema locks, data reads and event queue waits. "
                    + "Must be positive. Exceeding the deadline aborts the snapshot and closes its connection to release locks; "
                    + "increase this value explicitly when a larger initial snapshot is required.");
    public static final Field LOG_MINING_BATCH_SIZE = Field.create("log.mining.batch.size")
            .withDisplayName("Log mining batch size").withType(Type.INT).withDefault(1_000)
            .withWidth(Width.SHORT).withImportance(Importance.LOW)
            .withValidation(Field::isPositiveInteger)
            .withDescription("Maximum LSN span by which each mining iteration advances its upper bound. "
                    + "Re-reading uncommitted transactions can extend the lower bound beyond this span.");
    public static final Field LOG_MINING_BUFFER_MAX_BYTES = Field.create("log.mining.buffer.max.bytes")
            .withDisplayName("Log mining buffer byte limit").withType(Type.LONG).withDefault(LogMinerBatch.DEFAULT_MAX_BUFFER_BYTES)
            .withWidth(Width.MEDIUM).withImportance(Importance.HIGH)
            .withValidation(Field::isPositiveLong)
            .withDescription("Maximum cumulative estimated buffer bytes reserved by one complete mining window. "
                    + "Includes all tables, transactions, discarded rows and SQL continuation assembly. "
                    + "Exceeding the limit fails the connector without publishing a partial window or advancing its replay position. "
                    + "This is not a JVM heap limit; allow additional heap for JDBC, queues and event conversion.");
    public static final Field LOG_MINING_BUFFER_MAX_RECORDS = Field.create("log.mining.buffer.max.records")
            .withDisplayName("Log mining buffer record limit").withType(Type.LONG).withDefault(LogMinerBatch.DEFAULT_MAX_BUFFER_RECORDS)
            .withWidth(Width.MEDIUM).withImportance(Importance.HIGH)
            .withValidation(Field::isPositiveLong)
            .withDescription("Maximum input LogMiner rows in one complete mining window, across all tables and transactions, "
                    + "including transaction markers, discarded rows and SQL continuation pieces. "
                    + "Exceeding the limit fails the connector without publishing a partial window or advancing its replay position.");

    private static final ConfigDefinition CONFIG_DEFINITION = HistorizedRelationalDatabaseConnectorConfig.CONFIG_DEFINITION.edit()
            .excluding(CommonConnectorConfig.QUERY_FETCH_SIZE)
            .excluding(RelationalDatabaseConnectorConfig.QUERY_TIMEOUT_MS)
            .name("Dameng").type(HOSTNAME, PORT, FAILOVER_HOSTS, DATABASE_NAME, SOCKET_TIMEOUT_MS, QUERY_TIMEOUT_MS, RelationalDatabaseConnectorConfig.USER,
                    RelationalDatabaseConnectorConfig.PASSWORD)
            .connector(SNAPSHOT_MODE, SNAPSHOT_LOCKING_MODE, SNAPSHOT_MAX_DURATION_MS, CONNECTOR_ADAPTER, LOG_MINING_BATCH_SIZE, LOG_MINING_BUFFER_MAX_BYTES,
                    LOG_MINING_BUFFER_MAX_RECORDS, QUERY_FETCH_SIZE, UNAVAILABLE_VALUE_PLACEHOLDER)
            .events(SOURCE_INFO_STRUCT_MAKER)
            .create();
    public static final Field.Set ALL_FIELDS = Field.setOf(CONFIG_DEFINITION.all());

    private static final Map<String, String> FAILOVER_DRIVER_PROPERTIES = Map.of(
            "loginMode", "1", "loginStatus", "4", "rwSeparate", "0", "autoReconnect", "0", "switchTimes", "1", "switchInterval", "200");
    private static final Map<String, String> SINGLE_DRIVER_PROPERTIES = Map.of("rwSeparate", "0", "autoReconnect", "0");
    private static final Set<String> UNSAFE_DRIVER_PROPERTIES = Set.of("url", "host", "connection.factory.class", "dmsvcconf", "debezium", "unixsocketfile");

    private final SnapshotMode snapshotMode;
    private final SnapshotLockingMode snapshotLockingMode;
    private final ConnectorAdapter connectorAdapter;
    private final int logMiningBatchSize;
    private final String failoverHosts;

    public DamengConnectorConfig(Configuration config) {
        super(DamengConnector.class, config, new SystemTablesFilter(), table -> table.schema() + "." + table.table(), false, ColumnFilterMode.SCHEMA, false);
        snapshotMode = SnapshotMode.parse(config.getString(SNAPSHOT_MODE), SnapshotMode.INITIAL);
        snapshotLockingMode = SnapshotLockingMode.parse(config.getString(SNAPSHOT_LOCKING_MODE), SnapshotLockingMode.SHARED);
        connectorAdapter = ConnectorAdapter.parse(config.getString(CONNECTOR_ADAPTER));
        if (connectorAdapter != ConnectorAdapter.LOG_MINER) {
            throw new IllegalArgumentException("Dameng currently supports only the logminer adapter");
        }
        if (config.getBoolean(INCREMENTAL_SNAPSHOT_ALLOW_SCHEMA_CHANGES)) {
            throw new IllegalArgumentException(
                    "Dameng incremental.snapshot.allow.schema.changes=true is not implemented; keep schemas stable during incremental snapshots");
        }
        if (getSignalingDataCollectionIds().size() > 1) {
            throw new IllegalArgumentException("Dameng signal.data.collection must identify a single schema-qualified signal table");
        }
        logMiningBatchSize = config.getInteger(LOG_MINING_BATCH_SIZE);
        if (getLogMiningBufferMaxBytes() <= 0 || getLogMiningBufferMaxRecords() <= 0) {
            throw new IllegalArgumentException("Dameng log.mining.buffer.max.bytes and log.mining.buffer.max.records must both be positive");
        }
        if (getSnapshotMaxDurationMs() <= 0) {
            throw new IllegalArgumentException("Dameng snapshot.max.duration.ms must be positive");
        }
        if (getSnapshotMaxThreads() != 1) {
            throw new IllegalArgumentException("Dameng snapshot.max.threads must be 1; parallel snapshot connections are not supported");
        }
        if (getSocketTimeoutMs() <= 0) {
            throw new IllegalArgumentException("Dameng database.socket.timeout.ms must be positive");
        }
        if (config.getInteger(QUERY_TIMEOUT_MS) < 0) {
            throw new IllegalArgumentException("Dameng database.query.timeout.ms must not be negative");
        }
        if (config.getString("database.socketTimeout") != null) {
            throw new IllegalArgumentException("Use database.socket.timeout.ms instead of database.socketTimeout for Dameng");
        }
        failoverHosts = normalizeFailoverHosts(config.getString(FAILOVER_HOSTS));
        final var hostname = config.getString(HOSTNAME);
        if (hostname != null && !hostname.isBlank()) {
            validateHostValue(hostname);
        }
        validateDriverProperties(config);
    }

    public static ConfigDef configDef() {
        return CONFIG_DEFINITION.configDef();
    }

    @Override
    public SnapshotMode getSnapshotMode() {
        return snapshotMode;
    }

    public Optional<SnapshotLockingMode> getSnapshotLockingMode() {
        return Optional.of(snapshotLockingMode);
    }

    public ConnectorAdapter getConnectorAdapter() {
        return connectorAdapter;
    }

    public int getLogMiningBatchSize() {
        return logMiningBatchSize;
    }

    public int getQueryFetchSize() {
        return getConfig().getInteger(QUERY_FETCH_SIZE);
    }

    public long getLogMiningBufferMaxBytes() {
        return getConfig().getLong(LOG_MINING_BUFFER_MAX_BYTES);
    }

    public long getLogMiningBufferMaxRecords() {
        return getConfig().getLong(LOG_MINING_BUFFER_MAX_RECORDS);
    }

    public long getSnapshotMaxDurationMs() {
        return getConfig().getLong(SNAPSHOT_MAX_DURATION_MS);
    }

    public int getSocketTimeoutMs() {
        return getConfig().getInteger(SOCKET_TIMEOUT_MS);
    }

    public String getDatabaseName() {
        return getConfig().getString(DATABASE_NAME);
    }

    public String getFailoverHosts() {
        return failoverHosts;
    }

    public boolean isFailoverEnabled() {
        return !failoverHosts.isEmpty();
    }

    @Override
    public JdbcConfiguration getJdbcConfig() {
        final var builder = JdbcConfiguration.copy(super.getJdbcConfig())
                .without("socket.timeout.ms")
                .withPort(getConfig().getInteger(PORT))
                .with("socketTimeout", getSocketTimeoutMs())
                .with("failover.hosts", failoverHosts)
                .withDatabase(getDatabaseName());
        controlledDriverProperties().forEach(builder::with);
        return builder.build();
    }

    private Map<String, String> controlledDriverProperties() {
        return isFailoverEnabled() ? FAILOVER_DRIVER_PROPERTIES : SINGLE_DRIVER_PROPERTIES;
    }

    private void validateDriverProperties(Configuration rawConfig) {
        final var controlled = controlledDriverProperties();
        for (var property : super.getJdbcConfig().asMap().entrySet()) {
            final var key = property.getKey();
            final var normalized = key.replace("_", "").toLowerCase(Locale.ROOT);
            if (UNSAFE_DRIVER_PROPERTIES.contains(normalized)) {
                throw new IllegalArgumentException("Dameng database." + key + " / driver." + key
                        + " is not supported; configure database.hostname or database.failover.hosts");
            }
            if (normalized.equals("autoreconnect") || normalized.equals("reconnect") || normalized.equals("doswitch")) {
                if (!key.equals("autoReconnect") || !property.getValue().equals("0")) {
                    throw new IllegalArgumentException("Dameng autoReconnect must be 0; driver reconnect aliases are not supported because the connector owns recovery");
                }
            }
            for (var required : controlled.entrySet()) {
                if (normalized.equals(required.getKey().toLowerCase(Locale.ROOT))
                        && (!key.equals(required.getKey()) || !property.getValue().equals(required.getValue()))) {
                    throw new IllegalArgumentException("Dameng " + required.getKey() + " must be " + required.getValue()
                            + "; connection routing and retries are controlled by the connector");
                }
            }
        }
        for (String name : List.of("hostname", "port", "failover.hosts", "socketTimeout", "socket.timeout.ms")) {
            if (rawConfig.getString("driver." + name) != null) {
                throw new IllegalArgumentException("Use database." + (name.equals("socketTimeout") ? "socket.timeout.ms" : name)
                        + " instead of driver." + name + " for Dameng");
            }
        }
    }

    private static int validateDamengHostname(Configuration config, Field field, Field.ValidationOutput problems) {
        final var value = config.getString(field);
        if (value == null || value.isBlank()) {
            if (config.getString(FAILOVER_HOSTS, "").isBlank()) {
                problems.accept(field, value, "A hostname is required unless database.failover.hosts is configured");
                return 1;
            }
            return 0;
        }
        try {
            validateHostValue(value);
            return 0;
        }
        catch (IllegalArgumentException e) {
            problems.accept(field, value, e.getMessage());
            return 1;
        }
    }

    private static int validateFailoverHosts(Configuration config, Field field, Field.ValidationOutput problems) {
        try {
            normalizeFailoverHosts(config.getString(field));
            return 0;
        }
        catch (IllegalArgumentException e) {
            problems.accept(field, config.getString(field), e.getMessage());
            return 1;
        }
    }

    private static String normalizeFailoverHosts(String value) {
        if (value == null || value.isBlank()) {
            return "";
        }
        final List<String> endpoints = new ArrayList<>();
        for (String entry : value.split(",", -1)) {
            final var endpoint = entry.trim();
            final int separator = endpoint.lastIndexOf(':');
            if (separator <= 0 || separator == endpoint.length() - 1) {
                throw new IllegalArgumentException("database.failover.hosts entries must use host:port or [IPv6]:port");
            }
            final var host = endpoint.substring(0, separator);
            validateHostValue(host);
            if (host.contains(":") && !(host.startsWith("[") && host.endsWith("]"))) {
                throw new IllegalArgumentException("database.failover.hosts IPv6 addresses must be enclosed in brackets");
            }
            final int port;
            try {
                port = Integer.parseInt(endpoint.substring(separator + 1));
            }
            catch (NumberFormatException e) {
                throw new IllegalArgumentException("database.failover.hosts requires a numeric port", e);
            }
            if (port < 1 || port > 65535) {
                throw new IllegalArgumentException("database.failover.hosts ports must be between 1 and 65535");
            }
            endpoints.add(host + ':' + port);
        }
        return String.join(",", endpoints);
    }

    private static void validateHostValue(String value) {
        if (value.isBlank() || value.chars().anyMatch(character -> Character.isWhitespace(character)
                || "/\\?#@&=,();${}".indexOf(character) >= 0)) {
            throw new IllegalArgumentException("Dameng hostnames must contain only an address or service hostname, without JDBC URL parameters");
        }
        if ((value.contains("[") || value.contains("]")) && !(value.startsWith("[") && value.endsWith("]") && value.indexOf(']') == value.length() - 1)) {
            throw new IllegalArgumentException("Dameng IPv6 brackets must enclose the whole hostname");
        }
    }

    @Override
    protected SourceInfoStructMaker<? extends AbstractSourceInfo> getSourceInfoStructMaker(Version version) {
        return getSourceInfoStructMaker(SOURCE_INFO_STRUCT_MAKER, Module.name(), Module.version(), this);
    }

    @Override
    public HistoryRecordComparator getHistoryRecordComparator() {
        return new HistoryRecordComparator() {
            @Override
            protected boolean isPositionAtOrBefore(Document recorded, Document desired) {
                final long recordedCommit = recorded.getLong(SourceInfo.COMMIT_LSN_KEY, recorded.getLong(SourceInfo.LSN_KEY, 0L));
                final long desiredCommit = desired.getLong(SourceInfo.COMMIT_LSN_KEY, desired.getLong(SourceInfo.LSN_KEY, 0L));
                if (recordedCommit != desiredCommit) {
                    return recordedCommit < desiredCommit;
                }
                // Snapshot offsets cover all transactions through the snapshot LSN.
                final String recordedTransaction = recorded.getString(DamengOffsetContext.MINING_TRANSACTION_ID_KEY,
                        recorded.getString(SourceInfo.TRANSACTION_ID_KEY));
                final String desiredTransaction = desired.getString(DamengOffsetContext.MINING_TRANSACTION_ID_KEY,
                        desired.getString(SourceInfo.TRANSACTION_ID_KEY));
                if (desiredTransaction == null || recordedTransaction == null) {
                    return true;
                }
                final int transactionComparison = recordedTransaction.compareTo(desiredTransaction);
                if (transactionComparison != 0) {
                    return transactionComparison < 0;
                }
                return recorded.getLong(SourceInfo.EVENT_SERIAL_NO_KEY, 0L) <= desired.getLong(SourceInfo.EVENT_SERIAL_NO_KEY, 0L);
            }
        };
    }

    @Override
    public String getContextName() {
        return Module.contextName();
    }

    @Override
    public String getConnectorName() {
        return Module.name();
    }

    @Override
    public boolean supportsOperationFiltering() {
        return false;
    }

    public enum ConnectorAdapter implements EnumeratedValue {
        LOG_MINER("logminer");

        private final String value;

        ConnectorAdapter(String value) {
            this.value = value;
        }

        @Override
        public String getValue() {
            return value;
        }

        static ConnectorAdapter parse(String value) {
            if (value == null) {
                return LOG_MINER;
            }
            for (ConnectorAdapter adapter : values()) {
                if (adapter.value.equalsIgnoreCase(value)) {
                    return adapter;
                }
            }
            throw new IllegalArgumentException("Unknown Dameng connection adapter: " + value);
        }
    }

    public enum SnapshotLockingMode implements EnumeratedValue {
        SHARED("shared");

        private final String value;

        SnapshotLockingMode(String value) {
            this.value = value;
        }

        @Override
        public String getValue() {
            return value;
        }

        static SnapshotLockingMode parse(String value, SnapshotLockingMode fallback) {
            if (value == null) {
                return fallback;
            }
            for (SnapshotLockingMode mode : values()) {
                if (mode.value.equalsIgnoreCase(value)) {
                    return mode;
                }
            }
            throw new IllegalArgumentException("Unknown Dameng snapshot locking mode: " + value);
        }
    }

    public enum SnapshotMode implements EnumeratedValue {
        INITIAL("initial", true, true, true),
        INITIAL_ONLY("initial_only", true, true, false),
        NO_DATA("no_data", true, false, true),
        NEVER("never", false, false, true);

        private final String value;
        private final boolean schema;
        private final boolean data;
        private final boolean stream;

        SnapshotMode(String value, boolean schema, boolean data, boolean stream) {
            this.value = value;
            this.schema = schema;
            this.data = data;
            this.stream = stream;
        }

        @Override
        public String getValue() {
            return value;
        }

        public boolean includeSchema() {
            return schema;
        }

        public boolean includeData() {
            return data;
        }

        public boolean shouldStream() {
            return stream;
        }

        static SnapshotMode parse(String value, SnapshotMode fallback) {
            if (value == null) {
                return fallback;
            }
            for (SnapshotMode mode : values()) {
                if (mode.value.equalsIgnoreCase(value)) {
                    return mode;
                }
            }
            throw new IllegalArgumentException("Unknown Dameng snapshot mode: " + value);
        }
    }

    private static final class SystemTablesFilter implements TableFilter {
        @Override
        public boolean isIncluded(TableId tableId) {
            String schema = tableId.schema();
            return schema == null || !(schema.equalsIgnoreCase("SYS") || schema.equalsIgnoreCase("SYSTEM") || schema.startsWith("V$"));
        }
    }
}
