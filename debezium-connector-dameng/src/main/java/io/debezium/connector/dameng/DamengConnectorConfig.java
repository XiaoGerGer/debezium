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
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.ConfigDefinition;
import io.debezium.config.Configuration;
import io.debezium.config.ConfigurationNames;
import io.debezium.config.EnumeratedValue;
import io.debezium.config.Field;
import io.debezium.connector.AbstractSourceInfo;
import io.debezium.connector.SourceInfoStructMaker;
import io.debezium.document.Document;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.relational.ColumnFilterMode;
import io.debezium.relational.HistorizedRelationalDatabaseConnectorConfig;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables.TableFilter;
import io.debezium.relational.history.HistoryRecordComparator;
import io.debezium.spi.schema.DataCollectionId;

public final class DamengConnectorConfig extends HistorizedRelationalDatabaseConnectorConfig {
    private static final Logger LOGGER = LoggerFactory.getLogger(DamengConnectorConfig.class);
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
    public static final Field TIME_PRECISION_MODE = RelationalDatabaseConnectorConfig.TIME_PRECISION_MODE
            .withDefault(TemporalPrecisionMode.SQLSTRING.getValue())
            .withDescription("Dameng defaults to SQL-style strings without T or Z for DATE, TIME and ordinary TIMESTAMP values. "
                    + "Explicit isostring, adaptive, adaptive_time_microseconds, connect, microseconds and nanoseconds modes remain available. "
                    + "Changing this setting changes Kafka Connect field schemas and requires downstream consumers to adapt.");
    public static final Field STANDBY_HOSTS = Field.create("database.standby.hosts")
            .withDisplayName("Standby capture hosts").withType(Type.STRING).withDefault("")
            .withWidth(Width.LONG).withImportance(Importance.HIGH)
            .withValidation(DamengConnectorConfig::validateFailoverHosts)
            .withDescription("Optional comma-separated host:port endpoints for OPEN STANDBY capture. When set, mining and snapshot reads use a standby; "
                    + "database.failover.hosts or database.hostname/port select the primary for signal writes and snapshot locks. "
                    + "All endpoints must belong to the same physical database family and use the same credentials. No fallback to primary capture.");
    public static final Field STANDBY_REPLAY_TIMEOUT_MS = Field.create("database.standby.replay.timeout.ms")
            .withDisplayName("Standby replay wait timeout").withType(Type.LONG).withDefault(60_000L)
            .withWidth(Width.MEDIUM).withImportance(Importance.MEDIUM).withValidation(Field::isPositiveLong)
            .withDescription("Maximum milliseconds to wait for a standby to replay a primary snapshot or signal boundary. "
                    + "A timeout restarts the task without acknowledging the incomplete snapshot.");
    public static final Field SNAPSHOT_MODE = Field.create(CommonConnectorConfig.SNAPSHOT_MODE_PROPERTY_NAME)
            .withDisplayName("Snapshot mode").withEnum(SnapshotMode.class, SnapshotMode.NO_DATA)
            .withWidth(Width.SHORT).withImportance(Importance.LOW)
            .withDescription("Snapshot mode: initial, initial_only, no_data, or the legacy no_data alias never. "
                    + "Defaults to no_data: capture schemas and stream changes without scanning existing rows.");
    public static final Field RECOVERY_RESTART_NONCE = Field.create("recovery.restart.nonce")
            .withDisplayName("Explicit recovery request").withType(Type.STRING).withDefault("")
            .withWidth(Width.LONG).withImportance(Importance.HIGH)
            .withValidation(DamengConnectorConfig::validateRecoveryNonce)
            .withDescription("A new non-empty value explicitly abandons the previous replay position and creates a current schema/LSN baseline. "
                    + "May lose changes, including deletes. Reusing the same value resumes that baseline; never reuse an older value. "
                    + "Blank preserves the current generation. Requires retained, exclusively owned schema history.");
    public static final Field RECOVERY_INCREMENTAL_SNAPSHOT_AUTO_START = Field.create("recovery.incremental.snapshot.auto.start")
            .withDisplayName("Automatically backfill a new recovery baseline").withType(Type.BOOLEAN).withDefault(false)
            .withWidth(Width.SHORT).withImportance(Importance.MEDIUM)
            .withDescription("Queue an incremental snapshot of captured business tables only when creating a new recovery baseline. "
                    + "Default false leaves backfill to a later execute-snapshot signal. Requires a signal table and primary keys on all selected tables. "
                    + "Changing this flag alone does not restart CDC or schedule another snapshot.");
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
            .withDescription("Maximum forward LSN span in fixed window mode; unused in archive_bytes mode. "
                    + "Unfinished transactions remain in the memory/disk transaction cache across windows.");
    public static final Field LOG_MINING_WINDOW_STRATEGY = Field.create("log.mining.window.strategy")
            .withDisplayName("Mining window strategy").withEnum(MiningWindowStrategy.class, MiningWindowStrategy.ARCHIVE_BYTES)
            .withWidth(Width.MEDIUM).withImportance(Importance.MEDIUM)
            .withValidation(DamengConnectorConfig::validateRemovedMiningWindowSettings)
            .withDescription("Select forward windows using archive_bytes (archive file byte budget, the default) or fixed (LSN span).");
    public static final Field LOG_MINING_WINDOW_MAX_BYTES = Field.create("log.mining.window.max.bytes")
            .withDisplayName("Forward archive file byte budget").withType(Type.LONG).withDefault(64L * 1024 * 1024)
            .withWidth(Width.MEDIUM).withImportance(Importance.MEDIUM).withValidation(Field::isPositiveLong)
            .withDescription("Soft budget for the total LEN of archive files selected for one forward scan in archive_bytes mode. "
                    + "Defaults to 64 MiB. Whole files and shared boundary files can exceed it; reconstruction is never truncated. "
                    + "This is neither a JVM memory limit nor the number of bytes actually read.");
    public static final Field LOG_MINING_BUFFER_MAX_BYTES = Field.create("log.mining.buffer.max.bytes")
            .withDisplayName("Deprecated cumulative byte budget").withType(Type.STRING)
            .withWidth(Width.MEDIUM).withImportance(Importance.LOW)
            .withDescription("Deprecated and ignored. Cumulative mining input no longer stops a task. "
                    + "Use log.mining.buffer.memory.bytes for resident transaction caching and log.mining.buffer.max.sql.bytes for a single SQL payload.");
    public static final Field LOG_MINING_BUFFER_MAX_RECORDS = Field.create("log.mining.buffer.max.records")
            .withDisplayName("Deprecated cumulative row budget").withType(Type.STRING)
            .withWidth(Width.MEDIUM).withImportance(Importance.LOW)
            .withDescription("Deprecated and ignored. Cumulative mining input no longer stops a task. "
                    + "Use log.mining.buffer.max.transactions to limit resident transaction metadata entries.");
    public static final Field LOG_MINING_BUFFER_MAX_SQL_BYTES = Field.create("log.mining.buffer.max.sql.bytes")
            .withDisplayName("Single SQL payload limit").withType(Type.LONG).withDefault(16L * 1024 * 1024)
            .withWidth(Width.MEDIUM).withImportance(Importance.HIGH).withValidation(Field::isPositiveLong)
            .withDescription("Maximum UTF-16 payload bytes of one SQL_REDO, including all continuation pieces. "
                    + "Checked before concatenation; JDBC input, builder capacity and temporary copies require additional heap. "
                    + "SQL payloads cannot spill while being assembled. Exceeding the limit stops the task without skipping history.");
    public static final Field LOG_MINING_BUFFER_MAX_TRANSACTIONS = Field.create("log.mining.buffer.max.transactions")
            .withDisplayName("Resident transaction count limit").withType(Type.LONG).withDefault(1_000_000L)
            .withWidth(Width.MEDIUM).withImportance(Importance.HIGH).withValidation(Field::isPositiveLong)
            .withDescription("Maximum simultaneously retained transaction metadata entries, including committed transactions awaiting dispatch. "
                    + "Metadata cannot spill and also counts against log.mining.buffer.memory.bytes. Exceeding either limit stops the task.");
    public static final Field LOG_MINING_BUFFER_MEMORY_BYTES = Field.create("log.mining.buffer.memory.bytes")
            .withDisplayName("Transaction cache memory threshold").withType(Type.LONG).withDefault(64L * 1024 * 1024)
            .withWidth(Width.MEDIUM).withImportance(Importance.HIGH).withValidation(Field::isPositiveLong)
            .withDescription("Task-wide estimated transaction cache memory threshold. Spill transactions to local temporary files when exceeded. "
                    + "Includes retained transaction metadata; excludes JDBC, SQL continuation assembly and event queues. This is not a JVM heap limit.");
    public static final Field LOG_MINING_BUFFER_SPILL_PATH = Field.create("log.mining.buffer.spill.path")
            .withDisplayName("Transaction spill directory").withType(Type.STRING).withDefault(System.getProperty("java.io.tmpdir"))
            .withWidth(Width.LONG).withImportance(Importance.HIGH).withValidation(Field::isRequired)
            .withDescription("Writable parent directory for task-owned temporary transaction files. Use a disk-backed Docker mount. "
                    + "Files are disposable, are not a recovery source, and do not replace archive retention. Each run creates its own directory.");
    public static final Field LOG_MINING_BUFFER_SPILL_MAX_BYTES = Field.create("log.mining.buffer.spill.max.bytes")
            .withDisplayName("Transaction spill disk limit").withType(Type.LONG).withDefault(1024L * 1024 * 1024)
            .withWidth(Width.MEDIUM).withImportance(Importance.HIGH).withValidation(Field::isPositiveLong)
            .withDescription("Maximum total temporary transaction file bytes for one task run. Exceeding the limit or any local I/O failure "
                    + "stops the task without skipping history. Allow additional filesystem free space and retain archives for restart.");

    private static final ConfigDefinition CONFIG_DEFINITION = HistorizedRelationalDatabaseConnectorConfig.CONFIG_DEFINITION.edit()
            .excluding(CommonConnectorConfig.QUERY_FETCH_SIZE)
            .excluding(RelationalDatabaseConnectorConfig.QUERY_TIMEOUT_MS)
            .excluding(RelationalDatabaseConnectorConfig.TIME_PRECISION_MODE)
            .name("Dameng")
            .type(HOSTNAME, PORT, FAILOVER_HOSTS, STANDBY_HOSTS, STANDBY_REPLAY_TIMEOUT_MS, DATABASE_NAME, SOCKET_TIMEOUT_MS, QUERY_TIMEOUT_MS,
                    RelationalDatabaseConnectorConfig.USER,
                    RelationalDatabaseConnectorConfig.PASSWORD)
            .connector(SNAPSHOT_MODE, SNAPSHOT_LOCKING_MODE, SNAPSHOT_MAX_DURATION_MS, RECOVERY_RESTART_NONCE,
                    RECOVERY_INCREMENTAL_SNAPSHOT_AUTO_START, CONNECTOR_ADAPTER, LOG_MINING_BATCH_SIZE,
                    LOG_MINING_WINDOW_STRATEGY, LOG_MINING_WINDOW_MAX_BYTES,
                    LOG_MINING_BUFFER_MAX_BYTES,
                    LOG_MINING_BUFFER_MAX_RECORDS, LOG_MINING_BUFFER_MAX_SQL_BYTES, LOG_MINING_BUFFER_MAX_TRANSACTIONS, LOG_MINING_BUFFER_MEMORY_BYTES,
                    LOG_MINING_BUFFER_SPILL_PATH, LOG_MINING_BUFFER_SPILL_MAX_BYTES,
                    QUERY_FETCH_SIZE, TIME_PRECISION_MODE, UNAVAILABLE_VALUE_PLACEHOLDER)
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
    private final String standbyHosts;
    private boolean caseInsensitiveIdentifiers;
    private List<TableId> signalTableIds;

    boolean hasCaseInsensitiveIdentifiers() {
        return caseInsensitiveIdentifiers;
    }

    /** Set from the database before history, signals or snapshot services are started. */
    void configureIdentifierCase(boolean caseInsensitive) {
        caseInsensitiveIdentifiers = caseInsensitive;
        signalTableIds = null;
    }

    @Override
    public List<String> getSignalingDataCollectionIds() {
        return caseInsensitiveIdentifiers ? super.getSignalingDataCollectionIds().stream().map(name -> name.toUpperCase(Locale.ROOT)).toList()
                : super.getSignalingDataCollectionIds();
    }

    @Override
    public List<TableId> getSignalingDataCollectionTableIds() {
        // DM uses schema.table, while the common default parses two parts as catalog.table.
        if (signalTableIds == null) {
            signalTableIds = getSignalingDataCollectionIds().stream().map(name -> TableId.parse(name, false)).toList();
        }
        return signalTableIds;
    }

    @Override
    public boolean isSignalDataCollection(DataCollectionId id) {
        // This is checked for every emitted event; avoid allocating parsed IDs on the hot path.
        return getSignalingDataCollectionTableIds().contains(id);
    }

    public DamengConnectorConfig(Configuration config) {
        super(DamengConnector.class, withDefaultTimePrecision(config), new SystemTablesFilter(), table -> table.schema() + "." + table.table(), false,
                ColumnFilterMode.SCHEMA, false);
        snapshotMode = SnapshotMode.parse(config.getString(SNAPSHOT_MODE), SnapshotMode.NO_DATA);
        if (validateRecoveryNonce(config, RECOVERY_RESTART_NONCE, (field, value, problem) -> {
        }) != 0) {
            throw new IllegalArgumentException("Dameng recovery.restart.nonce must be blank or at most 128 printable ASCII characters without surrounding whitespace");
        }
        if (!getRecoveryRestartNonce().isEmpty() && snapshotMode == SnapshotMode.INITIAL_ONLY) {
            throw new IllegalArgumentException("Dameng recovery.restart.nonce requires a streaming snapshot mode; initial_only is not supported");
        }
        if (isRecoveryIncrementalSnapshotAutoStart() && getSignalingDataCollectionIds().isEmpty()) {
            throw new IllegalArgumentException("Dameng recovery.incremental.snapshot.auto.start requires signal.data.collection");
        }
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
        if (getLogMiningWindowMaxBytes() <= 0) {
            throw new IllegalArgumentException("Dameng mining window byte budget must be positive");
        }
        getMiningWindowStrategy();
        validateRemovedMiningWindowSettings(config, LOG_MINING_WINDOW_STRATEGY, (field, value, problem) -> {
            throw new IllegalArgumentException(problem);
        });
        if (logMiningBatchSize <= 0) {
            throw new IllegalArgumentException("Dameng log.mining.batch.size must be positive");
        }
        for (var legacy : List.of(LOG_MINING_BUFFER_MAX_BYTES, LOG_MINING_BUFFER_MAX_RECORDS)) {
            if (config.hasKey(legacy.name())) {
                LOGGER.warn("Dameng {} is deprecated and ignored; cumulative mining work no longer stops the task. Remove this setting; "
                        + "resident cache, transaction metadata, single SQL and spill disk limits remain active.", legacy.name());
            }
        }
        if (getLogMiningBufferMaxSqlBytes() <= 0 || getLogMiningBufferMaxTransactions() <= 0) {
            throw new IllegalArgumentException("Dameng log.mining.buffer.max.sql.bytes and log.mining.buffer.max.transactions must both be positive");
        }
        if (getLogMiningBufferMemoryBytes() <= 0 || getLogMiningBufferSpillMaxBytes() <= 0 || getLogMiningBufferSpillPath().isBlank()) {
            throw new IllegalArgumentException("Dameng transaction memory/disk limits must be positive and spill path must not be blank");
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
        standbyHosts = normalizeFailoverHosts(config.getString(STANDBY_HOSTS));
        if (getStandbyReplayTimeoutMs() <= 0) {
            throw new IllegalArgumentException("Dameng database.standby.replay.timeout.ms must be positive");
        }
        final var hostname = config.getString(HOSTNAME);
        if (hostname != null && !hostname.isBlank()) {
            validateHostValue(hostname);
        }
        validateDriverProperties(config);
    }

    private static Configuration withDefaultTimePrecision(Configuration config) {
        return config.hasKey(TIME_PRECISION_MODE) ? config
                : config.edit().with(TIME_PRECISION_MODE.name(), TemporalPrecisionMode.SQLSTRING.getValue()).build();
    }

    public static ConfigDef configDef() {
        return CONFIG_DEFINITION.configDef();
    }

    public String getRecoveryRestartNonce() {
        return getConfig().getString(RECOVERY_RESTART_NONCE).trim();
    }

    public boolean isRecoveryIncrementalSnapshotAutoStart() {
        return getConfig().getBoolean(RECOVERY_INCREMENTAL_SNAPSHOT_AUTO_START);
    }

    private static int validateRecoveryNonce(Configuration config, Field field, Field.ValidationOutput problems) {
        final var nonce = config.getString(field);
        if (nonce != null && !nonce.isBlank() && (nonce.length() > 128 || !nonce.equals(nonce.trim())
                || nonce.chars().anyMatch(character -> character < 33 || character > 126))) {
            problems.accept(field, nonce, "Must be blank or at most 128 printable ASCII characters without whitespace");
            return 1;
        }
        return 0;
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

    public MiningWindowStrategy getMiningWindowStrategy() {
        return MiningWindowStrategy.parse(getConfig().getString(LOG_MINING_WINDOW_STRATEGY));
    }

    public long getLogMiningWindowMaxBytes() {
        return getConfig().getLong(LOG_MINING_WINDOW_MAX_BYTES);
    }

    private static int validateRemovedMiningWindowSettings(Configuration config, Field field, Field.ValidationOutput problems) {
        int errors = 0;
        for (var removed : List.of("log.mining.window.adaptive.enabled", "log.mining.batch.size.min", "log.mining.batch.size.max", "log.mining.window.target.ms")) {
            if (config.hasKey(removed)) {
                problems.accept(field, config.getString(field), "Dameng " + removed + " has been removed. Remove this setting and use "
                        + "log.mining.window.strategy=archive_bytes (default) or fixed; configure fixed spans with log.mining.batch.size.");
                errors++;
            }
        }
        return errors;
    }

    public int getQueryFetchSize() {
        return getConfig().getInteger(QUERY_FETCH_SIZE);
    }

    public long getLogMiningBufferMaxSqlBytes() {
        return getConfig().getLong(LOG_MINING_BUFFER_MAX_SQL_BYTES);
    }

    public long getLogMiningBufferMaxTransactions() {
        return getConfig().getLong(LOG_MINING_BUFFER_MAX_TRANSACTIONS);
    }

    public long getLogMiningBufferMemoryBytes() {
        return getConfig().getLong(LOG_MINING_BUFFER_MEMORY_BYTES);
    }

    public String getLogMiningBufferSpillPath() {
        return getConfig().getString(LOG_MINING_BUFFER_SPILL_PATH);
    }

    public long getLogMiningBufferSpillMaxBytes() {
        return getConfig().getLong(LOG_MINING_BUFFER_SPILL_MAX_BYTES);
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

    public boolean isStandbyMiningEnabled() {
        return !standbyHosts.isEmpty();
    }

    public boolean requiresDatabaseLineage() {
        return isFailoverEnabled() || isStandbyMiningEnabled();
    }

    public long getStandbyReplayTimeoutMs() {
        return getConfig().getLong(STANDBY_REPLAY_TIMEOUT_MS);
    }

    @Override
    public JdbcConfiguration getJdbcConfig() {
        final var builder = JdbcConfiguration.copy(getPrimaryJdbcConfig());
        if (isStandbyMiningEnabled()) {
            builder.with("failover.hosts", standbyHosts).with("standby.hosts", standbyHosts).with("loginMode", "2");
        }
        return builder.build();
    }

    public JdbcConfiguration getPrimaryJdbcConfig() {
        final var builder = JdbcConfiguration.copy(super.getJdbcConfig())
                .without("socket.timeout.ms")
                .without("standby.hosts").without("standby.replay.timeout.ms")
                .withPort(getConfig().getInteger(PORT))
                .with("socketTimeout", getSocketTimeoutMs())
                .with("failover.hosts", failoverHosts)
                .withDatabase(getDatabaseName());
        (requiresDatabaseLineage() ? FAILOVER_DRIVER_PROPERTIES : SINGLE_DRIVER_PROPERTIES).forEach(builder::with);
        return builder.build();
    }

    private Map<String, String> controlledDriverProperties() {
        return requiresDatabaseLineage() ? FAILOVER_DRIVER_PROPERTIES : SINGLE_DRIVER_PROPERTIES;
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
                if (!Objects.equals(recorded.getString(DamengOffsetContext.RECOVERY_NONCE_KEY), desired.getString(DamengOffsetContext.RECOVERY_NONCE_KEY))) {
                    return false;
                }
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

    public enum MiningWindowStrategy implements EnumeratedValue {
        ARCHIVE_BYTES("archive_bytes"),
        FIXED("fixed");

        private final String value;

        MiningWindowStrategy(String value) {
            this.value = value;
        }

        @Override
        public String getValue() {
            return value;
        }

        static MiningWindowStrategy parse(String value) {
            for (var strategy : values()) {
                if (strategy.value.equalsIgnoreCase(value)) {
                    return strategy;
                }
            }
            throw new IllegalArgumentException("Unknown Dameng mining window strategy: " + value + ". Use archive_bytes or fixed.");
        }
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
