/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.bean.StandardBeanNames;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.base.QueueProviderService;
import io.debezium.connector.common.BaseSourceTask;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.connector.common.DebeziumHeaderProducer;
import io.debezium.connector.dameng.compatibility.DamengCompatibilityRegistry;
import io.debezium.document.DocumentReader;
import io.debezium.heartbeat.HeartbeatFactory;
import io.debezium.jdbc.DefaultMainConnectionProvidingConnectionFactory;
import io.debezium.jdbc.MainConnectionProvidingConnectionFactory;
import io.debezium.pipeline.ChangeEventSourceCoordinator;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.signal.SignalProcessor;
import io.debezium.pipeline.spi.Offsets;
import io.debezium.relational.CustomConverterRegistry;
import io.debezium.relational.TableId;
import io.debezium.schema.SchemaFactory;
import io.debezium.schema.SchemaNameAdjuster;
import io.debezium.snapshot.SnapshotterService;
import io.debezium.spi.topic.TopicNamingStrategy;
import io.debezium.util.Clock;

public final class DamengConnectorTask extends BaseSourceTask<DamengPartition, DamengOffsetContext> {
    private static final Logger LOGGER = LoggerFactory.getLogger(DamengConnectorTask.class);
    private static final String CONTEXT_NAME = "dameng-connector-task";
    private DamengConnectorConfig config;
    private DamengTaskContext taskContext;
    private ChangeEventQueue<DataChangeEvent> queue;
    private MainConnectionProvidingConnectionFactory<DamengConnection> connectionFactory;
    private DamengConnection incrementalSnapshotConnection;
    private DamengConnection primaryConnection;
    private DamengDatabaseSchema schema;
    private DamengErrorHandler errorHandler;
    private SignalProcessor<DamengPartition, DamengOffsetContext> startingSignals;
    private EventDispatcher<DamengPartition, TableId> startingDispatcher;
    private NotificationService<DamengPartition, DamengOffsetContext> startingNotifications;

    @Override
    public String version() {
        return Module.version();
    }

    @Override
    public CdcSourceTaskContext<? extends CommonConnectorConfig> preStart(Configuration rawConfig) {
        config = new DamengConnectorConfig(rawConfig);
        taskContext = new DamengTaskContext(rawConfig, config);
        return taskContext;
    }

    @Override
    public ChangeEventSourceCoordinator<DamengPartition, DamengOffsetContext> start(Configuration rawConfig) {
        // Startup can fail before a queue exists. Preserve the same retry budget
        // across those attempts and later streaming failures; the base task resets
        // it only after successfully polling change data.
        errorHandler = new DamengErrorHandler(config, null, errorHandler);
        try {
            return startTask(rawConfig);
        }
        catch (RuntimeException failure) {
            if (coordinator != null) {
                cleanup(coordinator::stop, failure);
                coordinator = null;
                startingSignals = null;
                startingDispatcher = null;
                startingNotifications = null;
            }
            else {
                if (startingNotifications != null) {
                    cleanup(startingNotifications::stop, failure);
                    startingNotifications = null;
                }
                if (startingDispatcher != null) {
                    cleanup(startingDispatcher::close, failure);
                    startingDispatcher = null;
                }
                if (startingSignals != null) {
                    cleanup(startingSignals::stop, failure);
                    startingSignals = null;
                }
                cleanup(config.getServiceRegistry()::close, failure);
            }
            closeTaskResources(failure);
            if (errorHandler.canRetry(failure)) {
                throw new RetriableException("Could not start Dameng connector because its database connection is unavailable; retrying within errors.max.retries",
                        failure);
            }
            throw failure;
        }
    }

    private ChangeEventSourceCoordinator<DamengPartition, DamengOffsetContext> startTask(Configuration rawConfig) {
        final Clock clock = Clock.system();
        final TopicNamingStrategy<TableId> topicNamingStrategy = config.getTopicNamingStrategy(CommonConnectorConfig.TOPIC_NAMING_STRATEGY);
        final SchemaNameAdjuster schemaNameAdjuster = config.schemaNameAdjuster();
        connectionFactory = new DefaultMainConnectionProvidingConnectionFactory<>(() -> new DamengConnection(config.getJdbcConfig()));
        incrementalSnapshotConnection = new DamengConnection(config.getJdbcConfig(), false);
        final DamengStandbySupport standby;
        if (config.isStandbyMiningEnabled()) {
            primaryConnection = new DamengConnection(config.getPrimaryJdbcConfig(), false);
            standby = new DamengStandbySupport(config, primaryConnection);
        }
        else {
            standby = null;
        }
        final DamengValueConverters valueConverters;
        try {
            connectionFactory.mainConnection().validateServerRole();
            if (standby != null) {
                standby.initialize(connectionFactory.mainConnection());
            }
            logDatabaseVersion(connectionFactory.mainConnection());
            final boolean caseInsensitive = "0".equals(connectionFactory.mainConnection().getCaseSensitiveFlag());
            config.configureIdentifierCase(caseInsensitive);
            if (caseInsensitive) {
                LOGGER.info("Dameng CASE_SENSITIVE=0: schema, table, column and signal table identifiers are canonicalized to uppercase; field values are preserved");
            }
            valueConverters = new DamengValueConverters(config, connectionFactory.mainConnection().getDatabaseCharset());
        }
        catch (SQLException e) {
            throw new ConnectException("Could not determine the Dameng database character set", e);
        }
        registerServiceProviders(config.getServiceRegistry());
        schema = new DamengDatabaseSchema(config, valueConverters, schemaNameAdjuster, topicNamingStrategy,
                config.getServiceRegistry().tryGetService(CustomConverterRegistry.class), taskContext);
        final var partition = new DamengPartition(config.getLogicalName());
        final var recovery = new DamengRecovery(config, partition, schema.getSchemaHistory());
        final var captureScopeChange = new DamengCaptureScopeChange(config, partition, schema.getSchemaHistory());
        final Offsets<DamengPartition, DamengOffsetContext> offsets;
        try {
            // Decide an explicit recovery before the legacy loader validates old
            // capture/compatibility signatures. Normal restarts still validate them.
            var offset = recovery.resolve(context.offsetStorageReader().offset(partition.getSourcePartition()), true);
            if (offset != null) {
                offset = captureScopeChange.resolve(offset);
            }
            offsets = Offsets.of(partition, offset);
            schema.setCaptureScopeChange(captureScopeChange, connectionFactory.mainConnection(), standby);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ConnectException("Interrupted while inspecting the Dameng recovery baseline", e);
        }
        LOGGER.info("Resolved Dameng startup offset: {}", offsets.getTheOnlyOffset() == null ? null : offsets.getTheOnlyOffset().getOffset());
        if (offsets.getTheOnlyOffset() != null) {
            try {
                connectionFactory.mainConnection().validateCompatibilityEnvironment(offsets.getTheOnlyOffset());
                if (config.requiresDatabaseLineage()) {
                    connectionFactory.mainConnection().validatePrimaryStandbyIdentity(offsets.getTheOnlyOffset());
                    if (standby != null) {
                        primaryConnection.validateCompatibilityEnvironment(offsets.getTheOnlyOffset());
                    }
                }
                else {
                    offsets.getTheOnlyOffset().validateDatabaseMagic(connectionFactory.mainConnection().databaseMagic());
                }
            }
            catch (SQLException e) {
                throw new ConnectException("Could not validate the database identity of the stored Dameng offset", e);
            }
        }
        config.getBeanRegistry().add(StandardBeanNames.CONFIGURATION, rawConfig);
        config.getBeanRegistry().add(StandardBeanNames.CONNECTOR_CONFIG, config);
        config.getBeanRegistry().add(StandardBeanNames.DATABASE_SCHEMA, schema);
        config.getBeanRegistry().add(StandardBeanNames.JDBC_CONNECTION, connectionFactory.mainConnection());
        config.getBeanRegistry().add(StandardBeanNames.VALUE_CONVERTER, valueConverters);
        config.getBeanRegistry().add(StandardBeanNames.OFFSETS, offsets);
        config.getBeanRegistry().add(StandardBeanNames.CDC_SOURCE_TASK_CONTEXT, taskContext);
        final SnapshotterService snapshots = config.getServiceRegistry().tryGetService(SnapshotterService.class);
        try {
            final var environment = connectionFactory.mainConnection().getCompatibilityEnvironment();
            LOGGER.info("Dameng COMPATIBLE_MODE={} selected compatibility profile {}; observed parameters: {}", environment.mode(),
                    DamengCompatibilityRegistry.builtIn().resolve(environment.mode()).id(), environment.parameters());
            if (snapshots.getSnapshotter().shouldStream()) {
                connectionFactory.mainConnection().validateLogMiningConfiguration();
            }
        }
        catch (SQLException e) {
            throw new ConnectException("Could not validate Dameng database configuration", e);
        }
        validateSchemaHistory(config, connectionFactory.mainConnection()::validateLogPosition, offsets, schema, snapshots.getSnapshotter());
        for (TableId signalTable : config.getSignalingDataCollectionTableIds()) {
            try {
                final boolean created = (standby == null ? incrementalSnapshotConnection : primaryConnection).ensureSignalTable(signalTable);
                if (standby != null) {
                    standby.awaitReplay(connectionFactory.mainConnection(), primaryConnection.currentLsn());
                }
                LOGGER.info("Dameng signal table {} is ready ({})", signalTable, created ? "created" : "existing table validated");
            }
            catch (SQLException e) {
                SQLException failure = e;
                if (standby != null) {
                    try {
                        standby.validate(connectionFactory.mainConnection());
                    }
                    catch (SQLException changed) {
                        failure = changed;
                    }
                }
                throw new ConnectException("Could not prepare Dameng signal table " + signalTable + ": " + e.getMessage(), failure);
            }
        }
        queue = new ChangeEventQueue.Builder<DataChangeEvent>().pollInterval(config.getPollInterval()).maxBatchSize(config.getMaxBatchSize())
                .maxQueueSize(config.getMaxQueueSize()).maxQueueSizeInBytes(config.getMaxQueueSizeInBytes())
                .queueProvider(config.getServiceRegistry().tryGetService(QueueProviderService.class).getQueueProvider())
                .loggingContextSupplier(() -> taskContext.configureLoggingContext(CONTEXT_NAME)).build();
        errorHandler = new DamengErrorHandler(config, queue, errorHandler);
        final DamengEventMetadataProvider metadata = new DamengEventMetadataProvider();
        final SignalProcessor<DamengPartition, DamengOffsetContext> signals = new SignalProcessor<>(DamengConnector.class, config, Map.of(),
                getAvailableSignalChannels(), DocumentReader.defaultReader(), offsets);
        startingSignals = signals;
        final EventDispatcher<DamengPartition, TableId> dispatcher = new EventDispatcher<>(config, topicNamingStrategy, schema, queue,
                config.getTableFilters().dataCollectionFilter(), DataChangeEvent::new, metadata,
                new HeartbeatFactory<>().getScheduledHeartbeat(config, () -> new DamengConnection(config.getPrimaryJdbcConfig()),
                        e -> LOGGER.warn("Heartbeat failed", e), queue),
                schemaNameAdjuster, signals, config.getServiceRegistry().tryGetService(DebeziumHeaderProducer.class));
        startingDispatcher = dispatcher;
        final NotificationService<DamengPartition, DamengOffsetContext> notifications = new NotificationService<>(getNotificationChannels(), config,
                SchemaFactory.get(), dispatcher::enqueueNotification);
        startingNotifications = notifications;
        final var streamingMetrics = new DamengStreamingChangeEventSourceMetrics(taskContext, queue, metadata, () -> schema.getTables().tableIds());
        final DamengChangeEventSourceFactory sourceFactory = new DamengChangeEventSourceFactory(config, connectionFactory, incrementalSnapshotConnection, schema,
                dispatcher,
                errorHandler, clock, snapshots, notifications, streamingMetrics);
        sourceFactory.setRecovery(recovery);
        sourceFactory.setStandbySupport(standby);
        sourceFactory.setCaptureScopeChange(captureScopeChange);
        coordinator = new ChangeEventSourceCoordinator<>(offsets, errorHandler,
                DamengConnector.class, config, sourceFactory, new DamengChangeEventSourceMetricsFactory(streamingMetrics), dispatcher, schema, signals, notifications,
                snapshots);
        coordinator.start(taskContext, queue, metadata);
        startingSignals = null;
        startingDispatcher = null;
        startingNotifications = null;
        return coordinator;
    }

    private void logDatabaseVersion(DamengConnection connection) {
        try {
            final var version = connection.getDatabaseVersion();
            LOGGER.info("Connected to {} database version {} (JDBC major={}, minor={}); matches tested JDBC version: {}",
                    version.productName(), version.productVersion(), version.major(), version.minor(), version.isTested());
            if (!version.isTested()) {
                LOGGER.warn("Dameng database version {} (JDBC major={}, minor={}) has not been tested with this connector. "
                        + "Tested JDBC product versions: {}. Startup will continue; validate JDBC metadata, snapshot and LogMiner behavior before production use.",
                        version.productVersion(), version.major(), version.minor(),
                        DamengDatabaseVersion.TESTED_VERSIONS.stream().map(DamengDatabaseVersion::productVersion).toList());
            }
        }
        catch (SQLException e) {
            LOGGER.warn("Could not read the Dameng database version through JDBC metadata; unable to check whether the version has been tested. "
                    + "Startup will continue.", e);
        }
    }

    @Override
    public List<SourceRecord> doPoll() throws InterruptedException {
        return pollRecords(queue);
    }

    @Override
    protected String connectorName() {
        return Module.name();
    }

    @Override
    protected Optional<ErrorHandler> getErrorHandler() {
        return Optional.ofNullable(errorHandler);
    }

    @Override
    protected void doStop() {
        closeTaskResources(null);
    }

    private void closeTaskResources(Throwable failure) {
        if (primaryConnection != null) {
            final var connection = primaryConnection;
            primaryConnection = null;
            cleanup(connection::close, failure);
        }
        if (incrementalSnapshotConnection != null) {
            final var connection = incrementalSnapshotConnection;
            incrementalSnapshotConnection = null;
            cleanup(connection::close, failure);
        }
        if (connectionFactory != null) {
            final var factory = connectionFactory;
            connectionFactory = null;
            cleanup(() -> factory.mainConnection().close(), failure);
        }
        if (schema != null) {
            final var existingSchema = schema;
            schema = null;
            cleanup(existingSchema::close, failure);
        }
        if (queue != null) {
            final var existingQueue = queue;
            queue = null;
            cleanup(existingQueue::close, failure);
        }
    }

    private static void cleanup(AutoCloseable resource, Throwable failure) {
        try {
            resource.close();
        }
        catch (Exception e) {
            if (failure != null) {
                failure.addSuppressed(e);
            }
            else {
                LOGGER.warn("Could not close Dameng task resource", e);
            }
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
        }
    }

    @Override
    protected Iterable<Field> getAllConfigurationFields() {
        return DamengConnectorConfig.ALL_FIELDS;
    }
}
