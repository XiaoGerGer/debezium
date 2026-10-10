/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng;

import java.util.Optional;

import io.debezium.jdbc.MainConnectionProvidingConnectionFactory;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.source.snapshot.incremental.IncrementalSnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.ChangeEventSourceFactory;
import io.debezium.pipeline.source.spi.DataChangeEventListener;
import io.debezium.pipeline.source.spi.SnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.relational.TableId;
import io.debezium.snapshot.SnapshotterService;
import io.debezium.spi.schema.DataCollectionId;
import io.debezium.util.Clock;

public final class DamengChangeEventSourceFactory implements ChangeEventSourceFactory<DamengPartition, DamengOffsetContext> {
    private final DamengConnectorConfig config;
    private final MainConnectionProvidingConnectionFactory<DamengConnection> connectionFactory;
    private final DamengConnection incrementalSnapshotConnection;
    private final DamengDatabaseSchema schema;
    private final EventDispatcher<DamengPartition, TableId> dispatcher;
    private final ErrorHandler errorHandler;
    private final Clock clock;
    private final SnapshotterService snapshotterService;
    private final NotificationService<DamengPartition, DamengOffsetContext> notificationService;
    private final DamengStreamingChangeEventSourceMetrics streamingMetrics;

    public DamengChangeEventSourceFactory(DamengConnectorConfig config, MainConnectionProvidingConnectionFactory<DamengConnection> connectionFactory,
                                          DamengConnection incrementalSnapshotConnection,
                                          DamengDatabaseSchema schema, EventDispatcher<DamengPartition, TableId> dispatcher, ErrorHandler errorHandler,
                                          Clock clock, SnapshotterService snapshotterService,
                                          NotificationService<DamengPartition, DamengOffsetContext> notificationService,
                                          DamengStreamingChangeEventSourceMetrics streamingMetrics) {
        this.config = config;
        this.connectionFactory = connectionFactory;
        this.incrementalSnapshotConnection = incrementalSnapshotConnection;
        this.schema = schema;
        this.dispatcher = dispatcher;
        this.errorHandler = errorHandler;
        this.clock = clock;
        this.snapshotterService = snapshotterService;
        this.notificationService = notificationService;
        this.streamingMetrics = streamingMetrics;
    }

    @Override
    public SnapshotChangeEventSource<DamengPartition, DamengOffsetContext> getSnapshotChangeEventSource(
                                                                                                        SnapshotProgressListener<DamengPartition> listener,
                                                                                                        NotificationService<DamengPartition, DamengOffsetContext> notifications) {
        return new DamengSnapshotChangeEventSource(config, connectionFactory, schema, dispatcher, clock, listener, notifications, snapshotterService);
    }

    @Override
    public StreamingChangeEventSource<DamengPartition, DamengOffsetContext> getStreamingChangeEventSource() {
        return new DamengStreamingChangeEventSource(config, connectionFactory, dispatcher, errorHandler, clock, schema, streamingMetrics);
    }

    @Override
    public Optional<IncrementalSnapshotChangeEventSource<DamengPartition, ? extends DataCollectionId>> getIncrementalSnapshotChangeEventSource(
                                                                                                                                               DamengOffsetContext offset,
                                                                                                                                               SnapshotProgressListener<DamengPartition> listener,
                                                                                                                                               DataChangeEventListener<DamengPartition> eventListener,
                                                                                                                                               NotificationService<DamengPartition, DamengOffsetContext> notifications) {
        if (config.getSignalingDataCollectionIds().isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(new DamengIncrementalSnapshotChangeEventSource(config, incrementalSnapshotConnection, dispatcher, schema,
                clock, listener, eventListener, notifications));
    }
}
