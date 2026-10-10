/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.jdbc.MainConnectionProvidingConnectionFactory;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.source.spi.DataChangeEventListener;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.relational.TableId;
import io.debezium.snapshot.SnapshotterService;
import io.debezium.util.Clock;

class DamengChangeEventSourceFactoryTest {
    private MainConnectionProvidingConnectionFactory<DamengConnection> connectionFactory;
    private DamengConnection incrementalConnection;
    private EventDispatcher<DamengPartition, TableId> dispatcher;
    private DamengDatabaseSchema schema;
    private SnapshotProgressListener<DamengPartition> progressListener;
    private DataChangeEventListener<DamengPartition> dataListener;
    private NotificationService<DamengPartition, DamengOffsetContext> notifications;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void before() {
        connectionFactory = mock(MainConnectionProvidingConnectionFactory.class);
        when(connectionFactory.mainConnection()).thenReturn(mock(DamengConnection.class));
        incrementalConnection = mock(DamengConnection.class);
        dispatcher = mock(EventDispatcher.class);
        schema = mock(DamengDatabaseSchema.class);
        progressListener = mock(SnapshotProgressListener.class);
        dataListener = mock(DataChangeEventListener.class);
        notifications = mock(NotificationService.class);
    }

    @Test
    void shouldCreateSnapshotAndLogMinerStreamingSources() throws Exception {
        final var config = new DamengConnectorConfig(DamengConnectorConfigTest.configuration().build());
        final var factory = factory(config);

        assertThat(factory.getSnapshotChangeEventSource(progressListener, notifications)).isInstanceOf(DamengSnapshotChangeEventSource.class);
        final var streaming = factory.getStreamingChangeEventSource();
        assertThat(streaming).isInstanceOf(DamengStreamingChangeEventSource.class);
        final var offset = new DamengOffsetContext(config, 123);
        streaming.init(offset);
        assertThat(streaming.getOffsetContext()).isSameAs(offset);
    }

    @Test
    void shouldDisableIncrementalSnapshotsWithoutASourceSignalTable() {
        final var config = new DamengConnectorConfig(DamengConnectorConfigTest.configuration().build());

        assertThat(factory(config).getIncrementalSnapshotChangeEventSource(new DamengOffsetContext(config, 123),
                progressListener, dataListener, notifications)).isEmpty();
        verify(connectionFactory, never()).mainConnection();
        verify(incrementalConnection, never()).chunkQueryBuilder(config);
    }

    @Test
    void shouldUseTheIndependentConnectionForSchemaQualifiedSourceSignals() {
        final var config = new DamengConnectorConfig(DamengConnectorConfigTest.configuration().with("signal.data.collection", "APP.SIGNALS").build());
        final var table = new TableId(null, "APP", "SIGNALS");
        when(incrementalConnection.quotedTableIdString(table)).thenReturn("\"APP\".\"SIGNALS\"");

        final var source = factory(config).getIncrementalSnapshotChangeEventSource(new DamengOffsetContext(config, 123),
                progressListener, dataListener, notifications).orElseThrow();

        assertThat(source).isInstanceOf(DamengIncrementalSnapshotChangeEventSource.class);
        final var incremental = (DamengIncrementalSnapshotChangeEventSource) source;
        assertThat(incremental.getSignalTableName("APP.SIGNALS")).isEqualTo("\"APP\".\"SIGNALS\"");
        assertThat(incremental.getDatabaseName("APP.SIGNALS")).isEmpty();
        verify(incrementalConnection).chunkQueryBuilder(config);
        verify(connectionFactory, never()).mainConnection();
    }

    private DamengChangeEventSourceFactory factory(DamengConnectorConfig config) {
        return new DamengChangeEventSourceFactory(config, connectionFactory, incrementalConnection, schema, dispatcher,
                mock(ErrorHandler.class), Clock.SYSTEM, mock(SnapshotterService.class), notifications,
                mock(DamengStreamingChangeEventSourceMetrics.class));
    }
}
