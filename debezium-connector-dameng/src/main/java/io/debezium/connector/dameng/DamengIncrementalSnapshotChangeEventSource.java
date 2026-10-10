/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng;

import java.sql.SQLException;
import java.util.Optional;

import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.source.snapshot.incremental.SignalBasedIncrementalSnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.DataChangeEventListener;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.relational.Attribute;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;

/** Uses an independent JDBC transaction for chunk reads and watermark signals. */
public final class DamengIncrementalSnapshotChangeEventSource extends SignalBasedIncrementalSnapshotChangeEventSource<DamengPartition, TableId> {
    private final DamengConnection connection;

    public DamengIncrementalSnapshotChangeEventSource(DamengConnectorConfig config, DamengConnection connection,
                                                      EventDispatcher<DamengPartition, TableId> dispatcher, DamengDatabaseSchema schema,
                                                      Clock clock, SnapshotProgressListener<DamengPartition> progressListener,
                                                      DataChangeEventListener<DamengPartition> dataChangeEventListener,
                                                      NotificationService<DamengPartition, DamengOffsetContext> notifications) {
        super(config, connection, dispatcher, schema, clock, progressListener, dataChangeEventListener, notifications);
        this.connection = connection;
    }

    @Override
    protected String getSignalTableName(String dataCollectionId) {
        return connection.quotedTableIdString(TableId.parse(dataCollectionId, false));
    }

    @Override
    protected Optional<String> getDatabaseName(Object dataCollectionId) {
        return Optional.empty();
    }

    @Override
    protected Table readSchemaForTable(TableId tableId) throws SQLException {
        final Table table = super.readSchemaForTable(tableId);
        return table.edit().addAttributes(table.attributes())
                .addAttribute(Attribute.editor().name("dameng.object.id").value(Long.toString(connection.getTableObjectId(tableId))).create())
                .create();
    }
}
