/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng;

import java.sql.SQLException;
import java.util.Collection;

import org.apache.kafka.connect.errors.ConnectException;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.pipeline.spi.Offsets;
import io.debezium.relational.CustomConverterRegistry;
import io.debezium.relational.HistorizedRelationalDatabaseSchema;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.TableSchemaBuilder;
import io.debezium.relational.Tables;
import io.debezium.relational.ddl.DdlParser;
import io.debezium.relational.history.TableChanges;
import io.debezium.schema.SchemaChangeEvent;
import io.debezium.schema.SchemaChangeEvent.SchemaChangeEventType;
import io.debezium.schema.SchemaNameAdjuster;
import io.debezium.spi.topic.TopicNamingStrategy;

public final class DamengDatabaseSchema extends HistorizedRelationalDatabaseSchema {
    private final DamengValueConverters valueConverters;
    private long tableVersion;
    private DamengCaptureScopeChange captureScopeChange;
    private DamengCaptureScopePreparation captureScopePreparation;
    private DamengConnection captureScopeConnection;
    private final DamengConnectorConfig config;

    /** Invalidates streaming's object index after snapshot, recovery or DDL. */
    long tableVersion() {
        return tableVersion;
    }

    public DamengDatabaseSchema(DamengConnectorConfig config, DamengValueConverters valueConverters,
                                SchemaNameAdjuster schemaNameAdjuster, TopicNamingStrategy<TableId> topicNamingStrategy,
                                CustomConverterRegistry customConverterRegistry,
                                CdcSourceTaskContext<? extends CommonConnectorConfig> taskContext) {
        super(config, topicNamingStrategy, config.getTableFilters().dataCollectionFilter(), config.getColumnFilter(),
                new TableSchemaBuilder(valueConverters, new DamengDefaultValueConverter(valueConverters), schemaNameAdjuster,
                        customConverterRegistry, config.getSourceInfoStructMaker().schema(), config.getFieldNamer(),
                        false, config.getEventConvertingFailureHandlingMode()),
                false, config.getKeyMapper(), taskContext);
        this.valueConverters = valueConverters;
        this.config = config;
    }

    void setCaptureScopeChange(DamengCaptureScopeChange change, DamengConnection connection, DamengStandbySupport standby) {
        captureScopeChange = change;
        captureScopeConnection = connection;
        captureScopePreparation = new DamengCaptureScopePreparation(config, connection, standby);
    }

    public Tables getTables() {
        return tables();
    }

    public DamengValueConverters getValueConverters() {
        return valueConverters;
    }

    @Override
    public void refresh(Table table) {
        tableVersion++;
        super.refresh(table);
    }

    void registerRecoveryTables(Collection<Table> baselineTables) {
        tableVersion++;
        for (Table table : baselineTables) {
            tables().overwriteTable(table);
            buildAndRegisterSchema(table);
        }
        validateObjectIdentities();
    }

    @Override
    public void recover(Offsets<?, ?> offsets) throws InterruptedException {
        tableVersion++;
        super.recover(offsets);
        validateObjectIdentities();
        if (captureScopeChange != null) {
            final var offset = (DamengOffsetContext) offsets.getTheOnlyOffset();
            try {
                if (offset != null && !java.util.Objects.equals(offset.getCaptureScopeManifest(), DamengCaptureScope.manifest(config))) {
                    captureScopeChange.prepare(offset, captureScopeConnection.getAllTableIds(null), captureScopePreparation, this);
                }
                captureScopeChange.recovered(this, offset);
            }
            catch (SQLException e) {
                throw new ConnectException("Could not prepare the Dameng capture scope change", e);
            }
        }
    }

    /** Ensures recovered tables can be bound to dictionary-free LogMiner records. */
    public void validateObjectIdentities() {
        for (TableId id : tables().tableIds()) {
            final var objectId = tables().forTable(id).attributeWithName("dameng.object.id");
            try {
                if (objectId != null && Long.parseLong(objectId.value()) > 0) {
                    continue;
                }
            }
            catch (NumberFormatException e) {
                throw new ConnectException("Invalid Dameng object identity in schema history for " + id + "; a new snapshot is required", e);
            }
            throw new ConnectException("Missing Dameng object identity in schema history for " + id + "; a new snapshot is required");
        }
    }

    @Override
    public void applySchemaChange(SchemaChangeEvent event) {
        tableVersion++;
        if (event.getType() == SchemaChangeEventType.TRUNCATE) {
            final TableChanges changes = new TableChanges();
            for (var table : event.getTables()) {
                tables().overwriteTable(table);
                buildAndRegisterSchema(table);
                changes.alter(table);
            }
            record(event, changes);
            return;
        }
        for (var change : event.getTableChanges()) {
            switch (change.getType()) {
                case DROP:
                    removeSchema(change.getId());
                    tables().removeTable(change.getId());
                    break;
                case CREATE:
                case ALTER:
                    if (change.getPreviousId() != null) {
                        removeSchema(change.getPreviousId());
                        tables().removeTable(change.getPreviousId());
                    }
                    // An ALTER may rename a table; register only its resulting identifier.
                    tables().overwriteTable(change.getTable());
                    buildAndRegisterSchema(change.getTable());
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported table change " + change.getType());
            }
        }
        record(event, event.getTableChanges());
    }

    @Override
    protected DdlParser getDdlParser() {
        return null;
    }
}
