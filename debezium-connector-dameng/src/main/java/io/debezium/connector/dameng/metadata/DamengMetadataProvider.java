/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng.metadata;

import java.util.List;

import io.debezium.connector.dameng.DamengConnector;
import io.debezium.connector.dameng.Module;
import io.debezium.metadata.ComponentMetadata;
import io.debezium.metadata.ComponentMetadataFactory;
import io.debezium.metadata.ComponentMetadataProvider;

public final class DamengMetadataProvider implements ComponentMetadataProvider {
    @Override
    public List<ComponentMetadata> getConnectorMetadata() {
        return List.of(new ComponentMetadataFactory().createComponentMetadata(new DamengConnector(),
                "Debezium Dameng Connector", "Captures row-level changes from a Dameng database.", Module.version()));
    }
}
