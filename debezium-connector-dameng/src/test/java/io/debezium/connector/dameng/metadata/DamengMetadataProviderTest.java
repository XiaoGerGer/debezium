/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng.metadata;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ServiceLoader;

import org.junit.jupiter.api.Test;

import io.debezium.connector.dameng.DamengConnector;
import io.debezium.connector.dameng.DamengConnectorConfig;
import io.debezium.connector.dameng.Module;
import io.debezium.metadata.ComponentMetadataProvider;

class DamengMetadataProviderTest {
    @Test
    void shouldDiscoverTheConnectorMetadataThroughItsServiceRegistration() {
        final var providers = ServiceLoader.load(ComponentMetadataProvider.class).stream()
                .filter(provider -> provider.type().equals(DamengMetadataProvider.class))
                .map(ServiceLoader.Provider::get).toList();

        assertThat(providers).singleElement().satisfies(provider -> {
            assertThat(provider.getConnectorMetadata()).singleElement().satisfies(metadata -> {
                final var descriptor = metadata.getComponentDescriptor();
                assertThat(descriptor.getClassName()).isEqualTo(DamengConnector.class.getName());
                assertThat(descriptor.getType()).isEqualTo("source-connector");
                assertThat(descriptor.getDisplayName()).isEqualTo("Debezium Dameng Connector");
                assertThat(descriptor.getDescription()).contains("Dameng database");
                assertThat(descriptor.getVersion()).isEqualTo(Module.version());
                assertThat(metadata.getComponentFields()).isSameAs(DamengConnectorConfig.ALL_FIELDS);
            });
        });
    }
}
