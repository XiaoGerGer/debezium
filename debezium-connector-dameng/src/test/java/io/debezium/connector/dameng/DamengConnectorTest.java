/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.source.ExactlyOnceSupport;
import org.junit.jupiter.api.Test;

class DamengConnectorTest {
    @Test
    void shouldProvideOneTaskAndProtectItsConfiguration() {
        final var connector = new DamengConnector();
        final Map<String, String> configuration = new HashMap<>(DamengConnectorConfigTest.configuration().build().asMap());
        connector.start(configuration);
        configuration.put("database.hostname", "changed");

        assertThat(connector.taskConfigs(1)).hasSize(1);
        assertThat(connector.taskConfigs(1).get(0).get("database.hostname")).isEqualTo("localhost");
        assertThat(connector.taskClass()).isEqualTo(DamengConnectorTask.class);
        assertThatThrownBy(() -> connector.taskConfigs(2)).isInstanceOf(IllegalArgumentException.class);
        connector.stop();
    }

    @Test
    void shouldPublishAllConfigurationFields() {
        final var connector = new DamengConnector();
        final var definition = connector.config();

        DamengConnectorConfig.ALL_FIELDS.forEach(field -> {
            assertThat(definition.names()).contains(field.name());
            final var metadata = definition.configKeys().get(field.name());
            assertThat(metadata.documentation).isEqualTo(field.description());
            assertThat(metadata.importance).isEqualTo(field.importance());
            assertThat(metadata.type).isEqualTo(field.type());
        });
    }

    @Test
    void shouldReportItsResolvedVersion() {
        assertThat(new DamengConnector().version()).isNotBlank().isEqualTo(Module.version()).doesNotContain("$" + "{");
        assertThat(Module.name()).isEqualTo("dameng");
    }

    @Test
    void shouldNotAdvertiseExactlyOnceDelivery() {
        assertThat(new DamengConnector().exactlyOnceSupport(Map.of())).isEqualTo(ExactlyOnceSupport.UNSUPPORTED);
    }

    @Test
    void shouldValidateAndCloseTheDatabaseConnection() throws Exception {
        final var config = DamengConnectorConfigTest.configuration().build();
        try (var connections = mockConstruction(DamengConnection.class,
                (connection, context) -> when(connection.connection()).thenReturn(mock(Connection.class)))) {
            final var result = new DamengConnector().validate(config.asMap());

            assertThat(result.configValues()).allSatisfy(value -> assertThat(value.errorMessages()).isEmpty());
            assertThat(connections.constructed()).hasSize(1);
            verify(connections.constructed().get(0)).connection();
            verify(connections.constructed().get(0)).close();
        }
    }

    @Test
    void shouldReportDatabaseValidationFailureOnHostnameAndCloseTheConnection() throws Exception {
        final var config = DamengConnectorConfigTest.configuration().build();
        try (var connections = mockConstruction(DamengConnection.class,
                (connection, context) -> when(connection.connection()).thenThrow(new SQLException("primary unavailable", "08001")))) {
            final var result = new DamengConnector().validate(config.asMap());

            assertThat(result.configValues()).filteredOn(value -> value.name().equals(DamengConnectorConfig.HOSTNAME.name()))
                    .singleElement().satisfies(value -> assertThat(value.errorMessages()).containsExactly("Unable to connect to Dameng: primary unavailable"));
            assertThat(connections.constructed()).hasSize(1);
            verify(connections.constructed().get(0)).close();
        }
    }

    @Test
    void shouldRejectInvalidFieldsBeforeOpeningADatabaseConnection() {
        final var config = DamengConnectorConfigTest.configuration().with("database.port", "not-a-port").build();
        try (var connections = mockConstruction(DamengConnection.class)) {
            final var result = new DamengConnector().validate(config.asMap());

            assertThat(result.configValues()).filteredOn(value -> value.name().equals(DamengConnectorConfig.PORT.name()))
                    .singleElement().satisfies(value -> assertThat(value.errorMessages()).isNotEmpty());
            assertThat(connections.constructed()).isEmpty();
        }
    }
}
