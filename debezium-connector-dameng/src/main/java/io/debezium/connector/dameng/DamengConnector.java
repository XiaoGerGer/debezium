/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.ExactlyOnceSupport;

import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.connector.common.RelationalBaseSourceConnector;
import io.debezium.metadata.ConfigDescriptor;

public final class DamengConnector extends RelationalBaseSourceConnector implements ConfigDescriptor {
    private Map<String, String> properties = Collections.emptyMap();

    @Override
    public String version() {
        return Module.version();
    }

    @Override
    public void start(Map<String, String> props) {
        properties = Collections.unmodifiableMap(new HashMap<>(props));
    }

    @Override
    public Class<? extends Task> taskClass() {
        return DamengConnectorTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        if (maxTasks > 1) {
            throw new IllegalArgumentException("Only a single connector task may be started");
        }
        return Collections.singletonList(properties);
    }

    @Override
    public void stop() {
    }

    @Override
    public org.apache.kafka.common.config.ConfigDef config() {
        return DamengConnectorConfig.configDef();
    }

    @Override
    public Field.Set getConfigFields() {
        return DamengConnectorConfig.ALL_FIELDS;
    }

    @Override
    protected Map<String, org.apache.kafka.common.config.ConfigValue> validateAllFields(Configuration config) {
        return config.validate(DamengConnectorConfig.ALL_FIELDS);
    }

    @Override
    protected void validateConnection(Map<String, org.apache.kafka.common.config.ConfigValue> values, Configuration config) {
        try (DamengConnection connection = new DamengConnection(new DamengConnectorConfig(config).getJdbcConfig())) {
            connection.connection();
        }
        catch (Exception e) {
            values.get(DamengConnectorConfig.HOSTNAME.name()).addErrorMessage("Unable to connect to Dameng: " + e.getMessage());
        }
    }

    @Override
    public ExactlyOnceSupport exactlyOnceSupport(Map<String, String> connectorConfig) {
        return ExactlyOnceSupport.UNSUPPORTED;
    }
}
