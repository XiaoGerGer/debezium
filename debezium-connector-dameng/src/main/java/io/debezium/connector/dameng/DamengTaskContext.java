/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng;

import io.debezium.config.Configuration;
import io.debezium.connector.common.CdcSourceTaskContext;

public final class DamengTaskContext extends CdcSourceTaskContext<DamengConnectorConfig> {
    public DamengTaskContext(Configuration rawConfig, DamengConnectorConfig config) {
        super(rawConfig, config, config.getCustomMetricTags());
    }
}
