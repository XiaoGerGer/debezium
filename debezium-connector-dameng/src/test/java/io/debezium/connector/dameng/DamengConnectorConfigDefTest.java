/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng;

import io.debezium.config.ConfigDefinitionMetadataTest;

class DamengConnectorConfigDefTest extends ConfigDefinitionMetadataTest {
    DamengConnectorConfigDefTest() {
        super(new DamengConnector());
    }
}
