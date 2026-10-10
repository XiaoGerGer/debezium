/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng.snapshot;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;

import io.debezium.annotation.ConnectorSpecific;
import io.debezium.connector.dameng.DamengConnector;
import io.debezium.snapshot.spi.SnapshotLock;

@ConnectorSpecific(connector = DamengConnector.class)
public final class SharedSnapshotLock implements SnapshotLock {
    @Override
    public String name() {
        return "shared";
    }

    @Override
    public void configure(Map<String, ?> properties) {
    }

    @Override
    public Optional<String> tableLockingStatement(Duration lockTimeout, String tableId) {
        return Optional.of("LOCK TABLE " + tableId + " IN SHARE MODE");
    }
}
