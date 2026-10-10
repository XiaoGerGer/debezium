/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import io.debezium.pipeline.spi.Partition;
import io.debezium.relational.AbstractPartition;
import io.debezium.util.Collect;

public final class DamengPartition extends AbstractPartition implements Partition {
    private static final String SERVER_KEY = "server";
    private final String serverName;
    private final Map<String, String> sourcePartition;
    private final int hashCode;

    public DamengPartition(String serverName) {
        super(null);
        this.serverName = Objects.requireNonNull(serverName);
        this.sourcePartition = Collect.hashMapOf(SERVER_KEY, serverName);
        this.hashCode = Objects.hash(serverName);
    }

    @Override
    public Map<String, String> getSourcePartition() {
        return sourcePartition;
    }

    @Override
    public List<Map<String, String>> getSupportedFormats() {
        return Collections.singletonList(sourcePartition);
    }

    public String getServerName() {
        return serverName;
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof DamengPartition && serverName.equals(((DamengPartition) other).serverName);
    }

    @Override
    public int hashCode() {
        return hashCode;
    }

    @Override
    public String toString() {
        return "DamengPartition [sourcePartition=" + sourcePartition + "]";
    }

    public static final class Provider implements Partition.Provider<DamengPartition> {
        private final DamengConnectorConfig config;

        public Provider(DamengConnectorConfig config) {
            this.config = config;
        }

        @Override
        public Set<DamengPartition> getPartitions() {
            return Collections.singleton(new DamengPartition(config.getLogicalName()));
        }
    }
}
