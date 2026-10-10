/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;

import org.junit.jupiter.api.Test;

import io.debezium.connector.common.AbstractPartitionTest;

class DamengPartitionTest extends AbstractPartitionTest<DamengPartition> {
    @Override
    protected DamengPartition createPartition1() {
        return new DamengPartition("server1");
    }

    @Override
    protected DamengPartition createPartition2() {
        return new DamengPartition("server2");
    }

    @Test
    void shouldProvideOneStableOffsetPartitionForTheLogicalServer() {
        final var config = new DamengConnectorConfig(DamengConnectorConfigTest.configuration().with("topic.prefix", "orders-dm").build());
        final var provider = new DamengPartition.Provider(config);

        assertThat(provider.getPartitions()).singleElement().satisfies(partition -> {
            assertThat(partition.getServerName()).isEqualTo("orders-dm");
            assertThat(partition.getSourcePartition()).isEqualTo(Map.of("server", "orders-dm"));
            assertThat(partition.getSupportedFormats()).containsExactly(Map.of("server", "orders-dm"));
            assertThat(partition.toString()).contains("server=orders-dm");
            assertThat(partition).isEqualTo(provider.getPartitions().iterator().next());
        });
    }

    @Test
    void shouldRejectMissingServerIdentityAndNotEqualUnrelatedPartitions() {
        assertThatThrownBy(() -> new DamengPartition(null)).isInstanceOf(NullPointerException.class);
        assertThat(new DamengPartition("server1")).isNotEqualTo(null).isNotEqualTo("server1");
    }
}
