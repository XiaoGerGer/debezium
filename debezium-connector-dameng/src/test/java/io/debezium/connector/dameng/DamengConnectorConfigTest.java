/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import io.debezium.config.Configuration;
import io.debezium.relational.TableId;

class DamengConnectorConfigTest {
    static Configuration.Builder configuration() {
        return Configuration.create()
                .with("topic.prefix", "dameng")
                .with("database.hostname", "localhost")
                .with("database.user", "SYSDBA")
                .with("database.password", "test");
    }

    @Test
    void shouldUseDamengDefaults() {
        final var config = new DamengConnectorConfig(configuration().build());

        assertThat(config.getDatabaseName()).isEqualTo("DAMENG");
        assertThat(config.getConfig().getInteger(DamengConnectorConfig.PORT)).isEqualTo(5236);
        assertThat(config.getSnapshotMode()).isEqualTo(DamengConnectorConfig.SnapshotMode.INITIAL);
        assertThat(config.getConnectorAdapter()).isEqualTo(DamengConnectorConfig.ConnectorAdapter.LOG_MINER);
        assertThat(config.getQueryFetchSize()).isEqualTo(2000);
        assertThat(DamengConnectorConfig.configDef().configKeys().get("query.fetch.size").defaultValue).isEqualTo(2000);
        assertThat(config.getLogMiningBufferMaxBytes()).isEqualTo(512L * 1024 * 1024);
        assertThat(config.getLogMiningBufferMaxRecords()).isEqualTo(1_000_000L);
        assertThat(config.getSnapshotMaxDurationMs()).isEqualTo(600_000L);
        assertThat(config.getSocketTimeoutMs()).isEqualTo(600_000);
        assertThat(config.getJdbcConfig().getInteger("socketTimeout")).isEqualTo(600_000);
        assertThat(config.isFailoverEnabled()).isFalse();
        assertThat(config.getJdbcConfig().getInteger("autoReconnect")).isZero();
        assertThat(config.getJdbcConfig().getInteger("rwSeparate")).isZero();
    }

    @Test
    void shouldConfigureOnlyOpenPrimariesFromExplicitFailoverEndpoints() {
        final var raw = configuration().without("database.hostname")
                .with("database.failover.hosts", " dm-primary:5236, [2001:db8::2]:5237 ").build();
        final var config = new DamengConnectorConfig(raw);
        assertThat(config.isFailoverEnabled()).isTrue();
        assertThat(config.getFailoverHosts()).isEqualTo("dm-primary:5236,[2001:db8::2]:5237");
        assertThat(config.getJdbcConfig().getInteger("loginMode")).isEqualTo(1);
        assertThat(config.getJdbcConfig().getInteger("loginStatus")).isEqualTo(4);
        assertThat(config.getJdbcConfig().getInteger("autoReconnect")).isZero();
        assertThat(config.getJdbcConfig().getInteger("rwSeparate")).isZero();
        assertThat(config.getJdbcConfig().getInteger("switchTimes")).isEqualTo(1);
        assertThat(config.getJdbcConfig().getInteger("switchInterval")).isEqualTo(200);
        assertThat(raw.validate(DamengConnectorConfig.ALL_FIELDS).get("database.hostname").errorMessages()).isEmpty();
        assertThat(configuration().without("database.hostname").build().validate(DamengConnectorConfig.ALL_FIELDS)
                .get("database.hostname").errorMessages()).isNotEmpty();
    }

    @ParameterizedTest
    @ValueSource(strings = { "host", "host:0", "host:65536", "host:abc", "host:5236,", ",host:5236", "2001:db8::2:5236", "host:5236?loginMode=2",
            "host/path:5236", "host&loginMode=2:5236", "host):5236", "host@other:5236" })
    void shouldRejectMalformedOrInjectedFailoverEndpoints(String endpoints) {
        assertThatThrownBy(() -> new DamengConnectorConfig(configuration().with("database.failover.hosts", endpoints).build()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @ParameterizedTest
    @ValueSource(strings = { "localhost?loginMode=2", "localhost/path", "localhost#fragment", "localhost&autoReconnect=1", "localhost@other", "${url}" })
    void shouldRejectJdbcParametersInsideSingleHostnames(String hostname) {
        assertThatThrownBy(() -> new DamengConnectorConfig(configuration().with("database.hostname", hostname).build()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("hostname");
    }

    @ParameterizedTest
    @ValueSource(strings = { "database.url", "driver.url", "database.host", "driver.host", "driver.hostname", "driver.port", "driver.failover.hosts",
            "database.connection.factory.class", "database.dmsvcconf", "database.unixSocketFile", "database.debezium" })
    void shouldRejectUncontrolledConnectionRouting(String property) {
        assertThatThrownBy(() -> new DamengConnectorConfig(configuration().with(property, "other").build()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @ParameterizedTest
    @ValueSource(strings = { "database.autoReconnect", "database.reconnect", "driver.doSwitch", "database.rwSeparate", "driver.AUTO_RECONNECT" })
    void shouldRejectTransparentSessionSwitching(String property) {
        assertThatThrownBy(() -> new DamengConnectorConfig(configuration().with(property, "1").build()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @ParameterizedTest
    @ValueSource(strings = { "database.loginMode", "driver.loginMode", "database.loginStatus", "database.switchTimes", "database.switchInterval",
            "driver.LOGIN_MODE" })
    void shouldRejectOverridesOfFailoverSelectionAndRetryPolicy(String property) {
        assertThatThrownBy(() -> new DamengConnectorConfig(configuration().with("database.failover.hosts", "primary:5236,standby:5236")
                .with(property, "0").build())).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void shouldAllowExplicitCanonicalSafeDriverValues() {
        final var config = new DamengConnectorConfig(configuration().with("database.failover.hosts", "primary:5236,standby:5236")
                .with("database.loginMode", 1).with("driver.loginStatus", 4).with("database.autoReconnect", 0).with("driver.rwSeparate", 0).build());
        assertThat(config.getJdbcConfig().getInteger("loginMode")).isEqualTo(1);
        assertThat(config.getJdbcConfig().getInteger("loginStatus")).isEqualTo(4);
    }

    @Test
    void shouldAllowExplicitLongBufferBudgets() {
        final var config = new DamengConnectorConfig(configuration().with("log.mining.buffer.max.bytes", 3L * 1024 * 1024 * 1024)
                .with("log.mining.buffer.max.records", 2_000_000L).build());

        assertThat(config.getLogMiningBufferMaxBytes()).isEqualTo(3L * 1024 * 1024 * 1024);
        assertThat(config.getLogMiningBufferMaxRecords()).isEqualTo(2_000_000L);
    }

    @ParameterizedTest
    @ValueSource(longs = { 0, -1, Long.MIN_VALUE })
    void shouldRejectNonpositiveBufferBudgets(long limit) {
        assertThatThrownBy(() -> new DamengConnectorConfig(configuration().with("log.mining.buffer.max.bytes", limit).build()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("log.mining.buffer.max.bytes");
        assertThatThrownBy(() -> new DamengConnectorConfig(configuration().with("log.mining.buffer.max.records", limit).build()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("log.mining.buffer.max.records");
    }

    @Test
    void shouldMapTheFiniteSocketTimeoutAndAllowLargerSnapshotDeadlines() {
        final var config = new DamengConnectorConfig(configuration().with("database.socket.timeout.ms", 1234)
                .with("snapshot.max.duration.ms", 3_000_000_000L).with("database.query.timeout.ms", 1).build());

        assertThat(config.getSocketTimeoutMs()).isEqualTo(1234);
        assertThat(config.getJdbcConfig().getInteger("socketTimeout")).isEqualTo(1234);
        assertThat(config.getSnapshotMaxDurationMs()).isEqualTo(3_000_000_000L);
        assertThat(config.getJdbcConfig().getInteger("query.timeout.ms")).isEqualTo(1);
    }

    @ParameterizedTest
    @ValueSource(ints = { 0, -1, Integer.MIN_VALUE })
    void shouldRejectUnboundedSocketOrSnapshotTimeouts(int timeout) {
        assertThatThrownBy(() -> new DamengConnectorConfig(configuration().with("database.socket.timeout.ms", timeout).build()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("database.socket.timeout.ms");
        assertThatThrownBy(() -> new DamengConnectorConfig(configuration().with("snapshot.max.duration.ms", timeout).build()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("snapshot.max.duration.ms");
    }

    @Test
    void shouldRejectNegativeQueryTimeoutButKeepTheCommonZeroSemantics() {
        assertThatThrownBy(() -> new DamengConnectorConfig(configuration().with("database.query.timeout.ms", -1).build()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("database.query.timeout.ms");
        final var config = new DamengConnectorConfig(configuration().with("database.query.timeout.ms", 0).build());

        assertThat(config.getJdbcConfig().getInteger("query.timeout.ms")).isZero();
        assertThat(config.getJdbcConfig().getInteger("socketTimeout")).isPositive();
    }

    @Test
    void shouldRejectTheAmbiguousRawDriverSocketTimeout() {
        assertThatThrownBy(() -> new DamengConnectorConfig(configuration().with("database.socketTimeout", 0).build()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("database.socket.timeout.ms");
    }

    @ParameterizedTest
    @ValueSource(ints = { 0, 2, 8 })
    void shouldRejectUnsupportedParallelSnapshotConnections(int threads) {
        assertThatThrownBy(() -> new DamengConnectorConfig(configuration().with("snapshot.max.threads", threads).build()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("snapshot.max.threads");
    }

    @Test
    void shouldRejectUnimplementedIncrementalSchemaChanges() {
        assertThatThrownBy(() -> new DamengConnectorConfig(configuration().with("incremental.snapshot.allow.schema.changes", true).build()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("incremental.snapshot.allow.schema.changes");
    }

    @Test
    void shouldRejectAmbiguousWatermarkTableConfiguration() {
        assertThatThrownBy(() -> new DamengConnectorConfig(configuration().with("signal.data.collection", "APP.SIGNAL1,APP.SIGNAL2").build()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("signal.data.collection");
    }

    @Test
    void shouldCaptureSignalTableOutsideExplicitDataFilter() {
        final var config = new DamengConnectorConfig(configuration().with("table.include.list", "APP.EVENTS")
                .with("signal.data.collection", "APP.SIGNAL").build());
        assertThat(config.getTableFilters().dataCollectionFilter().isIncluded(new TableId(null, "APP", "SIGNAL"))).isTrue();
        assertThat(config.getTableFilters().dataCollectionFilter().isIncluded(new TableId(null, "OTHER", "SIGNAL"))).isFalse();
    }

    @ParameterizedTest
    @ValueSource(strings = { "initial", "initial_only", "no_data", "never" })
    void shouldAcceptSupportedSnapshotModes(String mode) {
        final var config = new DamengConnectorConfig(configuration().with("snapshot.mode", mode).build());

        assertThat(config.getSnapshotMode().getValue()).isEqualTo(mode);
    }

    @ParameterizedTest
    @ValueSource(strings = { "xstream", "olr", "logminer_unbuffered", "unknown" })
    void shouldRejectUnsupportedAdapters(String adapter) {
        assertThatThrownBy(() -> new DamengConnectorConfig(configuration().with("database.connection.adapter", adapter).build()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void shouldRejectUnknownSnapshotMode() {
        assertThatThrownBy(() -> new DamengConnectorConfig(configuration().with("snapshot.mode", "unknown").build()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void shouldApplySchemaAndTableFilters() {
        final var config = new DamengConnectorConfig(configuration()
                .with("schema.include.list", "APP")
                .with("table.include.list", "APP\\.EVENTS")
                .build());

        assertThat(config.getTableFilters().dataCollectionFilter().isIncluded(new TableId("DAMENG", "APP", "EVENTS"))).isTrue();
        assertThat(config.getTableFilters().dataCollectionFilter().isIncluded(new TableId("DAMENG", "APP", "OTHER"))).isFalse();
        assertThat(config.getTableFilters().dataCollectionFilter().isIncluded(new TableId("DAMENG", "OTHER", "EVENTS"))).isFalse();
    }

    @Test
    void shouldExcludeSystemSchemas() {
        final var config = new DamengConnectorConfig(configuration().build());

        assertThat(config.getTableFilters().dataCollectionFilter().isIncluded(new TableId("DAMENG", "SYS", "SYSTABLES"))).isFalse();
        assertThat(config.getTableFilters().dataCollectionFilter().isIncluded(new TableId("DAMENG", "APP", "EVENTS"))).isTrue();
    }

    @Test
    void shouldReportInvalidHostsThroughKafkaConfigValidation() {
        final var invalidHostname = configuration().with("database.hostname", "host?loginMode=2").build();
        assertThat(invalidHostname.validate(DamengConnectorConfig.ALL_FIELDS).get("database.hostname").errorMessages())
                .anySatisfy(message -> assertThat(message).contains("JDBC URL parameters"));

        final var invalidFailoverHosts = configuration().with("database.failover.hosts", "primary:5236,standby:65536").build();
        assertThat(invalidFailoverHosts.validate(DamengConnectorConfig.ALL_FIELDS).get("database.failover.hosts").errorMessages())
                .anySatisfy(message -> assertThat(message).contains("between 1 and 65535"));

        assertThat(configuration().build().validate(DamengConnectorConfig.ALL_FIELDS).get("database.hostname").errorMessages()).isEmpty();
    }

    @Test
    void shouldRejectIpv6BracketsThatDoNotEncloseTheEntireHost() {
        assertThatThrownBy(() -> new DamengConnectorConfig(configuration().with("database.failover.hosts", "[::1]extra:5236").build()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("brackets");
    }

    @ParameterizedTest
    @CsvSource({ "initial,true,true,true", "initial_only,true,true,false", "no_data,true,false,true", "never,false,false,true" })
    void shouldExposeSnapshotModeCapabilities(String mode, boolean schema, boolean data, boolean stream) {
        final var config = new DamengConnectorConfig(configuration().with("snapshot.mode", mode).build());

        assertThat(config.getSnapshotMode().includeSchema()).isEqualTo(schema);
        assertThat(config.getSnapshotMode().includeData()).isEqualTo(data);
        assertThat(config.getSnapshotMode().shouldStream()).isEqualTo(stream);
        assertThat(config.supportsOperationFiltering()).isFalse();
    }

    @Test
    void shouldUseExplicitFallbacksForAbsentSnapshotSettingsAndRejectUnknownLocks() {
        assertThat(DamengConnectorConfig.ConnectorAdapter.parse(null)).isEqualTo(DamengConnectorConfig.ConnectorAdapter.LOG_MINER);
        assertThat(DamengConnectorConfig.SnapshotMode.parse(null, DamengConnectorConfig.SnapshotMode.NO_DATA))
                .isEqualTo(DamengConnectorConfig.SnapshotMode.NO_DATA);
        assertThat(DamengConnectorConfig.SnapshotLockingMode.parse(null, DamengConnectorConfig.SnapshotLockingMode.SHARED))
                .isEqualTo(DamengConnectorConfig.SnapshotLockingMode.SHARED);
        assertThat(new DamengConnectorConfig(configuration().build()).getSnapshotLockingMode())
                .contains(DamengConnectorConfig.SnapshotLockingMode.SHARED);
        assertThatThrownBy(() -> new DamengConnectorConfig(configuration().with("snapshot.locking.mode", "none").build()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("snapshot locking mode");
    }
}
