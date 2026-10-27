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
import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.junit.logging.LogInterceptor;
import io.debezium.relational.TableId;

class DamengConnectorConfigTest {
    @Test
    void shouldKeepStandbyReadsAndPrimaryWritesOnSeparateControlledRoutes() {
        final var raw = configuration().with("database.failover.hosts", "a:5236,b:5236")
                .with("database.standby.hosts", " b:5236,a:5236 ").build();
        final var config = new DamengConnectorConfig(raw);
        assertThat(config.isStandbyMiningEnabled()).isTrue();
        assertThat(config.requiresDatabaseLineage()).isTrue();
        assertThat(config.getJdbcConfig().getString("failover.hosts")).isEqualTo("b:5236,a:5236");
        assertThat(config.getJdbcConfig().getInteger("loginMode")).isEqualTo(2);
        assertThat(config.getJdbcConfig().getInteger("loginStatus")).isEqualTo(4);
        assertThat(config.getPrimaryJdbcConfig().getString("failover.hosts")).isEqualTo("a:5236,b:5236");
        assertThat(config.getPrimaryJdbcConfig().getInteger("loginMode")).isEqualTo(1);
        assertThat(config.getPrimaryJdbcConfig().hasKey("standby.hosts")).isFalse();
        assertThat(config.getStandbyReplayTimeoutMs()).isEqualTo(60_000);
        final var direct = new DamengConnectorConfig(configuration().with("database.standby.hosts", "replica:5236").build());
        assertThat(direct.requiresDatabaseLineage()).isTrue();
        assertThat(direct.isFailoverEnabled()).isFalse();
        assertThat(direct.getPrimaryJdbcConfig().getString("hostname")).isEqualTo("localhost");
        assertThat(direct.getPrimaryJdbcConfig().getInteger("loginMode")).isEqualTo(1);
    }

    @ParameterizedTest
    @ValueSource(strings = { "bad", "bad:0", "bad:5236?loginMode=1", "bad:5236," })
    void shouldRejectInvalidStandbyEndpoints(String endpoints) {
        final var raw = configuration().with("database.standby.hosts", endpoints).build();
        assertThatThrownBy(() -> new DamengConnectorConfig(raw)).isInstanceOf(IllegalArgumentException.class);
        assertThat(raw.validate(DamengConnectorConfig.ALL_FIELDS).get("database.standby.hosts").errorMessages()).isNotEmpty();
    }

    @Test
    void shouldRequireAPositiveReplayTimeout() {
        assertThatThrownBy(() -> new DamengConnectorConfig(configuration().with("database.standby.replay.timeout.ms", 0).build()))
                .hasMessageContaining("must be positive");
    }

    @Test
    void shouldUseCanonicalSignalIdentityOnlyForCaseInsensitiveDatabases() {
        final var config = new DamengConnectorConfig(configuration().with("signal.data.collection", "App.My_Signal").build());
        final var mixed = new io.debezium.relational.TableId(null, "App", "My_Signal");
        final var upper = new io.debezium.relational.TableId(null, "APP", "MY_SIGNAL");
        assertThat(config.getSignalingDataCollectionIds()).containsExactly("App.My_Signal");
        assertThat(config.getSignalingDataCollectionTableIds().get(0).schema()).isEqualTo("App");
        assertThat(config.getSignalingDataCollectionTableIds().get(0).catalog()).isNull();
        assertThat(config.isSignalDataCollection(mixed)).isTrue();
        assertThat(config.isSignalDataCollection(upper)).isFalse();
        config.configureIdentifierCase(true);
        assertThat(config.getSignalingDataCollectionIds()).containsExactly("APP.MY_SIGNAL");
        assertThat(config.getSignalingDataCollectionTableIds()).containsExactly(upper);
        assertThat(config.isSignalDataCollection(upper)).isTrue();
        config.configureIdentifierCase(false);
        assertThat(config.getSignalingDataCollectionTableIds()).containsExactly(mixed);
        final var quoted = new DamengConnectorConfig(configuration().with("signal.data.collection", "\"App\".\"My_Signal\"").build());
        assertThat(quoted.isSignalDataCollection(mixed)).isTrue();
        quoted.configureIdentifierCase(true);
        assertThat(quoted.isSignalDataCollection(upper)).isTrue();
    }

    @Test
    void shouldRequireExplicitRecoveryAndLeaveBackfillManualByDefault() {
        final var defaults = new DamengConnectorConfig(configuration().build());
        assertThat(defaults.getRecoveryRestartNonce()).isEmpty();
        assertThat(defaults.isRecoveryIncrementalSnapshotAutoStart()).isFalse();
        assertThat(DamengConnectorConfig.configDef().configKeys().get("recovery.incremental.snapshot.auto.start").defaultValue).isEqualTo(false);
        assertThat(new DamengConnectorConfig(configuration().with("recovery.restart.nonce", "incident-1").build()).getRecoveryRestartNonce())
                .isEqualTo("incident-1");
        assertThat(new DamengConnectorConfig(configuration().with("recovery.restart.nonce", "  ").build()).getRecoveryRestartNonce()).isEmpty();
        assertThatThrownBy(() -> new DamengConnectorConfig(configuration().with("recovery.restart.nonce", "new")
                .with("snapshot.mode", "initial_only").build())).hasMessageContaining("initial_only");
        assertThatThrownBy(() -> new DamengConnectorConfig(configuration().with("recovery.incremental.snapshot.auto.start", true).build()))
                .hasMessageContaining("signal.data.collection");
    }

    @ParameterizedTest
    @ValueSource(strings = { " leading", "trailing ", "new\nline", "中文", "has space" })
    void shouldRejectAmbiguousRecoveryNonces(String nonce) {
        final var raw = configuration().with("recovery.restart.nonce", nonce).build();
        assertThatThrownBy(() -> new DamengConnectorConfig(raw)).hasMessageContaining("recovery.restart.nonce");
        assertThat(raw.validate(DamengConnectorConfig.ALL_FIELDS).get("recovery.restart.nonce").errorMessages()).isNotEmpty();
        assertThatThrownBy(() -> new DamengConnectorConfig(configuration().with("recovery.restart.nonce", "x".repeat(129)).build()))
                .hasMessageContaining("128");
    }

    static Configuration.Builder configuration() {
        return Configuration.create()
                .with("topic.prefix", "dameng")
                .with("database.hostname", "localhost")
                .with("database.user", "SYSDBA")
                .with("database.password", "test");
    }

    @Test
    void shouldDefaultToArchiveWindows() {
        final var defaults = new DamengConnectorConfig(configuration().build());
        assertThat(defaults.getMiningWindowStrategy()).isEqualTo(DamengConnectorConfig.MiningWindowStrategy.ARCHIVE_BYTES);
        assertThat(defaults.getLogMiningWindowMaxBytes()).isEqualTo(64L * 1024 * 1024);
        assertThat(defaults.getLogMiningBatchSize()).isEqualTo(1000);
        assertThat(DamengConnectorConfig.configDef().configKeys().get("log.mining.window.strategy").defaultValue).isEqualTo("archive_bytes");
    }

    @ParameterizedTest
    @ValueSource(ints = { 0, -1 })
    void shouldRejectNonPositiveFixedSpan(int span) {
        final var raw = configuration().with("log.mining.window.strategy", "fixed").with("log.mining.batch.size", span).build();
        assertThatThrownBy(() -> new DamengConnectorConfig(raw)).hasMessageContaining("log.mining.batch.size must be positive");
        assertThat(raw.validate(DamengConnectorConfig.ALL_FIELDS).get("log.mining.batch.size").errorMessages()).isNotEmpty();
    }

    @ParameterizedTest
    @CsvSource({ "archive_bytes,ARCHIVE_BYTES", "fixed,FIXED", "ARCHIVE_BYTES,ARCHIVE_BYTES", "FIXED,FIXED" })
    void shouldAcceptSupportedWindowStrategies(String strategy, String expected) {
        final var raw = configuration().with("log.mining.window.strategy", strategy).with("log.mining.batch.size", 20).build();
        final var config = new DamengConnectorConfig(raw);
        assertThat(config.getMiningWindowStrategy().name()).isEqualTo(expected);
        assertThat(config.getLogMiningBatchSize()).isEqualTo(20);
        assertThat(raw.validate(DamengConnectorConfig.ALL_FIELDS).get("log.mining.window.strategy").errorMessages()).isEmpty();
    }

    @ParameterizedTest
    @CsvSource({ "log.mining.window.adaptive.enabled,true", "log.mining.window.adaptive.enabled,false",
            "log.mining.batch.size.min,1000", "log.mining.batch.size.max,10000", "log.mining.window.target.ms,1000" })
    void shouldRejectRemovedWindowSettingsWithMigrationAdvice(String name, String value) {
        assertThat(DamengConnectorConfig.configDef().configKeys()).doesNotContainKey(name);
        for (var strategy : new String[]{ null, "archive_bytes", "fixed" }) {
            final var builder = configuration().with(name, value);
            if (strategy != null) {
                builder.with("log.mining.window.strategy", strategy);
            }
            final var raw = builder.build();
            assertThatThrownBy(() -> new DamengConnectorConfig(raw)).hasMessageContaining(name)
                    .hasMessageContaining("has been removed").hasMessageContaining("archive_bytes").hasMessageContaining("fixed");
            // The REST validation path must return a configuration error before attempting JDBC.
            final var result = new DamengConnector().validate(raw.asMap());
            assertThat(result.configValues()).filteredOn(item -> item.name().equals("log.mining.window.strategy"))
                    .singleElement().satisfies(item -> assertThat(item.errorMessages()).anySatisfy(message -> assertThat(message)
                            .contains(name, "has been removed", "archive_bytes", "fixed")));
        }
    }

    @ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(longs = { 0, -1 })
    void shouldRejectNonPositiveArchiveBudget(long budget) {
        final var raw = configuration().with("log.mining.window.max.bytes", budget).build();
        assertThatThrownBy(() -> new DamengConnectorConfig(raw)).hasMessageContaining("byte budget");
        assertThat(raw.validate(DamengConnectorConfig.ALL_FIELDS).get("log.mining.window.max.bytes").errorMessages()).isNotEmpty();
    }

    @ParameterizedTest
    @ValueSource(strings = { "typo", "adaptive", "ADAPTIVE" })
    void shouldRejectUnknownWindowStrategy(String strategy) {
        final var raw = configuration().with("log.mining.window.strategy", strategy).build();
        assertThatThrownBy(() -> new DamengConnectorConfig(raw))
                .hasMessageContaining("window strategy");
        assertThat(raw.validate(DamengConnectorConfig.ALL_FIELDS).get("log.mining.window.strategy").errorMessages()).isNotEmpty();
    }

    @Test
    void shouldUseDamengDefaults() {
        final var config = new DamengConnectorConfig(configuration().build());

        assertThat(config.getTemporalPrecisionMode()).isEqualTo(TemporalPrecisionMode.SQLSTRING);
        assertThat(config.getConfig().getString(DamengConnectorConfig.TIME_PRECISION_MODE)).isEqualTo("sqlstring");
        assertThat(DamengConnectorConfig.configDef().configKeys().get("time.precision.mode").defaultValue).isEqualTo("sqlstring");
        assertThat(config.getDatabaseName()).isEqualTo("DAMENG");
        assertThat(config.getConfig().getInteger(DamengConnectorConfig.PORT)).isEqualTo(5236);
        assertThat(config.getSnapshotMode()).isEqualTo(DamengConnectorConfig.SnapshotMode.NO_DATA);
        assertThat(config.getSnapshotMode().includeSchema()).isTrue();
        assertThat(config.getSnapshotMode().includeData()).isFalse();
        assertThat(config.getSnapshotMode().shouldStream()).isTrue();
        assertThat(DamengConnectorConfig.configDef().configKeys().get("snapshot.mode").defaultValue).isEqualTo("no_data");
        assertThat(config.getConnectorAdapter()).isEqualTo(DamengConnectorConfig.ConnectorAdapter.LOG_MINER);
        assertThat(config.getQueryFetchSize()).isEqualTo(2000);
        assertThat(DamengConnectorConfig.configDef().configKeys().get("query.fetch.size").defaultValue).isEqualTo(2000);
        assertThat(config.getLogMiningBufferMaxSqlBytes()).isEqualTo(16L * 1024 * 1024);
        assertThat(config.getLogMiningBufferMaxTransactions()).isEqualTo(1_000_000L);
        assertThat(config.getLogMiningBufferMemoryBytes()).isEqualTo(64L * 1024 * 1024);
        assertThat(config.getLogMiningBufferSpillMaxBytes()).isEqualTo(1024L * 1024 * 1024);
        assertThat(config.getLogMiningBufferSpillPath()).isEqualTo(System.getProperty("java.io.tmpdir"));
        assertThat(config.getSnapshotMaxDurationMs()).isEqualTo(600_000L);
        assertThat(config.getSocketTimeoutMs()).isEqualTo(600_000);
        assertThat(config.getJdbcConfig().getInteger("socketTimeout")).isEqualTo(600_000);
        assertThat(config.isFailoverEnabled()).isFalse();
        assertThat(config.getJdbcConfig().getInteger("autoReconnect")).isZero();
        assertThat(config.getJdbcConfig().getInteger("rwSeparate")).isZero();
    }

    @Test
    void shouldHonorExplicitTimePrecisionMode() {
        final var config = new DamengConnectorConfig(configuration().with("time.precision.mode", "adaptive").build());
        assertThat(config.getTemporalPrecisionMode()).isEqualTo(TemporalPrecisionMode.ADAPTIVE);
        assertThat(config.getConfig().getString(DamengConnectorConfig.TIME_PRECISION_MODE)).isEqualTo("adaptive");
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
    void shouldIgnoreLegacyBudgetsWithoutReinterpretingThemAsResourceLimits() {
        final var logs = LogInterceptor.forClass(DamengConnectorConfig.class);
        final var raw = configuration().with("log.mining.buffer.max.bytes", 1).with("log.mining.buffer.max.records", 1).build();
        final var config = new DamengConnectorConfig(raw);
        assertThat(config.getLogMiningBufferMaxSqlBytes()).isEqualTo(16L * 1024 * 1024);
        assertThat(config.getLogMiningBufferMaxTransactions()).isEqualTo(1_000_000L);
        assertThat(raw.validate(DamengConnectorConfig.ALL_FIELDS).get("log.mining.buffer.max.bytes").errorMessages()).isEmpty();
        assertThat(raw.validate(DamengConnectorConfig.ALL_FIELDS).get("log.mining.buffer.max.records").errorMessages()).isEmpty();
        assertThat(logs.containsWarnMessage("log.mining.buffer.max.bytes is deprecated and ignored")).isTrue();
        assertThat(logs.containsWarnMessage("log.mining.buffer.max.records is deprecated and ignored")).isTrue();
    }

    @Test
    void shouldConfigureIndependentSqlAndTransactionLimits() {
        final var config = new DamengConnectorConfig(configuration().with("log.mining.buffer.max.sql.bytes", 32L * 1024 * 1024)
                .with("log.mining.buffer.max.transactions", 100).build());
        assertThat(config.getLogMiningBufferMaxSqlBytes()).isEqualTo(32L * 1024 * 1024);
        assertThat(config.getLogMiningBufferMaxTransactions()).isEqualTo(100);
    }

    @ParameterizedTest
    @ValueSource(longs = { 0, -1, Long.MIN_VALUE })
    void shouldRejectNonpositiveResourceLimits(long limit) {
        assertThatThrownBy(() -> new DamengConnectorConfig(configuration().with("log.mining.buffer.max.sql.bytes", limit).build()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("log.mining.buffer.max.sql.bytes");
        assertThatThrownBy(() -> new DamengConnectorConfig(configuration().with("log.mining.buffer.max.transactions", limit).build()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("log.mining.buffer.max.transactions");
        assertThatThrownBy(() -> new DamengConnectorConfig(configuration().with("log.mining.buffer.memory.bytes", limit).build()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("memory/disk limits");
        assertThatThrownBy(() -> new DamengConnectorConfig(configuration().with("log.mining.buffer.spill.max.bytes", limit).build()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("memory/disk limits");
    }

    @Test
    void shouldValidateTransactionSpillPathAndExposeResourceSettings() {
        assertThatThrownBy(() -> new DamengConnectorConfig(configuration().with("log.mining.buffer.spill.path", " ").build()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("spill path");
        final var config = new DamengConnectorConfig(configuration().with("log.mining.buffer.memory.bytes", 2048)
                .with("log.mining.buffer.spill.path", "/tmp/my-spill").with("log.mining.buffer.spill.max.bytes", 8L * 1024 * 1024 * 1024).build());
        assertThat(config.getLogMiningBufferMemoryBytes()).isEqualTo(2048);
        assertThat(config.getLogMiningBufferSpillPath()).isEqualTo("/tmp/my-spill");
        assertThat(config.getLogMiningBufferSpillMaxBytes()).isEqualTo(8L * 1024 * 1024 * 1024);
        assertThat(DamengConnectorConfig.configDef().configKeys()).containsKeys("log.mining.buffer.memory.bytes",
                "log.mining.buffer.spill.path", "log.mining.buffer.spill.max.bytes");
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
