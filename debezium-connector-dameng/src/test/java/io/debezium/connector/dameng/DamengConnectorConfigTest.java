/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng;

import static io.debezium.connector.dameng.DamengTestConfiguration.configuration;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mockConstruction;

import org.apache.kafka.common.config.ConfigDef.Importance;
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
    void shouldRequireAnExplicitPartitionExchangeOptIn() {
        assertThat(new DamengConnectorConfig(configuration().build()).isPartitionExchangeIgnored()).isFalse();
        assertThat(new DamengConnectorConfig(configuration().with(DamengConnectorConfig.PARTITION_EXCHANGE_IGNORE, true).build())
                .isPartitionExchangeIgnored()).isTrue();
        final var key = DamengConnectorConfig.configDef().configKeys().get("partition.exchange.ignore");
        assertThat(key.defaultValue).isEqualTo(false);
        assertThat(key.importance).isEqualTo(Importance.HIGH);
        assertThat(configuration().with(DamengConnectorConfig.PARTITION_EXCHANGE_IGNORE, "invalid").build()
                .validate(DamengConnectorConfig.ALL_FIELDS).get("partition.exchange.ignore").errorMessages()).isNotEmpty();
    }

    @Test
    void shouldRequireAnExplicitCascadeDropOptIn() {
        assertThat(new DamengConnectorConfig(configuration().build()).isCascadeDropIgnored()).isFalse();
        assertThat(new DamengConnectorConfig(configuration().with(DamengConnectorConfig.CASCADE_DROP_IGNORE, true).build())
                .isCascadeDropIgnored()).isTrue();
        assertThat(DamengConnectorConfig.configDef().configKeys().get("cascade.drop.ignore").defaultValue).isEqualTo(false);
        assertThat(configuration().with(DamengConnectorConfig.CASCADE_DROP_IGNORE, "invalid").build()
                .validate(DamengConnectorConfig.ALL_FIELDS).get("cascade.drop.ignore").errorMessages()).isNotEmpty();
    }

    @Test
    void shouldRequireAnExplicitPartitionDropOptIn() {
        assertThat(new DamengConnectorConfig(configuration().build()).isPartitionDropIgnored()).isFalse();
        assertThat(new DamengConnectorConfig(configuration().with(DamengConnectorConfig.PARTITION_DROP_IGNORE, true).build())
                .isPartitionDropIgnored()).isTrue();
        assertThat(DamengConnectorConfig.configDef().configKeys().get("partition.drop.ignore").defaultValue).isEqualTo(false);
        assertThat(configuration().with(DamengConnectorConfig.PARTITION_DROP_IGNORE, "invalid").build()
                .validate(DamengConnectorConfig.ALL_FIELDS).get("partition.drop.ignore").errorMessages()).isNotEmpty();
    }

    @ParameterizedTest
    @ValueSource(strings = { "MTAB$_MV", "MLOG$_BASE", "mtab$_mv", "Mlog$_Base", "BMJ$_I", "bmj$_i" })
    void shouldExcludeInternalTablesByDefault(String name) {
        final var config = new DamengConnectorConfig(configuration().with("table.include.list", "APP\\..*").build());
        final var id = new TableId(null, "APP", name);
        assertThat(config.getTableFilters().dataCollectionFilter().isIncluded(id)).isFalse();
        assertThat(config.getTableFilters().eligibleDataCollectionFilter().isIncluded(id)).isFalse();
        assertThat(config.getTableFilters().eligibleForSchemaDataCollectionFilter().isIncluded(id)).isFalse();
        assertThat(config.isInternalTableExcluded(id)).isTrue();
    }

    @ParameterizedTest
    @ValueSource(strings = { "MTAB$_MV", "MLOG$_BASE", "BMJ$_I" })
    void shouldHonorTheExistingBuiltinFilterSwitch(String name) {
        final var config = new DamengConnectorConfig(configuration().with("table.ignore.builtin", false).build());
        final var id = new TableId(null, "APP", name);
        assertThat(config.getTableFilters().dataCollectionFilter().isIncluded(id)).isTrue();
        assertThat(config.isInternalTableExcluded(id)).isFalse();
    }

    @ParameterizedTest
    @ValueSource(strings = { "ORDERS", "MTAB$ORDERS", "MLOG_ORDERS", "MY_MTAB$_MV", "BMJ_ORDERS", "MY_BMJ$_I" })
    void shouldRetainOrdinaryTablesWithSimilarNames(String name) {
        final var config = new DamengConnectorConfig(configuration().build());
        assertThat(config.getTableFilters().dataCollectionFilter().isIncluded(new TableId(null, "APP", name))).isTrue();
    }

    @ParameterizedTest
    @ValueSource(strings = { "APP.MTAB$_MV", "APP.MLOG$_BASE", "\"App\".\"mtab$_mv\"", "APP.BMJ$_I", "\"App\".\"bmj$_i\"" })
    void shouldRejectInternalTablesAsSignalCollections(String name) {
        assertThatThrownBy(() -> new DamengConnectorConfig(configuration().with("signal.data.collection", name).build()))
                .hasMessageContaining("signal.data.collection", "internal table");
    }

    @Test
    void shouldKeepStandbyReadsAndPrimaryWritesOnTheSameMemberList() {
        final var raw = configuration().with("database.hosts", " a:5236, b:5237 ")
                .with("database.capture.role", "standby").build();
        final var config = new DamengConnectorConfig(raw);
        assertThat(config.isStandbyMiningEnabled()).isTrue();
        assertThat(config.getCaptureRole()).isEqualTo(DamengConnectorConfig.CaptureRole.STANDBY);
        assertThat(config.getHosts()).isEqualTo("a:5236,b:5237");
        assertThat(config.getJdbcConfig().getString("hosts")).isEqualTo("a:5236,b:5237");
        assertThat(config.getJdbcConfig().getString("capture.role")).isEqualTo("standby");
        assertThat(config.getJdbcConfig().getInteger("loginMode")).isEqualTo(2);
        assertThat(config.getJdbcConfig().getInteger("loginStatus")).isEqualTo(4);
        assertThat(config.getPrimaryJdbcConfig().getString("hosts")).isEqualTo("a:5236,b:5237");
        assertThat(config.getPrimaryJdbcConfig().getString("capture.role")).isEqualTo("primary");
        assertThat(config.getPrimaryJdbcConfig().getInteger("loginMode")).isEqualTo(1);
        assertThat(config.getStandbyReplayTimeoutMs()).isEqualTo(60_000);
        assertThat(config.getJdbcConfig().asMap()).doesNotContainKeys("hostname", "port", "failover.hosts", "standby.hosts");
        assertThat(config.getPrimaryJdbcConfig().asMap()).doesNotContainKeys("hostname", "port", "failover.hosts", "standby.hosts");
    }

    @ParameterizedTest
    @ValueSource(strings = { "primary", "standby" })
    void shouldUseExplicitCaptureRolesForSingleAndMultipleEndpoints(String role) {
        for (var hosts : new String[]{ "node:5236", "a:5236,b:5237" }) {
            final var raw = configuration().with("database.hosts", hosts).with("database.capture.role", role).build();
            final var config = new DamengConnectorConfig(raw);
            assertThat(config.getHosts()).isEqualTo(hosts);
            assertThat(config.getCaptureRole().getValue()).isEqualTo(role);
            assertThat(config.isStandbyMiningEnabled()).isEqualTo(role.equals("standby"));
            assertThat(raw.validate(DamengConnectorConfig.ALL_FIELDS).get("database.capture.role").errorMessages()).isEmpty();
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "", " ", "unknown", "normal", "auto", "primary,standby" })
    void shouldRejectUnknownOrEmptyCaptureRolesBeforeOpeningJdbc(String role) {
        final var raw = configuration().with("database.capture.role", role).build();
        assertThatThrownBy(() -> new DamengConnectorConfig(raw)).hasMessageContaining("database.capture.role");
        assertThat(raw.validate(DamengConnectorConfig.ALL_FIELDS).get("database.capture.role").errorMessages()).isNotEmpty();
        assertRejectedBeforeJdbc(raw, "database.capture.role", "database.capture.role");
    }

    @ParameterizedTest
    @ValueSource(strings = { "database.hostname", "database.port", "database.failover.hosts", "database.standby.hosts" })
    void shouldRejectRemovedEndpointSettingsWithAndWithoutTheNewMemberList(String property) {
        assertThat(DamengConnectorConfig.configDef().configKeys()).doesNotContainKey(property);
        for (var value : new String[]{ "", "legacy:5236" }) {
            for (var includeHosts : new boolean[]{ false, true }) {
                final var builder = configuration().with(property, value);
                if (!includeHosts) {
                    builder.without("database.hosts");
                }
                final var raw = builder.build();
                assertThatThrownBy(() -> new DamengConnectorConfig(raw)).hasMessageContaining(property, "database.hosts");
                assertThat(raw.validate(DamengConnectorConfig.ALL_FIELDS).get("database.hosts").errorMessages()).isNotEmpty();
                assertRejectedBeforeJdbc(raw, "database.hosts", property, "database.hosts");
            }
        }
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
        assertThat(config.getHosts()).isEqualTo("localhost:5236");
        assertThat(config.getCaptureRole()).isEqualTo(DamengConnectorConfig.CaptureRole.PRIMARY);
        assertThat(config.isStandbyMiningEnabled()).isFalse();
        assertThat(DamengConnectorConfig.configDef().configKeys().get("database.capture.role").defaultValue).isEqualTo("primary");
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
        assertThat(config.getJdbcConfig().getString("hosts")).isEqualTo("localhost:5236");
        assertThat(config.getJdbcConfig().getString("capture.role")).isEqualTo("primary");
        assertThat(config.getJdbcConfig().getInteger("loginMode")).isEqualTo(1);
        assertThat(config.getJdbcConfig().getInteger("loginStatus")).isEqualTo(4);
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
    void shouldNormalizeMemberEndpointsAndControlPrimarySelection() {
        final var raw = configuration().with("database.hosts", " dm-primary:5236, [2001:db8::2]:5237 ").build();
        final var config = new DamengConnectorConfig(raw);
        assertThat(config.getHosts()).isEqualTo("dm-primary:5236,[2001:db8::2]:5237");
        assertThat(config.getJdbcConfig().getString("hosts")).isEqualTo(config.getHosts());
        assertThat(config.getJdbcConfig().getInteger("loginMode")).isEqualTo(1);
        assertThat(config.getJdbcConfig().getInteger("loginStatus")).isEqualTo(4);
        assertThat(config.getJdbcConfig().getInteger("autoReconnect")).isZero();
        assertThat(config.getJdbcConfig().getInteger("rwSeparate")).isZero();
        assertThat(config.getJdbcConfig().getInteger("switchTimes")).isEqualTo(1);
        assertThat(config.getJdbcConfig().getInteger("switchInterval")).isEqualTo(200);
        assertThat(raw.validate(DamengConnectorConfig.ALL_FIELDS).get("database.hosts").errorMessages()).isEmpty();
    }

    @Test
    void shouldRejectMissingMemberEndpointsBeforeOpeningJdbc() {
        final var raw = configuration().without("database.hosts").build();
        assertThatThrownBy(() -> new DamengConnectorConfig(raw)).hasMessageContaining("database.hosts");
        assertThat(raw.validate(DamengConnectorConfig.ALL_FIELDS).get("database.hosts").errorMessages()).isNotEmpty();
        assertRejectedBeforeJdbc(raw, "database.hosts", "database.hosts");
    }

    @ParameterizedTest
    @ValueSource(strings = { "", " ", "host", "host:0", "host:65536", "host:abc", "host:5236,", ",host:5236", "2001:db8::2:5236",
            "host:5236?loginMode=2", "host/path:5236", "host&loginMode=2:5236", "host):5236", "host@other:5236", "${url}:5236" })
    void shouldRejectMalformedOrInjectedMemberEndpointsBeforeOpeningJdbc(String endpoints) {
        final var raw = configuration().with("database.hosts", endpoints).build();
        assertThatThrownBy(() -> new DamengConnectorConfig(raw)).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("database.hosts");
        assertThat(raw.validate(DamengConnectorConfig.ALL_FIELDS).get("database.hosts").errorMessages()).isNotEmpty();
        assertRejectedBeforeJdbc(raw, "database.hosts", "database.hosts");
    }

    @ParameterizedTest
    @ValueSource(strings = { "database.url", "driver.url", "database.host", "driver.host", "driver.hostname", "driver.port", "driver.hosts", "driver.capture.role",
            "driver.failover.hosts",
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
    void shouldRejectOverridesOfConnectorManagedSelectionAndRetryPolicy(String property) {
        assertThatThrownBy(() -> new DamengConnectorConfig(configuration().with("database.hosts", "primary:5236,standby:5236")
                .with(property, "0").build())).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void shouldAllowExplicitCanonicalSafeDriverValues() {
        final var config = new DamengConnectorConfig(configuration().with("database.hosts", "primary:5236,standby:5236")
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
        final var injected = configuration().with("database.hosts", "host?loginMode=2:5236").build();
        assertThat(injected.validate(DamengConnectorConfig.ALL_FIELDS).get("database.hosts").errorMessages()).isNotEmpty();

        final var invalidPort = configuration().with("database.hosts", "primary:5236,standby:65536").build();
        assertThat(invalidPort.validate(DamengConnectorConfig.ALL_FIELDS).get("database.hosts").errorMessages())
                .anySatisfy(message -> assertThat(message).contains("between 1 and 65535"));

        assertThat(configuration().build().validate(DamengConnectorConfig.ALL_FIELDS).get("database.hosts").errorMessages()).isEmpty();
    }

    @Test
    void shouldRejectIpv6BracketsThatDoNotEncloseTheEntireHost() {
        assertThatThrownBy(() -> new DamengConnectorConfig(configuration().with("database.hosts", "[::1]extra:5236").build()))
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

    private static void assertRejectedBeforeJdbc(Configuration raw, String fieldName, String... expectedMessages) {
        try (var connections = mockConstruction(DamengConnection.class)) {
            final var result = new DamengConnector().validate(raw.asMap());
            assertThat(result.configValues()).filteredOn(value -> value.name().equals(fieldName))
                    .singleElement().satisfies(value -> assertThat(value.errorMessages())
                            .anySatisfy(message -> assertThat(message).contains(expectedMessages)));
            assertThat(connections.constructed()).isEmpty();
        }
    }

}
