/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mockStatic;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.errors.ConnectException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class DamengCaptureScopeTest {
    private final DamengConnectorConfig original = new DamengConnectorConfig(DamengConnectorConfigTest.configuration().build());

    @ParameterizedTest
    @CsvSource({
            "database.include.list, DAMENG",
            "database.exclude.list, OTHER",
            "schema.include.list, APP",
            "schema.exclude.list, OTHER",
            "table.include.list, APP\\.A",
            "table.exclude.list, APP\\.B",
            "table.ignore.builtin, false",
            "signal.data.collection, APP.SIGNAL"
    })
    void shouldRejectEveryCaptureFilterChangeBeforeLoadingThePosition(String field, String value) {
        final var position = new DamengOffsetContext(original, 123).getOffset();
        final var changed = new DamengConnectorConfig(original.getConfig().edit().with(field, value).build());

        assertThatThrownBy(() -> new DamengOffsetContext.Loader(changed).load(position))
                .isInstanceOf(ConnectException.class).hasMessageContaining("capture scope", "initial snapshot");
    }

    @ParameterizedTest
    @CsvSource({ "APP\\.A, APP\\.(A|B)", "APP\\.(A|B), APP\\.A", "APP\\.A, APP[.]A" })
    void shouldRejectExpansionNarrowingAndEquivalentRegexRewrites(String before, String after) {
        final var initial = new DamengConnectorConfig(original.getConfig().edit().with("table.include.list", before).build());
        final var changed = new DamengConnectorConfig(original.getConfig().edit().with("table.include.list", after).build());
        final var position = new DamengOffsetContext(initial, 123).getOffset();

        assertThatThrownBy(() -> new DamengOffsetContext.Loader(changed).load(position))
                .isInstanceOf(ConnectException.class).hasMessageContaining("capture scope");
    }

    @Test
    void shouldPreserveScopeWithoutAnyTableHistoryAndAcceptAnExplicitDefault() {
        final var position = new DamengOffsetContext(original, 123).getOffset();
        final var explicitDefault = new DamengConnectorConfig(original.getConfig().edit().with("table.ignore.builtin", true).build());

        assertThat(new DamengOffsetContext.Loader(explicitDefault).load(position).getOffset()).isEqualTo(position);
        assertThat(position.get(DamengOffsetContext.CAPTURE_SCOPE_KEY)).asString().startsWith("v1:");
    }

    @Test
    void shouldAllowChangesThatDoNotBroadenHistoricalTableCoverage() {
        final var position = new DamengOffsetContext(original, 123).getOffset();
        final var changed = new DamengConnectorConfig(original.getConfig().edit()
                .with("snapshot.include.collection.list", "APP.A")
                .with("column.exclude.list", "APP.A.SECRET")
                .with("poll.interval.ms", 25)
                .with("database.password", "different credential").build());

        assertThat(new DamengOffsetContext.Loader(changed).load(position).getOffset()).isEqualTo(position);
    }

    @Test
    void shouldRejectLegacyOffsetsWithoutSilentlyBindingTheirCaptureScope() {
        assertThatThrownBy(() -> new DamengOffsetContext.Loader(original).load(Map.of("lsn", 123L)))
                .isInstanceOf(ConnectException.class).hasMessageContaining("no capture scope signature", "new connector identity", "initial snapshot");
    }

    @Test
    void shouldRejectUnknownSignatureVersions() {
        final Map<String, Object> position = new HashMap<>(new DamengOffsetContext(original, 123).getOffset());
        position.put(DamengOffsetContext.CAPTURE_SCOPE_KEY, "v2:unknown");

        assertThatThrownBy(() -> new DamengOffsetContext.Loader(original).load(position))
                .isInstanceOf(ConnectException.class).hasMessageContaining("capture scope");
    }

    @Test
    void shouldRefuseToCreateAnUnverifiableScopeWhenSha256IsUnavailable() {
        final var unavailable = new NoSuchAlgorithmException("SHA-256 unavailable");
        try (var digests = mockStatic(MessageDigest.class)) {
            digests.when(() -> MessageDigest.getInstance("SHA-256")).thenThrow(unavailable);

            assertThatThrownBy(() -> DamengCaptureScope.signature(original)).isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("SHA-256", "capture scope").hasCause(unavailable);
        }
        assertThat(DamengCaptureScope.signature(original)).matches("v1:[0-9a-f]{64}");
    }
}
