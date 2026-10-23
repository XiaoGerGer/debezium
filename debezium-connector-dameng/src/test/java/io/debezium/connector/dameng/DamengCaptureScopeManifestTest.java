/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.apache.kafka.connect.errors.ConnectException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.debezium.document.Array;
import io.debezium.document.Document;
import io.debezium.document.DocumentReader;
import io.debezium.document.DocumentWriter;
import io.debezium.relational.history.SchemaHistory;

class DamengCaptureScopeManifestTest {
    private static final List<String> FIELDS = List.of(
            "database.include.list", "database.exclude.list", "schema.include.list", "schema.exclude.list",
            "table.include.list", "table.exclude.list", "signal.data.collection", "table.ignore.builtin", "database.dbname");
    private final DamengConnectorConfig original = new DamengConnectorConfig(DamengConnectorConfigTest.configuration().build());

    @Test
    void shouldRoundTripManifestAndKeepItsSignatureBoundToEveryValue() {
        final String manifest = DamengCaptureScope.manifest(original);
        final var document = DamengCaptureScope.readManifest(manifest);
        assertThat(document.getString("database.dbname")).isEqualTo(original.getDatabaseName());
        assertThat(DamengCaptureScope.signature(DamengCaptureScope.previousConfig(original, manifest))).isEqualTo(DamengCaptureScope.signature(original));

        final var changed = new DamengConnectorConfig(original.getConfig().edit().with("table.include.list", "APP\\.OTHER").build());
        assertThat(DamengCaptureScope.signature(changed)).isNotEqualTo(DamengCaptureScope.signature(original));
    }

    @ParameterizedTest
    @MethodSource("invalidManifests")
    void shouldRejectIncompleteMalformedOrWronglyTypedManifest(String manifest) {
        assertThatThrownBy(() -> DamengCaptureScope.readManifest(manifest)).isInstanceOf(ConnectException.class);
    }

    private static Stream<Arguments> invalidManifests() throws Exception {
        final var valid = DamengCaptureScope.manifest(new DamengConnectorConfig(DamengConnectorConfigTest.configuration().build()));
        final var cases = Stream.<Arguments> builder();
        cases.add(Arguments.of("{}")).add(Arguments.of("not-json"));
        for (var field : FIELDS) {
            final var missing = DocumentReader.defaultReader().read(valid);
            missing.remove(field);
            cases.add(Arguments.of(write(missing)));
            final List<Object> invalid = field.equals("table.ignore.builtin")
                    ? List.of(17, "true", Array.create(), Document.create())
                    : List.of(17, true, Array.create(), Document.create());
            for (var value : invalid) {
                final var wrongType = DocumentReader.defaultReader().read(valid);
                wrongType.set(field, value);
                cases.add(Arguments.of(write(wrongType)));
            }
        }
        final var missingBoolean = DocumentReader.defaultReader().read(valid);
        missingBoolean.set("table.ignore.builtin", null);
        cases.add(Arguments.of(write(missingBoolean)));
        return cases.build();
    }

    @ParameterizedTest
    @MethodSource("immutableFields")
    void shouldRejectChangesToImmutableDatabaseAndCaptureIdentity(String field) {
        final var before = DamengCaptureScope.manifest(original);
        final var changedConfig = switch (field) {
            case "database.dbname" -> new DamengConnectorConfig(original.getConfig().edit().with("database.dbname", "OTHER").build());
            case "signal.data.collection" -> new DamengConnectorConfig(original.getConfig().edit().with(field, "APP.SIGNAL").build());
            case "table.ignore.builtin" -> new DamengConnectorConfig(original.getConfig().edit().with(field, false).build());
            case "database.include.list" -> new DamengConnectorConfig(original.getConfig().edit().with(field, "DAMENG").build());
            case "database.exclude.list" -> new DamengConnectorConfig(original.getConfig().edit().with(field, "OTHER").build());
            default -> throw new IllegalArgumentException(field);
        };
        assertThatThrownBy(() -> DamengCaptureScope.validateChange(before, DamengCaptureScope.manifest(changedConfig)))
                .isInstanceOf(ConnectException.class).hasMessageContaining(field);
    }

    private static Stream<Arguments> immutableFields() {
        return Stream.of(Arguments.of("database.dbname"), Arguments.of("database.include.list"), Arguments.of("database.exclude.list"),
                Arguments.of("signal.data.collection"), Arguments.of("table.ignore.builtin"));
    }

    @Test
    void shouldAllowTableFilterChangesButPreserveCaseInsensitiveIdentifierMode() {
        final var config = new DamengConnectorConfig(original.getConfig().edit().with("table.include.list", "APP\\.A").build());
        final String manifest = DamengCaptureScope.manifest(config);
        final var current = new DamengConnectorConfig(original.getConfig().edit().with("table.include.list", "APP\\.(A|B)").build());
        current.configureIdentifierCase(true);
        DamengCaptureScope.validateChange(manifest, DamengCaptureScope.manifest(current));
        final var previous = DamengCaptureScope.previousConfig(current, manifest);
        assertThat(previous.hasCaseInsensitiveIdentifiers()).isTrue();
        assertThat(previous.getConfig().getString("table.include.list")).isEqualTo("APP\\.A");
    }

    @Test
    void shouldClearMissingOptionalPreviousFilterValuesInsteadOfKeepingTheCurrentValue() {
        final var config = new DamengConnectorConfig(original.getConfig().edit().without("table.exclude.list").build());
        final var current = new DamengConnectorConfig(config.getConfig().edit().with("table.exclude.list", "APP\\.CURRENT_ONLY").build());
        final var previous = DamengCaptureScope.previousConfig(current, DamengCaptureScope.manifest(config));
        assertThat(previous.getConfig().getString("table.exclude.list")).isNull();
        assertThat(current.getConfig().getString("table.exclude.list")).isEqualTo("APP\\.CURRENT_ONLY");
    }

    @Test
    void shouldKeepLegacyOffsetCompatibleOnlyWhenItsSignatureMatchesCurrentScope() {
        final Map<String, Object> legacy = new HashMap<>(new DamengOffsetContext(original, 123).getOffset());
        legacy.remove(DamengOffsetContext.CAPTURE_SCOPE_MANIFEST_KEY);
        final var loaded = new DamengOffsetContext.Loader(original).load(legacy);
        assertThat(loaded.getCaptureScopeManifest()).isNotNull();

        final var changed = new DamengConnectorConfig(original.getConfig().edit().with("table.include.list", "APP\\.OTHER").build());
        assertThatThrownBy(() -> new DamengOffsetContext.Loader(changed).load(legacy))
                .isInstanceOf(ConnectException.class).hasMessageContaining("capture scope");
        final var pending = new DamengOffsetContext.Loader(changed, true).load(legacy);
        assertThat(pending.getCaptureScopeManifest()).isNull();
        final var scope = new DamengCaptureScopeChange(changed, new DamengPartition("dameng"), mock(SchemaHistory.class));
        assertThatThrownBy(() -> scope.prepare(pending, List.of(), mock(DamengCaptureScopePreparation.class)))
                .isInstanceOf(ConnectException.class).hasMessageContaining("no capture scope manifest");
    }

    @Test
    void shouldRejectMissingOrMalformedStoredManifestWhenItsSignatureIsPresent() {
        final Map<String, Object> offset = new HashMap<>(new DamengOffsetContext(original, 123).getOffset());
        offset.put(DamengOffsetContext.CAPTURE_SCOPE_MANIFEST_KEY, "{}");
        assertThatThrownBy(() -> new DamengOffsetContext.Loader(original).load(offset)).isInstanceOf(ConnectException.class);
        offset.put(DamengOffsetContext.CAPTURE_SCOPE_MANIFEST_KEY, 42);
        assertThatThrownBy(() -> new DamengOffsetContext.Loader(original).load(offset)).isInstanceOf(ConnectException.class);
    }

    @Test
    void shouldRejectAWellFormedManifestThatDoesNotMatchTheStoredSignatureInEitherLoader() {
        final Map<String, Object> offset = new HashMap<>(new DamengOffsetContext(original, 123).getOffset());
        final var changed = new DamengConnectorConfig(original.getConfig().edit().with("table.include.list", "APP\\.OTHER").build());
        offset.put(DamengOffsetContext.CAPTURE_SCOPE_MANIFEST_KEY, DamengCaptureScope.manifest(changed));
        assertThatThrownBy(() -> new DamengOffsetContext.Loader(original).load(offset))
                .isInstanceOf(ConnectException.class).hasMessageContaining("manifest does not match");
        assertThatThrownBy(() -> new DamengOffsetContext.Loader(changed, true).load(offset))
                .isInstanceOf(ConnectException.class).hasMessageContaining("manifest does not match");
    }

    @ParameterizedTest
    @MethodSource("invalidGenerations")
    void shouldRejectInvalidGenerationTypesAndBlankValues(Object generation) {
        final Map<String, Object> offset = new HashMap<>(new DamengOffsetContext(original, 123).getOffset());
        offset.put(DamengOffsetContext.CAPTURE_SCOPE_GENERATION_KEY, generation);
        assertThatThrownBy(() -> new DamengOffsetContext.Loader(original).load(offset)).isInstanceOf(ConnectException.class);
    }

    private static Stream<Arguments> invalidGenerations() {
        return Stream.of(Arguments.of(17), Arguments.of(true), Arguments.of(""), Arguments.of(" "));
    }

    private static String write(Document document) throws IOException {
        return DocumentWriter.defaultWriter().write(document);
    }
}
