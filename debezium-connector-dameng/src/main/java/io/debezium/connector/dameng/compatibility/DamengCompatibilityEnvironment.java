/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng.compatibility;

import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/** Observed server parameters; an absent parameter is distinct from an explicit zero. */
public record DamengCompatibilityEnvironment(DamengCompatibilityMode mode, Map<String, String> parameters) {
    public DamengCompatibilityEnvironment {
        Objects.requireNonNull(mode, "mode");
        parameters = Map.copyOf(parameters);
    }

    public String parameter(String name) {
        return parameters.get(name);
    }

    /** Length-prefixed, deterministic manifest. Missing parameters remain distinct from explicit zero. */
    public String signature() {
        final var manifest = new StringBuilder("v1:");
        append(manifest, mode.value());
        new TreeMap<>(parameters).forEach((name, value) -> {
            append(manifest, name);
            append(manifest, value);
        });
        return manifest.toString();
    }

    private static void append(StringBuilder manifest, String value) {
        manifest.append(value.length()).append(':').append(value);
    }
}
