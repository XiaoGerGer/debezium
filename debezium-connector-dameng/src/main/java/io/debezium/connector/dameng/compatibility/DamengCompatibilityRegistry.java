/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng.compatibility;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** Explicit registration controls which semantics can be used for production capture. */
public final class DamengCompatibilityRegistry {
    private static final DamengCompatibilityRegistry BUILT_IN = new DamengCompatibilityRegistry(List.of(new DamengNativeCompatibilityProfile()));
    private final Map<DamengCompatibilityMode, DamengCompatibilityProfile> profiles;

    public DamengCompatibilityRegistry(Collection<? extends DamengCompatibilityProfile> implementations) {
        final Map<DamengCompatibilityMode, DamengCompatibilityProfile> registered = new LinkedHashMap<>();
        for (var profile : implementations) {
            if (profile.id() == null || profile.id().isBlank() || profile.modes().isEmpty()) {
                throw new IllegalArgumentException("A Dameng compatibility profile must have an id and at least one mode");
            }
            for (var mode : profile.modes()) {
                Objects.requireNonNull(mode, "mode");
                final var previous = registered.putIfAbsent(mode, profile);
                if (previous != null) {
                    throw new IllegalArgumentException("Conflicting Dameng compatibility profiles for COMPATIBLE_MODE=" + mode
                            + ": " + previous.id() + " and " + profile.id());
                }
            }
        }
        profiles = Collections.unmodifiableMap(registered);
    }

    public static DamengCompatibilityRegistry builtIn() {
        return BUILT_IN;
    }

    public Set<DamengCompatibilityMode> registeredModes() {
        return profiles.keySet();
    }

    public DamengCompatibilityProfile resolve(DamengCompatibilityMode mode) {
        Objects.requireNonNull(mode, "mode");
        final var profile = profiles.get(mode);
        if (profile == null) {
            throw new IllegalStateException("No implemented Dameng compatibility profile for COMPATIBLE_MODE=" + mode
                    + "; registered modes: " + registeredModes()
                    + ". Add and validate a compatibility profile before enabling capture; native semantics will not be assumed.");
        }
        return profile;
    }
}
