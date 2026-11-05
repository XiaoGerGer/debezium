/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng.compatibility;

import java.util.HashSet;
import java.util.Set;

/** DM PostgreSQL compatibility retains DM physical types and identifier folding. */
final class DamengPostgresCompatibilityProfile extends AbstractDamengCompatibilityProfile {
    private static final Set<String> VERIFIED_ZERO_PARAMETERS = Set.of("FLOAT_MODE", "NUMBER_MODE", "ORA_DATE_FMT", "BACKSLASH_ESCAPE");

    @Override
    public String id() {
        return "postgres-v1";
    }

    @Override
    public Set<DamengCompatibilityMode> modes() {
        return Set.of(new DamengCompatibilityMode("7"));
    }

    @Override
    public Set<String> parameterNames() {
        final Set<String> names = new HashSet<>(super.parameterNames());
        names.addAll(VERIFIED_ZERO_PARAMETERS);
        return Set.copyOf(names);
    }

    @Override
    public void validate(DamengCompatibilityEnvironment environment) {
        super.validate(environment);
        // Bind type and lexical settings to history. BACKSLASH_ESCAPE=1 changes
        // both defaults and redo strings on recent DM builds.
        for (var name : VERIFIED_ZERO_PARAMETERS) {
            if (!"0".equals(environment.parameter(name))) {
                throw new IllegalStateException("Dameng compatibility profile " + id() + " requires " + name
                        + "=0 for verified PostgreSQL-mode semantics; actual value was " + environment.parameter(name));
            }
        }
    }
}
