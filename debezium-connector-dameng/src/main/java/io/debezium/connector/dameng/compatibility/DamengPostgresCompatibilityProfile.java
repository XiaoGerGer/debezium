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
    private static final Set<String> BINARY_PARAMETERS = Set.of("FLOAT_MODE", "NUMBER_MODE", "BACKSLASH_ESCAPE");

    @Override
    public String id() {
        return "postgres-v1";
    }

    @Override
    public Set<DamengCompatibilityMode> modes() {
        return Set.of(DamengCompatibilityMode.POSTGRES);
    }

    @Override
    public Set<String> parameterNames() {
        final Set<String> names = new HashSet<>(super.parameterNames());
        names.addAll(BINARY_PARAMETERS);
        return Set.copyOf(names);
    }

    @Override
    public void validate(DamengCompatibilityEnvironment environment) {
        super.validate(environment);
        for (var name : BINARY_PARAMETERS) {
            if (!"0".equals(environment.parameter(name)) && !"1".equals(environment.parameter(name))) {
                throw new IllegalStateException("Dameng compatibility profile " + id() + " requires " + name
                        + "=0 or 1 for verified PostgreSQL-mode semantics; actual value was " + environment.parameter(name));
            }
        }
    }

    @Override
    protected DamengTypeRules typeRules(DamengCompatibilityEnvironment environment) {
        return "1".equals(environment.parameter("NUMBER_MODE")) ? DamengTypeRules.NATIVE_NUMBERS : DamengTypeRules.NATIVE;
    }

    @Override
    protected boolean backslashEscape(DamengCompatibilityEnvironment environment) {
        return "1".equals(environment.parameter("BACKSLASH_ESCAPE"));
    }
}
