/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng.compatibility;

import java.util.HashSet;
import java.util.Set;

/** DM Oracle-mode temporal and numeric definition rules. */
final class DamengOracleCompatibilityProfile extends AbstractDamengCompatibilityProfile {
    @Override
    public String id() {
        return "oracle-v1";
    }

    @Override
    public Set<DamengCompatibilityMode> modes() {
        return Set.of(DamengCompatibilityMode.ORACLE);
    }

    @Override
    public Set<String> parameterNames() {
        final Set<String> names = new HashSet<>(super.parameterNames());
        names.addAll(Set.of("FLOAT_MODE", "NUMBER_MODE", "ORA_DATE_FMT"));
        return Set.copyOf(names);
    }

    @Override
    public void validate(DamengCompatibilityEnvironment environment) {
        super.validate(environment);
        for (var name : Set.of("FLOAT_MODE", "ORA_DATE_FMT", "NUMBER_MODE")) {
            if (!"0".equals(environment.parameter(name)) && !"1".equals(environment.parameter(name))) {
                throw new IllegalStateException("Dameng compatibility profile " + id() + " requires " + name
                        + "=0 or 1 for verified Oracle-mode semantics; actual value was " + environment.parameter(name));
            }
        }
    }

    @Override
    protected DamengTypeRules typeRules(DamengCompatibilityEnvironment environment) {
        return "1".equals(environment.parameter("NUMBER_MODE")) ? DamengTypeRules.ORACLE_NUMBERS : DamengTypeRules.ORACLE;
    }

    @Override
    protected boolean backslashEscape(DamengCompatibilityEnvironment environment) {
        return "1".equals(environment.parameter("BACKSLASH_ESCAPE"));
    }
}
