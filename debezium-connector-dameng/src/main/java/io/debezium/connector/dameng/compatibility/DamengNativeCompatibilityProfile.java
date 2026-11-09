/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng.compatibility;

import java.util.HashSet;
import java.util.Set;

/** Native DM semantics. */
final class DamengNativeCompatibilityProfile extends AbstractDamengCompatibilityProfile {
    @Override
    public String id() {
        return "native-v1";
    }

    @Override
    public Set<DamengCompatibilityMode> modes() {
        return Set.of(DamengCompatibilityMode.NATIVE);
    }

    @Override
    public Set<String> parameterNames() {
        final Set<String> names = new HashSet<>(super.parameterNames());
        names.add("NUMBER_MODE");
        return Set.copyOf(names);
    }

    @Override
    public void validate(DamengCompatibilityEnvironment environment) {
        super.validate(environment);
        if (!"0".equals(environment.parameter("NUMBER_MODE")) && !"1".equals(environment.parameter("NUMBER_MODE"))) {
            throw new IllegalStateException("Dameng compatibility profile " + id()
                    + " requires NUMBER_MODE=0 or 1 for verified native-mode semantics; actual value was " + environment.parameter("NUMBER_MODE"));
        }
        final var backslashSemantics = environment.parameter("NATIVE_BACKSLASH_SEMANTICS");
        if ("1".equals(environment.parameter("BACKSLASH_ESCAPE")) && !"raw".equals(backslashSemantics) && !"escaped".equals(backslashSemantics)) {
            throw new IllegalStateException("Dameng native BACKSLASH_ESCAPE=1 requires observed NATIVE_BACKSLASH_SEMANTICS=raw or escaped");
        }
        final var realSemantics = environment.parameter("NATIVE_REAL_SEMANTICS");
        if ("1".equals(environment.parameter("NUMBER_MODE")) && !"binary".equals(realSemantics) && !"decimal".equals(realSemantics)) {
            throw new IllegalStateException("Dameng native NUMBER_MODE=1 requires observed NATIVE_REAL_SEMANTICS=binary or decimal");
        }
    }

    @Override
    protected DamengTypeRules typeRules(DamengCompatibilityEnvironment environment) {
        if (!"1".equals(environment.parameter("NUMBER_MODE"))) {
            return DamengTypeRules.NATIVE;
        }
        return "binary".equals(environment.parameter("NATIVE_REAL_SEMANTICS"))
                ? DamengTypeRules.NATIVE_NUMBERS_BINARY_REAL
                : DamengTypeRules.NATIVE_NUMBERS;
    }

    @Override
    protected boolean backslashEscape(DamengCompatibilityEnvironment environment) {
        return "1".equals(environment.parameter("BACKSLASH_ESCAPE")) && "escaped".equals(environment.parameter("NATIVE_BACKSLASH_SEMANTICS"));
    }
}
