/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng.compatibility;

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
}
