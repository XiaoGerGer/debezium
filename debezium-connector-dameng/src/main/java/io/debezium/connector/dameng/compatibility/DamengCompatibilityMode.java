/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng.compatibility;

import java.math.BigInteger;

/** Server mode identifier, independent of the modes implemented by this connector. */
public record DamengCompatibilityMode(String value) {
    public DamengCompatibilityMode {
        if (value == null || !value.trim().matches("[0-9]+")) {
            throw new IllegalArgumentException("Dameng COMPATIBLE_MODE must be a nonnegative integer; actual value was " + value);
        }
        value = new BigInteger(value.trim()).toString();
    }

    @Override
    public String toString() {
        return value;
    }
}
