/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng.compatibility;

import java.math.BigInteger;

/** Server compatibility modes supported by this connector. */
public enum DamengCompatibilityMode {
    NATIVE("0"),
    ORACLE("2"),
    MYSQL("4"),
    POSTGRES("7");

    private final String value;

    DamengCompatibilityMode(String value) {
        this.value = value;
    }

    public String value() {
        return value;
    }

    /** Parses an observed server value and rejects unsupported modes immediately. */
    public static DamengCompatibilityMode parse(String value) {
        if (value != null && value.trim().matches("[0-9]+")) {
            final var canonical = new BigInteger(value.trim()).toString();
            for (var mode : values()) {
                if (mode.value.equals(canonical)) {
                    return mode;
                }
            }
        }
        throw new IllegalArgumentException("Unsupported Dameng COMPATIBLE_MODE=" + value
                + "; supported values: 0 (native), 2 (Oracle), 4 (MySQL), 7 (PostgreSQL)");
    }

    @Override
    public String toString() {
        return value;
    }
}
