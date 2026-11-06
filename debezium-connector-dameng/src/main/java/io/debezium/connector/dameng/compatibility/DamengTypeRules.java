/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng.compatibility;

/** Physical type semantics used when rebuilding historical DDL. */
public enum DamengTypeRules {
    NATIVE(false, false),
    NATIVE_NUMBERS(false, true),
    ORACLE(true, false),
    ORACLE_NUMBERS(true, true);

    private final boolean dateAsTimestamp;
    private final boolean decimalFloat;

    DamengTypeRules(boolean dateAsTimestamp, boolean decimalFloat) {
        this.dateAsTimestamp = dateAsTimestamp;
        this.decimalFloat = decimalFloat;
    }

    public boolean dateAsTimestamp() {
        return dateAsTimestamp;
    }

    public boolean decimalFloat() {
        return decimalFloat;
    }
}
