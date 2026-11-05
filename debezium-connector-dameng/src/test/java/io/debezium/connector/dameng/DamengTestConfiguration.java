/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng;

import io.debezium.config.Configuration;

/** Minimal configuration for tests that do not connect to a real database. */
final class DamengTestConfiguration {
    private DamengTestConfiguration() {
    }

    static Configuration.Builder configuration() {
        return Configuration.create()
                .with("topic.prefix", "dameng")
                .with("database.hostname", "localhost")
                .with("database.user", "SYSDBA")
                .with("database.password", "test");
    }
}
