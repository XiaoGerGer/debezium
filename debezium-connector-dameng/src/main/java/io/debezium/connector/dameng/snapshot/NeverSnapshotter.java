/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng.snapshot;

import io.debezium.snapshot.mode.NoDataSnapshotter;

/** Legacy never mode reads the required table schemas without emitting snapshot rows. */
public final class NeverSnapshotter extends NoDataSnapshotter {
    @Override
    public String name() {
        return "never";
    }

}
