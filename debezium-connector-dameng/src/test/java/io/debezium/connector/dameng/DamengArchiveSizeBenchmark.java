/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng;

/** Explicit manual entry point, without JUnit annotations or automatic test discovery. */
public final class DamengArchiveSizeBenchmark {
    private DamengArchiveSizeBenchmark() {
    }

    public static void main(String[] args) throws Exception {
        if (!Boolean.getBoolean("dameng.archive.benchmark.enabled")) {
            throw new IllegalArgumentException("Set dameng.archive.benchmark.enabled=true to explicitly run this performance experiment");
        }
        new DamengDatagenPerformanceIT().runArchiveSizeBenchmark();
    }
}
