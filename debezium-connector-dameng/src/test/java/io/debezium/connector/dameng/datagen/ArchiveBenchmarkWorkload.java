/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng.datagen;

import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.DriverManager;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

/** Manually launched generator with bounded operations and commit timestamps. */
public final class ArchiveBenchmarkWorkload {
    private ArchiveBenchmarkWorkload() {
    }

    public static void main(String[] args) throws Exception {
        final var config = Config.load(args);
        final var stopping = new AtomicBoolean();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> stopping.set(true)));
        final long targetRate = Long.getLong("dameng.archive.benchmark.target.rate", 0L);
        final long[] committed = { 0 };
        final long started = System.nanoTime();
        try (var writer = Files.newBufferedWriter(Path.of(System.getProperty("dameng.archive.benchmark.commits")));
                var connection = DriverManager.getConnection(config.jdbcUrl(), config.user, config.password)) {
            writer.write("transaction,operations,commit_start_ms,commit_end_ms\n");
            final long[] ordinal = { 0 };
            new DamengGenerator(config, connection, stopping, (size, times) -> {
                try {
                    writer.write(++ordinal[0] + "," + size + "," + times[0] + "," + times[1] + "\n");
                    writer.flush();
                }
                catch (java.io.IOException e) {
                    throw new UncheckedIOException(e);
                }
                committed[0] += size;
                if (targetRate > 0) {
                    final long deadline = started + (long) (committed[0] * 1_000_000_000.0 / targetRate);
                    while (!stopping.get() && System.nanoTime() < deadline) {
                        LockSupport.parkNanos(Math.min(100_000_000L, deadline - System.nanoTime()));
                    }
                }
            }).run();
        }
    }
}
