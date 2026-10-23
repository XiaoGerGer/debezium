/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng;

import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Coordinates primary locks/watermarks with reads from a physical standby. */
final class DamengStandbySupport {
    private static final Logger LOGGER = LoggerFactory.getLogger(DamengStandbySupport.class);
    private final DamengConnectorConfig config;
    private final DamengConnection primary;
    private DamengDatabaseIdentity primaryIdentity;

    DamengStandbySupport(DamengConnectorConfig config, DamengConnection primary) {
        this.config = config;
        this.primary = primary;
    }

    DamengConnection primary() {
        return primary;
    }

    void initialize(DamengConnection reader) throws SQLException {
        primaryIdentity = primary.readPrimaryStandbyIdentity();
        primary.validateLogMiningConfiguration();
        validate(reader);
        awaitReplay(reader, primary.currentLsn());
        if (!primary.getCompatibilityEnvironment().equals(reader.getCompatibilityEnvironment())
                || !primary.getDatabaseCharset().equals(reader.getDatabaseCharset())) {
            throw new ConnectException("Dameng primary and standby have different compatibility settings or character sets");
        }
        LOGGER.info("Dameng standby capture initialized: primary DB_MAGIC={}, reader DB_MAGIC={}, PERMANENT_MAGIC={}, primary open={}",
                primaryIdentity.databaseMagic(), reader.databaseMagic(), primaryIdentity.permanentMagic(), primaryIdentity.openGuid());
    }

    void validate(DamengConnection reader) throws SQLException {
        validatePrimary();
        final var identity = reader.readPrimaryStandbyIdentity();
        if (identity.permanentMagic() != primaryIdentity.permanentMagic()) {
            throw new ConnectException("Dameng standby and primary belong to different database families (PERMANENT_MAGIC mismatch)");
        }
        if (identity.readingDatabaseMagic() == primaryIdentity.readingDatabaseMagic()) {
            throw new ConnectException("Dameng standby capture and primary writes must use different physical members");
        }
        if (identity.databaseMagic() != primaryIdentity.databaseMagic() || !identity.openGuid().equals(primaryIdentity.openGuid())) {
            throw new SQLException("Dameng standby and primary do not share the same current primary-open history; reconnect after replication catches up", "08006");
        }
    }

    private void validatePrimary() throws SQLException {
        final var current = primary.readPrimaryStandbyIdentity();
        if (current.permanentMagic() != primaryIdentity.permanentMagic() || current.databaseMagic() != primaryIdentity.databaseMagic()
                || !current.openGuid().equals(primaryIdentity.openGuid())) {
            throw new SQLException("Dameng primary changed during standby snapshot coordination; reconnect and restart the incomplete operation", "08006");
        }
    }

    long snapshotBoundary(DamengConnection reader) throws SQLException {
        validate(reader);
        final long boundary = primary.currentLsn();
        awaitReplay(reader, boundary);
        // End any metadata read transaction opened before the replay barrier.
        // The primary retains the captured-table locks in its own transaction.
        reader.commit();
        return boundary;
    }

    void awaitReplay(DamengConnection reader, long boundary) throws SQLException {
        final long started = System.nanoTime();
        final long timeout = TimeUnit.MILLISECONDS.toNanos(config.getStandbyReplayTimeoutMs());
        while (reader.readStandbyProgress().safeLsn() < boundary) {
            validatePrimary();
            if (System.nanoTime() - started >= timeout) {
                throw new SQLException("Dameng standby replay did not reach primary LSN " + boundary + " within "
                        + config.getStandbyReplayTimeoutMs() + " ms; the snapshot must be retried", "08006");
            }
            try {
                Thread.sleep(100);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new SQLException("Interrupted while waiting for Dameng standby replay", "08006", e);
            }
        }
        validate(reader);
        LOGGER.debug("Dameng standby replay reached primary LSN {} after {} ms", boundary,
                TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started));
    }
}
