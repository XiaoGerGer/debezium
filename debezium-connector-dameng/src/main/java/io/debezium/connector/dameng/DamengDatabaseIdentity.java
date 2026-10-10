/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng;

import java.time.Instant;
import java.util.Comparator;
import java.util.List;

import org.apache.kafka.connect.errors.ConnectException;

/** Database family and retained primary-open history used to reject divergent recovery. */
public record DamengDatabaseIdentity(long permanentMagic, long databaseMagic, long currentLsn, List<OpenRecord> history) {
    public record OpenRecord(String guid, Instant openedAt, long databaseMagic, long appliedLsn, long previousPrimaryMagic) {
        public OpenRecord(String guid, Instant openedAt, long databaseMagic, long appliedLsn) {
            this(guid, openedAt, databaseMagic, appliedLsn, databaseMagic);
        }
    }

    public DamengDatabaseIdentity {
        history = history.stream().sorted(Comparator.comparing(OpenRecord::openedAt)).toList();
        if (permanentMagic == 0 || databaseMagic == 0 || history.isEmpty()) {
            throw new ConnectException("Dameng primary/standby recovery requires a database permanent magic and retained SYSOPENHISTORY records");
        }
        for (int index = 0; index < history.size(); index++) {
            final var entry = history.get(index);
            if (entry.guid() == null || entry.guid().isBlank() || entry.databaseMagic() == 0 || entry.appliedLsn() < 0
                    || (index > 0 && (!entry.openedAt().isAfter(history.get(index - 1).openedAt())
                            || entry.appliedLsn() < history.get(index - 1).appliedLsn()))) {
                throw new ConnectException("Dameng primary-open history is incomplete or ambiguously ordered; refusing automatic recovery");
            }
            if (index > 0 && entry.previousPrimaryMagic() != history.get(index - 1).databaseMagic()) {
                throw new ConnectException("Dameng primary-open history has a discontinuous primary identity chain");
            }
        }
        if (history.get(history.size() - 1).databaseMagic() != databaseMagic) {
            throw new ConnectException("Dameng latest primary-open record does not identify the connected primary");
        }
    }

    public String openGuid() {
        return history.get(history.size() - 1).guid();
    }

    public void validateResume(long storedPermanentMagic, long storedDatabaseMagic, String storedOpenGuid, long requiredLsn) {
        if (storedPermanentMagic != permanentMagic) {
            throw new ConnectException("Stored Dameng offset belongs to a different database family (PERMANENT_MAGIC mismatch)");
        }
        int anchor = -1;
        for (int index = 0; index < history.size(); index++) {
            if (history.get(index).guid().equals(storedOpenGuid)) {
                anchor = index;
                break;
            }
        }
        if (anchor < 0 || history.get(anchor).databaseMagic() != storedDatabaseMagic) {
            throw new ConnectException("Stored Dameng primary-open anchor is absent from SYSOPENHISTORY or belongs to another database branch; "
                    + "the history may have rotated or the primary may have diverged. Reconcile source data before starting a new snapshot");
        }
        if (currentLsn < requiredLsn) {
            throw new ConnectException("Dameng primary is behind the acknowledged offset LSN " + requiredLsn);
        }
        for (int index = anchor + 1; index < history.size(); index++) {
            if (history.get(index).appliedLsn() < requiredLsn) {
                throw new ConnectException("Dameng primary transition " + history.get(index).guid()
                        + " only applied through LSN " + history.get(index).appliedLsn()
                        + ", before acknowledged offset LSN " + requiredLsn + "; refusing to skip a lost or divergent source history");
            }
        }
    }

    /** A source change must not consume logs beyond the successor primary's applied boundary. */
    public boolean containsArchiveSource(long sourceMagic, long firstLsn, long lastLsn) {
        int runStart = 0;
        while (runStart < history.size()) {
            int nextRun = runStart + 1;
            while (nextRun < history.size() && history.get(nextRun).databaseMagic() == history.get(runStart).databaseMagic()) {
                nextRun++;
            }
            final long upper = nextRun < history.size() ? history.get(nextRun).appliedLsn() : Long.MAX_VALUE;
            // Archive metadata can share the inclusive LSN at adjacent file boundaries.
            if (sourceMagic == history.get(runStart).databaseMagic()
                    && firstLsn >= history.get(runStart).appliedLsn() && lastLsn <= upper) {
                return true;
            }
            runStart = nextRun;
        }
        return false;
    }
}
