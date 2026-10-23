/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng.logminer;

import java.util.Comparator;
import java.util.List;

import io.debezium.connector.dameng.DamengConnection.ArchiveLogFile;

/**
 * Selects a forward window using whole archive file lengths. Recovery history and
 * archive identity/continuity validation remain independent of this soft budget.
 */
public record LogMinerArchiveWindow(long endLsn, long fileBytes, int fileCount) {
    public static LogMinerArchiveWindow select(List<ArchiveLogFile> archives, long startLsn, long scannedThrough,
                                               long safeUpperLsn, long budgetBytes) {
        if (budgetBytes <= 0 || startLsn < 0 || scannedThrough < 0 || safeUpperLsn <= scannedThrough || startLsn > safeUpperLsn) {
            throw new IllegalArgumentException("Invalid Dameng forward archive window or byte budget");
        }
        final List<ArchiveLogFile> candidates = archives.stream()
                .filter(file -> file.lastLsn() >= startLsn && file.firstLsn() <= safeUpperLsn)
                .sorted(Comparator.comparingLong(ArchiveLogFile::firstLsn).thenComparing(ArchiveLogFile::path))
                .toList();
        long end = scannedThrough;
        long bytes = 0;
        int count = 0;
        boolean capped = false;
        for (var file : candidates) {
            // Inclusive archive boundaries can require the next file even when the
            // byte budget has been met. Account for it without extending the range.
            if (capped && file.firstLsn() > end) {
                break;
            }
            if (file.bytes() <= 0) {
                throw new IllegalStateException("Dameng archive LEN is missing or non-positive for " + file.path()
                        + "; cannot apply archive_bytes budget. Verify V$ARCH_FILE or explicitly select fixed windows");
            }
            bytes = file.bytes() > Long.MAX_VALUE - bytes ? Long.MAX_VALUE : bytes + file.bytes();
            count++;
            if (!capped) {
                end = Math.max(end, Math.min(file.lastLsn(), safeUpperLsn));
                capped = end > scannedThrough && (bytes >= budgetBytes || end == safeUpperLsn);
            }
        }
        if (end <= scannedThrough) {
            throw new IllegalStateException("No Dameng archive file advances the forward scan beyond LSN " + scannedThrough);
        }
        return new LogMinerArchiveWindow(end, bytes, count);
    }
}
