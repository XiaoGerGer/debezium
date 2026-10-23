/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng.logminer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.zip.CRC32C;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class LogMinerSpillStoreTest {
    @TempDir
    Path directory;

    @Test
    void shouldPreserveDiagnosticsAndEveryUtf16CodeUnit() throws Exception {
        final char[] characters = new char[Character.MAX_VALUE + 1];
        for (int i = 0; i < characters.length; i++) {
            characters[i] = (char) i;
        }
        final var row = row(1, new String(characters));
        try (var store = new LogMinerSpillStore(directory, 1048576)) {
            final var file = store.create();
            final long size = store.append(file, row);
            store.flush();
            final byte[] legacy = referenceFrame(referencePayload(row));
            assertThat(Files.readAllBytes(file)).containsExactly(legacy);
            try (var reader = store.read(file, size, 1, size)) {
                assertThat(reader.next()).isEqualTo(row);
                assertThat(reader.next()).isNull();
            }
        }
    }

    @Test
    void shouldReuseBuffersWithoutChangingEarlierRowsOrIncludingStaleBytes() throws Exception {
        final List<LogMinerRow> rows = new ArrayList<>();
        for (int length : new int[]{ 1, 4097, 16000, 32768, 50000, 0, 32, 9000 }) {
            rows.add(row(rows.size(), "中x\uD83D\uDE00\uD800\uDC00".repeat(length / 6 + 1).substring(0, length)));
        }
        rows.add(row(rows.size(), null));
        try (var store = new LogMinerSpillStore(directory, 2097152)) {
            final var first = store.create();
            final var second = store.create();
            long size = 0;
            long maxFrame = 0;
            for (var row : rows) {
                final long frame = store.append(first, row);
                size += frame;
                maxFrame = Math.max(maxFrame, frame);
                assertThat(store.append(second, row)).isEqualTo(frame);
            }
            store.flush();
            final var expected = new ByteArrayOutputStream();
            for (var row : rows) {
                expected.write(referenceFrame(referencePayload(row)));
            }
            assertThat(Files.readAllBytes(first)).containsExactly(expected.toByteArray());
            assertThat(Files.readAllBytes(second)).containsExactly(expected.toByteArray());
            // Two independent cursors must also remain isolated from each other's scratch space.
            final List<LogMinerRow> firstRead = new ArrayList<>();
            final List<LogMinerRow> secondRead = new ArrayList<>();
            try (var left = store.read(first, size, rows.size(), maxFrame);
                    var right = store.read(second, size, rows.size(), maxFrame)) {
                for (int i = 0; i < rows.size(); i++) {
                    firstRead.add(left.next());
                    secondRead.add(right.next());
                }
                assertThat(left.next()).isNull();
                assertThat(right.next()).isNull();
            }
            assertThat(firstRead).containsExactlyElementsOf(rows);
            assertThat(secondRead).containsExactlyElementsOf(rows);
            assertThat(store.bytes()).isEqualTo(2 * size);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "negative-string", "oversized-string", "truncated-fields", "trailing-fields" })
    void shouldRejectInvalidFrameContentsEvenWithAValidChecksum(String damage) throws Exception {
        byte[] payload = referencePayload(row(1, "small"));
        if (damage.equals("negative-string")) {
            ByteBuffer.wrap(payload).putInt(3 * Long.BYTES, -2);
        }
        else if (damage.equals("oversized-string")) {
            ByteBuffer.wrap(payload).putInt(3 * Long.BYTES, Integer.MAX_VALUE);
        }
        else if (damage.equals("truncated-fields")) {
            payload = Arrays.copyOf(payload, payload.length - 1);
        }
        else {
            payload = Arrays.copyOf(payload, payload.length + 1);
        }
        try (var store = new LogMinerSpillStore(directory, 1048576)) {
            final var file = store.create();
            final var frame = referenceFrame(payload);
            Files.write(file, frame);
            try (var reader = store.read(file, frame.length, 1, frame.length)) {
                assertThatThrownBy(reader::next).isInstanceOf(LogMinerSpillStore.StorageException.class)
                        .hasMessageContaining("decode temporary transaction file");
            }
        }
    }

    @Test
    void shouldRejectTruncationAfterOpeningTheFile() throws Exception {
        try (var store = new LogMinerSpillStore(directory, 1048576)) {
            final var file = store.create();
            final long size = store.append(file, row(1, "value".repeat(10000)));
            try (var reader = store.read(file, size, 1, size)) {
                final var original = Files.readAllBytes(file);
                Files.write(file, Arrays.copyOf(original, original.length - 1));
                assertThatThrownBy(reader::next).isInstanceOf(LogMinerSpillStore.StorageException.class);
            }
        }
    }

    // Independent reference encoder for the disposable transaction frame format.
    private static byte[] referencePayload(LogMinerRow row) throws Exception {
        final var bytes = new ByteArrayOutputStream();
        try (var data = new DataOutputStream(bytes)) {
            data.writeLong(row.lsn());
            data.writeLong(row.startLsn());
            data.writeLong(row.commitLsn());
            referenceString(data, row.transactionId());
            data.writeInt(row.operation());
            data.writeInt(row.rollback());
            referenceString(data, row.owner());
            referenceString(data, row.tableName());
            referenceString(data, row.username());
            referenceString(data, row.redo());
            referenceString(data, row.recordSetId());
            data.writeInt(row.sequence());
            data.writeInt(row.continuation());
            data.writeLong(row.timestamp().getEpochSecond());
            data.writeInt(row.timestamp().getNano());
            data.writeLong(row.objectId());
            data.writeLong(row.objectVersion());
            data.writeLong(row.archiveSequence());
            data.writeLong(row.blockNumber());
            data.writeInt(row.byteOffset());
        }
        return bytes.toByteArray();
    }

    private static void referenceString(DataOutputStream data, String value) throws Exception {
        data.writeInt(value == null ? -1 : value.length());
        if (value != null) {
            data.writeChars(value);
        }
    }

    private static byte[] referenceFrame(byte[] payload) throws Exception {
        final var bytes = new ByteArrayOutputStream();
        final var checksum = new CRC32C();
        checksum.update(payload);
        try (var data = new DataOutputStream(bytes)) {
            data.writeInt(payload.length);
            data.writeInt((int) checksum.getValue());
            data.write(payload);
        }
        return bytes.toByteArray();
    }

    private static LogMinerRow row(long serial, String sql) {
        return new LogMinerRow(Long.MAX_VALUE - serial, serial, Long.MAX_VALUE, "transaction-" + serial, 3, 0,
                "模式", "mixed\"Table", "", sql, null, (int) serial, 0, Instant.parse("2026-10-16T01:02:03.123456789Z"), 7, serial, 0, 123456, 78);
    }
}
