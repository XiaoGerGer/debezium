/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng.logminer;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.HashSet;
import java.util.Set;
import java.util.zip.CRC32C;

import org.apache.kafka.connect.errors.ConnectException;

/** Task-owned, disposable transaction files. Never used as a durable offset or recovery source. */
final class LogMinerSpillStore implements AutoCloseable {
    // Five longs, five ints and six nullable string length prefixes per row.
    private static final int FIXED_ROW_BYTES = 8 * Long.BYTES + 12 * Integer.BYTES;
    private static final int MAX_RETAINED_BUFFER_BYTES = 64 * 1024;
    private final Path parent;
    private final long limit;
    private final Set<Path> files = new HashSet<>();
    private final FrameBuffer encodingBuffer = new FrameBuffer();
    private final CRC32C checksum = new CRC32C();
    private Path directory;
    private Path writing;
    private DataOutputStream output;
    private long bytes;

    LogMinerSpillStore(Path parent, long limit) {
        this.parent = parent;
        this.limit = limit;
    }

    Path create() {
        try {
            if (directory == null) {
                Files.createDirectories(parent);
                directory = Files.createTempDirectory(parent, "dameng-tx-");
            }
            final var file = Files.createTempFile(directory, "tx-", ".bin");
            files.add(file);
            return file;
        }
        catch (IOException e) {
            throw failure("create temporary transaction file", e);
        }
    }

    long append(Path file, LogMinerRow row) {
        try {
            final long payloadBytes = FIXED_ROW_BYTES + stringBytes(row.transactionId()) + stringBytes(row.owner())
                    + stringBytes(row.tableName()) + stringBytes(row.username()) + stringBytes(row.redo()) + stringBytes(row.recordSetId());
            final long size = 8L + payloadBytes;
            if (size > limit - bytes) {
                throw new StorageException("Dameng transaction spill exceeds log.mining.buffer.spill.max.bytes=" + limit
                        + "; retain archive logs and free space or raise the limit before restarting", null);
            }
            if (payloadBytes > Integer.MAX_VALUE) {
                throw new IOException("Transaction frame exceeds the supported length");
            }
            final int length = (int) payloadBytes;
            final byte[] payload = encodingBuffer.get(length);
            final var data = ByteBuffer.wrap(payload, 0, length);
            data.putLong(row.lsn());
            data.putLong(row.startLsn());
            data.putLong(row.commitLsn());
            writeString(data, row.transactionId());
            data.putInt(row.operation());
            data.putInt(row.rollback());
            writeString(data, row.owner());
            writeString(data, row.tableName());
            writeString(data, row.username());
            writeString(data, row.redo());
            writeString(data, row.recordSetId());
            data.putInt(row.sequence());
            data.putInt(row.continuation());
            data.putLong(row.timestamp().getEpochSecond());
            data.putInt(row.timestamp().getNano());
            data.putLong(row.objectId());
            data.putLong(row.objectVersion());
            data.putLong(row.archiveSequence());
            data.putLong(row.blockNumber());
            data.putInt(row.byteOffset());
            if (!file.equals(writing)) {
                flush();
                output = new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(file, StandardOpenOption.APPEND)));
                writing = file;
            }
            checksum.reset();
            checksum.update(payload, 0, length);
            output.writeInt(length);
            output.writeInt((int) checksum.getValue());
            output.write(payload, 0, length);
            bytes += size;
            return size;
        }
        catch (IOException e) {
            throw failure("write temporary transaction file " + file, e);
        }
    }

    void flush() {
        if (output != null) {
            try {
                output.close();
            }
            catch (IOException e) {
                throw failure("close temporary transaction file " + writing, e);
            }
            finally {
                output = null;
                writing = null;
            }
        }
    }

    Reader read(Path file, long expectedBytes, long rows, long maxFrameBytes) {
        flush();
        try {
            if (Files.size(file) != expectedBytes) {
                throw new IOException("Transaction file length differs from the recorded length");
            }
            return new Reader(file, expectedBytes, rows, maxFrameBytes);
        }
        catch (IOException e) {
            throw failure("read temporary transaction file " + file, e);
        }
    }

    void delete(Path file, long size) {
        if (file.equals(writing)) {
            flush();
        }
        try {
            Files.delete(file);
            files.remove(file);
            bytes -= size;
        }
        catch (IOException e) {
            throw failure("delete temporary transaction file " + file, e);
        }
    }

    long bytes() {
        return bytes;
    }

    @Override
    public void close() {
        StorageException problem = null;
        try {
            flush();
        }
        catch (StorageException e) {
            problem = e;
        }
        for (var file : files) {
            try {
                Files.deleteIfExists(file);
            }
            catch (IOException e) {
                final var next = failure("clean temporary transaction file " + file, e);
                if (problem == null) {
                    problem = next;
                }
                else {
                    problem.addSuppressed(next);
                }
            }
        }
        if (directory != null) {
            try {
                Files.deleteIfExists(directory);
            }
            catch (IOException e) {
                final var next = failure("clean temporary transaction directory " + directory, e);
                if (problem == null) {
                    problem = next;
                }
                else {
                    problem.addSuppressed(next);
                }
            }
        }
        if (problem != null) {
            throw problem;
        }
        files.clear();
        bytes = 0;
    }

    private static long stringBytes(String value) {
        return value == null ? 0 : 2L * value.length();
    }

    // Preserve the existing big-endian UTF-16 code units, including unpaired
    // surrogates. Encode directly into the frame without per-character streams
    // or an intermediate byte array for each string.
    private static void writeString(ByteBuffer data, String value) {
        data.putInt(value == null ? -1 : value.length());
        if (value != null) {
            final byte[] target = data.array();
            int position = data.position();
            for (int i = 0; i < value.length(); i++) {
                final char character = value.charAt(i);
                target[position++] = (byte) (character >>> 8);
                target[position++] = (byte) character;
            }
            data.position(position);
        }
    }

    /** Reuse small frames; a single huge SQL must not permanently enlarge the scratch buffer. */
    private static final class FrameBuffer {
        private byte[] bytes = new byte[0];

        byte[] get(int size) {
            if (size > MAX_RETAINED_BUFFER_BYTES) {
                return new byte[size];
            }
            if (bytes.length < size) {
                bytes = new byte[Math.min(MAX_RETAINED_BUFFER_BYTES, Math.max(size, Math.max(8192, bytes.length * 2)))];
            }
            return bytes;
        }
    }

    static final class Reader implements AutoCloseable {
        private final DataInputStream input;
        private final Path file;
        private long remainingBytes;
        private long remainingRows;
        private final long maxFrameBytes;
        private final FrameBuffer decodingBuffer = new FrameBuffer();
        private final CRC32C checksum = new CRC32C();
        private char[] characters = new char[0];

        private Reader(Path file, long bytes, long rows, long maxFrameBytes) throws IOException {
            this.file = file;
            remainingBytes = bytes;
            remainingRows = rows;
            this.maxFrameBytes = maxFrameBytes;
            input = new DataInputStream(new BufferedInputStream(Files.newInputStream(file)));
        }

        LogMinerRow next() {
            try {
                if (remainingRows == 0) {
                    if (remainingBytes != 0 || input.read() != -1) {
                        throw new IOException("Unexpected trailing transaction data");
                    }
                    return null;
                }
                final int size = input.readInt();
                final int expectedChecksum = input.readInt();
                if (size <= 0 || size > remainingBytes - 8 || size > maxFrameBytes - 8) {
                    throw new IOException("Invalid transaction frame length");
                }
                final byte[] payload = decodingBuffer.get(size);
                input.readFully(payload, 0, size);
                checksum.reset();
                checksum.update(payload, 0, size);
                if ((int) checksum.getValue() != expectedChecksum) {
                    throw new IOException("Incomplete or corrupt transaction frame");
                }
                try {
                    final var data = ByteBuffer.wrap(payload, 0, size);
                    final var row = new LogMinerRow(data.getLong(), data.getLong(), data.getLong(), readString(data), data.getInt(), data.getInt(),
                            readString(data), readString(data), readString(data), readString(data), readString(data), data.getInt(), data.getInt(),
                            Instant.ofEpochSecond(data.getLong(), data.getInt()), data.getLong(), data.getLong(), data.getLong(), data.getLong(), data.getInt());
                    if (data.hasRemaining()) {
                        throw new IOException("Unexpected transaction frame data");
                    }
                    remainingBytes -= 8L + size;
                    remainingRows--;
                    return row;
                }
                catch (BufferUnderflowException e) {
                    throw new IOException("Incomplete transaction frame fields", e);
                }
            }
            catch (IOException e) {
                throw failure("decode temporary transaction file " + file, e);
            }
        }

        private String readString(ByteBuffer data) throws IOException {
            final int length = data.getInt();
            if (length == -1) {
                return null;
            }
            if (length < 0 || length > data.remaining() / Character.BYTES) {
                throw new IOException("Invalid transaction string length");
            }
            final char[] value;
            if (length > MAX_RETAINED_BUFFER_BYTES / Character.BYTES) {
                value = new char[length];
            }
            else {
                if (characters.length < length) {
                    characters = new char[Math.min(MAX_RETAINED_BUFFER_BYTES / Character.BYTES,
                            Math.max(length, Math.max(4096, characters.length * 2)))];
                }
                value = characters;
            }
            final byte[] source = data.array();
            int position = data.position();
            for (int i = 0; i < length; i++) {
                value[i] = (char) ((source[position++] & 0xff) << 8 | source[position++] & 0xff);
            }
            data.position(position);
            // Copy out of the reusable array so later rows cannot change this row.
            return new String(value, 0, length);
        }

        @Override
        public void close() {
            try {
                input.close();
            }
            catch (IOException e) {
                throw failure("close temporary transaction reader " + file, e);
            }
        }
    }

    private static StorageException failure(String action, IOException cause) {
        return new StorageException("Cannot " + action + "; stopping Dameng CDC without skipping history. "
                + "Check log.mining.buffer.spill.path, permissions and free disk space; retain archives for restart", cause);
    }

    /** Local storage failures require intervention, not a JDBC reconnect loop. */
    public static final class StorageException extends ConnectException {
        StorageException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
