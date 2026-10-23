/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Base64;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;

import io.debezium.config.Configuration;
import io.debezium.data.VariableScaleDecimal;

import dm.jdbc.driver.DmdbIntervalDT;
import dm.jdbc.driver.DmdbIntervalYM;

/** Independent JDBC expectations; never calls a connector value converter to obtain expected values. */
final class DamengDatatypeAssertions {
    private DamengDatatypeAssertions() {
    }

    static Map<String, Object> storedRow(Connection connection, String table, int id, Configuration config) throws Exception {
        final Map<String, Object> values = new LinkedHashMap<>();
        try (var statement = connection.prepareStatement("SELECT * FROM " + table + " WHERE ID=?")) {
            statement.setInt(1, id);
            try (var result = statement.executeQuery()) {
                assertThat(result.next()).isTrue();
                final var metadata = result.getMetaData();
                for (int index = 1; index <= metadata.getColumnCount(); index++) {
                    final Object value;
                    if (result.getObject(index) == null) {
                        value = null;
                    }
                    else {
                        final int scale = metadata.getScale(index);
                        value = switch (metadata.getColumnType(index)) {
                            case Types.TINYINT -> result.getByte(index);
                            case Types.SMALLINT -> result.getShort(index);
                            case Types.INTEGER -> result.getInt(index);
                            case Types.BIGINT -> result.getLong(index);
                            case Types.NUMERIC, Types.DECIMAL -> decimal(result.getBigDecimal(index), config);
                            case Types.REAL -> result.getFloat(index);
                            case Types.FLOAT, Types.DOUBLE -> result.getDouble(index);
                            case Types.BOOLEAN, Types.BIT -> result.getBoolean(index);
                            case Types.DATE -> Math.toIntExact(LocalDate.parse(result.getString(index)).toEpochDay());
                            case Types.TIME -> {
                                final long nanos = LocalTime.parse(result.getString(index)).toNanoOfDay();
                                if (scale <= 3) {
                                    yield Math.toIntExact(nanos / 1_000_000);
                                }
                                yield nanos / (scale <= 6 ? 1_000 : 1);
                            }
                            case Types.TIMESTAMP -> timestamp(result.getString(index), scale);
                            // DM's native driver uses 94/95 in ResultSetMetaData.
                            case Types.TIMESTAMP_WITH_TIMEZONE, 95 -> OffsetDateTime.parse(result.getString(index)
                                    .replaceFirst(" ", "T").replace(" +", "+").replace(" -", "-"))
                                    .withOffsetSameInstant(ZoneOffset.UTC).format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
                            case Types.TIME_WITH_TIMEZONE, 94 -> OffsetTime.parse(result.getString(index).replace(" ", ""))
                                    .withOffsetSameInstant(ZoneOffset.UTC).format(DateTimeFormatter.ISO_OFFSET_TIME);
                            case Types.BINARY, Types.VARBINARY, Types.LONGVARBINARY -> binary(result.getBytes(index), config);
                            case Types.OTHER -> interval(result.getObject(index));
                            default -> result.getString(index);
                        };
                    }
                    values.put(metadata.getColumnName(index), value);
                }
                assertThat(result.next()).isFalse();
            }
        }
        return values;
    }

    private static String interval(Object value) {
        // Decode JDBC's binary-backed object independently of production's SQL literal parser.
        if (value instanceof DmdbIntervalYM interval) {
            return "P" + interval.getYear() + "Y" + interval.getMonth() + "M0DT0H0M0S";
        }
        if (value instanceof DmdbIntervalDT interval) {
            final int sign = interval.toString().contains("-'") ? -1 : 1;
            final var seconds = BigDecimal.valueOf(interval.getSecond())
                    .add(BigDecimal.valueOf(interval.getMsec(), 6)).multiply(BigDecimal.valueOf(sign));
            return "P0Y0M" + interval.getDay() * sign + "DT" + interval.getHour() * sign + "H"
                    + interval.getMinute() * sign + "M" + seconds.stripTrailingZeros().toPlainString() + "S";
        }
        throw new AssertionError("Unexpected JDBC OTHER value: " + value.getClass());
    }

    private static Object decimal(BigDecimal value, Configuration config) {
        return switch (config.getString("decimal.handling.mode", "precise")) {
            case "string" -> value.toPlainString();
            case "double" -> value.doubleValue();
            default -> value;
        };
    }

    private static Object binary(byte[] value, Configuration config) {
        return switch (config.getString("binary.handling.mode", "bytes")) {
            case "base64" -> Base64.getEncoder().encodeToString(value);
            case "base64-url-safe" -> Base64.getUrlEncoder().encodeToString(value);
            case "hex" -> HexFormat.of().formatHex(value);
            default -> value;
        };
    }

    private static Object timestamp(String value, int scale) {
        // Use the stored textual calendar representation, including BCE and the calendar transition,
        // independently of java.sql.Timestamp's hybrid GregorianCalendar conversion.
        final var time = LocalDateTime.parse(value.replaceFirst(" ", "T"));
        final long units = scale <= 3 ? 1_000 : scale <= 6 ? 1_000_000 : 1_000_000_000;
        final var expected = BigInteger.valueOf(time.toEpochSecond(ZoneOffset.UTC)).multiply(BigInteger.valueOf(units))
                .add(BigInteger.valueOf(time.getNano() / (1_000_000_000 / units)));
        // Retain an unrepresentable nanosecond boundary as an exact expectation, so wrapping
        // a long cannot masquerade as success and the fixture itself does not overflow.
        return expected.bitLength() < 64 ? expected.longValue() : expected;
    }

    static void assertRow(Struct row, Map<String, Object> expected, String stage) {
        assertAll(stage, () -> assertColumns(row, expected, stage + "/Connect"), () -> {
            if (row != null) {
                try (var converter = new JsonConverter()) {
                    converter.configure(Map.of("schemas.enable", true), false);
                    final var encoded = converter.fromConnectData("dameng.types", row.schema(), row);
                    final var decoded = converter.toConnectData("dameng.types", encoded);
                    assertThat(decoded.schema()).as(stage + "/JSON schema").isEqualTo(row.schema());
                    assertColumns((Struct) decoded.value(), expected, stage + "/JSON");
                }
            }
        });
    }

    /** Separate connector-output regression; callers must not treat this as a JSON round-trip check. */
    static void assertConnectRow(Struct row, Map<String, Object> expected, String stage) {
        assertColumns(row, expected, stage + "/Connect");
    }

    private static void assertColumns(Struct row, Map<String, Object> expected, String stage) {
        if (expected == null) {
            assertThat(row).as(stage).isNull();
            return;
        }
        assertThat(row).as(stage).isNotNull();
        assertThat(row.schema().fields()).as(stage + "/fields").hasSize(expected.size());
        assertAll(stage, expected.entrySet().stream().map(entry -> () -> {
            final var name = entry.getKey();
            final var value = entry.getValue();
            final var actual = row.get(name);
            final var label = stage + "/" + name;
            if (value instanceof byte[] bytes) {
                final byte[] actualBytes;
                if (actual instanceof ByteBuffer buffer) {
                    final var copy = buffer.duplicate();
                    actualBytes = new byte[copy.remaining()];
                    copy.get(actualBytes);
                }
                else {
                    actualBytes = (byte[]) actual;
                }
                assertThat(actualBytes).as(label).containsExactly(bytes);
            }
            else if (value instanceof Float number) {
                assertThat(actual).as(label + "/Java type").isInstanceOf(Float.class);
                assertThat(Float.floatToRawIntBits((Float) actual)).as("%s stored bits: JDBC=%s, event=%s", label, number, actual)
                        .isEqualTo(Float.floatToRawIntBits(number));
            }
            else if (value instanceof Double number) {
                assertThat(actual).as(label + "/Java type").isInstanceOf(Double.class);
                assertThat(Double.doubleToRawLongBits((Double) actual)).as("%s stored bits: JDBC=%s, event=%s", label, number, actual)
                        .isEqualTo(Double.doubleToRawLongBits(number));
            }
            else if (value instanceof BigDecimal decimal) {
                final var converted = actual instanceof Struct struct
                        ? VariableScaleDecimal.toLogical(struct).getDecimalValue().orElseThrow()
                        : (BigDecimal) actual;
                assertThat(converted).as(label).isEqualByComparingTo(decimal);
                if (!(actual instanceof Struct)) {
                    assertThat(converted.scale()).as(label + "/declared decimal scale").isEqualTo(decimal.scale());
                }
            }
            else if (value instanceof BigInteger integer) {
                assertThat(BigInteger.valueOf(((Number) actual).longValue())).as(label + "/timestamp exceeds signed 64-bit representation")
                        .isEqualTo(integer);
            }
            else {
                assertThat(actual).as(label).isEqualTo(value);
            }
        }));
    }
}
