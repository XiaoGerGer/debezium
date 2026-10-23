/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.ResolverStyle;
import java.time.format.SignStyle;
import java.time.temporal.ChronoField;

/** DM prints years without zero padding; retain its proleptic calendar and signed BCE years. */
final class DamengTemporalValues {
    private static final DateTimeFormatter DATE = new DateTimeFormatterBuilder()
            .appendValue(ChronoField.YEAR, 1, 9, SignStyle.NORMAL).appendLiteral('-')
            .appendValue(ChronoField.MONTH_OF_YEAR, 2).appendLiteral('-')
            .appendValue(ChronoField.DAY_OF_MONTH, 2).toFormatter().withResolverStyle(ResolverStyle.STRICT);
    private static final DateTimeFormatter TIMESTAMP = new DateTimeFormatterBuilder()
            .append(DATE).appendLiteral('T').append(DateTimeFormatter.ISO_LOCAL_TIME).toFormatter().withResolverStyle(ResolverStyle.STRICT);
    private static final DateTimeFormatter OFFSET_TIMESTAMP = new DateTimeFormatterBuilder()
            .append(TIMESTAMP).appendOffsetId().toFormatter().withResolverStyle(ResolverStyle.STRICT);

    private DamengTemporalValues() {
    }

    static LocalDate date(String value) {
        return value == null ? null : LocalDate.parse(value, DATE);
    }

    static LocalDateTime timestamp(String value) {
        return value == null ? null : LocalDateTime.parse(value.replace(' ', 'T'), TIMESTAMP);
    }

    static OffsetDateTime offsetTimestamp(String value) {
        if (value == null) {
            return null;
        }
        // The date can contain a sign and a variable-width year, so the separator has no fixed position.
        return OffsetDateTime.parse(value.trim().replaceFirst(" ", "T").replace(" +", "+").replace(" -", "-"), OFFSET_TIMESTAMP);
    }
}
