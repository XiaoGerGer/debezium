/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Types;
import java.util.Locale;
import java.util.regex.Pattern;

import org.apache.kafka.connect.errors.DataException;

import io.debezium.connector.dameng.logminer.parser.LogMinerDmlParser.SqlExpression;
import io.debezium.relational.Column;
import io.debezium.time.Interval;

/** Exact native interval decoding shared by snapshot, redo and constant defaults. */
final class DamengInterval {
    private static final String UNIT = "YEAR|MONTH|DAY|HOUR|MINUTE|SECOND";
    private static final Pattern TYPE = Pattern.compile("^INTERVAL\\s+(" + UNIT + ")"
            + "(?:\\s*\\(\\s*(\\d+)\\s*(?:,\\s*(\\d+)\\s*)?\\))?"
            + "(?:\\s+TO\\s+(" + UNIT + ")(?:\\s*\\(\\s*(\\d+)\\s*\\))?)?$", Pattern.CASE_INSENSITIVE);
    private static final Pattern LITERAL = Pattern.compile("^INTERVAL\\s*([+-]?)\\s*'([+-]?[^']*)'\\s*(.+)$", Pattern.CASE_INSENSITIVE);

    enum Unit {
        YEAR,
        MONTH,
        DAY,
        HOUR,
        MINUTE,
        SECOND
    }

    record Type(Unit first, Unit last, int leadingPrecision, int fractionalPrecision) {
        String typeName() {
            return "INTERVAL " + first + (first == last ? "" : " TO " + last);
        }
    }

    private DamengInterval() {
    }

    static boolean isInterval(Column column) {
        return column.jdbcType() == Types.OTHER && isInterval(column.typeName());
    }

    static boolean isInterval(String typeName) {
        return typeName != null && typeName.strip().toUpperCase(Locale.ROOT).startsWith("INTERVAL ");
    }

    static Type type(String declaration) {
        return type(declaration, 2);
    }

    private static Type type(String declaration, int defaultLeadingPrecision) {
        final var matcher = TYPE.matcher(declaration.strip());
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Invalid Dameng interval declaration: " + declaration);
        }
        final var first = Unit.valueOf(matcher.group(1).toUpperCase(Locale.ROOT));
        final var last = matcher.group(4) == null ? first : Unit.valueOf(matcher.group(4).toUpperCase(Locale.ROOT));
        final int leading = matcher.group(2) == null ? defaultLeadingPrecision : Integer.parseInt(matcher.group(2));
        final var fraction = first == Unit.SECOND ? matcher.group(3) : matcher.group(5);
        final int fractional = last == Unit.SECOND ? fraction == null ? 6 : Integer.parseInt(fraction) : 0;
        if (leading < 1 || leading > 9 || fractional < 0 || fractional > 6
                || first.ordinal() > last.ordinal() || (first.ordinal() < Unit.DAY.ordinal()) != (last.ordinal() < Unit.DAY.ordinal())
                || matcher.group(3) != null && first != Unit.SECOND
                || matcher.group(5) != null && last != Unit.SECOND || matcher.group(4) != null && first == last) {
            throw new IllegalArgumentException("Invalid Dameng interval qualifier or precision: " + declaration);
        }
        return new Type(first, last, leading, fractional);
    }

    static String convert(Object value, Column column) {
        if (value == null) {
            return null;
        }
        final String text;
        if (value instanceof SqlExpression expression) {
            text = expression.sql();
        }
        else if (value instanceof String literal) {
            text = literal;
        }
        else {
            throw new DataException("Unsupported Dameng interval value for column " + column.name() + ": " + value.getClass().getName());
        }
        try {
            final var matcher = LITERAL.matcher(text.strip());
            if (!matcher.matches()) {
                throw new IllegalArgumentException("Expected a native INTERVAL literal");
            }
            // DM distinguishes a declaration's default precision (2) from a literal's (9).
            final var literalType = type("INTERVAL " + matcher.group(3), 9);
            final var columnType = type(column.typeName());
            if (literalType.first() != columnType.first() || literalType.last() != columnType.last()) {
                throw new IllegalArgumentException("Interval literal qualifier differs from the historical column");
            }
            String body = matcher.group(2).strip();
            String sign = matcher.group(1);
            if (body.startsWith("-") || body.startsWith("+")) {
                if (!sign.isEmpty()) {
                    throw new IllegalArgumentException("Multiple interval signs");
                }
                sign = body.substring(0, 1);
                body = body.substring(1);
            }
            final int multiplier = sign.equals("-") ? -1 : 1;
            // Only DAY separates its following field with whitespace; time fields use colons.
            final String separator = literalType.first() == Unit.YEAR ? "-" : ":";
            final var components = literalType.first() == Unit.DAY && literalType.last() != Unit.DAY
                    ? body.replaceFirst("\\s+", ":").split(separator, -1)
                    : body.split(separator, -1);
            if (literalType.first() == Unit.DAY && literalType.last() != Unit.DAY && !body.matches("[0-9]+\\s+[^\\s]+")) {
                throw new IllegalArgumentException("Expected a space between interval day and time");
            }
            if (components.length != literalType.last().ordinal() - literalType.first().ordinal() + 1) {
                throw new IllegalArgumentException("Incorrect interval component count");
            }
            final int[] units = new int[Unit.values().length];
            BigDecimal seconds = BigDecimal.ZERO;
            for (int index = 0; index < components.length; index++) {
                final var unit = Unit.values()[literalType.first().ordinal() + index];
                final var component = components[index];
                if (!component.matches(unit == Unit.SECOND ? "[0-9]+(?:\\.[0-9]+)?" : "[0-9]+")) {
                    throw new IllegalArgumentException("Invalid interval component");
                }
                final var number = new BigDecimal(component);
                final var limit = index == 0 ? BigDecimal.TEN.pow(literalType.leadingPrecision())
                        : BigDecimal.valueOf(unit == Unit.MONTH ? 12 : unit == Unit.HOUR ? 24 : 60);
                if (number.compareTo(limit) >= 0) {
                    throw new IllegalArgumentException("Interval component exceeds its range");
                }
                if (unit == Unit.SECOND) {
                    seconds = number.setScale(literalType.fractionalPrecision(), RoundingMode.UNNECESSARY).multiply(BigDecimal.valueOf(multiplier));
                }
                else {
                    units[unit.ordinal()] = number.intValueExact() * multiplier;
                }
            }
            return Interval.toIsoString(units[0], units[1], units[2], units[3], units[4], seconds);
        }
        catch (IllegalArgumentException | ArithmeticException failure) {
            throw new DataException("Invalid Dameng interval value for column " + column.name() + ": " + text, failure);
        }
    }
}
