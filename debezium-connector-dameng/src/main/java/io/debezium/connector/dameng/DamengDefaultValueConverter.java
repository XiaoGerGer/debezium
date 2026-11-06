/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng;

import java.sql.Types;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.Optional;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;

import io.debezium.connector.dameng.antlr.DamengAntlrSqlLexer;
import io.debezium.connector.dameng.antlr.DamengSqlToken.Kind;
import io.debezium.connector.dameng.logminer.parser.LogMinerDmlParser.SqlExpression;
import io.debezium.relational.Column;
import io.debezium.relational.DefaultValueConverter;

/** Decodes constant defaults without evaluating database expressions. */
public final class DamengDefaultValueConverter implements DefaultValueConverter {
    private final DamengValueConverters converters;

    public DamengDefaultValueConverter(DamengValueConverters converters) {
        this.converters = converters;
    }

    @Override
    public Optional<Object> parseDefaultValue(Column column, String expression) {
        if (expression == null || expression.equalsIgnoreCase("NULL")) {
            return Optional.empty();
        }
        final String literal = expression.trim();
        final Object value;
        if (literal.startsWith("'") || literal.regionMatches(true, 0, "N'", 0, 2) || literal.regionMatches(true, 0, "Q'", 0, 2)
                || literal.regionMatches(true, 0, "E'", 0, 2)) {
            final var tokens = new DamengAntlrSqlLexer().tokenize(literal);
            if (tokens.size() != 1 || tokens.get(0).kind() != Kind.STRING) {
                return Optional.empty();
            }
            value = tokens.get(0).stringValue();
        }
        else if (literal.regionMatches(true, 0, "0X", 0, 2)) {
            final var tokens = new DamengAntlrSqlLexer().tokenize(literal);
            if (tokens.size() != 1 || tokens.get(0).kind() != Kind.BINARY || !isBinary(column)) {
                return Optional.empty();
            }
            final var digits = tokens.get(0).text().substring(2);
            // DM supplies a zero high nibble when the first byte has only one hex digit.
            final var bytes = HexFormat.of().parseHex(digits.length() % 2 == 0 ? digits : "0" + digits);
            value = column.jdbcType() == Types.BINARY && column.length() > bytes.length ? Arrays.copyOf(bytes, column.length()) : bytes;
        }
        else if (literal.matches("[-+]?(?:\\d+(?:\\.\\d*)?|\\.\\d+)(?:[eE][-+]?\\d+)?")
                || literal.equalsIgnoreCase("true") || literal.equalsIgnoreCase("false")) {
            value = literal;
        }
        else if (DamengInterval.isInterval(column) && literal.regionMatches(true, 0, "INTERVAL", 0, "INTERVAL".length())) {
            value = new SqlExpression(literal);
        }
        else if (isTemporal(column) && DamengValueConverters.isTemporalExpression(literal)) {
            try {
                return convert(column, new SqlExpression(literal));
            }
            catch (DataException unsupported) {
                // Unknown formats remain database expressions, without evaluating
                // them against today's session or inventing a schema default.
                return Optional.empty();
            }
        }
        else {
            return Optional.empty();
        }
        return convert(column, value);
    }

    private Optional<Object> convert(Column column, Object value) {
        final SchemaBuilder builder = converters.schemaBuilder(column);
        if (builder == null) {
            return Optional.empty();
        }
        final Field field = new Field(column.name(), 0, builder.optional().build());
        final Object converted = converters.converter(column, field).convert(value);
        // KAFKA-12694: a Struct default cannot be attached to a SchemaBuilder.
        // Keep the database default expression in history and capture its stored
        // value, but omit the incompatible Connect schema default, as Oracle does.
        return converted instanceof Struct ? Optional.empty() : Optional.ofNullable(converted);
    }

    private static boolean isTemporal(Column column) {
        return switch (column.jdbcType()) {
            case Types.DATE, Types.TIME, Types.TIMESTAMP, Types.TIME_WITH_TIMEZONE, Types.TIMESTAMP_WITH_TIMEZONE -> true;
            default -> false;
        };
    }

    private static boolean isBinary(Column column) {
        return switch (column.jdbcType()) {
            case Types.BINARY, Types.VARBINARY, Types.LONGVARBINARY, Types.BLOB -> true;
            default -> false;
        };
    }
}
