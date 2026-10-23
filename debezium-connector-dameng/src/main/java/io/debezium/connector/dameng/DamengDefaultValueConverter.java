/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng;

import java.util.Optional;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.SchemaBuilder;

import io.debezium.connector.dameng.ddl.DamengSqlLexer;
import io.debezium.connector.dameng.ddl.DamengSqlLexer.Kind;
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
        if (literal.startsWith("'") || literal.regionMatches(true, 0, "N'", 0, 2) || literal.regionMatches(true, 0, "Q'", 0, 2)) {
            final var tokens = new DamengSqlLexer().tokenize(literal);
            if (tokens.size() != 1 || tokens.get(0).kind() != Kind.STRING) {
                return Optional.empty();
            }
            value = tokens.get(0).stringValue();
        }
        else if (literal.matches("[-+]?(?:\\d+(?:\\.\\d*)?|\\.\\d+)(?:[eE][-+]?\\d+)?")
                || literal.equalsIgnoreCase("true") || literal.equalsIgnoreCase("false")) {
            value = literal;
        }
        else if (DamengInterval.isInterval(column) && literal.regionMatches(true, 0, "INTERVAL", 0, "INTERVAL".length())) {
            value = new SqlExpression(literal);
        }
        else {
            return Optional.empty();
        }
        final SchemaBuilder builder = converters.schemaBuilder(column);
        if (builder == null) {
            return Optional.empty();
        }
        final Field field = new Field(column.name(), 0, builder.optional().build());
        return Optional.ofNullable(converters.converter(column, field).convert(value));
    }
}
