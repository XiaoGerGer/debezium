/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.SQLException;
import java.sql.Types;
import java.time.DateTimeException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.HexFormat;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.errors.DataException;

import io.debezium.data.VariableScaleDecimal;
import io.debezium.jdbc.JdbcValueConverters;
import io.debezium.relational.Column;
import io.debezium.relational.ValueConverter;

public final class DamengValueConverters extends JdbcValueConverters {
    private static final Pattern STRING_FUNCTION = Pattern.compile("^([A-Za-z_]+)\\s*\\(\\s*'((?:''|[^'])*)'\\s*\\)$");
    private static final Pattern TEMPORAL_FUNCTION = Pattern.compile(
            "^(TO_DATE|TO_TIMESTAMP|TO_TIMESTAMP_TZ)\\s*\\(\\s*'((?:''|[^'])*)'\\s*,\\s*'((?:''|[^'])*)'\\s*\\)$",
            Pattern.CASE_INSENSITIVE);
    private static final Pattern TIMESTAMP_FORMAT = Pattern.compile("YYYY-MM-DD HH24:MI:SS(?:\\.FF[1-9]?)?");
    private static final Pattern CHARACTER_LENGTH = Pattern.compile("(?is).*\\bCHAR\\s*\\).*", Pattern.CASE_INSENSITIVE);
    private final String unavailableValuePlaceholder;
    private final Charset databaseCharset;

    public DamengValueConverters(DamengConnectorConfig config) {
        this(config, StandardCharsets.UTF_8);
    }

    public DamengValueConverters(DamengConnectorConfig config, Charset databaseCharset) {
        super(config.getDecimalMode(), config.getTemporalPrecisionMode(), ZoneOffset.UTC, null, null, config.binaryHandlingMode());
        unavailableValuePlaceholder = config.getConfig().getString(DamengConnectorConfig.UNAVAILABLE_VALUE_PLACEHOLDER);
        this.databaseCharset = databaseCharset;
    }

    @Override
    public SchemaBuilder schemaBuilder(Column column) {
        if (hasVariableScale(column) && decimalMode == DecimalMode.PRECISE) {
            return VariableScaleDecimal.builder();
        }
        final SchemaBuilder builder = super.schemaBuilder(column);
        if (builder == null) {
            throw new DataException("Unsupported Dameng column type " + column.typeName() + " (JDBC " + column.jdbcType()
                    + ") for column " + column.name() + "; exclude this column with column.exclude.list");
        }
        return builder;
    }

    @Override
    public ValueConverter converter(Column column, Field field) {
        final ValueConverter delegate = hasVariableScale(column) && decimalMode == DecimalMode.PRECISE
                ? value -> convertVariableScale(column, field, value)
                : super.converter(column, field);
        return delegate == null ? null : value -> convertLogMinerValue(value, column, delegate);
    }

    @Override
    protected Object convertDecimal(Column column, Field field, Object data) {
        final Object converted = super.convertDecimal(column, field, data);
        if (converted instanceof BigDecimal decimal && !hasVariableScale(column) && column.scale().isPresent()) {
            return decimal.setScale(column.scale().get(), RoundingMode.UNNECESSARY);
        }
        return converted;
    }

    @Override
    protected Object convertTimestampWithZone(Column column, Field field, Object data) {
        final Object converted = super.convertTimestampWithZone(column, field, data);
        // JDBC presents a stored instant in the session time zone, while LogMiner can
        // retain the original literal's offset. Normalize both representations to UTC.
        return converted instanceof String timestamp
                ? OffsetDateTime.parse(timestamp).withOffsetSameInstant(ZoneOffset.UTC).format(DateTimeFormatter.ISO_OFFSET_DATE_TIME)
                : converted;
    }

    @Override
    protected Object convertTimeWithZone(Column column, Field field, Object data) {
        final Object converted = super.convertTimeWithZone(column, field, data);
        return converted instanceof String time
                ? OffsetTime.parse(time).withOffsetSameInstant(ZoneOffset.UTC).format(DateTimeFormatter.ISO_OFFSET_TIME)
                : converted;
    }

    private Object convertVariableScale(Column column, Field field, Object data) {
        final Object converted = super.convertDecimal(column, field, data);
        return converted == null ? null : VariableScaleDecimal.fromLogical(field.schema(), (BigDecimal) converted);
    }

    private static boolean hasVariableScale(Column column) {
        return (column.jdbcType() == Types.NUMERIC || column.jdbcType() == Types.DECIMAL) && column.length() <= 0;
    }

    /** Converts LogMiner literals using the same mapping as snapshot JDBC values. */
    public Object convertLogMinerValue(Object value, Column column, ValueConverter converter) {
        if (value == null) {
            return null;
        }
        try {
            if (value instanceof Clob clob) {
                return converter.convert(clob.getSubString(1, Math.toIntExact(clob.length())));
            }
            if (value instanceof Blob blob) {
                return converter.convert(blob.getBytes(1, Math.toIntExact(blob.length())));
            }
        }
        catch (SQLException | ArithmeticException e) {
            throw new DataException("Could not read Dameng LOB column " + column.name(), e);
        }
        if (value instanceof io.debezium.connector.dameng.logminer.parser.LogMinerDmlParser.SqlExpression expression) {
            return normalize(column, convertExpression(expression.sql(), column, converter));
        }
        if (value instanceof String text && isNumeric(column.jdbcType()) && text.matches("[-+]?(?:\\d+(?:\\.\\d*)?|\\.\\d+)(?:[eE][-+]?\\d+)?")) {
            return normalize(column, converter.convert(new BigDecimal(text)));
        }
        if (value instanceof String text) {
            switch (column.jdbcType()) {
                case Types.DATE:
                    return converter.convert(LocalDate.parse(text));
                case Types.TIMESTAMP:
                    return converter.convert(parseTimestamp(text));
                case Types.TIME:
                    return converter.convert(LocalTime.parse(text));
                case Types.TIMESTAMP_WITH_TIMEZONE:
                    return converter.convert(parseOffsetTimestamp(text));
                case Types.TIME_WITH_TIMEZONE:
                    return converter.convert(parseOffsetTime(text));
                case Types.BOOLEAN:
                case Types.BIT:
                    if (text.equals("0") || text.equalsIgnoreCase("false")) {
                        return converter.convert(false);
                    }
                    if (text.equals("1") || text.equalsIgnoreCase("true")) {
                        return converter.convert(true);
                    }
                    throw new DataException("Invalid Dameng boolean literal for column " + column.name());
                default:
            }
        }
        return normalize(column, converter.convert(value));
    }

    private Object normalize(Column column, Object value) {
        if (value == null) {
            return null;
        }
        return switch (column.jdbcType()) {
            case Types.TINYINT -> value instanceof Number ? ((Number) value).byteValue() : value;
            case Types.SMALLINT -> value instanceof Number ? ((Number) value).shortValue() : value;
            case Types.INTEGER -> value instanceof Number ? ((Number) value).intValue() : value;
            case Types.BIGINT -> value instanceof Number ? ((Number) value).longValue() : value;
            case Types.CHAR, Types.NCHAR -> value instanceof String text ? padCharacter(column, text) : value;
            default -> value;
        };
    }

    private String padCharacter(Column column, String text) {
        final boolean characterLength = column.typeExpression() != null && CHARACTER_LENGTH.matcher(column.typeExpression()).matches();
        final Charset charset = column.charsetName() == null ? databaseCharset : Charset.forName(column.charsetName());
        final int length = characterLength ? text.codePointCount(0, text.length()) : text.getBytes(charset).length;
        return length < column.length() ? text + " ".repeat(column.length() - length) : text;
    }

    @Override
    protected int getTimePrecision(Column column) {
        if ((column.jdbcType() == Types.TIME || column.jdbcType() == Types.TIMESTAMP
                || column.jdbcType() == Types.TIME_WITH_TIMEZONE || column.jdbcType() == Types.TIMESTAMP_WITH_TIMEZONE)
                && column.scale().isPresent() && column.scale().get() >= 0) {
            return column.scale().get();
        }
        return super.getTimePrecision(column);
    }

    private Object convertExpression(String expression, Column column, ValueConverter converter) {
        final String text = expression.trim();
        if (text.equalsIgnoreCase("OUT_CLOB") && (column.jdbcType() == Types.CLOB || column.jdbcType() == Types.NCLOB)) {
            return converter.convert(unavailableValuePlaceholder);
        }
        if (text.equalsIgnoreCase("OUT_BLOB") && column.jdbcType() == Types.BLOB) {
            return converter.convert(unavailableValuePlaceholder.getBytes(StandardCharsets.UTF_8));
        }
        final Matcher argument = STRING_FUNCTION.matcher(text);
        try {
            if (argument.matches()) {
                final String literal = argument.group(2).replace("''", "'");
                switch (argument.group(1).toUpperCase(Locale.ROOT)) {
                    case "HEXTORAW":
                        return converter.convert(HexFormat.of().parseHex(literal));
                    case "UNISTR":
                        return converter.convert(decodeUnicode(literal));
                    case "TO_NUMBER":
                        return converter.convert(new BigDecimal(literal));
                    default:
                }
            }
            final Matcher temporal = TEMPORAL_FUNCTION.matcher(text);
            if (temporal.matches()) {
                return converter.convert(parseTemporalFunction(temporal.group(1), temporal.group(2).replace("''", "'"), temporal.group(3)));
            }
            final Matcher dateLiteral = Pattern.compile("^DATE\\s*'([^']+)'$", Pattern.CASE_INSENSITIVE).matcher(text);
            if (dateLiteral.matches()) {
                return converter.convert(LocalDate.parse(dateLiteral.group(1), DateTimeFormatter.ISO_LOCAL_DATE));
            }
            final Matcher timestampLiteral = Pattern.compile("^TIMESTAMP\\s*'([^']+)'$", Pattern.CASE_INSENSITIVE).matcher(text);
            if (timestampLiteral.matches()) {
                return converter.convert(column.jdbcType() == Types.TIMESTAMP_WITH_TIMEZONE
                        ? parseOffsetTimestamp(timestampLiteral.group(1))
                        : parseTimestamp(timestampLiteral.group(1)));
            }
            final Matcher timeLiteral = Pattern.compile("^TIME\\s*'([^']+)'$", Pattern.CASE_INSENSITIVE).matcher(text);
            if (timeLiteral.matches()) {
                return converter.convert(column.jdbcType() == Types.TIME_WITH_TIMEZONE
                        ? parseOffsetTime(timeLiteral.group(1))
                        : LocalTime.parse(timeLiteral.group(1)));
            }
            if (text.equalsIgnoreCase("EMPTY_BLOB()")) {
                return converter.convert(new byte[0]);
            }
            if (text.equalsIgnoreCase("EMPTY_CLOB()")) {
                return converter.convert("");
            }
        }
        catch (IllegalArgumentException | DateTimeException e) {
            throw new DataException("Invalid LogMiner value expression for column " + column.name() + ": " + text, e);
        }
        throw new DataException("Unsupported LogMiner value expression for column " + column.name() + ": " + text);
    }

    private static Object parseTemporalFunction(String function, String literal, String format) {
        final String normalizedFormat = format.trim().toUpperCase(Locale.ROOT);
        if (function.equalsIgnoreCase("TO_DATE") && normalizedFormat.equals("YYYY-MM-DD")) {
            return LocalDate.parse(literal);
        }
        if (function.equalsIgnoreCase("TO_TIMESTAMP_TZ")) {
            final String suffix = " TZH:TZM";
            if (normalizedFormat.endsWith(suffix)
                    && TIMESTAMP_FORMAT.matcher(normalizedFormat.substring(0, normalizedFormat.length() - suffix.length())).matches()) {
                return parseOffsetTimestamp(literal);
            }
        }
        else if (TIMESTAMP_FORMAT.matcher(normalizedFormat).matches()) {
            return parseTimestamp(literal);
        }
        throw new DataException("Unsupported LogMiner temporal format: " + format);
    }

    private static LocalDateTime parseTimestamp(String value) {
        return LocalDateTime.parse(value.replace(' ', 'T'));
    }

    private static OffsetDateTime parseOffsetTimestamp(String value) {
        String normalized = value.trim();
        if (normalized.length() > 10 && normalized.charAt(10) == ' ') {
            normalized = normalized.substring(0, 10) + "T" + normalized.substring(11);
        }
        return OffsetDateTime.parse(normalized.replace(" +", "+").replace(" -", "-"));
    }

    private static OffsetTime parseOffsetTime(String value) {
        return OffsetTime.parse(value.trim().replace(" +", "+").replace(" -", "-"));
    }

    private static String decodeUnicode(String value) {
        Matcher matcher = Pattern.compile("\\\\([0-9a-fA-F]{4})").matcher(value);
        StringBuilder result = new StringBuilder();
        while (matcher.find()) {
            matcher.appendReplacement(result, Matcher.quoteReplacement(String.valueOf((char) Integer.parseInt(matcher.group(1), 16))));
        }
        matcher.appendTail(result);
        return result.toString();
    }

    private static boolean isNumeric(int type) {
        return type == Types.TINYINT || type == Types.SMALLINT || type == Types.INTEGER || type == Types.BIGINT
                || type == Types.NUMERIC || type == Types.DECIMAL || type == Types.REAL || type == Types.FLOAT || type == Types.DOUBLE;
    }
}
