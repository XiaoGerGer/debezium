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
import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.relational.Column;
import io.debezium.relational.ValueConverter;
import io.debezium.time.Interval;
import io.debezium.time.IsoTimestamp;
import io.debezium.time.ZonedTimestamp;

public final class DamengValueConverters extends JdbcValueConverters {
    private static final Pattern STRING_FUNCTION = Pattern.compile("^([A-Za-z_]+)\\s*\\(\\s*'((?:''|[^'])*)'\\s*\\)$");
    private static final Pattern TEMPORAL_FUNCTION = Pattern.compile(
            "^(TO_DATE|TO_TIMESTAMP|TO_TIMESTAMP_TZ)\\s*\\(\\s*'((?:''|[^'])*)'\\s*,\\s*'((?:''|[^'])*)'\\s*\\)$",
            Pattern.CASE_INSENSITIVE);
    private static final Pattern TIMESTAMP_FORMAT = Pattern.compile("YYYY-MM-DD HH24:MI:SS(?:\\.FF[1-9]?)?");
    private static final Pattern CHARACTER_LENGTH = Pattern.compile("(?is).*\\bCHAR\\s*\\).*", Pattern.CASE_INSENSITIVE);
    private static final Pattern NUMERIC_LITERAL = Pattern.compile("[-+]?(?:\\d+(?:\\.\\d*)?|\\.\\d+)(?:[eE][-+]?\\d+)?");
    private static final Pattern DATE_LITERAL = Pattern.compile("^DATE\\s*'([^']+)'$", Pattern.CASE_INSENSITIVE);
    private static final Pattern TIMESTAMP_LITERAL = Pattern.compile("^TIMESTAMP\\s*'([^']+)'$", Pattern.CASE_INSENSITIVE);
    private static final Pattern TIME_LITERAL = Pattern.compile("^TIME\\s*'([^']+)'$", Pattern.CASE_INSENSITIVE);
    private static final Pattern UNICODE_ESCAPE = Pattern.compile("\\\\([0-9a-fA-F]{4})");
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
        if (DamengInterval.isInterval(column)) {
            DamengInterval.type(column.typeName());
            return Interval.builder();
        }
        if (isLocalTimeZone(column)) {
            return ZonedTimestamp.builder();
        }
        if (useWideTimestampIsoString(column)) {
            return IsoTimestamp.builder();
        }
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
        if (DamengInterval.isInterval(column)) {
            return value -> DamengInterval.convert(value, column);
        }
        final ValueConverter delegate = isLocalTimeZone(column)
                ? value -> convertLocalTimeZoneAsUtc(value, column)
                : useWideTimestampIsoString(column)
                        ? value -> convertTimestampToUtcIsoString(column, field, value)
                        : hasVariableScale(column) && decimalMode == DecimalMode.PRECISE
                                ? value -> convertVariableScale(column, field, value)
                                : super.converter(column, field);
        if (delegate == null) {
            return null;
        }
        // A converter belongs to one historical column definition. Schema changes
        // rebuild it; no global cache can confuse equal names from different tables/versions.
        final ValueConverter normalizer = normalizer(column);
        return value -> convertLogMinerValue(value, column, delegate, normalizer);
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

    private static boolean isLocalTimeZone(Column column) {
        return column.jdbcType() == Types.TIMESTAMP && column.typeName() != null
                && column.typeName().toUpperCase(Locale.ROOT).replaceAll("\\s+", " ").contains("WITH LOCAL TIME ZONE");
    }

    private boolean useWideTimestampIsoString(Column column) {
        return column.jdbcType() == Types.TIMESTAMP && getTimePrecision(column) > 6
                && (column.typeName() == null || !column.typeName().toUpperCase(Locale.ROOT).replaceAll("\\s+", " ").contains("TIME ZONE"))
                && (temporalPrecisionMode == TemporalPrecisionMode.ADAPTIVE
                        || temporalPrecisionMode == TemporalPrecisionMode.ADAPTIVE_TIME_MICROSECONDS);
    }

    private static Object convertLocalTimeZoneAsUtc(Object value, Column column) {
        if (value == null) {
            return null;
        }
        final LocalDateTime timestamp;
        if (value instanceof LocalDateTime localDateTime) {
            timestamp = localDateTime;
        }
        else if (value instanceof String text) {
            timestamp = parseTimestamp(text);
        }
        else {
            throw new DataException("Unsupported Dameng TIMESTAMP WITH LOCAL TIME ZONE value for column " + column.name()
                    + ": " + value.getClass().getName());
        }
        // LogMiner omits the writer's zone. Treat its local calendar as UTC by policy.
        return timestamp.atOffset(ZoneOffset.UTC).format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
    }

    /** Converts LogMiner literals using the same mapping as snapshot JDBC values. */
    public Object convertLogMinerValue(Object value, Column column, ValueConverter converter) {
        return convertLogMinerValue(value, column, converter, normalizer(column));
    }

    private Object convertLogMinerValue(Object value, Column column, ValueConverter converter, ValueConverter normalizer) {
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
            return normalizer.convert(convertExpression(expression.sql(), column, converter));
        }
        if (value instanceof String text && isNumeric(column.jdbcType()) && NUMERIC_LITERAL.matcher(text).matches()) {
            // TODO Restore lossless REAL/FLOAT/DOUBLE streaming with a verified DM log API.
            // Tested build 08134283904-20220804-166351-20005 renders SQL_REDO with six
            // fractional digits (1.23456789012345 -> 1.234568; tiny nonzero values -> zero).
            // Online-dictionary MINE_VALUE returns the same rounded values. Parsing cannot
            // recover the original bits; reading the current row cannot reconstruct historical
            // updates/deletes. The remaining lossless-recovery limitation is documented in the connector manual.
            // BigDecimal erases the sign of zero. Parse floating types directly from the literal.
            if (column.jdbcType() == Types.REAL) {
                return converter.convert(Float.valueOf(text));
            }
            if (column.jdbcType() == Types.FLOAT || column.jdbcType() == Types.DOUBLE) {
                return converter.convert(Double.valueOf(text));
            }
            return normalizer.convert(converter.convert(new BigDecimal(text)));
        }
        if (value instanceof String text) {
            switch (column.jdbcType()) {
                case Types.BINARY:
                case Types.VARBINARY:
                case Types.LONGVARBINARY:
                    // DM represents an empty binary image as the quoted string '0x'.
                    // Interpret it only for a physical binary column, never for character data.
                    if (text.equalsIgnoreCase("0x")) {
                        return converter.convert(new byte[0]);
                    }
                    break;
                case Types.DATE:
                    return converter.convert(DamengTemporalValues.date(text));
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
        return normalizer.convert(converter.convert(value));
    }

    private ValueConverter normalizer(Column column) {
        return switch (column.jdbcType()) {
            case Types.TINYINT -> value -> value instanceof Number ? ((Number) value).byteValue() : value;
            case Types.SMALLINT -> value -> value instanceof Number ? ((Number) value).shortValue() : value;
            case Types.INTEGER -> value -> value instanceof Number ? ((Number) value).intValue() : value;
            case Types.BIGINT -> value -> value instanceof Number ? ((Number) value).longValue() : value;
            case Types.CHAR, Types.NCHAR -> characterPadding(column);
            default -> ValueConverter.passthrough();
        };
    }

    private ValueConverter characterPadding(Column column) {
        final boolean characterLength = column.typeExpression() != null && CHARACTER_LENGTH.matcher(column.typeExpression()).matches();
        final Charset charset = column.charsetName() == null ? databaseCharset : Charset.forName(column.charsetName());
        final int width = column.length();
        return value -> value instanceof String text ? padCharacter(text, width, characterLength, charset) : value;
    }

    private static String padCharacter(String text, int width, boolean characterLength, Charset charset) {
        final int length = characterLength ? text.codePointCount(0, text.length()) : text.getBytes(charset).length;
        return length < width ? text + " ".repeat(width - length) : text;
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
        if (text.equalsIgnoreCase("NAN")) {
            if (column.jdbcType() == Types.REAL) {
                return converter.convert(Float.NaN);
            }
            if (column.jdbcType() == Types.FLOAT || column.jdbcType() == Types.DOUBLE) {
                return converter.convert(Double.NaN);
            }
            throw new DataException("LogMiner NaN requires a floating-point column: " + column.name());
        }
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
            final Matcher dateLiteral = DATE_LITERAL.matcher(text);
            if (dateLiteral.matches()) {
                return converter.convert(DamengTemporalValues.date(dateLiteral.group(1)));
            }
            final Matcher timestampLiteral = TIMESTAMP_LITERAL.matcher(text);
            if (timestampLiteral.matches()) {
                return converter.convert(column.jdbcType() == Types.TIMESTAMP_WITH_TIMEZONE
                        ? parseOffsetTimestamp(timestampLiteral.group(1))
                        : parseTimestamp(timestampLiteral.group(1)));
            }
            final Matcher timeLiteral = TIME_LITERAL.matcher(text);
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
            return DamengTemporalValues.date(literal);
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
        return DamengTemporalValues.timestamp(value);
    }

    private static OffsetDateTime parseOffsetTimestamp(String value) {
        return DamengTemporalValues.offsetTimestamp(value);
    }

    private static OffsetTime parseOffsetTime(String value) {
        // TODO Preserve TIME(p > 0) WITH TIME ZONE fractions when a verified DM log API supplies them.
        // Tested build 08134283904-20220804-166351-20005 retains .123456 in JDBC storage,
        // but SQL_REDO emits TIME'12:34:56 +05:45' without those fractional seconds.
        // Online-dictionary MINE_VALUE also omits them. This parser cannot infer missing digits;
        // querying the current row would corrupt historical replay. Validate snapshot and CRUD
        // consistency on the replacement API/build before claiming this limitation is resolved.
        return OffsetTime.parse(value.trim().replace(" +", "+").replace(" -", "-"));
    }

    private static String decodeUnicode(String value) {
        Matcher matcher = UNICODE_ESCAPE.matcher(value);
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
