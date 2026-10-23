/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng;

import java.math.BigInteger;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/** Shared real-database inputs for both complete-engine and JDBC/redo component tests. */
final class DamengDatatypeCases {
    record Value(String sql, Object parameter) {
        static Value sql(String expression) {
            return new Value(expression, null);
        }

        static Value text(String text) {
            return new Value("?", text);
        }

        void insert(Connection connection, String table, int id) throws Exception {
            try (var statement = connection.prepareStatement("INSERT INTO " + table + " VALUES(?," + sql + ")")) {
                statement.setInt(1, id);
                if (parameter instanceof Float number) {
                    statement.setFloat(2, number);
                }
                else if (parameter instanceof Double number) {
                    statement.setDouble(2, number);
                }
                else if (parameter instanceof byte[] bytes) {
                    statement.setBytes(2, bytes);
                }
                else if (parameter instanceof String text) {
                    statement.setString(2, text);
                }
                statement.executeUpdate();
            }
            catch (SQLException rejected) {
                throw new SQLException("Database rejected input " + description(), rejected.getSQLState(), rejected.getErrorCode(), rejected);
            }
        }

        void update(Connection connection, String table, int id) throws Exception {
            try (var statement = connection.prepareStatement("UPDATE " + table + " SET VALUE=" + sql + " WHERE ID=" + id)) {
                if (parameter instanceof Float number) {
                    statement.setFloat(1, number);
                }
                else if (parameter instanceof Double number) {
                    statement.setDouble(1, number);
                }
                else if (parameter instanceof byte[] bytes) {
                    statement.setBytes(1, bytes);
                }
                else if (parameter instanceof String text) {
                    statement.setString(1, text);
                }
                statement.executeUpdate();
            }
        }

        String description() {
            if (parameter instanceof String text) {
                return "JDBC string(length=" + text.length() + "):" + text.substring(0, Math.min(text.length(), 80))
                        .replace('\n', ' ').replace('\t', ' ').replace("\u0000", "\\0");
            }
            if (parameter instanceof byte[] bytes) {
                return "JDBC bytes(length=" + bytes.length + ")";
            }
            return parameter == null ? sql : "JDBC " + parameter;
        }
    }

    record TypeCase(String name, String declaration, List<Value> values, List<Value> rejected, Map<String, String> settings, String storage) {
        @Override
        public String toString() {
            return name;
        }
    }

    private DamengDatatypeCases() {
    }

    static Stream<TypeCase> boundaries() {
        final List<TypeCase> cases = new ArrayList<>();
        integer(cases, "TINYINT", "-128", "127");
        integer(cases, "SMALLINT", "-32768", "32767");
        integer(cases, "INT", "-2147483648", "2147483647");
        integer(cases, "BIGINT", "-9223372036854775808", "9223372036854775807");
        for (var declaration : List.of("INTEGER", "PLS_INTEGER", "BYTE")) {
            cases.add(type(declaration, List.of(Value.sql("-1"), Value.sql("0"), Value.sql("1")), List.of()));
        }
        cases.add(type("BIT", List.of(Value.sql("0"), Value.sql("1"), Value.sql("-1"), Value.sql("2")), List.of()));

        // Exercise every legal precision, and scale endpoints/midpoints at each precision.
        // DECIMAL, NUMERIC and NUMBER are synonyms in native DM, but metadata and DDL must agree.
        for (int precision = 1; precision <= 38; precision++) {
            for (int scale : IntStream.of(0, precision / 2, precision - 1, precision).distinct().toArray()) {
                final var maximum = (precision == scale ? "0" : "9".repeat(precision - scale))
                        + (scale == 0 ? "" : "." + "9".repeat(scale));
                final var unit = scale == 0 ? "1" : "0." + "0".repeat(scale - 1) + "1";
                final var overflow = "1" + "0".repeat(precision - scale);
                final var declaration = "DECIMAL(" + precision + "," + scale + ")";
                cases.add(type(declaration, List.of(Value.sql(maximum), Value.sql("-" + maximum), Value.sql(unit), Value.sql("0")),
                        List.of(Value.sql(overflow), Value.sql("-" + overflow))));
            }
        }
        for (var declaration : List.of("NUMERIC(38,18)", "NUMBER(38,18)", "DEC(12,6)", "DECIMAL(12,6)")) {
            cases.add(type(declaration, List.of(Value.sql("123.000000"), Value.sql("123.1234567"), Value.sql("-123.1234567")), List.of()));
        }
        // TODO Retest +/-1E125 after a DM upgrade: 8.1.2.128 rejects JDBC and SQL inputs
        // with overflow (-6102). Verify column storage separately from expression-result limits;
        // retain these inputs and compare accepted snapshot/redo values using each decimal mode.
        for (var declaration : List.of("NUMBER", "DECIMAL", "NUMERIC")) {
            for (var mode : List.of("precise", "string", "double")) {
                cases.add(new TypeCase(declaration + "/" + mode, declaration,
                        List.of(Value.sql("0"), Value.sql("1E-128"), Value.sql("-1E-128"), Value.sql("1E125"), Value.sql("-1E125"),
                                Value.sql("12345678901234567890123456789012345678"), Value.sql("1.234567890123456789E-30")),
                        List.of(Value.sql("1E126"), Value.sql("-1E126")), Map.of("decimal.handling.mode", mode), ""));
            }
        }
        for (var mode : List.of("string", "double")) {
            cases.add(new TypeCase("DECIMAL(38,18)/" + mode, "DECIMAL(38,18)",
                    List.of(Value.sql("99999999999999999999.999999999999999999"), Value.sql("0.000000000000000001")),
                    List.of(), Map.of("decimal.handling.mode", mode), ""));
        }

        for (var declaration : List.of("REAL", "FLOAT(1)", "FLOAT(24)", "FLOAT(25)", "FLOAT", "FLOAT(53)", "FLOAT(126)", "DOUBLE", "DOUBLE PRECISION")) {
            final boolean single = declaration.equals("REAL") || declaration.equals("FLOAT(1)") || declaration.equals("FLOAT(24)");
            final double bound = single ? 3.4e38f : Double.MAX_VALUE;
            final double normal = single ? Float.MIN_NORMAL : Double.MIN_NORMAL;
            final double subnormal = single ? Float.MIN_VALUE : Double.MIN_VALUE;
            final double aboveOne = single ? Math.nextUp(1.0f) : Math.nextUp(1.0d);
            final double belowOne = single ? Math.nextDown(1.0f) : Math.nextDown(1.0d);
            final List<Value> values = new ArrayList<>();
            for (double value : new double[]{ -bound, -normal, -subnormal, -1.23456789012345, -0.0, 0.0,
                    subnormal, normal, belowOne, 1.0, aboveOne, 1.23456789012345, bound }) {
                // Preserve the parameter's Java type (a float/double ternary would promote to double).
                final Object parameter;
                if (single) {
                    parameter = (float) value;
                }
                else {
                    parameter = value;
                }
                values.add(new Value("?", parameter));
            }
            cases.add(type(declaration, values, List.of(Value.sql(single ? "3.5E38" : "1E309"))));
        }
        cases.add(type("REAL", List.of(new Value("?", Float.NaN)), List.of()));
        cases.add(type("DOUBLE", List.of(new Value("?", Double.NaN)), List.of()));

        cases.add(type("DATE", Stream.of("-4712-01-01", "0001-01-01", "1582-10-04", "1582-10-10", "1582-10-15",
                "1969-12-31", "1970-01-01", "2000-02-29", "9999-12-31")
                .map(value -> Value.sql("DATE'" + value + "'")).toList(),
                List.of(Value.sql("DATE'2023-02-29'"), Value.sql("DATE'10000-01-01'"))));
        for (int precision = 0; precision <= 6; precision++) {
            final var fraction = precision == 0 ? "" : "." + "9".repeat(precision);
            cases.add(type("TIME(" + precision + ")", List.of(Value.sql("TIME'00:00:00'"),
                    Value.sql("TIME'23:59:59" + fraction + "'"), Value.sql("TIME'12:34:56.123456'")), List.of(Value.sql("TIME'24:00:00'"))));
            cases.add(type("TIME(" + precision + ") WITH TIME ZONE", List.of(Value.sql("TIME'00:00:00" + fraction + " +14:00'"),
                    Value.sql("TIME'23:59:59" + fraction + " -12:59'"), Value.sql("TIME'12:34:56.123456 +05:45'")), List.of()));
        }
        // TODO Retest TIMESTAMP(7..9), including WITH TIME ZONE, after a DM upgrade:
        // 8.1.2.128 rejects CREATE with "Precision is out of range". Once accepted, verify
        // fractional seconds and representable temporal bounds across snapshot, redo and JSON.
        for (int precision = 0; precision <= 9; precision++) {
            final var fraction = precision == 0 ? "" : "." + "9".repeat(precision);
            cases.add(type("TIMESTAMP(" + precision + ")", List.of(Value.sql("TIMESTAMP'0001-01-01 00:00:00'"),
                    Value.sql("TIMESTAMP'1969-12-31 23:59:59" + fraction + "'"),
                    Value.sql("TIMESTAMP'2000-02-29 12:34:56.123456789'"),
                    Value.sql("TIMESTAMP'9999-12-31 23:59:59" + fraction + "'")), List.of()));
            cases.add(type("TIMESTAMP(" + precision + ") WITH TIME ZONE", List.of(Value.sql("TIMESTAMP'1969-12-31 23:59:59" + fraction + " +14:00'"),
                    Value.sql("TIMESTAMP'2000-02-29 00:00:00" + fraction + " -12:59'")), List.of()));
        }
        cases.add(type("TIMESTAMP(6)", List.of(Value.sql("TIMESTAMP'-4712-01-01 00:00:00'")), List.of()));
        for (var declaration : List.of("TIME", "TIMESTAMP", "DATETIME", "TIMESTAMP(6) WITH LOCAL TIME ZONE")) {
            cases.add(type(declaration, List.of(Value.sql(declaration.equals("TIME") ? "TIME'23:59:59.123456'"
                    : "TIMESTAMP'1969-12-31 23:59:59.123456'")), List.of()));
        }
        for (var declaration : List.of("CHAR(8 BYTE)", "CHAR(8 CHAR)", "NCHAR(8)", "VARCHAR(8)", "VARCHAR(8 CHAR)", "NVARCHAR(8)", "VARCHAR2(8)")) {
            cases.add(type(declaration, Stream.of("", " ", "a", "abcdefgh", "达梦", "a'b\\c\n", "中文ab", "a\u0000b")
                    .map(Value::text).toList(), List.of(Value.text("a".repeat(9)))));
        }
        final int unicodeBytes = "梦".getBytes(DamengTestDatabase.charset()).length;
        final var byteBoundary = "梦".repeat(8 / unicodeBytes) + "a".repeat(8 % unicodeBytes);
        final var belowByteBoundary = "梦".repeat(7 / unicodeBytes) + "a".repeat(7 % unicodeBytes);
        for (var declaration : List.of("CHAR(8 BYTE)", "VARCHAR(8)")) {
            cases.add(type(declaration, List.of(Value.text(byteBoundary), Value.text(belowByteBoundary), Value.text("😀ab")),
                    List.of(Value.text(byteBoundary + "a"))));
        }
        cases.add(type("NVARCHAR(8)", List.of(Value.text("梦".repeat(8)), Value.text("梦".repeat(7)), Value.text("")),
                List.of(Value.text("梦".repeat(9)))));
        // TODO Retest CHAR(8 CHAR) after a DM upgrade: 8.1.2.128 with GB18030 and
        // LENGTH_IN_CHAR=0 rejects 7/8 Chinese characters and 8 emoji through both JDBC
        // (-5403) and SQL (-6169). Verify explicit character-length semantics, stored padding
        // and snapshot/redo consistency; retain the original multibyte boundary inputs.
        for (var declaration : List.of("CHAR(8 CHAR)", "VARCHAR(8 CHAR)")) {
            cases.add(type(declaration, List.of(Value.text("梦".repeat(8)), Value.text("梦".repeat(7)), Value.text("😀".repeat(8))),
                    List.of(Value.text("梦".repeat(9)))));
        }
        for (var declaration : List.of("VARCHAR", "NVARCHAR", "VARCHAR2", "NVARCHAR2(8)")) {
            cases.add(type(declaration, List.of(Value.text(""), Value.text("达梦ab")), List.of()));
        }
        cases.add(type("ROWID", List.of(Value.text("AAABAAAAACAAAAAAAy")), List.of()));
        cases.add(type("CHAR", List.of(Value.text(""), Value.text("a")), List.of(Value.text("ab"))));
        for (var declaration : List.of("BINARY(8)", "RAW(8)", "VARBINARY(8)")) {
            for (var mode : List.of("bytes", "base64", "base64-url-safe", "hex")) {
                cases.add(new TypeCase(declaration + "/" + mode, declaration,
                        Stream.concat(Stream.of(new Value("?", new byte[0])),
                                Stream.of("0x00", "0xFF", "0x000180FF", "0x00010203040580FF").map(Value::sql)).toList(),
                        List.of(Value.sql("0x000102030405060708")), Map.of("binary.handling.mode", mode), ""));
            }
        }
        cases.add(type("BINARY", List.of(new Value("?", new byte[0]), Value.sql("0xFF")), List.of(Value.sql("0xFF00"))));
        // LONG ROW is required to separate the declared SQL length limit from the page's row limit.
        cases.add(new TypeCase("VARCHAR declared maximum / LONG ROW", "VARCHAR(32767)",
                List.of(Value.text("a".repeat(32766)), Value.text("a".repeat(32767))),
                List.of(Value.text("a".repeat(32768))), Map.of(), " STORAGE(USING LONG ROW)"));
        cases.add(new TypeCase("VARBINARY declared maximum / LONG ROW", "VARBINARY(32767)",
                List.of(new Value("?", new byte[32766]), new Value("?", new byte[32767])),
                List.of(new Value("?", new byte[32768])), Map.of(), " STORAGE(USING LONG ROW)"));
        intervals().forEach(cases::add);
        // Fatal decoding of one ancient date must not prevent an independent future/leap-day
        // input from reaching the full engine. Keep these inputs in separate test invocations.
        return cases.stream().flatMap(type -> {
            if (type.declaration().equals("DATE") || type.declaration().startsWith("TIMESTAMP(") && !type.declaration().contains("TIME ZONE")
                    || List.of("NUMBER", "NUMERIC", "DECIMAL", "CHAR(8 CHAR)").contains(type.declaration())) {
                return IntStream.range(0, type.values().size()).mapToObj(index -> new TypeCase(
                        type.name() + "/input=" + type.values().get(index).description(), type.declaration(), List.of(type.values().get(index)),
                        index == 0 ? type.rejected() : List.of(), type.settings(), type.storage()));
            }
            return Stream.of(type);
        });
    }

    static Stream<TypeCase> intervals() {
        final List<TypeCase> cases = new ArrayList<>();
        final List<String> fields = List.of("YEAR", "MONTH", "DAY", "HOUR", "MINUTE", "SECOND");
        for (int first = 0; first < fields.size(); first++) {
            for (int last = first; last < (first < 2 ? 2 : fields.size()); last++) {
                final var qualifier = fields.get(first) + (first == last ? "" : " TO " + fields.get(last));
                final var defaultDeclaration = "INTERVAL " + qualifier;
                cases.add(intervalCase(defaultDeclaration, qualifier, first, last, 2, last == 5 ? 6 : 0));
                for (int precision = 1; precision <= 9; precision++) {
                    for (int fraction = 0; fraction <= (last == 5 ? 6 : 0); fraction++) {
                        final var preciseQualifier = fields.get(first) + "(" + precision
                                + (first == 5 ? "," + fraction : "") + ")"
                                + (first == last ? "" : " TO " + fields.get(last) + (last == 5 ? "(" + fraction + ")" : ""));
                        cases.add(intervalCase("INTERVAL " + preciseQualifier, preciseQualifier, first, last, precision, fraction));
                    }
                }
            }
        }
        return cases.stream();
    }

    private static TypeCase intervalCase(String declaration, String qualifier, int first, int last, int precision, int fraction) {
        final StringBuilder maximum = new StringBuilder("9".repeat(precision));
        final StringBuilder zero = new StringBuilder("0");
        final StringBuilder unit = new StringBuilder(first == last ? fraction == 0 ? "1" : "0" : "0");
        for (int field = first + 1; field <= last; field++) {
            final var separator = field == 1 ? "-" : field == 3 && first == 2 ? " " : ":";
            maximum.append(separator).append(field == 1 ? "11" : field == 3 ? "23" : "59");
            zero.append(separator).append("0");
            unit.append(separator).append(field == last && fraction == 0 ? "1" : "0");
        }
        if (fraction > 0) {
            maximum.append('.').append("9".repeat(fraction));
            unit.append('.').append("0".repeat(fraction - 1)).append('1');
        }
        final List<Value> values = Stream.of(maximum.toString(), "-" + maximum, zero.toString(), unit.toString(), "-" + unit)
                .map(body -> Value.sql("INTERVAL '" + body + "' " + qualifier)).toList();
        // Deliberately invalid leading values must be rejected by DM or checked if DM coerces them.
        final var overflow = "1" + "0".repeat(precision) + maximum.substring(precision);
        return type(declaration, values, Stream.of(overflow, "-" + overflow)
                .map(body -> Value.sql("INTERVAL '" + body + "' " + qualifier)).toList());
    }

    private static void integer(List<TypeCase> cases, String declaration, String minimum, String maximum) {
        final var min = new BigInteger(minimum);
        final var max = new BigInteger(maximum);
        cases.add(type(declaration, Stream.of(min, min.add(BigInteger.ONE), BigInteger.valueOf(-1), BigInteger.ZERO,
                BigInteger.ONE, max.subtract(BigInteger.ONE), max).map(value -> Value.sql(value.toString())).toList(),
                List.of(Value.sql(min.subtract(BigInteger.ONE).toString()), Value.sql(max.add(BigInteger.ONE).toString()))));
    }

    private static TypeCase type(String declaration, List<Value> values, List<Value> rejected) {
        return new TypeCase(declaration + " / " + values.size() + " values", declaration, values, rejected, Map.of(), "");
    }
}
