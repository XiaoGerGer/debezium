/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng.datagen;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

final class BsegSchema {
    static final List<Column> COLUMNS = load();

    static String createTableSql(Config config) {
        String fields = COLUMNS.stream().map(c -> "    \"" + c.name + "\" " + c.sqlType()
                + " NOT NULL").collect(Collectors.joining(",\n"));
        String keys = COLUMNS.stream().filter(c -> c.key).map(c -> "\"" + c.name + "\"")
                .collect(Collectors.joining(", "));
        return "CREATE TABLE " + config.qualifiedTable() + " (\n" + fields
                + ",\n    PRIMARY KEY (" + keys + ")\n)";
    }

    static String insertSql(Config config) {
        String fields = COLUMNS.stream().map(c -> "\"" + c.name + "\"")
                .collect(Collectors.joining(","));
        return "INSERT INTO " + config.qualifiedTable() + " (" + fields + ") VALUES ("
                + String.join(",", Collections.nCopies(COLUMNS.size(), "?")) + ")";
    }

    static void validateExistingTable(Connection connection, Config config) throws SQLException {
        Map<String, ActualColumn> actual = new HashMap<>();
        try (ResultSet rs = connection.getMetaData().getColumns(null, config.schema, config.table, null)) {
            while (rs.next()) {
                if (!config.schema.equals(rs.getString("TABLE_SCHEM"))
                        || !config.table.equals(rs.getString("TABLE_NAME"))) {
                    continue;
                }
                actual.put(rs.getString("COLUMN_NAME"), new ActualColumn(
                        rs.getInt("COLUMN_SIZE"), rs.getInt("DECIMAL_DIGITS"), rs.getInt("DATA_TYPE")));
            }
        }
        for (Column expected : COLUMNS) {
            ActualColumn column = actual.get(expected.name);
            if (column == null || !column.accepts(expected)) {
                throw new SQLException("已有表结构不兼容，字段 " + expected.name
                        + " 应为 " + expected.sqlType() + "。请更换 table 配置使用新表。");
            }
        }
        List<String> primaryKey = new ArrayList<>();
        try (ResultSet rs = connection.getMetaData().getPrimaryKeys(null, config.schema, config.table)) {
            while (rs.next()) {
                primaryKey.add(rs.getString("COLUMN_NAME"));
            }
        }
        List<String> expectedKey = COLUMNS.stream().filter(c -> c.key).map(c -> c.name)
                .collect(Collectors.toList());
        if (primaryKey.size() != expectedKey.size() || !primaryKey.containsAll(expectedKey)) {
            throw new SQLException("已有表必须使用 BSEG 五字段复合主键: " + expectedKey);
        }
    }

    private static List<Column> load() {
        List<Column> columns = new ArrayList<>();
        InputStream in = BsegSchema.class.getResourceAsStream("/datagen/bseg-columns.csv");
        if (in == null) {
            throw new IllegalStateException("缺少 bseg-columns.csv");
        }
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.isBlank() || line.startsWith("#")) {
                    continue;
                }
                String[] parts = line.split(",");
                columns.add(new Column(parts[0], parts[1], Integer.parseInt(parts[2]),
                        Integer.parseInt(parts[3]), Boolean.parseBoolean(parts[4])));
            }
        }
        catch (IOException e) {
            throw new IllegalStateException("读取 BSEG 字段定义失败", e);
        }
        return Collections.unmodifiableList(columns);
    }

    static final class Column {
        final String name;
        final String sapType;
        final int length;
        final int scale;
        final boolean key;

        Column(String name, String sapType, int length, int scale, boolean key) {
            this.name = name;
            this.sapType = sapType;
            this.length = length;
            this.scale = scale;
            this.key = key;
        }

        boolean numeric() {
            return sapType.equals("CURR") || sapType.equals("QUAN") || sapType.equals("DEC");
        }

        String sqlType() {
            return numeric() ? "DECIMAL(" + length + "," + scale + ")" : "CHAR(" + length + ")";
        }
    }

    private static final class ActualColumn {
        final int size;
        final int scale;
        final int jdbcType;

        ActualColumn(int size, int scale, int jdbcType) {
            this.size = size;
            this.scale = scale;
            this.jdbcType = jdbcType;
        }

        boolean accepts(Column expected) {
            if (expected.numeric()) {
                return (jdbcType == java.sql.Types.DECIMAL || jdbcType == java.sql.Types.NUMERIC)
                        && scale >= expected.scale && size - scale >= expected.length - expected.scale;
            }
            return (jdbcType == java.sql.Types.CHAR || jdbcType == java.sql.Types.VARCHAR
                    || jdbcType == java.sql.Types.NCHAR || jdbcType == java.sql.Types.NVARCHAR)
                    && size >= expected.length;
        }
    }
}
