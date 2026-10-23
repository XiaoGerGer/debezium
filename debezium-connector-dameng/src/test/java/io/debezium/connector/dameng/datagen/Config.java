/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng.datagen;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;
import java.util.Properties;

final class Config {
    final String hostname;
    final int port;
    final String user;
    final String password;
    final String dbname;
    final String schema;
    final String table;
    final String mandt;
    final String bukrs;
    final String gjahr;
    final int initialRows;
    final int initialBatchSize;
    final int jdbcBatchSize;
    final int largeSize;
    final int smallSize;
    final long cycles;
    final long intervalMs;
    final long seed;
    final boolean initOnly;
    final boolean help;

    Config(Properties p) {
        hostname = p.getProperty("database.hostname", "localhost");
        port = positiveInt(p, "database.port", 5236);
        if (port > 65535) {
            throw new IllegalArgumentException("database.port 必须 <= 65535");
        }
        user = p.getProperty("database.user", "SYSDBA");
        password = p.getProperty("database.password", "SYSDBA001");
        dbname = p.getProperty("database.dbname", "DAMENG");
        schema = identifier(p.getProperty("schema", "BIZ"));
        table = identifier(p.getProperty("table", "BSEG"));
        mandt = digits(p.getProperty("sap.mandt", "100"), 3, "sap.mandt");
        bukrs = p.getProperty("sap.bukrs", "1000");
        if (!bukrs.matches("[A-Za-z0-9]{4}")) {
            throw new IllegalArgumentException("sap.bukrs 必须为 4 位字母或数字");
        }
        gjahr = digits(p.getProperty("sap.gjahr", "2026"), 4, "sap.gjahr");
        initialRows = positiveInt(p, "initial.rows", 100_000);
        initialBatchSize = positiveInt(p, "initial.batch.size", 1000);
        jdbcBatchSize = positiveInt(p, "jdbc.batch.size", 500);
        largeSize = positiveInt(p, "transaction.large.size", 10_000);
        smallSize = positiveInt(p, "transaction.small.size", 100);
        if (largeSize <= smallSize || largeSize % 100 != 0 || smallSize % 100 != 0) {
            throw new IllegalArgumentException("大事务必须大于小事务，且两者操作数均须为 100 的倍数，以精确满足 7/90/3 比例");
        }
        cycles = nonnegativeLong(p, "cycles", 0);
        intervalMs = nonnegativeLong(p, "interval.ms", 1000);
        seed = Long.parseLong(p.getProperty("random.seed", "20261012"));
        initOnly = booleanValue(p, "init.only");
        help = booleanValue(p, "help");
    }

    static Config load(String[] args) throws IOException {
        Path path = Path.of("config/datagen.properties");
        boolean explicitPath = false;
        Properties overrides = new Properties();
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--config":
                    path = Path.of(argument(args, ++i, "--config"));
                    explicitPath = true;
                    break;
                case "--cycles":
                    overrides.setProperty("cycles", argument(args, ++i, "--cycles"));
                    break;
                case "--interval-ms":
                    overrides.setProperty("interval.ms", argument(args, ++i, "--interval-ms"));
                    break;
                case "--init-only":
                    overrides.setProperty("init.only", "true");
                    break;
                case "--help":
                case "-h":
                    overrides.setProperty("help", "true");
                    break;
                default:
                    throw new IllegalArgumentException("未知参数: " + args[i]);
            }
        }
        Properties p = new Properties();
        if (Files.exists(path)) {
            try (InputStream in = Files.newInputStream(path)) {
                p.load(in);
            }
        }
        else if (explicitPath) {
            throw new IllegalArgumentException("配置文件不存在: " + path);
        }
        String password = System.getenv("DM_PASSWORD");
        if (password != null) {
            p.setProperty("database.password", password);
        }
        p.putAll(overrides);
        return new Config(p);
    }

    String jdbcUrl() {
        // DM selects an instance by host/port, without a /dbname suffix.
        return "jdbc:dm://" + hostname + ":" + port + "?loginTimeout=10&socketTimeout=300000";
    }

    String qualifiedTable() {
        return "\"" + schema + "\".\"" + table + "\"";
    }

    private static String argument(String[] args, int index, String option) {
        if (index >= args.length) {
            throw new IllegalArgumentException(option + " 缺少参数值");
        }
        return args[index];
    }

    private static String identifier(String value) {
        if (!value.matches("[A-Za-z][A-Za-z0-9_]{0,127}")) {
            throw new IllegalArgumentException("非法数据库标识符: " + value);
        }
        return value.toUpperCase(Locale.ROOT);
    }

    private static String digits(String value, int length, String key) {
        if (!value.matches("[0-9]{" + length + "}")) {
            throw new IllegalArgumentException(key + " 必须为 " + length + " 位数字");
        }
        return value;
    }

    private static int positiveInt(Properties p, String key, int fallback) {
        int result = Integer.parseInt(p.getProperty(key, Integer.toString(fallback)));
        if (result <= 0) {
            throw new IllegalArgumentException(key + " 必须 > 0");
        }
        return result;
    }

    private static long nonnegativeLong(Properties p, String key, long fallback) {
        long result = Long.parseLong(p.getProperty(key, Long.toString(fallback)));
        if (result < 0) {
            throw new IllegalArgumentException(key + " 必须 >= 0");
        }
        return result;
    }

    private static boolean booleanValue(Properties p, String key) {
        String value = p.getProperty(key, "false");
        if (!value.equalsIgnoreCase("true") && !value.equalsIgnoreCase("false")) {
            throw new IllegalArgumentException(key + " 必须为 true 或 false");
        }
        return Boolean.parseBoolean(value);
    }
}
