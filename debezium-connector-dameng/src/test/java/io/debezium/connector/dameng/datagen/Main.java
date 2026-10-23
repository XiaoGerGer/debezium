/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng.datagen;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.concurrent.atomic.AtomicBoolean;

public final class Main {
    private Main() {
    }

    public static void main(String[] args) {
        try {
            Config config = Config.load(args);
            if (config.help) {
                System.out.println("达梦 SAP BSEG 测试数据生成器\n"
                        + "用法: java -cp <test classpath> io.debezium.connector.dameng.datagen.Main --config 路径 [选项]\n"
                        + "  --init-only       建表并补齐初始数据后退出\n"
                        + "  --cycles N        执行 N 轮，0 表示持续循环（默认）\n"
                        + "  --interval-ms N   每轮完成后暂停的毫秒数（默认 1000）\n"
                        + "  --help            显示帮助\n"
                        + "每轮 10 个事务：1 大 + 9 小；每个事务增 7%、改 90%、删 3%。\n"
                        + "默认配置文件: config/datagen.properties；环境变量 DM_PASSWORD 可覆盖密码。");
                return;
            }
            AtomicBoolean stopping = new AtomicBoolean();
            Thread runner = Thread.currentThread();
            Thread shutdownHook = new Thread(() -> {
                stopping.set(true);
                runner.interrupt();
                try {
                    runner.join(30_000);
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }, "datagen-shutdown");
            Runtime.getRuntime().addShutdownHook(shutdownHook);
            try {
                Class.forName("dm.jdbc.driver.DmDriver");
                try (Connection connection = DriverManager.getConnection(
                        config.jdbcUrl(), config.user, config.password)) {
                    new DamengGenerator(config, connection, stopping).run();
                }
            }
            finally {
                if (!stopping.get()) {
                    Runtime.getRuntime().removeShutdownHook(shutdownHook);
                }
            }
        }
        catch (Exception e) {
            System.err.println("生成器运行失败: " + e.getMessage());
            e.printStackTrace(System.err);
            System.exit(1);
        }
    }
}
