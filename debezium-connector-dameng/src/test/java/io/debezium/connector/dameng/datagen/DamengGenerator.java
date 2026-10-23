/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng.datagen;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

final class DamengGenerator {
    private static final String KEY_WHERE = " WHERE \"MANDT\"=? AND \"BUKRS\"=?"
            + " AND \"BELNR\"=? AND \"GJAHR\"=? AND \"BUZEI\"=?";
    private static final String NAMESPACE_WHERE = " WHERE \"MANDT\"=? AND \"BUKRS\"=? AND \"GJAHR\"=?";
    private final Config config;
    private final Connection connection;
    private final AtomicBoolean stopping;
    private final Random random;
    private final BsegRow row;
    private final KeyPool keys = new KeyPool();
    private long nextSequence = 1;
    private long committedTransactions;
    private long committedLarge;
    private long committedSmall;
    private long committedInserts;
    private long committedUpdates;
    private long committedDeletes;
    private long updateSequence;
    private long completedCycles;

    DamengGenerator(Config config, Connection connection, AtomicBoolean stopping) {
        this.config = config;
        this.connection = connection;
        this.stopping = stopping;
        this.random = new Random(config.seed);
        this.row = new BsegRow(config, random);
    }

    void run() throws SQLException {
        log("连接 %s:%d，实例配置=%s，目标=%s", config.hostname, config.port, config.dbname,
                config.qualifiedTable());
        prepareTable();
        connection.setAutoCommit(false);
        loadKeys();
        try (PreparedStatement insert = connection.prepareStatement(BsegSchema.insertSql(config))) {
            initialize(insert);
            if (config.initOnly || stopping.get()) {
                log("初始化结束，当前行数=%d", keys.size());
                return;
            }
            int maximumDeletes = WorkloadPlan.deletes(config.largeSize);
            if (keys.size() <= maximumDeletes) {
                throw new SQLException("当前行数必须大于大事务删除数 " + maximumDeletes
                        + "，请调大 initial.rows 或调小 transaction.large.size");
            }
            String updateSql = "UPDATE " + config.qualifiedTable()
                    + " SET \"DMBTR\"=CASE WHEN \"DMBTR\">=99999999999.99 THEN 0 ELSE \"DMBTR\"+0.01 END,"
                    + " \"WRBTR\"=CASE WHEN \"WRBTR\">=99999999999.99 THEN 0 ELSE \"WRBTR\"+0.01 END,"
                    + " \"SGTXT\"=?, \"ZUONR\"=?, \"KOSTL\"=?, \"PRCTR\"=?, \"XREF3\"=?" + KEY_WHERE;
            try (PreparedStatement update = connection.prepareStatement(updateSql);
                    PreparedStatement delete = connection.prepareStatement(
                            "DELETE FROM " + config.qualifiedTable() + KEY_WHERE)) {
                log("持续循环: 每轮 1 大(%d 操作) + 9 小(%d 操作)，增/改/删=7%%/90%%/3%%，轮后暂停=%dms",
                        config.largeSize, config.smallSize, config.intervalMs);
                for (long cycle = 1; !stopping.get() && (config.cycles == 0 || cycle <= config.cycles); cycle++) {
                    long started = System.nanoTime();
                    for (int size : WorkloadPlan.cycle(config, random)) {
                        if (stopping.get()) {
                            break;
                        }
                        transact(size, insert, update, delete);
                    }
                    if (stopping.get()) {
                        break;
                    }
                    completedCycles++;
                    log("轮次=%d，累计事务=%d(大=%d 小=%d)，累计增=%d 改=%d 删=%d，当前行数=%d，本轮耗时=%dms",
                            cycle, committedTransactions, committedLarge, committedSmall,
                            committedInserts, committedUpdates, committedDeletes, keys.size(),
                            (System.nanoTime() - started) / 1_000_000);
                    if (config.cycles != 0 && cycle == config.cycles) {
                        break;
                    }
                    try {
                        Thread.sleep(config.intervalMs);
                    }
                    catch (InterruptedException e) {
                        stopping.set(true);
                        Thread.currentThread().interrupt();
                    }
                }
            }
        }
        finally {
            connection.rollback();
            log("已停止。完整轮次=%d，已提交事务=%d(大=%d 小=%d)，增=%d 改=%d 删=%d，当前行数=%d",
                    completedCycles, committedTransactions, committedLarge, committedSmall,
                    committedInserts, committedUpdates, committedDeletes, keys.size());
        }
    }

    private void prepareTable() throws SQLException {
        connection.setAutoCommit(true);
        boolean schemaExists = false;
        try (ResultSet schemas = connection.getMetaData().getSchemas()) {
            while (schemas.next()) {
                if (config.schema.equals(schemas.getString("TABLE_SCHEM"))) {
                    schemaExists = true;
                    break;
                }
            }
        }
        if (!schemaExists) {
            try (Statement statement = connection.createStatement()) {
                statement.execute("CREATE SCHEMA \"" + config.schema + "\"");
            }
        }
        boolean tableExists = false;
        try (ResultSet tables = connection.getMetaData().getTables(null, config.schema,
                config.table, new String[]{ "TABLE" })) {
            while (tables.next()) {
                if (config.schema.equals(tables.getString("TABLE_SCHEM"))
                        && config.table.equals(tables.getString("TABLE_NAME"))) {
                    tableExists = true;
                    break;
                }
            }
        }
        if (!tableExists) {
            try (Statement statement = connection.createStatement()) {
                statement.execute(BsegSchema.createTableSql(config));
            }
            log("已创建 BSEG 表，字段数=%d，主键=MANDT/BUKRS/BELNR/GJAHR/BUZEI", BsegSchema.COLUMNS.size());
        }
        BsegSchema.validateExistingTable(connection, config);
    }

    private void loadKeys() throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT \"BELNR\",\"BUZEI\" FROM " + config.qualifiedTable() + NAMESPACE_WHERE)) {
            bindNamespace(statement);
            statement.setFetchSize(1000);
            try (ResultSet rs = statement.executeQuery()) {
                while (rs.next()) {
                    long key = BsegRow.sequence(rs.getString(1), rs.getString(2));
                    keys.add(key);
                    nextSequence = Math.max(nextSequence, key + 1);
                }
            }
        }
        connection.commit();
        log("已加载现有主键=%d，SAP 命名空间=%s/%s/%s", keys.size(), config.mandt, config.bukrs, config.gjahr);
    }

    private void initialize(PreparedStatement insert) throws SQLException {
        while (!stopping.get() && keys.size() < config.initialRows) {
            int count = Math.min(config.initialBatchSize, config.initialRows - keys.size());
            List<Long> inserted = new ArrayList<>(count);
            try {
                for (int i = 0; i < count; i++) {
                    long key = nextSequence++;
                    row.bindInsert(insert, key);
                    insert.addBatch();
                    inserted.add(key);
                    if ((i + 1) % config.jdbcBatchSize == 0) {
                        executeChecked(insert, config.jdbcBatchSize);
                    }
                }
                if (count % config.jdbcBatchSize != 0) {
                    executeChecked(insert, count % config.jdbcBatchSize);
                }
                connection.commit();
                inserted.forEach(keys::add);
            }
            catch (SQLException | RuntimeException e) {
                rollback(e);
                throw e;
            }
            if (keys.size() % 10_000 == 0 || keys.size() >= config.initialRows) {
                log("初始数据进度=%d/%d", keys.size(), config.initialRows);
            }
        }
    }

    private void transact(int size, PreparedStatement insert, PreparedStatement update,
                          PreparedStatement delete)
            throws SQLException {
        List<Long> inserted = new ArrayList<>(WorkloadPlan.inserts(size));
        List<Long> deleted = new ArrayList<>(WorkloadPlan.deletes(size));
        Set<Long> deleting = new HashSet<>();
        while (deleted.size() < WorkloadPlan.deletes(size)) {
            long key = keys.random(random);
            if (deleting.add(key)) {
                deleted.add(key);
            }
        }
        int deleteIndex = 0;
        int pending = 0;
        WorkloadPlan.Operation previous = null;
        try {
            // Only consecutive operations of the same type are batched: SQL order remains shuffled.
            for (WorkloadPlan.Operation operation : WorkloadPlan.transaction(size, random)) {
                if (previous != null && (operation != previous || pending == config.jdbcBatchSize)) {
                    executeChecked(statement(previous, insert, update, delete), pending);
                    pending = 0;
                }
                switch (operation) {
                    case INSERT:
                        long key = nextSequence++;
                        row.bindInsert(insert, key);
                        insert.addBatch();
                        inserted.add(key);
                        break;
                    case UPDATE:
                        long target;
                        do {
                            target = keys.random(random);
                        } while (deleting.contains(target));
                        update.setString(1, "BSEG update " + (++updateSequence));
                        update.setString(2, "REF" + updateSequence);
                        update.setString(3, Integer.toString(100_000 + random.nextInt(900_000)));
                        update.setString(4, Integer.toString(100_000 + random.nextInt(900_000)));
                        update.setString(5, "CHANGE" + updateSequence);
                        row.bindKey(update, 6, target);
                        update.addBatch();
                        break;
                    case DELETE:
                        row.bindKey(delete, 1, deleted.get(deleteIndex++));
                        delete.addBatch();
                        break;
                    default:
                        throw new IllegalStateException("未知操作");
                }
                previous = operation;
                pending++;
            }
            executeChecked(statement(previous, insert, update, delete), pending);
            connection.commit();
            // In-memory changes and counters become visible only after the database commit succeeds.
            inserted.forEach(keys::add);
            deleted.forEach(keys::remove);
            committedTransactions++;
            if (size == config.largeSize) {
                committedLarge++;
            }
            else {
                committedSmall++;
            }
            committedInserts += WorkloadPlan.inserts(size);
            committedUpdates += WorkloadPlan.updates(size);
            committedDeletes += WorkloadPlan.deletes(size);
        }
        catch (SQLException | RuntimeException e) {
            rollback(e);
            throw e;
        }
    }

    private static PreparedStatement statement(WorkloadPlan.Operation operation,
                                               PreparedStatement insert, PreparedStatement update, PreparedStatement delete) {
        switch (operation) {
            case INSERT:
                return insert;
            case UPDATE:
                return update;
            case DELETE:
                return delete;
            default:
                throw new IllegalStateException("未知操作");
        }
    }

    private static void executeChecked(PreparedStatement statement, int expected) throws SQLException {
        int[] counts = statement.executeBatch();
        statement.clearBatch();
        if (counts.length != expected) {
            throw new SQLException("批处理结果数不符: " + counts.length + "，期望 " + expected);
        }
        for (int count : counts) {
            if (count != 1) {
                // SUCCESS_NO_INFO cannot prove that UPDATE/DELETE affected a live row.
                throw new SQLException("每条操作必须恰好影响 1 行，驱动返回=" + count
                        + "；请检查是否存在其他写入者或驱动是否返回真实批处理行数");
            }
        }
    }

    private void bindNamespace(PreparedStatement statement) throws SQLException {
        statement.setString(1, config.mandt);
        statement.setString(2, config.bukrs);
        statement.setString(3, config.gjahr);
    }

    private void rollback(Exception original) {
        try {
            connection.rollback();
        }
        catch (SQLException rollbackFailure) {
            original.addSuppressed(rollbackFailure);
        }
    }

    private static void log(String format, Object... args) {
        String time = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        System.out.printf("[%s] %s%n", time, String.format(format, args));
    }
}
