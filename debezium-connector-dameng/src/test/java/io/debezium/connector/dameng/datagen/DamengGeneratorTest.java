/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng.datagen;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import org.h2.api.Trigger;
import org.junit.jupiter.api.Test;

class DamengGeneratorTest {
    @Test
    void actualCommittedOperationsAndTransactionSizesMatchEveryCycle() throws Exception {
        Config config = config(2);
        List<long[]> commits = new ArrayList<>();
        try (Connection raw = database(config)) {
            long[] previous = new long[3];
            Connection tracked = (Connection) Proxy.newProxyInstance(getClass().getClassLoader(),
                    new Class<?>[]{ Connection.class }, (proxy, method, args) -> {
                        Object result = invoke(method, raw, args);
                        if (method.getName().equals("commit")) {
                            long[] now = auditCounts(raw);
                            long[] delta = { now[0] - previous[0], now[1] - previous[1], now[2] - previous[2] };
                            System.arraycopy(now, 0, previous, 0, now.length);
                            if (delta[1] != 0 || delta[2] != 0) {
                                commits.add(delta);
                            }
                        }
                        return result;
                    });
            new DamengGenerator(config, tracked, new AtomicBoolean()).run();
            assertEquals(1152, count(raw));
            assertEquals(20, commits.size());
            for (int cycle = 0; cycle < 2; cycle++) {
                int large = 0;
                int small = 0;
                for (long[] operations : commits.subList(cycle * 10, (cycle + 1) * 10)) {
                    long total = operations[0] + operations[1] + operations[2];
                    assertEquals(total * 7 / 100, operations[0]);
                    assertEquals(total * 90 / 100, operations[1]);
                    assertEquals(total * 3 / 100, operations[2]);
                    if (total == 1000) {
                        large++;
                    }
                    else {
                        assertEquals(100, total);
                        small++;
                    }
                }
                assertEquals(1, large);
                assertEquals(9, small);
            }
            // Restart preserves live keys and does not insert another initial set.
            Properties p = baseProperties();
            p.setProperty("init.only", "true");
            new DamengGenerator(new Config(p), raw, new AtomicBoolean()).run();
            assertEquals(1152, count(raw));
        }
    }

    @Test
    void batchFailureRollsBackTheWholeTransaction() throws Exception {
        Properties initial = baseProperties();
        initial.setProperty("init.only", "true");
        Config config = config(1);
        try (Connection raw = database(config)) {
            new DamengGenerator(new Config(initial), raw, new AtomicBoolean()).run();
            Connection failing = (Connection) Proxy.newProxyInstance(getClass().getClassLoader(),
                    new Class<?>[]{ Connection.class }, (proxy, method, args) -> {
                        Object result = invoke(method, raw, args);
                        if (method.getName().equals("prepareStatement") && ((String) args[0]).startsWith("UPDATE")) {
                            PreparedStatement statement = (PreparedStatement) result;
                            return Proxy.newProxyInstance(getClass().getClassLoader(),
                                    new Class<?>[]{ PreparedStatement.class }, (p, m, a) -> {
                                        if (m.getName().equals("executeBatch")) {
                                            invoke(m, statement, a);
                                            throw new SQLException("injected failure after UPDATE batch execution");
                                        }
                                        return invoke(m, statement, a);
                                    });
                        }
                        return result;
                    });
            SQLException error = assertThrows(SQLException.class,
                    () -> new DamengGenerator(config, failing, new AtomicBoolean()).run());
            assertTrue(error.getMessage().contains("injected failure"));
            assertEquals(1000, count(raw));
            long[] counts = auditCounts(raw);
            assertEquals(1000, counts[0]);
            assertEquals(0, counts[1]);
            assertEquals(0, counts[2]);
        }
    }

    @Test
    void intervalPausesBetweenCompletedCycles() throws Exception {
        Properties p = baseProperties();
        p.setProperty("cycles", "2");
        p.setProperty("interval.ms", "150");
        p.setProperty("initial.rows", "100");
        p.setProperty("transaction.large.size", "200");
        Config config = new Config(p);
        try (Connection raw = database(config)) {
            List<Long> commits = new ArrayList<>();
            Connection tracked = (Connection) Proxy.newProxyInstance(getClass().getClassLoader(),
                    new Class<?>[]{ Connection.class }, (proxy, method, args) -> {
                        Object result = invoke(method, raw, args);
                        if (method.getName().equals("commit")) {
                            commits.add(System.nanoTime());
                        }
                        return result;
                    });
            new DamengGenerator(config, tracked, new AtomicBoolean()).run();
            // load + initial + ten first-cycle commits precede the pause.
            assertEquals(22, commits.size());
            assertTrue(commits.get(12) - commits.get(11) >= 140_000_000L);
            assertEquals(188, count(raw));
        }
    }

    @Test
    void validatesSizesAndRoundTripsCompositeKeys() {
        Properties p = baseProperties();
        p.setProperty("transaction.small.size", "101");
        assertThrows(IllegalArgumentException.class, () -> new Config(p));
        for (long sequence : new long[]{ 1, 999, 1000, 100_000, BsegRow.MAX_SEQUENCE }) {
            assertEquals(sequence, BsegRow.sequence(BsegRow.document(sequence), BsegRow.item(sequence)));
        }
        assertThrows(IllegalArgumentException.class, () -> BsegRow.sequence("0000000000", "001"));
        assertThrows(IllegalArgumentException.class, () -> BsegRow.item(BsegRow.MAX_SEQUENCE + 1));
    }

    private static Config config(int cycles) {
        Properties p = baseProperties();
        p.setProperty("cycles", Integer.toString(cycles));
        return new Config(p);
    }

    private static Properties baseProperties() {
        Properties p = new Properties();
        p.setProperty("initial.rows", "1000");
        p.setProperty("initial.batch.size", "1000");
        p.setProperty("jdbc.batch.size", "17");
        p.setProperty("transaction.large.size", "1000");
        p.setProperty("transaction.small.size", "100");
        p.setProperty("interval.ms", "0");
        return p;
    }

    private static Connection database(Config config) throws SQLException {
        Connection connection = DriverManager.getConnection("jdbc:h2:mem:" + UUID.randomUUID() + ";MODE=Oracle");
        try (Statement statement = connection.createStatement()) {
            statement.execute("CREATE SCHEMA BIZ");
            statement.execute(BsegSchema.createTableSql(config));
            statement.execute("CREATE TABLE AUDIT (OP VARCHAR(1))");
            for (String operation : new String[]{ "INSERT", "UPDATE", "DELETE" }) {
                statement.execute("CREATE TRIGGER AUDIT_" + operation + " AFTER " + operation
                        + " ON BIZ.BSEG FOR EACH ROW CALL '" + AuditTrigger.class.getName() + "'");
            }
        }
        return connection;
    }

    private static long count(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement();
                ResultSet rs = statement.executeQuery("SELECT COUNT(*) FROM BIZ.BSEG")) {
            rs.next();
            return rs.getLong(1);
        }
    }

    private static long[] auditCounts(Connection connection) throws SQLException {
        long[] counts = new long[3];
        try (Statement statement = connection.createStatement();
                ResultSet rs = statement.executeQuery("SELECT OP,COUNT(*) FROM AUDIT GROUP BY OP")) {
            while (rs.next()) {
                counts["IUD".indexOf(rs.getString(1))] = rs.getLong(2);
            }
        }
        return counts;
    }

    private static Object invoke(java.lang.reflect.Method method, Object target, Object[] args) throws Throwable {
        try {
            return method.invoke(target, args);
        }
        catch (InvocationTargetException e) {
            throw e.getCause();
        }
    }

    public static class AuditTrigger implements Trigger {
        @Override
        public void fire(Connection connection, Object[] oldRow, Object[] newRow) throws SQLException {
            String operation = oldRow == null ? "I" : newRow == null ? "D" : "U";
            try (PreparedStatement statement = connection.prepareStatement("INSERT INTO AUDIT VALUES (?)")) {
                statement.setString(1, operation);
                statement.executeUpdate();
            }
        }
    }
}
