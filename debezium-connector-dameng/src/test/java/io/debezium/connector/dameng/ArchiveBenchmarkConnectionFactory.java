/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

import io.debezium.config.Field;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.jdbc.JdbcConnection;

/** JDBC timing for the explicitly launched archive benchmark; never packaged in the connector. */
public final class ArchiveBenchmarkConnectionFactory implements JdbcConnection.ConnectionFactory {
    private static final AtomicLong IDS = new AtomicLong();
    private static final Pattern RANGE = Pattern.compile("STARTSCN => (\\d+), ENDSCN => (\\d+)");
    private static BufferedWriter output;
    private final JdbcConnection.ConnectionFactory delegate;

    private ArchiveBenchmarkConnectionFactory(JdbcConnection.ConnectionFactory delegate) {
        this.delegate = delegate;
    }

    public static JdbcConnection.ConnectionFactory patternBasedFactory(String pattern, String driver, ClassLoader loader, Field... fields) {
        return new ArchiveBenchmarkConnectionFactory(JdbcConnection.patternBasedFactory(pattern, driver, loader, fields));
    }

    private static final class Session {
        private final long id = IDS.incrementAndGet();
        private long startLsn;
        private long endLsn;
        private int files;
        private long addNanos;
        private long startNanos;
        private long queryNanos;
        private long firstRowNanos;
        private long nextNanos;
        private long readNanos;
        private long endNanos;
        private long rows;
    }

    @Override
    public Connection connect(JdbcConfiguration config) throws SQLException {
        final var connection = delegate.connect(config);
        final Session[] session = new Session[1];
        return (Connection) Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[]{ Connection.class }, (proxy, method, args) -> {
            final var result = invoke(method, connection, args);
            if (result instanceof Statement statement && (method.getName().equals("createStatement") || method.getName().equals("prepareStatement"))) {
                final String prepared = method.getName().equals("prepareStatement") ? (String) args[0] : null;
                return Proxy.newProxyInstance(getClass().getClassLoader(),
                        new Class<?>[]{ prepared == null ? Statement.class : PreparedStatement.class }, (p, operation, values) -> {
                            final String sql = prepared != null ? prepared : values != null && values.length > 0 && values[0] instanceof String ? (String) values[0] : "";
                            if (!operation.getName().startsWith("execute") || !sql.contains("LOGMNR")) {
                                return invoke(operation, statement, values);
                            }
                            if (sql.contains("ADD_LOGFILE")) {
                                if (session[0] == null) {
                                    session[0] = new Session();
                                }
                                final long started = System.nanoTime();
                                final var value = invoke(operation, statement, values);
                                session[0].addNanos += System.nanoTime() - started;
                                session[0].files++;
                                return value;
                            }
                            if (sql.contains("START_LOGMNR")) {
                                final var range = RANGE.matcher(sql);
                                if (range.find()) {
                                    session[0].startLsn = Long.parseLong(range.group(1));
                                    session[0].endLsn = Long.parseLong(range.group(2));
                                }
                                final long started = System.nanoTime();
                                final var value = invoke(operation, statement, values);
                                session[0].startNanos = System.nanoTime() - started;
                                return value;
                            }
                            if (sql.contains("END_LOGMNR")) {
                                final long started = System.nanoTime();
                                final var value = invoke(operation, statement, values);
                                if (session[0] != null) {
                                    session[0].endNanos = System.nanoTime() - started;
                                    write(session[0]);
                                    session[0] = null;
                                }
                                return value;
                            }
                            final long started = System.nanoTime();
                            final var value = invoke(operation, statement, values);
                            if (!(value instanceof ResultSet rows) || session[0] == null) {
                                return value;
                            }
                            final var current = session[0];
                            current.queryNanos = System.nanoTime() - started;
                            final long readStarted = System.nanoTime();
                            return Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[]{ ResultSet.class }, (r, accessor, parameters) -> {
                                if (accessor.getName().equals("next")) {
                                    final long nextStarted = System.nanoTime();
                                    final var available = invoke(accessor, rows, parameters);
                                    current.nextNanos += System.nanoTime() - nextStarted;
                                    if (Boolean.TRUE.equals(available)) {
                                        if (current.rows++ == 0) {
                                            current.firstRowNanos = System.nanoTime() - started;
                                        }
                                    }
                                    else {
                                        current.readNanos = System.nanoTime() - readStarted;
                                    }
                                    return available;
                                }
                                return invoke(accessor, rows, parameters);
                            });
                        });
            }
            return result;
        });
    }

    private static Object invoke(Method method, Object target, Object[] args) throws Throwable {
        try {
            return method.invoke(target, args);
        }
        catch (InvocationTargetException e) {
            throw e.getCause();
        }
    }

    private static synchronized void write(Session session) {
        try {
            if (output == null) {
                output = Files.newBufferedWriter(Path.of("/tmp/archive-benchmark-jdbc.csv"), StandardOpenOption.CREATE, StandardOpenOption.APPEND);
                output.write("session,start_lsn,end_lsn,files,rows,add_ns,start_ns,query_ns,first_row_ns,next_ns,read_decode_buffer_ns,end_ns\n");
            }
            output.write(session.id + "," + session.startLsn + "," + session.endLsn + "," + session.files + "," + session.rows
                    + "," + session.addNanos + "," + session.startNanos + "," + session.queryNanos + "," + session.firstRowNanos
                    + "," + session.nextNanos + "," + session.readNanos + "," + session.endNanos + "\n");
            output.flush();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
