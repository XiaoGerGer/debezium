/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/** Loopback-only TCP relay that faults this test's connections without stopping the database. */
final class DamengTcpProxy implements AutoCloseable {
    private final ServerSocket listener;
    private final InetSocketAddress target;
    private final ExecutorService workers = Executors.newCachedThreadPool(runnable -> {
        final var thread = new Thread(runnable, "dameng-test-tcp-proxy");
        thread.setDaemon(true);
        return thread;
    });
    private final Set<Socket> sockets = ConcurrentHashMap.newKeySet();
    private final AtomicInteger rejectedConnections = new AtomicInteger();
    private final AtomicReference<Throwable> failure = new AtomicReference<>();
    private volatile boolean available;
    private volatile boolean rejectAfterNextConnection;
    private volatile boolean closed;

    DamengTcpProxy(String hostname, int port) throws IOException {
        target = new InetSocketAddress(hostname, port);
        listener = new ServerSocket(0, 50, InetAddress.getByName("127.0.0.1"));
        workers.submit(this::acceptConnections);
    }

    int port() {
        return listener.getLocalPort();
    }

    int rejectedConnections() {
        checkHealthy();
        return rejectedConnections.get();
    }

    void setAvailable(boolean available) {
        rejectAfterNextConnection = false;
        this.available = available;
        if (!available) {
            sockets.forEach(this::disconnect);
        }
    }

    void allowOneConnectionThenReject() {
        rejectAfterNextConnection = true;
        available = true;
    }

    void checkHealthy() {
        if (failure.get() != null) {
            throw new AssertionError("Test TCP proxy failed", failure.get());
        }
    }

    private void acceptConnections() {
        try {
            while (!closed) {
                final var client = listener.accept();
                sockets.add(client);
                if (!available) {
                    rejectedConnections.incrementAndGet();
                    disconnect(client);
                    continue;
                }
                final var server = new Socket();
                sockets.add(server);
                try {
                    server.connect(target, (int) Duration.ofSeconds(10).toMillis());
                    if (!available || closed) {
                        disconnect(client);
                        disconnect(server);
                        continue;
                    }
                    workers.submit(() -> copy(client, server));
                    workers.submit(() -> copy(server, client));
                    if (rejectAfterNextConnection) {
                        rejectAfterNextConnection = false;
                        available = false;
                    }
                }
                catch (IOException e) {
                    disconnect(client);
                    disconnect(server);
                    if (available && !closed) {
                        failure.compareAndSet(null, e);
                    }
                }
            }
        }
        catch (IOException e) {
            if (!closed) {
                failure.compareAndSet(null, e);
            }
        }
    }

    private void copy(Socket source, Socket destination) {
        try {
            source.getInputStream().transferTo(destination.getOutputStream());
        }
        catch (IOException e) {
            // Socket resets are the intentional fault and may also occur when a
            // JDBC connection closes while the opposite relay direction is reading.
        }
        finally {
            disconnect(source);
            disconnect(destination);
        }
    }

    private void disconnect(Socket socket) {
        sockets.remove(socket);
        try {
            socket.setSoLinger(true, 0);
        }
        catch (IOException e) {
            // The opposite relay direction may have already closed this socket.
        }
        finally {
            try {
                socket.close();
            }
            catch (IOException e) {
                // Closing a socket from either relay direction is idempotent.
            }
        }
    }

    @Override
    public void close() throws IOException, InterruptedException {
        closed = true;
        listener.close();
        sockets.forEach(this::disconnect);
        workers.shutdownNow();
        if (!workers.awaitTermination(10, TimeUnit.SECONDS)) {
            throw new AssertionError("Test TCP proxy threads did not stop");
        }
        checkHealthy();
    }
}
