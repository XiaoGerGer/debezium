/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.relational.Attribute;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;

/** Establishes a forward-only schema boundary without changing existing tables' replay position. */
final class DamengCaptureScopePreparation {
    private static final Logger LOGGER = LoggerFactory.getLogger(DamengCaptureScopePreparation.class);
    private final DamengConnectorConfig config;
    private final DamengConnection reader;
    private final DamengStandbySupport standby;

    record Baseline(long lsn, List<Table> tables, long databaseMagic, DamengDatabaseIdentity primaryStandbyIdentity) {
        Baseline {
            tables = List.copyOf(tables);
        }
    }

    DamengCaptureScopePreparation(DamengConnectorConfig config, DamengConnection reader, DamengStandbySupport standby) {
        this.config = config;
        this.reader = reader;
        this.standby = standby;
    }

    /**
     * Called during task startup, before streaming or incremental snapshot reads.
     * The owner must durably persist this result before publishing any event under
     * the expanded scope. Sessions must not contain pending application writes.
     */
    Baseline prepare(Collection<TableId> addedTables) throws SQLException, InterruptedException {
        return prepare(addedTables, baseline -> {
        });
    }

    /** Persists the validated baseline while shared locks still protect its schema boundary. */
    Baseline prepare(Collection<TableId> addedTables, Consumer<Baseline> persist) throws SQLException, InterruptedException {
        Objects.requireNonNull(persist, "Dameng capture scope baseline persistence callback");
        final List<TableId> tables = addedTables.stream().distinct().sorted().toList();
        if (tables.isEmpty()) {
            throw new IllegalArgumentException("Dameng capture scope preparation requires at least one added table");
        }
        return prepareInternal(tables, persist);
    }

    /**
     * Establishes a capture boundary when the new filter matches no currently visible table.
     * The callback is still invoked after validating the session identity and boundary;
     * no table lock is acquired because there is no table metadata to protect.
     */
    Baseline prepareBoundary(Consumer<Baseline> persist) throws SQLException, InterruptedException {
        Objects.requireNonNull(persist, "Dameng capture scope boundary persistence callback");
        return prepareInternal(List.of(), persist);
    }

    private Baseline prepareInternal(List<TableId> tables, Consumer<Baseline> persist) throws SQLException, InterruptedException {
        checkInterrupted();
        final var primary = standby == null ? reader : standby.primary();
        final var readerConnection = reader.connection();
        final var primaryConnection = standby == null ? readerConnection : primary.connection();
        final var readerDeadline = new DamengSnapshotDeadline(readerConnection, config.getSnapshotMaxDurationMs());
        final var primaryDeadline = standby == null ? null : new DamengSnapshotDeadline(primaryConnection, config.getSnapshotMaxDurationMs());
        Throwable failure = null;
        try {
            readerConnection.setAutoCommit(false);
            readerConnection.rollback();
            if (standby != null) {
                primaryConnection.setAutoCommit(false);
                primaryConnection.rollback();
            }
            final var before = config.requiresDatabaseLineage() ? reader.readPrimaryStandbyIdentity() : null;
            final long magic = before == null ? reader.databaseMagic() : before.databaseMagic();
            for (TableId table : tables) {
                checkInterrupted();
                // SHARE locks drain existing writers and prevent conflicting DDL.
                // Existing captured tables remain unlocked and continue accumulating redo.
                LOGGER.info("Dameng capture scope acquiring shared lock on added table {}", table);
                primary.executeWithoutCommitting("LOCK TABLE " + primary.quotedTableIdString(table) + " IN SHARE MODE");
            }
            final long boundary = standby == null ? reader.currentLsn() : standby.snapshotBoundary(reader);
            final var structures = new Tables();
            for (String schema : tables.stream().map(TableId::schema).distinct().toList()) {
                checkInterrupted();
                // Preserve excluded physical columns: dictionary-free redo uses their ordinals.
                reader.readSchema(structures, null, schema, tables::contains, null, false);
            }
            final List<Table> baselineTables = new ArrayList<>();
            for (TableId table : tables) {
                checkInterrupted();
                final var structure = structures.forTable(table);
                if (structure == null || structure.columns().isEmpty()) {
                    throw new SQLException("Missing complete JDBC table metadata for added Dameng table " + table
                            + "; check metadata visibility and SELECT permissions");
                }
                baselineTables.add(structure.edit().addAttributes(structure.attributes()).addAttribute(Attribute.editor()
                        .name("dameng.object.id").value(Long.toString(reader.getTableObjectId(table))).create()).create());
            }
            reader.validateUnchangedPrimary();
            reader.validateUnchangedCompatibility();
            if (standby != null) {
                standby.validate(reader);
                primary.validateUnchangedCompatibility();
            }
            final var after = config.requiresDatabaseLineage() ? reader.readPrimaryStandbyIdentity() : null;
            if (before != null) {
                if (after.permanentMagic() != before.permanentMagic() || after.databaseMagic() != before.databaseMagic()
                        || after.readingDatabaseMagic() != before.readingDatabaseMagic() || !after.openGuid().equals(before.openGuid())) {
                    throw new SQLException("Dameng database changed while preparing added tables; retry the capture scope change", "08006");
                }
            }
            else if (reader.databaseMagic() != magic) {
                throw new SQLException("Dameng database changed while preparing added tables; retry the capture scope change", "08006");
            }
            LOGGER.info("Dameng capture scope prepared added tables {} at LSN {}; existing rows require an incremental snapshot", tables, boundary);
            checkInterrupted();
            final var baseline = new Baseline(boundary, baselineTables, magic, after);
            persist.accept(baseline);
            checkInterrupted();
            return baseline;
        }
        catch (SQLException | InterruptedException | RuntimeException | Error e) {
            failure = e;
            throw e;
        }
        finally {
            Throwable releaseFailure = rollback(readerConnection, null);
            if (standby != null) {
                releaseFailure = rollback(primaryConnection, releaseFailure);
            }
            readerDeadline.close();
            if (primaryDeadline != null) {
                primaryDeadline.close();
            }
            if (readerDeadline.expired() || (primaryDeadline != null && primaryDeadline.expired())) {
                Thread.interrupted();
                if (failure != null && releaseFailure != null) {
                    failure.addSuppressed(releaseFailure);
                }
                throw new DamengSnapshotDeadline.SnapshotTimeoutException(config.getSnapshotMaxDurationMs(), failure == null ? releaseFailure : failure);
            }
            if (releaseFailure != null) {
                if (failure != null) {
                    failure.addSuppressed(releaseFailure);
                }
                else {
                    if (releaseFailure instanceof SQLException sqlException) {
                        throw sqlException;
                    }
                    if (releaseFailure instanceof RuntimeException runtimeException) {
                        throw runtimeException;
                    }
                    throw (Error) releaseFailure;
                }
            }
            else {
                LOGGER.info("Dameng capture scope preparation transactions rolled back; shared locks released for added tables {}", tables);
            }
        }
    }

    private static Throwable rollback(Connection connection, Throwable previous) {
        try {
            connection.rollback();
        }
        catch (SQLException | RuntimeException | Error e) {
            LOGGER.warn("Dameng capture scope preparation rollback failed; shared lock release is unconfirmed. Check the database session.", e);
            if (previous == null) {
                return e;
            }
            previous.addSuppressed(e);
        }
        return previous;
    }

    private static void checkInterrupted() throws InterruptedException {
        if (Thread.currentThread().isInterrupted()) {
            throw new InterruptedException("Interrupted while preparing the Dameng capture scope");
        }
    }
}
