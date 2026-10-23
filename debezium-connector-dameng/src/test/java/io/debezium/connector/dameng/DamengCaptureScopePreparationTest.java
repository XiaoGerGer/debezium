/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Types;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import org.apache.kafka.connect.errors.ConnectException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import io.debezium.config.Configuration;
import io.debezium.relational.Attribute;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.relational.Tables.TableFilter;

class DamengCaptureScopePreparationTest {
    private static final TableId FIRST = new TableId(null, "APP", "ADDED_A");
    private static final TableId SECOND = new TableId(null, "APP", "ADDED_B");
    private final DamengConnection reader = mock(DamengConnection.class);
    private final Connection jdbc = mock(Connection.class);

    @BeforeEach
    void before() throws Exception {
        when(reader.connection()).thenReturn(jdbc);
        when(reader.currentLsn()).thenReturn(300L);
        when(reader.databaseMagic()).thenReturn(42L);
        final var quoting = new DamengConnection(DamengConnectorConfigTest.configuration().build());
        when(reader.quotedTableIdString(any())).thenAnswer(invocation -> quoting.quotedTableIdString(invocation.getArgument(0)));
        when(reader.getTableObjectId(any())).thenReturn(81L);
        metadata(reader, List.of(table(FIRST), table(SECOND)));
    }

    @Test
    void shouldLockOnlySortedAddedTablesBeforeReadingBoundaryAndFullSchema() throws Exception {
        final var baseline = preparation(configuration(), null).prepare(List.of(SECOND, FIRST, SECOND));
        final var order = inOrder(jdbc, reader);
        order.verify(jdbc).setAutoCommit(false);
        order.verify(jdbc).rollback();
        order.verify(reader).databaseMagic();
        order.verify(reader).executeWithoutCommitting("LOCK TABLE \"APP\".\"ADDED_A\" IN SHARE MODE");
        order.verify(reader).executeWithoutCommitting("LOCK TABLE \"APP\".\"ADDED_B\" IN SHARE MODE");
        order.verify(reader).currentLsn();
        order.verify(reader).readSchema(any(), isNull(), eq("APP"), any(), isNull(), eq(false));
        order.verify(reader).getTableObjectId(FIRST);
        order.verify(reader).getTableObjectId(SECOND);
        order.verify(reader).validateUnchangedPrimary();
        order.verify(reader).validateUnchangedCompatibility();
        order.verify(reader).databaseMagic();
        order.verify(jdbc).rollback();
        assertThat(baseline.lsn()).isEqualTo(300);
        assertThat(baseline.databaseMagic()).isEqualTo(42);
        assertThat(baseline.primaryStandbyIdentity()).isNull();
        assertThat(baseline.tables()).extracting(Table::id).containsExactly(FIRST, SECOND);
        final var first = baseline.tables().get(0);
        assertThat(first.columns()).extracting(Column::name).containsExactly("ID", "EXCLUDED_FROM_EVENTS");
        assertThat(first.primaryKeyColumnNames()).containsExactly("ID");
        assertThat(first.attributes()).extracting(Attribute::name).contains("existing.attribute", "dameng.object.id");
        assertThat(first.attributes()).filteredOn(attribute -> attribute.name().equals("dameng.object.id"))
                .extracting(Attribute::value).containsExactly("81");
        assertThatThrownBy(() -> baseline.tables().clear()).isInstanceOf(UnsupportedOperationException.class);
        verify(jdbc, never()).commit();
    }

    @Test
    void shouldEscapeIdentifiersRatherThanConcatenatingRawNames() throws Exception {
        final var unusual = new TableId(null, "APP\";DROP SCHEMA X", "ADDED\"--");
        metadata(reader, List.of(table(unusual)));
        preparation(configuration(), null).prepare(List.of(unusual));
        verify(reader).executeWithoutCommitting("LOCK TABLE \"APP\"\";DROP SCHEMA X\".\"ADDED\"\"--\" IN SHARE MODE");
    }

    @Test
    void shouldReadEachSchemaWithTheExactAddedTableFilter() throws Exception {
        final var other = new TableId(null, "OTHER", "ADDED");
        metadata(reader, List.of(table(FIRST), table(other), table(new TableId(null, "APP", "EXISTING"))));
        final var baseline = preparation(configuration(), null).prepare(List.of(FIRST, other));
        assertThat(baseline.tables()).extracting(Table::id).containsExactly(FIRST, other);
        verify(reader).readSchema(any(), isNull(), eq("APP"), any(), isNull(), eq(false));
        verify(reader).readSchema(any(), isNull(), eq("OTHER"), any(), isNull(), eq(false));
    }

    @Test
    void shouldUsePrimaryLocksAndTheReplayedStandbyBoundary() throws Exception {
        final var primary = mock(DamengConnection.class);
        final var primaryJdbc = mock(Connection.class);
        final var standby = mock(DamengStandbySupport.class);
        final var identity = identity(42, "open");
        when(standby.primary()).thenReturn(primary);
        when(primary.connection()).thenReturn(primaryJdbc);
        when(primary.quotedTableIdString(FIRST)).thenReturn(FIRST.toDoubleQuotedString());
        when(standby.snapshotBoundary(reader)).thenReturn(400L);
        when(reader.readPrimaryStandbyIdentity()).thenReturn(identity);
        final var baseline = preparation(configuration().with("database.standby.hosts", "replica:5236"), standby).prepare(List.of(FIRST));
        final var order = inOrder(primaryJdbc, primary, standby, reader, jdbc);
        order.verify(jdbc).setAutoCommit(false);
        order.verify(jdbc).rollback();
        order.verify(primaryJdbc).setAutoCommit(false);
        order.verify(primaryJdbc).rollback();
        order.verify(reader).readPrimaryStandbyIdentity();
        order.verify(primary).executeWithoutCommitting("LOCK TABLE \"APP\".\"ADDED_A\" IN SHARE MODE");
        order.verify(standby).snapshotBoundary(reader);
        order.verify(reader).readSchema(any(), isNull(), eq("APP"), any(), isNull(), eq(false));
        order.verify(reader).getTableObjectId(FIRST);
        order.verify(reader).validateUnchangedPrimary();
        order.verify(reader).validateUnchangedCompatibility();
        order.verify(standby).validate(reader);
        order.verify(primary).validateUnchangedCompatibility();
        order.verify(reader).readPrimaryStandbyIdentity();
        order.verify(jdbc).rollback();
        order.verify(primaryJdbc).rollback();
        assertThat(baseline.lsn()).isEqualTo(400);
        assertThat(baseline.primaryStandbyIdentity()).isSameAs(identity);
        verify(reader, never()).executeWithoutCommitting(any());
        verify(primaryJdbc, never()).commit();
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldPersistTheValidatedBaselineBeforeReleasingAddedTableLocks() throws Exception {
        final Consumer<DamengCaptureScopePreparation.Baseline> persist = mock(Consumer.class);
        doAnswer(invocation -> {
            verify(jdbc).rollback();
            final DamengCaptureScopePreparation.Baseline baseline = invocation.getArgument(0);
            assertThat(baseline.lsn()).isEqualTo(300);
            assertThat(baseline.tables()).extracting(Table::id).containsExactly(FIRST);
            return null;
        }).when(persist).accept(any());
        final var baseline = preparation(configuration(), null).prepare(List.of(FIRST), persist);
        final var order = inOrder(reader, jdbc, persist);
        order.verify(jdbc).setAutoCommit(false);
        order.verify(jdbc).rollback();
        order.verify(reader).executeWithoutCommitting(any());
        order.verify(reader).validateUnchangedCompatibility();
        order.verify(persist).accept(baseline);
        order.verify(jdbc).rollback();
    }

    @Test
    void shouldReleaseLocksWhenDurableHistoryPersistenceFails() throws Exception {
        final var failure = new ConnectException("history acknowledgement failed");
        assertThatThrownBy(() -> preparation(configuration(), null).prepare(List.of(FIRST), baseline -> {
            assertThatCode(() -> verify(jdbc).rollback()).doesNotThrowAnyException();
            throw failure;
        })).isSameAs(failure);
        verify(jdbc, times(2)).rollback();
        verify(jdbc, never()).commit();
    }

    @Test
    void shouldNotPersistAnInvalidSchemaBaseline() throws Exception {
        metadata(reader, List.of());
        final var called = new AtomicBoolean();
        assertThatThrownBy(() -> preparation(configuration(), null).prepare(List.of(FIRST), baseline -> called.set(true)))
                .isInstanceOf(SQLException.class);
        assertThat(called.get()).isFalse();
        verify(jdbc, times(2)).rollback();
    }

    @ParameterizedTest
    @ValueSource(strings = { "lock", "metadata", "object", "compatibility" })
    void shouldReleaseLocksWithoutReturningAnIncompleteBaselineOnFailure(String fault) throws Exception {
        final var failure = new SQLException("failure during " + fault);
        switch (fault) {
            case "lock" -> doThrow(failure).when(reader).executeWithoutCommitting(any());
            case "metadata" -> doThrow(failure).when(reader).readSchema(any(), any(), any(), any(), any(), eq(false));
            case "object" -> doThrow(failure).when(reader).getTableObjectId(any());
            default -> doThrow(failure).when(reader).validateUnchangedCompatibility();
        }
        assertThatThrownBy(() -> preparation(configuration(), null).prepare(List.of(FIRST))).isSameAs(failure);
        verify(jdbc, times(2)).rollback();
        verify(jdbc, never()).commit();
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void shouldRejectMissingOrEmptyMetadata(boolean missing) throws Exception {
        metadata(reader, missing ? List.of() : List.of(Table.editor().tableId(FIRST).create()));
        assertThatThrownBy(() -> preparation(configuration(), null).prepare(List.of(FIRST)))
                .isInstanceOf(SQLException.class).hasMessageContaining("Missing complete JDBC table metadata", "APP.ADDED_A");
        verify(jdbc, times(2)).rollback();
        verify(reader, never()).getTableObjectId(any());
    }

    @Test
    void shouldRejectAnIdentityChangeDuringMetadataRead() throws Exception {
        when(reader.databaseMagic()).thenReturn(42L, 43L);
        assertThatThrownBy(() -> preparation(configuration(), null).prepare(List.of(FIRST)))
                .isInstanceOf(SQLException.class).hasMessageContaining("database changed");
        verify(jdbc, times(2)).rollback();
    }

    @Test
    void shouldRejectPrimaryHistoryChangesEvenWhenTheDatabaseMagicIsUnchanged() throws Exception {
        when(reader.readPrimaryStandbyIdentity()).thenReturn(identity(42, "old"), identity(42, "new"));
        assertThatThrownBy(() -> preparation(configuration().with("database.failover.hosts", "primary:5236"), null).prepare(List.of(FIRST)))
                .isInstanceOf(SQLException.class).hasMessageContaining("database changed");
        verify(jdbc, times(2)).rollback();
    }

    @Test
    void shouldPreserveTheOriginalFailureAndReleaseBothSessionsWhenRollbackFails() throws Exception {
        final var primary = mock(DamengConnection.class);
        final var primaryJdbc = mock(Connection.class);
        final var standby = mock(DamengStandbySupport.class);
        when(standby.primary()).thenReturn(primary);
        when(primary.connection()).thenReturn(primaryJdbc);
        when(reader.readPrimaryStandbyIdentity()).thenReturn(identity(42, "open"));
        when(primary.quotedTableIdString(FIRST)).thenReturn(FIRST.toDoubleQuotedString());
        final var failure = new SQLException("lock failed");
        final var readerFailure = new SQLException("reader rollback failed");
        final var primaryFailure = new SQLException("primary rollback failed");
        doThrow(failure).when(primary).executeWithoutCommitting(any());
        doNothing().doThrow(readerFailure).when(jdbc).rollback();
        doNothing().doThrow(primaryFailure).when(primaryJdbc).rollback();
        assertThatThrownBy(() -> preparation(configuration().with("database.standby.hosts", "replica:5236"), standby).prepare(List.of(FIRST)))
                .isSameAs(failure).satisfies(error -> assertThat(error.getSuppressed()).containsExactly(readerFailure));
        assertThat(readerFailure.getSuppressed()).containsExactly(primaryFailure);
        verify(primaryJdbc, times(2)).rollback();
    }

    @Test
    void shouldNotAcknowledgeSuccessWhenRollbackFails() throws Exception {
        doNothing().doThrow(new SQLException("rollback failed")).when(jdbc).rollback();
        assertThatThrownBy(() -> preparation(configuration(), null).prepare(List.of(FIRST)))
                .isInstanceOf(SQLException.class).hasMessageContaining("rollback failed");
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void shouldNotAcknowledgeSuccessWhenTheDriverFailsUnexpectedlyDuringRollback(boolean runtimeException) throws Exception {
        final Throwable failure = runtimeException ? new IllegalStateException("driver closed") : new AssertionError("driver error");
        doNothing().doThrow(failure).when(jdbc).rollback();
        assertThatThrownBy(() -> preparation(configuration(), null).prepare(List.of(FIRST))).isSameAs(failure);
    }

    @Test
    void shouldAbortTheSessionWhenPreparationExceedsTheSnapshotDeadline() throws Exception {
        final var aborted = new CountDownLatch(1);
        doAnswer(invocation -> {
            aborted.countDown();
            return null;
        }).when(jdbc).abort(any());
        doAnswer(invocation -> {
            try {
                Thread.sleep(30_000);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return null;
        }).when(reader).executeWithoutCommitting(any());
        assertThatThrownBy(() -> preparation(configuration().with("snapshot.max.duration.ms", 25), null).prepare(List.of(FIRST)))
                .isInstanceOf(DamengSnapshotDeadline.SnapshotTimeoutException.class).hasMessageContaining("snapshot.max.duration.ms=25");
        assertThat(Thread.currentThread().isInterrupted()).isFalse();
        // The deadline interrupts this thread before it aborts the JDBC session.
        // Wait for that required asynchronous request rather than racing its scheduler.
        assertThat(aborted.await(5, TimeUnit.SECONDS)).as("the expired deadline must request connection abort").isTrue();
        verify(jdbc).abort(any());
        verify(jdbc, times(2)).rollback();
    }

    @Test
    void shouldRejectAnEmptyRequestBeforeOpeningSessions() {
        assertThatThrownBy(() -> preparation(configuration(), null).prepare(List.of()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("at least one");
        verifyNoInteractions(jdbc);
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldPersistAnEmptyTableBoundaryWithoutAcquiringTableLocks() throws Exception {
        final Consumer<DamengCaptureScopePreparation.Baseline> persist = mock(Consumer.class);
        final var baseline = preparation(configuration(), null).prepareBoundary(persist);
        final var order = inOrder(reader, jdbc, persist);
        order.verify(jdbc).setAutoCommit(false);
        order.verify(jdbc).rollback();
        order.verify(reader).databaseMagic();
        order.verify(reader).currentLsn();
        order.verify(reader).validateUnchangedPrimary();
        order.verify(reader).validateUnchangedCompatibility();
        order.verify(reader).databaseMagic();
        order.verify(persist).accept(baseline);
        order.verify(jdbc).rollback();
        assertThat(baseline.lsn()).isEqualTo(300);
        assertThat(baseline.tables()).isEmpty();
        assertThat(baseline.databaseMagic()).isEqualTo(42);
        assertThat(baseline.primaryStandbyIdentity()).isNull();
        verify(reader, never()).executeWithoutCommitting(any());
        verify(reader, never()).readSchema(any(), any(), any(), any(), any(), eq(false));
        verify(reader, never()).getTableObjectId(any());
        verify(jdbc, never()).commit();
    }

    @Test
    void shouldUseTheReplayedPrimaryBoundaryEvenWithoutVisibleAddedTables() throws Exception {
        final var primary = mock(DamengConnection.class);
        final var primaryJdbc = mock(Connection.class);
        final var standby = mock(DamengStandbySupport.class);
        final var identity = identity(42, "open");
        when(standby.primary()).thenReturn(primary);
        when(primary.connection()).thenReturn(primaryJdbc);
        when(standby.snapshotBoundary(reader)).thenReturn(450L);
        when(reader.readPrimaryStandbyIdentity()).thenReturn(identity);
        final var baseline = preparation(configuration().with("database.standby.hosts", "replica:5236"), standby)
                .prepareBoundary(value -> {
                    assertThat(value.lsn()).isEqualTo(450);
                    assertThatCode(() -> verify(jdbc).rollback()).doesNotThrowAnyException();
                    assertThatCode(() -> verify(primaryJdbc).rollback()).doesNotThrowAnyException();
                });
        final var order = inOrder(reader, jdbc, primary, primaryJdbc, standby);
        order.verify(jdbc).setAutoCommit(false);
        order.verify(jdbc).rollback();
        order.verify(primaryJdbc).setAutoCommit(false);
        order.verify(primaryJdbc).rollback();
        order.verify(reader).readPrimaryStandbyIdentity();
        order.verify(standby).snapshotBoundary(reader);
        order.verify(reader).validateUnchangedPrimary();
        order.verify(reader).validateUnchangedCompatibility();
        order.verify(standby).validate(reader);
        order.verify(primary).validateUnchangedCompatibility();
        order.verify(reader).readPrimaryStandbyIdentity();
        order.verify(jdbc).rollback();
        order.verify(primaryJdbc).rollback();
        assertThat(baseline.tables()).isEmpty();
        assertThat(baseline.primaryStandbyIdentity()).isSameAs(identity);
        verify(reader, never()).executeWithoutCommitting(any());
        verify(primary, never()).executeWithoutCommitting(any());
        verify(reader, never()).readSchema(any(), any(), any(), any(), any(), eq(false));
    }

    @Test
    void shouldReleaseAnEmptyBoundaryTransactionWhenHistoryPersistenceFails() throws Exception {
        final var failure = new ConnectException("empty boundary history failed");
        assertThatThrownBy(() -> preparation(configuration(), null).prepareBoundary(baseline -> {
            assertThat(baseline.tables()).isEmpty();
            assertThatCode(() -> verify(jdbc).rollback()).doesNotThrowAnyException();
            throw failure;
        })).isSameAs(failure);
        verify(jdbc, times(2)).rollback();
        verify(reader, never()).executeWithoutCommitting(any());
    }

    @Test
    void shouldValidateAnEmptyBoundaryBeforePersistingIt() throws Exception {
        final var called = new AtomicBoolean();
        doThrow(new SQLException("compatibility changed")).when(reader).validateUnchangedCompatibility();
        assertThatThrownBy(() -> preparation(configuration(), null).prepareBoundary(baseline -> called.set(true)))
                .isInstanceOf(SQLException.class).hasMessageContaining("compatibility changed");
        assertThat(called.get()).isFalse();
        verify(jdbc, times(2)).rollback();
    }

    @Test
    void shouldRequireABoundaryPersistenceCallbackBeforeOpeningSessions() {
        assertThatThrownBy(() -> preparation(configuration(), null).prepareBoundary(null))
                .isInstanceOf(NullPointerException.class).hasMessageContaining("persistence callback");
        verifyNoInteractions(jdbc);
    }

    @Test
    void shouldRejectAnAlreadyInterruptedRequestBeforeOpeningSessions() {
        Thread.currentThread().interrupt();
        try {
            assertThatThrownBy(() -> preparation(configuration(), null).prepare(List.of(FIRST))).isInstanceOf(InterruptedException.class);
            verifyNoInteractions(jdbc);
        }
        finally {
            Thread.interrupted();
        }
    }

    @Test
    void shouldPreserveInterruptionAndReleaseAcquiredLocks() throws Exception {
        doAnswer(invocation -> {
            Thread.currentThread().interrupt();
            return null;
        }).when(reader).executeWithoutCommitting(any());
        try {
            assertThatThrownBy(() -> preparation(configuration(), null).prepare(List.of(FIRST, SECOND)))
                    .isInstanceOf(InterruptedException.class);
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
            verify(jdbc, times(2)).rollback();
            verify(reader, never()).currentLsn();
        }
        finally {
            Thread.interrupted();
        }
    }

    private DamengCaptureScopePreparation preparation(Configuration.Builder configuration, DamengStandbySupport standby) {
        return new DamengCaptureScopePreparation(new DamengConnectorConfig(configuration.build()), reader, standby);
    }

    private static Configuration.Builder configuration() {
        return DamengConnectorConfigTest.configuration();
    }

    private static Table table(TableId tableId) {
        return Table.editor().tableId(tableId)
                .addColumn(Column.editor().name("ID").position(1).jdbcType(Types.INTEGER).create())
                .addColumn(Column.editor().name("EXCLUDED_FROM_EVENTS").position(2).jdbcType(Types.VARCHAR).create())
                .setPrimaryKeyNames("ID")
                .addAttribute(Attribute.editor().name("existing.attribute").value("preserved").create()).create();
    }

    private static void metadata(DamengConnection connection, List<Table> tables) throws Exception {
        doAnswer(invocation -> {
            final Tables structures = invocation.getArgument(0);
            final String schema = invocation.getArgument(2);
            final TableFilter filter = invocation.getArgument(3);
            tables.stream().filter(table -> table.id().schema().equals(schema)).filter(table -> filter.isIncluded(table.id()))
                    .forEach(structures::overwriteTable);
            return null;
        }).when(connection).readSchema(any(), isNull(), any(), any(), isNull(), eq(false));
    }

    private static DamengDatabaseIdentity identity(long magic, String guid) {
        return new DamengDatabaseIdentity(1, magic, 500,
                List.of(new DamengDatabaseIdentity.OpenRecord(guid, Instant.EPOCH, magic, 0)));
    }
}
