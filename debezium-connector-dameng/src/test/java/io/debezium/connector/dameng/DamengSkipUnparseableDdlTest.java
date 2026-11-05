/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.LoggerFactory;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.connector.dameng.antlr.DamengDdlParser;
import io.debezium.connector.dameng.antlr.DamengDdlStatementAnalyzer.CascadeDropException;
import io.debezium.connector.dameng.antlr.DamengDdlStatementAnalyzer.PartitionExchangeException;
import io.debezium.connector.dameng.logminer.parser.DmlParserException;
import io.debezium.jdbc.MainConnectionProvidingConnectionFactory;
import io.debezium.junit.logging.LogInterceptor;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.ChangeEventSource.ChangeEventSourceContext;
import io.debezium.pipeline.spi.Offsets;
import io.debezium.pipeline.spi.SchemaChangeEventEmitter;
import io.debezium.relational.Attribute;
import io.debezium.relational.Column;
import io.debezium.relational.CustomConverterRegistry;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.relational.history.MemorySchemaHistory;
import io.debezium.schema.SchemaChangeEvent;
import io.debezium.util.Clock;

import ch.qos.logback.classic.Logger;

class DamengSkipUnparseableDdlTest {
    private static final String SKIP_UNPARSEABLE_DDL = "schema.history.internal.skip.unparseable.ddl";
    private static final String UNLEXABLE_DDL = "/* unclosed";
    private static final String UNPARSEABLE_DDL = "ALTER TABLE APP.A ADD LOST INT UNKNOWN";
    private static final String UNPLANNABLE_DDL = "ALTER TABLE APP.A ADD (LOST INT, ID INT)";
    private static final TableId CAPTURED = new TableId(null, "APP", "A");

    private final LogInterceptor logs = LogInterceptor.forClass(DamengStreamingChangeEventSource.class);
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final AtomicReference<Throwable> failure = new AtomicReference<>();
    private final AtomicInteger polls = new AtomicInteger();
    private final List<Table> deliveredTables = new ArrayList<>();
    private final List<Map<String, ?>> deliveredOffsets = new ArrayList<>();
    private final List<Map<String, ?>> offeredHeartbeatOffsets = new ArrayList<>();
    private final List<String> schemaStatements = new ArrayList<>();
    private DamengConnectorConfig config;
    private DamengPartition partition;
    private DamengOffsetContext offset;
    private DamengDatabaseSchema schema;
    private DamengStreamingChangeEventSource source;
    private ChangeEventSourceContext context;
    private EventDispatcher<DamengPartition, TableId> dispatcher;
    private ResultSet rows;

    static Stream<Arguments> partitionExchangeSettings() {
        return Stream.of("ALTER TABLE APP.A EXCHANGE PARTITION P1 WITH TABLE OTHER.B",
                "ALTER TABLE OTHER.B EXCHANGE SUBPARTITION S1 WITH TABLE APP.A")
                .flatMap(sql -> Stream.of(false, true).map(skip -> Arguments.of(sql, skip)));
    }

    @ParameterizedTest
    @MethodSource("partitionExchangeSettings")
    void shouldRequireTheExchangeOptInEvenWhenOtherIgnoreSettingsAreEnabled(String sql, boolean skip) throws Exception {
        configure(Boolean.toString(skip), true, true);
        final var original = schema.getTables().forTable(CAPTURED);
        final var before = offset.getOffset();
        run(List.of(ddl(101, 120, sql), dml(102, 120, false), commit(120)));
        assertThat(failure.get()).isInstanceOf(IllegalArgumentException.class).hasCauseInstanceOf(PartitionExchangeException.class)
                .hasMessageContaining("partition.exchange.ignore=true", "APP.A", "OTHER.B", "LSN 101", "XID 78", "DATA_OBJ# 7", sql);
        assertThat(logs.containsErrorMessage(sql)).isTrue();
        assertThat(logs.containsWarnMessage(sql)).isFalse();
        assertThat(deliveredTables).isEmpty();
        assertThat(schemaStatements).isEmpty();
        assertThat(schema.getTables().forTable(CAPTURED)).isSameAs(original);
        assertThat(offset.getOffset()).isEqualTo(before);
    }

    @ParameterizedTest
    @MethodSource("partitionExchangeSettings")
    void shouldIgnoreExchangesOnlyWhenEnabledAndResumeWithUnchangedHistory(String sql, boolean skip) throws Exception {
        configure(Boolean.toString(skip), false, false, true);
        final var original = schema.getTables().forTable(CAPTURED);
        final var exchange = ddl(101, 120, sql);
        run(List.of(exchange, dml(102, 120, false), commit(120)));
        assertThat(failure.get()).isNull();
        assertThat(logs.containsWarnMessage("partition.exchange.ignore=true")).isTrue();
        assertThat(logs.containsWarnMessage(sql)).isTrue();
        assertThat(logs.containsWarnMessage("wrong table downstream")).isTrue();
        assertThat(deliveredTables).containsExactly(original);
        assertThat(schemaStatements).isEmpty();
        assertThat(recover().forTable(CAPTURED)).isEqualTo(original);
        assertThat(offset.getCommitLsn()).isEqualTo(120);

        offset = new DamengOffsetContext.Loader(config).load(deliveredOffsets.get(0));
        deliveredTables.clear();
        polls.set(0);
        running.set(true);
        run(List.of(exchange, dml(102, 120, false), commit(120), dml(131, 140, false), commit(140)));
        assertThat(failure.get()).isNull();
        assertThat(deliveredTables).containsExactly(original);
        assertThat(recover().forTable(CAPTURED)).isEqualTo(original);
        assertThat(offset.getCommitLsn()).isEqualTo(140);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "ALTER TABLE OTHER.B EXCHANGE PARTITION P1 WITH TABLE APP.A UNKNOWN",
            "ALTER TABLE OTHER.B EXCHANGE PARTITION P1 WITH TABLE APP.A; DROP TABLE APP.A",
            "ALTER TABLE OTHER.B EXCHANGE PARTITION P1 WITH TABLE"
    })
    void shouldNotUseTheExchangeOptInForIncompleteOrCompoundStatements(String sql) throws Exception {
        configure("false", false, false, true);
        final var before = offset.getOffset();
        run(List.of(ddl(101, 120, sql), commit(120)));
        assertThat(failure.get()).isInstanceOf(IllegalArgumentException.class).hasCauseInstanceOf(IllegalArgumentException.class);
        assertThat(failure.get().getCause()).isNotInstanceOf(PartitionExchangeException.class);
        assertThat(schemaStatements).isEmpty();
        assertThat(offset.getOffset()).isEqualTo(before);
    }

    static Stream<Arguments> cascadeDropSettings() {
        return Stream.of(
                Arguments.of("DROP USER EXCLUDED_USER CASCADE", 0L),
                Arguments.of("DROP SCHEMA EXCLUDED_SCHEMA CASCADE", -1L),
                Arguments.of("DROP DOMAIN OTHER.D CASCADE", 419430400L),
                Arguments.of("DROP TYPE OTHER.TY CASCADE", 234882103L),
                Arguments.of("DROP CLASS OTHER.C CASCADE", 234882104L))
                .flatMap(statement -> Stream.of(false, true)
                        .map(skip -> Arguments.of(skip, statement.get()[0], statement.get()[1])));
    }

    @ParameterizedTest
    @MethodSource("cascadeDropSettings")
    void shouldRejectCascadingDropsBeforeFilteringEvenWhenGenericSkippingIsEnabled(boolean skipUnparseable, String sql, long objectId) throws Exception {
        configure(Boolean.toString(skipUnparseable));
        final var original = schema.getTables().forTable(CAPTURED);
        final var before = offset.getOffset();
        final Map<String, Object> drop = new HashMap<>(ddl(101, 120, sql));
        drop.put("DATA_OBJ#", objectId);
        run(List.of(drop, dml(102, 120, false), commit(120)));

        assertThat(failure.get()).isInstanceOf(IllegalArgumentException.class).hasCauseInstanceOf(CascadeDropException.class)
                .hasMessageContaining("LSN 101", "XID 78", "DATA_OBJ# " + objectId, sql, "cascade.drop.ignore=true",
                        "downstream data may remain", "same-name recreation", "incorrect decoding");
        assertThat(logs.containsErrorMessage(sql)).isTrue();
        assertThat(logs.containsErrorMessage("cascade.drop.ignore=true")).isTrue();
        assertThat(logs.containsWarnMessage(sql)).isFalse();
        assertThat(deliveredTables).isEmpty();
        assertThat(schemaStatements).isEmpty();
        assertThat(schema.getTables().forTable(CAPTURED)).isSameAs(original);
        assertThat(recover().forTable(CAPTURED)).isEqualTo(original);
        assertThat(offset.getOffset()).isEqualTo(before);
    }

    @ParameterizedTest
    @MethodSource("cascadeDropSettings")
    void shouldIgnoreOnlyOptedInCascadingDropsAndResumeWithUnchangedHistory(boolean skipUnparseable, String sql, long objectId) throws Exception {
        configure(Boolean.toString(skipUnparseable), false, true);
        final var original = schema.getTables().forTable(CAPTURED);
        final Map<String, Object> drop = new HashMap<>(ddl(101, 120, sql));
        drop.put("DATA_OBJ#", objectId);
        run(List.of(drop, dml(102, 120, false), commit(120)));

        assertThat(failure.get()).isNull();
        assertThat(deliveredTables).containsExactly(original);
        assertThat(schemaStatements).isEmpty();
        assertThat(recover().forTable(CAPTURED)).isEqualTo(original);
        assertThat(offset.getCommitLsn()).isEqualTo(120L);
        for (final var detail : List.of(sql, "cascade.drop.ignore=true", "LSN 101", "XID 78", "DATA_OBJ# " + objectId,
                "This DDL was ignored", "historical schemas", "downstream data may remain", "same-name recreation", "incorrect decoding")) {
            assertThat(logs.containsWarnMessage(detail)).as("warning contains %s", detail).isTrue();
        }
        assertThat(logs.containsErrorMessage(sql)).isFalse();

        offset = new DamengOffsetContext.Loader(config).load(deliveredOffsets.get(0));
        deliveredTables.clear();
        logs.clear();
        polls.set(0);
        running.set(true);
        run(List.of(drop, dml(102, 120, false), commit(120), dml(131, 140, false), commit(140)));
        assertThat(failure.get()).isNull();
        assertThat(deliveredTables).containsExactly(original);
        assertThat(logs.containsWarnMessage(sql)).isFalse();
        assertThat(offset.getCommitLsn()).isEqualTo(140L);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "DROP USER", "DROP SCHEMA S CASCADE UNKNOWN", "DROP TYPE APP.TY CASCADE; DROP PROFILE P",
            "DROP PROFILE P; DROP CLASS APP.C CASCADE", "DROP USER U CASCADE; ALTER TABLE APP.A ADD LOST INT",
            "DROP DOMAIN APP.D CASCADE INSERT INTO APP.A VALUES(1)", "TRUNCATE TABLE APP.A CASCADE",
            "ALTER TABLE APP.A DROP PARTITION P1"
    })
    void shouldNotUseCascadeDropOptInForMalformedCompoundOrOtherOperations(String sql) throws Exception {
        configure("false", false, true);
        final var before = offset.getOffset();
        run(List.of(ddl(101, 120, sql), dml(102, 120, false), commit(120)));
        assertThat(failure.get()).isInstanceOf(IllegalArgumentException.class);
        assertThat(deliveredTables).isEmpty();
        assertThat(schemaStatements).isEmpty();
        assertThat(offset.getOffset()).isEqualTo(before);
        assertThat(logs.containsWarnMessage("This DDL was ignored")).isFalse();
    }

    @ParameterizedTest
    @ValueSource(strings = { "CREATE TABLE APP.A(ID INT)", "CREATE TABLE APP.A AS SELECT 1 AS ID FROM DUAL" })
    void shouldKeepRejectingSameNameRecreationAfterIgnoringACascadingDrop(String sql) throws Exception {
        configure("false", false, true);
        final var original = schema.getTables().forTable(CAPTURED);
        run(List.of(ddl(101, 120, "DROP SCHEMA APP CASCADE"), commit(120), ddl(131, 140, sql), commit(140)));
        assertThat(failure.get()).isInstanceOf(RuntimeException.class).hasMessageContaining("existing", "APP.A");
        assertThat(schema.getTables().forTable(CAPTURED)).isSameAs(original);
        assertThat(recover().forTable(CAPTURED)).isEqualTo(original);
        assertThat(schemaStatements).isEmpty();
        assertThat(offset.getCommitLsn()).isEqualTo(120L);
    }

    @Test
    void shouldNotSkipDmlErrorsAfterIgnoringACascadingDrop() throws Exception {
        configure("false", false, true);
        final Map<String, Object> invalid = new HashMap<>(dml(102, 120, false));
        invalid.put("SQL_REDO", "not a valid DML statement");
        run(List.of(ddl(101, 120, "DROP TYPE OTHER.TY CASCADE"), invalid, commit(120)));
        assertThat(failure.get()).isInstanceOf(DmlParserException.class);
        assertThat(deliveredTables).isEmpty();
        assertThat(offeredHeartbeatOffsets).isEmpty();
    }

    @Test
    void shouldContinueProcessingSupportedTableDropsWhenCascadeDropIgnoringIsEnabled() throws Exception {
        configure("false", false, true);
        final var sql = "DROP TABLE APP.A CASCADE CONSTRAINTS";
        run(List.of(ddl(101, 120, sql), commit(120)));
        assertThat(failure.get()).isNull();
        assertThat(schemaStatements).containsExactly(sql);
        assertThat(schema.getTables().forTable(CAPTURED)).isNull();
        assertThat(recover().forTable(CAPTURED)).isNull();
        assertThat(logs.containsWarnMessage("This DDL was ignored")).isFalse();
    }

    static Stream<Arguments> partitionDataRemovalStatements() {
        return Stream.concat(Stream.of(
                "DROP PARTITION P1", "DROP PARTITION IF EXISTS P1 UPDATE GLOBAL INDEXES",
                "DROP PARTITION FOR(1)", "DROP SUBPARTITION FOR IF EXISTS(1,'a')",
                "DROP SUBPARTITION IF EXISTS P1 UPDATE GLOBAL INDEXES", "DROP SUBPARTITION FOR(1) IGNORE GLOBAL INDEXES")
                .map(clause -> Arguments.of("ALTER TABLE APP.A " + clause, 7L)),
                // Real partition TRUNCATE redo can name a physical child absent from the root-table cache.
                Stream.of("ALTER TABLE APP.A TRUNCATE PARTITION P1", "ALTER TABLE APP.A TRUNCATE PARTITION(P1) DROP STORAGE",
                        "ALTER TABLE APP.A TRUNCATE PARTITION FOR(1)", "ALTER TABLE APP.A TRUNCATE PARTITION P1 REUSE STORAGE UPDATE GLOBAL INDEXES",
                        "ALTER TABLE APP.A TRUNCATE SUBPARTITION S1", "ALTER TABLE APP.A TRUNCATE SUBPARTITION(S1) REUSE STORAGE",
                        "ALTER TABLE APP.A TRUNCATE SUBPARTITION FOR(1,1) IGNORE GLOBAL INDEXES", "TRUNCATE TABLE APP.A PARTITION P1",
                        "TRUNCATE TABLE APP.A(P1)")
                        .map(sql -> Arguments.of(sql, 99L)));
    }

    static Stream<Arguments> strictPartitionDataRemovalSettings() {
        return partitionDataRemovalStatements().flatMap(statement -> Stream.of(false, true)
                .map(skip -> Arguments.of(skip, statement.get()[0], statement.get()[1])));
    }

    @ParameterizedTest
    @MethodSource("strictPartitionDataRemovalSettings")
    void shouldRejectPartitionDataRemovalAndLogSqlAndOptInEvenWhenGenericSkippingIsEnabled(boolean skipUnparseable, String sql, long objectId) throws Exception {
        configure(Boolean.toString(skipUnparseable));
        final var before = offset.getOffset();
        final var original = schema.getTables().forTable(CAPTURED);
        final Map<String, Object> removal = new HashMap<>(ddl(101, 120, sql));
        removal.put("DATA_OBJ#", objectId);
        run(List.of(removal, dml(102, 120, false), commit(120)));
        assertThat(failure.get()).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unsupported", "partition.drop.ignore=true", "per-row DELETE", "continue CDC", "LSN 101", sql);
        assertThat(logs.containsErrorMessage(sql)).isTrue();
        assertThat(logs.containsErrorMessage("Unsupported")).isTrue();
        assertThat(logs.containsErrorMessage("partition.drop.ignore=true")).isTrue();
        assertThat(logs.containsErrorMessage("DATA_OBJ# " + objectId)).isTrue();
        assertThat(logs.containsWarnMessage(sql)).isFalse();
        assertThat(deliveredTables).isEmpty();
        assertThat(schemaStatements).isEmpty();
        assertThat(offset.getOffset()).isEqualTo(before);
        assertThat(schema.getTables().forTable(CAPTURED)).isSameAs(original);
    }

    @ParameterizedTest
    @MethodSource("partitionDataRemovalStatements")
    void shouldIgnorePartitionDataRemovalOnlyWhenExplicitlyEnabledAndResumeFromHistory(String sql, long objectId) throws Exception {
        configure("false", true);
        final var original = schema.getTables().forTable(CAPTURED);
        final Map<String, Object> removal = new HashMap<>(ddl(101, 120, sql));
        removal.put("DATA_OBJ#", objectId);
        run(List.of(removal, dml(102, 120, false), commit(120)));
        assertThat(failure.get()).isNull();
        assertThat(deliveredTables).containsExactly(original);
        assertThat(schemaStatements).isEmpty();
        assertThat(recover().forTable(CAPTURED)).isEqualTo(original);
        assertThat(offset.getCommitLsn()).isEqualTo(120);
        assertThat(logs.containsWarnMessage("partition.drop.ignore=true")).isTrue();
        assertThat(logs.containsWarnMessage(sql)).isTrue();
        assertThat(logs.containsWarnMessage("DATA_OBJ# " + objectId)).isTrue();

        offset = new DamengOffsetContext.Loader(config).load(deliveredOffsets.get(0));
        deliveredTables.clear();
        polls.set(0);
        running.set(true);
        run(List.of(removal, dml(102, 120, false), commit(120), dml(131, 140, false), commit(140)));
        assertThat(failure.get()).isNull();
        assertThat(deliveredTables).containsExactly(original);
        assertThat(offset.getCommitLsn()).isEqualTo(140);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "MERGE PARTITIONS P1,P2 INTO PARTITION P3",
            "SPLIT PARTITION P1 AT(10) INTO(PARTITION PA,PARTITION PB)"
    })
    void shouldSkipPartitionReorganizationDdlAndKeepEveryFollowingRowChange(String clause) throws Exception {
        configure("false");
        final var original = schema.getTables().forTable(CAPTURED);
        final var delete = row(104, 120, 2, "DELETE FROM \"150\".\"OBJ# 7\" WHERE \"COL 1\"=1");
        run(List.of(ddl(101, 120, "ALTER TABLE APP.A " + clause), dml(102, 120, false), dml(103, 120, false), delete, commit(120)));
        assertThat(failure.get()).isNull();
        assertThat(deliveredTables).containsExactly(original, original, original);
        assertThat(schemaStatements).isEmpty();
        assertThat(recover().forTable(CAPTURED)).isEqualTo(original);
        assertThat(offset.getCommitLsn()).isEqualTo(120);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "ALTER TABLE APP.A DROP PARTITION", "ALTER TABLE APP.A DROP PARTITION P1; DROP TABLE APP.A",
            "ALTER TABLE APP.A DROP PARTITION P1 INSERT INTO APP.A VALUES(1)",
            "ALTER TABLE APP.A TRUNCATE PARTITION", "ALTER TABLE APP.A TRUNCATE SUBPARTITION FOR()",
            "ALTER TABLE APP.A TRUNCATE PARTITION P1; DROP TABLE APP.A", "TRUNCATE TABLE APP.A PARTITION P1; DELETE FROM APP.A",
            "ALTER TABLE APP.A EXCHANGE PARTITION P1 WITH TABLE APP.B", "ALTER TABLE APP.A EXCHANGE SUBPARTITION S1 WITH TABLE APP.B"
    })
    void shouldNotUsePartitionDataRemovalOptInForMalformedOrDifferentOperations(String sql) throws Exception {
        configure("false", true);
        final var before = offset.getOffset();
        run(List.of(ddl(101, 120, sql), commit(120)));
        assertThat(failure.get()).isInstanceOf(IllegalArgumentException.class);
        assertThat(schemaStatements).isEmpty();
        assertThat(offset.getOffset()).isEqualTo(before);
    }

    @AfterEach
    void close() {
        if (schema != null) {
            schema.close();
        }
        logs.stop();
        ((Logger) LoggerFactory.getLogger(DamengStreamingChangeEventSource.class)).detachAppender(logs);
    }

    static Stream<Arguments> strictDdlSettings() {
        return Stream.of(UNLEXABLE_DDL, UNPARSEABLE_DDL, UNPLANNABLE_DDL).flatMap(sql -> Stream.of(
                Arguments.of(null, sql), Arguments.of("false", sql)));
    }

    @ParameterizedTest
    @MethodSource("strictDdlSettings")
    void shouldFailWithoutChangingHistoryOrAcknowledgingTheFollowingMarkerByDefaultOrWhenDisabled(String setting, String sql) throws Exception {
        configure(setting);
        final var original = schema.getTables().forTable(CAPTURED);
        final var before = offset.getOffset();

        run(List.of(ddl(101, 120, sql), dml(102, 120, false), commit(120)));

        assertThat(failure.get()).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("LSN 101");
        assertThat(deliveredTables).isEmpty();
        assertThat(schemaStatements).isEmpty();
        assertThat(schema.getTables().forTable(CAPTURED)).isSameAs(original);
        assertThat(recover().forTable(CAPTURED)).isEqualTo(original);
        assertThat(offset.getOffset()).isEqualTo(before);
        assertThat(logs.containsWarnMessage(sql)).isFalse();
    }

    @ParameterizedTest
    @ValueSource(strings = { UNLEXABLE_DDL, UNPARSEABLE_DDL, UNPLANNABLE_DDL })
    void shouldSkipParserFailuresAndContinueWithDataAndLaterSchemaChanges(String sql) throws Exception {
        configure("true");
        final var original = schema.getTables().forTable(CAPTURED);
        final var supportedDdl = "ALTER TABLE APP.A ADD NAME VARCHAR(30)";

        run(List.of(ddl(101, 120, sql), dml(102, 120, false), commit(120),
                ddl(131, 140, supportedDdl), dml(132, 140, true), commit(140)));

        assertThat(failure.get()).isNull();
        assertThat(deliveredTables).hasSize(2);
        assertThat(deliveredTables.get(0)).isSameAs(original);
        assertThat(deliveredTables.get(1).retrieveColumnNames()).containsExactly("ID", "NAME");
        assertThat(schemaStatements).containsExactly(supportedDdl);
        final var recovered = recover();
        assertThat(recovered.tableIds()).containsExactly(CAPTURED);
        assertThat(recovered.forTable(CAPTURED).retrieveColumnNames()).containsExactly("ID", "NAME");
        assertThat(recovered.forTable(CAPTURED).attributeWithName("dameng.object.id").asLong()).isEqualTo(7L);
        assertThat(offset.getCommitLsn()).isEqualTo(140L);
        assertThat(logs.containsWarnMessage(sql)).isTrue();
        assertThat(logs.containsWarnMessage("LSN 101")).isTrue();
        assertThat(logs.containsWarnMessage("DATA_OBJ# 7")).isTrue();
        assertThat(logs.containsWarnMessage(SKIP_UNPARSEABLE_DDL)).isTrue();
        assertThat(logs.containsErrorMessage("Full LogMinerRow:")).isFalse();
    }

    @ParameterizedTest
    @ValueSource(strings = { UNPARSEABLE_DDL, UNPLANNABLE_DDL })
    void shouldAdvancePastAnEntirelySkippedTransactionWithoutWritingSchemaHistory(String sql) throws Exception {
        configure("true");
        final var original = schema.getTables().forTable(CAPTURED);

        run(List.of(ddl(101, 120, sql), commit(120)));

        assertThat(failure.get()).isNull();
        assertThat(deliveredTables).isEmpty();
        assertThat(schemaStatements).isEmpty();
        assertThat(schema.getTables().forTable(CAPTURED)).isSameAs(original);
        assertThat(recover().forTable(CAPTURED)).isEqualTo(original);
        assertThat(offset.getCommitLsn()).isEqualTo(120L);
        assertThat(offset.getEventSerialNo()).isEqualTo(Long.MAX_VALUE);
        assertThat(offeredHeartbeatOffsets).isNotEmpty().allSatisfy(recorded -> {
            assertThat(recorded.get("lsn")).isEqualTo(150L);
            assertThat(recorded.get("commit_lsn")).isEqualTo(120L);
            assertThat(recorded.get("event_serial_no")).isEqualTo(Long.MAX_VALUE);
        });
    }

    @ParameterizedTest
    @ValueSource(strings = { UNPARSEABLE_DDL, UNPLANNABLE_DDL })
    void shouldRetainEarlierValidSchemaChangesInTheSameTransaction(String sql) throws Exception {
        configure("true");
        final var supportedDdl = "ALTER TABLE APP.A ADD NAME VARCHAR(30)";

        run(List.of(ddl(100, 120, supportedDdl), ddl(101, 120, sql), dml(102, 120, true), commit(120)));

        assertThat(failure.get()).isNull();
        assertThat(deliveredTables).singleElement().satisfies(table -> assertThat(table.retrieveColumnNames()).containsExactly("ID", "NAME"));
        assertThat(schemaStatements).containsExactly(supportedDdl);
        assertThat(recover().forTable(CAPTURED).retrieveColumnNames()).containsExactly("ID", "NAME");
        assertThat(offset.getCommitLsn()).isEqualTo(120L);
    }

    @Test
    void shouldNotReplayASkippedDdlOrAnAcknowledgedFollowingMarkerAfterReloadingMarkerOffset() throws Exception {
        configure("true");
        final var skipped = ddl(101, 120, UNPARSEABLE_DDL);
        final var marker = dml(102, 120, false);
        run(List.of(skipped, marker, commit(120)));
        assertThat(failure.get()).isNull();
        assertThat(deliveredOffsets).hasSize(1);
        final var markerOffset = deliveredOffsets.get(0);
        assertThat(markerOffset.get("lsn")).isEqualTo(90L);
        assertThat(markerOffset.get("commit_lsn")).isEqualTo(120L);
        assertThat(markerOffset.get("event_serial_no")).isEqualTo(1L);

        offset = new DamengOffsetContext.Loader(config).load(markerOffset);
        deliveredTables.clear();
        deliveredOffsets.clear();
        logs.clear();
        polls.set(0);
        running.set(true);

        run(List.of(skipped, marker, commit(120), dml(131, 140, false), commit(140)));

        assertThat(failure.get()).isNull();
        assertThat(deliveredTables).hasSize(1);
        assertThat(deliveredOffsets).singleElement().satisfies(recorded -> assertThat(recorded.get("commit_lsn")).isEqualTo(140L));
        assertThat(logs.containsWarnMessage(UNPARSEABLE_DDL)).isFalse();
        assertThat(offset.getCommitLsn()).isEqualTo(140L);
        assertThat(recover().forTable(CAPTURED).retrieveColumnNames()).containsExactly("ID");
    }

    @Test
    void shouldNotSkipDataParsingFailuresAfterSkippingDdl() throws Exception {
        configure("true");
        final Map<String, Object> invalidDml = new HashMap<>(dml(102, 120, false));
        invalidDml.put("SQL_REDO", "not a valid DML statement");

        run(List.of(ddl(101, 120, UNPARSEABLE_DDL), invalidDml, commit(120)));

        assertThat(failure.get()).isInstanceOf(DmlParserException.class).hasMessageContaining("Expected INSERT, UPDATE or DELETE", "OBJ# 7");
        assertThat(deliveredTables).isEmpty();
        assertThat(schemaStatements).isEmpty();
        assertThat(offeredHeartbeatOffsets).isEmpty();
        assertThat(offset.getLsn()).isEqualTo(90L);
        assertThat(logs.containsWarnMessage(UNPARSEABLE_DDL)).isTrue();
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = " ")
    void shouldStillRejectMissingDdlWhenSkippingIsEnabled(String sql) throws Exception {
        configure("true");
        final Map<String, Object> record = new HashMap<>(ddl(101, 120, "unused"));
        record.put("SQL_REDO", sql);
        final var original = schema.getTables().forTable(CAPTURED);
        final var before = offset.getOffset();

        run(List.of(record, dml(102, 120, false), commit(120)));

        assertThat(failure.get()).isInstanceOf(IllegalStateException.class).hasMessageContaining("Missing Dameng DDL SQL_REDO", "101");
        assertThat(deliveredTables).isEmpty();
        assertThat(schemaStatements).isEmpty();
        assertThat(schema.getTables().forTable(CAPTURED)).isSameAs(original);
        assertThat(offset.getOffset()).isEqualTo(before);
    }

    @ParameterizedTest
    @ValueSource(longs = { 0, -1 })
    void shouldStillRejectInvalidTableIdentityWhenSkippingIsEnabled(long objectId) throws Exception {
        configure("true");
        final Map<String, Object> record = new HashMap<>(ddl(101, 120, "ALTER TABLE APP.A ADD LOST INT"));
        record.put("DATA_OBJ#", objectId);
        final var original = schema.getTables().forTable(CAPTURED);
        final var before = offset.getOffset();

        run(List.of(record, dml(102, 120, false), commit(120)));

        assertThat(failure.get()).isInstanceOf(IllegalStateException.class).hasMessageContaining("DATA_OBJ#", "APP.A", "101");
        assertThat(deliveredTables).isEmpty();
        assertThat(schemaStatements).isEmpty();
        assertThat(schema.getTables().forTable(CAPTURED)).isSameAs(original);
        assertThat(offset.getOffset()).isEqualTo(before);
    }

    @Test
    void shouldNotSkipFailuresOutsideTheParserWhenSkippingIsEnabled() throws Exception {
        configure("true");
        final var dispatchFailure = new IllegalArgumentException("schema history delivery failed");
        doThrow(dispatchFailure).when(dispatcher).dispatchSchemaChangeEvent(any(), any(), any(TableId.class), any());

        run(List.of(ddl(101, 120, "ALTER TABLE APP.A ADD NAME VARCHAR(30)"), dml(102, 120, true), commit(120)));

        assertThat(failure.get()).isSameAs(dispatchFailure);
        assertThat(deliveredTables).isEmpty();
        assertThat(schema.getTables().forTable(CAPTURED).retrieveColumnNames()).containsExactly("ID");
        assertThat(logs.containsWarnMessage(SKIP_UNPARSEABLE_DDL)).isFalse();
    }

    @SuppressWarnings("unchecked")
    private void configure(String setting) throws Exception {
        configure(setting, false);
    }

    private void configure(String setting, boolean ignorePartitionDrops) throws Exception {
        configure(setting, ignorePartitionDrops, false);
    }

    private void configure(String setting, boolean ignorePartitionDrops, boolean ignoreCascadeDrops) throws Exception {
        configure(setting, ignorePartitionDrops, ignoreCascadeDrops, false);
    }

    private void configure(String setting, boolean ignorePartitionDrops, boolean ignoreCascadeDrops, boolean ignorePartitionExchanges) throws Exception {
        final var builder = DamengConnectorConfigTest.configuration()
                .with("log.mining.window.strategy", "fixed")
                .with("poll.interval.ms", 1)
                .with("table.include.list", "APP.A")
                .with(DamengConnectorConfig.PARTITION_DROP_IGNORE, ignorePartitionDrops)
                .with(DamengConnectorConfig.CASCADE_DROP_IGNORE, ignoreCascadeDrops)
                .with(DamengConnectorConfig.PARTITION_EXCHANGE_IGNORE, ignorePartitionExchanges)
                .with("schema.history.internal", MemorySchemaHistory.class.getName())
                .with("schema.history.internal.store.only.captured.tables.ddl", true);
        if (setting != null) {
            builder.with(SKIP_UNPARSEABLE_DDL, setting);
        }
        config = new DamengConnectorConfig(builder.build());
        partition = new DamengPartition(config.getLogicalName());
        offset = new DamengOffsetContext(config, 90);
        offset.validateDatabaseMagic(0);
        schema = new DamengDatabaseSchema(config, new DamengValueConverters(config), config.schemaNameAdjuster(),
                config.getTopicNamingStrategy(CommonConnectorConfig.TOPIC_NAMING_STRATEGY), new CustomConverterRegistry(List.of()),
                new DamengTaskContext(config.getConfig(), config));
        final var table = Table.editor().tableId(CAPTURED)
                .addColumn(Column.editor().name("ID").position(1).jdbcType(Types.INTEGER).type("INT").create())
                .setPrimaryKeyNames("ID").addAttribute(Attribute.editor().name("dameng.object.id").value("7").create()).create();
        schema.applySchemaChange(SchemaChangeEvent.ofSnapshotCreate(partition, offset, "DAMENG", table));
        context = mock(ChangeEventSourceContext.class);
        when(context.isRunning()).thenAnswer(invocation -> running.get());
        final var connection = mock(DamengConnection.class);
        when(connection.createDdlParser()).thenReturn(new DamengDdlParser());
        final var jdbc = mock(Connection.class);
        final var statement = mock(PreparedStatement.class);
        rows = mock(ResultSet.class);
        when(connection.connection()).thenReturn(jdbc);
        when(connection.logMinerContentsQuery(false)).thenReturn("mine");
        when(connection.readTablePreparedStatement(config, "mine", OptionalLong.empty())).thenReturn(statement);
        when(statement.executeQuery()).thenReturn(rows);
        when(connection.latestArchivedLsn()).thenAnswer(invocation -> {
            if (polls.getAndIncrement() > 0) {
                running.set(false);
            }
            return 150L;
        });
        final MainConnectionProvidingConnectionFactory<DamengConnection> factory = mock(MainConnectionProvidingConnectionFactory.class);
        when(factory.mainConnection()).thenReturn(connection);
        dispatcher = mock(EventDispatcher.class);
        doAnswer(invocation -> {
            deliveredTables.add(schema.getTables().forTable(invocation.getArgument(1)));
            deliveredOffsets.add(new HashMap<>(offset.getOffset()));
            return true;
        }).when(dispatcher).dispatchDataChangeEvent(any(), any(), any());
        doAnswer(invocation -> {
            final DamengOffsetContext heartbeatOffset = invocation.getArgument(1);
            offeredHeartbeatOffsets.add(new HashMap<>(heartbeatOffset.getOffset()));
            return null;
        }).when(dispatcher).dispatchHeartbeatEvent(any(), any());
        doAnswer(invocation -> {
            final SchemaChangeEventEmitter emitter = invocation.getArgument(3);
            emitter.emitSchemaChangeEvent(event -> {
                schemaStatements.add(event.getDdl());
                schema.applySchemaChange(event);
            });
            return null;
        }).when(dispatcher).dispatchSchemaChangeEvent(any(), any(), any(TableId.class), any());
        final var handler = mock(ErrorHandler.class);
        doAnswer(invocation -> {
            failure.set(invocation.getArgument(0));
            return null;
        }).when(handler).setProducerThrowable(any());
        source = new DamengStreamingChangeEventSource(config, factory, dispatcher, handler, Clock.SYSTEM, schema,
                mock(DamengStreamingChangeEventSourceMetrics.class));
    }

    private Tables recover() throws Exception {
        final var recovered = new Tables();
        schema.getSchemaHistory().recover(Offsets.of(partition, offset), recovered, null);
        return recovered;
    }

    private void run(List<Map<String, Object>> records) throws Exception {
        final var index = new AtomicInteger(-1);
        doAnswer(invocation -> index.incrementAndGet() < records.size()).when(rows).next();
        doAnswer(invocation -> ((Number) records.get(index.get()).getOrDefault(invocation.getArgument(0), 0L)).longValue()).when(rows).getLong(anyString());
        doAnswer(invocation -> ((Number) records.get(index.get()).getOrDefault(invocation.getArgument(0), 0)).intValue()).when(rows).getInt(anyString());
        doAnswer(invocation -> records.get(index.get()).get(invocation.getArgument(0))).when(rows).getString(anyString());
        doAnswer(invocation -> records.get(index.get()).get("XID")).when(rows).getBytes("XID");
        doAnswer(invocation -> Timestamp.valueOf("2026-09-24 12:00:00")).when(rows).getTimestamp("TIMESTAMP");
        source.execute(context, partition, offset);
    }

    private static Map<String, Object> ddl(long lsn, long commitLsn, String sql) {
        return row(lsn, commitLsn, 5, sql);
    }

    private static Map<String, Object> dml(long lsn, long commitLsn, boolean withName) {
        return row(lsn, commitLsn, 1, withName
                ? "INSERT INTO \"150\".\"OBJ# 7\"(\"COL 1\",\"COL 2\") VALUES(1,'after DDL')"
                : "INSERT INTO \"150\".\"OBJ# 7\"(\"COL 1\") VALUES(1)");
    }

    private static Map<String, Object> commit(long lsn) {
        return row(lsn, lsn, 7, "");
    }

    private static Map<String, Object> row(long lsn, long commitLsn, int operation, String sql) {
        return Map.of("SCN", lsn, "START_SCN", 90L, "COMMIT_SCN", commitLsn, "XID", new byte[]{ (byte) commitLsn },
                "OPERATION_CODE", operation, "DATA_OBJ#", 7L, "SQL_REDO", sql);
    }
}
