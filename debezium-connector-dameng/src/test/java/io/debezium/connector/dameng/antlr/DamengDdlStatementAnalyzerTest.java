/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng.antlr;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.sql.Types;
import java.util.Set;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import io.debezium.connector.dameng.DamengTableType;
import io.debezium.connector.dameng.antlr.DamengDdlParser.Type;
import io.debezium.connector.dameng.antlr.DamengDdlStatementAnalyzer.CascadeDropException;
import io.debezium.connector.dameng.antlr.DamengDdlStatementAnalyzer.Impact;
import io.debezium.connector.dameng.compatibility.DamengIdentifierRules;
import io.debezium.relational.Attribute;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;

class DamengDdlStatementAnalyzerTest {
    private final DamengDdlStatementAnalyzer analyzer = new DamengDdlStatementAnalyzer();

    static Stream<Arguments> cascadingObjectDropStatements() {
        return Stream.of("USER", "SCHEMA", "DOMAIN", "TYPE", "CLASS")
                .flatMap(kind -> Stream.of("", "IF EXISTS ").flatMap(exists -> {
                    final var targets = Stream.of("CDC_OBJECT", "\"CaScAdE\"");
                    final var names = kind.equals("USER") || kind.equals("SCHEMA") ? targets
                            : Stream.concat(targets, Stream.of("APP.CDC_OBJECT", "\"App\".\"Quoted Object\""));
                    return names.map(target -> Arguments.of("DROP " + kind + " " + exists + target + " /* policy */ CASCADE;",
                            "DROP " + kind, target));
                }));
    }

    @ParameterizedTest
    @MethodSource("cascadingObjectDropStatements")
    void shouldParseCascadingObjectDropsCompletelyAndReportTheirConsequences(String sql, String operation, String target) {
        assertThat(DamengParseTree.parseScope(sql, false).tree().statement()).hasSize(1);
        final var analysis = analyzer.analyze(sql, "APP");
        assertThat(analysis.impact()).isEqualTo(Impact.UNSUPPORTED);
        assertThat(analysis.failure()).isInstanceOf(CascadeDropException.class)
                .hasMessageContaining(operation, target, "cannot determine all affected tables", "historical schemas", "object-ID mappings",
                        "no deletion events", "downstream data may remain", "same-name recreation", "incorrect decoding", "cascade.drop.ignore=true");
        assertThatThrownBy(analysis::requireSupported).isSameAs(analysis.failure());
        assertThatThrownBy(() -> new DamengDdlParser().parse(sql, new Tables(), "APP")).isInstanceOf(CascadeDropException.class);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "DROP USER", "DROP SCHEMA S CASCADE UNKNOWN", "DROP DOMAIN APP.D CASCADE CONSTRAINTS",
            "DROP TYPE APP.T CASCADE RESTRICT", "DROP CLASS APP.C CASCADE CASCADE", "DROP CLASS BODY APP.C CASCADE",
            "DROP USER U CASCADE; DROP PROFILE P", "DROP PROFILE P; DROP TYPE APP.T CASCADE",
            "DROP SCHEMA S CASCADE; ALTER TABLE APP.T ADD V INT", "DROP TYPE APP.T CASCADE INSERT INTO APP.T VALUES(1)"
    })
    void shouldNotClassifyIncompleteUnknownOrCompoundStatementsAsOptInCascadingDrops(String sql) {
        final var analysis = analyzer.analyze(sql, "APP");
        assertThat(analysis.impact()).isEqualTo(Impact.UNSUPPORTED);
        assertThat(analysis.failure()).isNotInstanceOf(CascadeDropException.class);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "DROP USER CASCADE", "DROP SCHEMA CASCADE", "DROP DOMAIN APP.CASCADE", "DROP TYPE APP.CASCADE", "DROP CLASS APP.CASCADE",
            "DROP USER U /* CASCADE */", "DROP PROFILE P CASCADE", "DROP TYPE BODY APP.O CASCADE",
            "DROP TYPE BODY CASCADE", "DROP CLASS BODY CASCADE", "DROP TYPE BODY IF EXISTS CASCADE", "DROP CLASS BODY IF EXISTS CASCADE",
            "ALTER PACKAGE APP.P COMPILE CASCADE", "CREATE PROCEDURE APP.P AS BEGIN PRINT 'DROP USER U CASCADE'; END;"
    })
    void shouldKeepOtherCascadeUsagesOnTheirExistingNonTablePath(String sql) {
        assertThat(analyzer.analyze(sql, "APP").requireSupported().impact()).isEqualTo(Impact.NO_TABLE_CHANGE);
    }

    // DM SQL Language Manual 3.5.1.1, 3.5.2.1, 3.5.3 and 3.5.4.
    // https://eco.dameng.com/document/dm/zh-cn/pm/definition-statement.html
    // TABLE classification alone is insufficient: plan and parse must both produce
    // the correct historical columns, keys and lifecycle event without mutating a failed plan.
    @ParameterizedTest
    @MethodSource("ordinaryTableCreateStatements")
    void shouldCreateOrdinaryTablesWithOfficialOptions(String sql) {
        final var tables = new Tables();
        final var table = applyTableDdl(tables, sql, Type.CREATE);
        assertThat(table.retrieveColumnNames()).containsExactly("ID", "V");
        assertThat(table.primaryKeyColumnNames()).containsExactly("ID");
        assertThat(table.columnWithName("ID").jdbcType()).isEqualTo(Types.INTEGER);
        assertThat(table.columnWithName("ID").isOptional()).isFalse();
        assertThat(table.columnWithName("V").length()).isEqualTo(30);
        assertThat(table.columnWithName("V").defaultValueExpression()).contains("'initial'");
        assertThat(table.columnWithName("V").comment()).isEqualTo("value");
        assertThat(table.attributeWithName(DamengDdlParser.CONSTRAINT_PREFIX + "PK_T").value()).isEqualTo("PRIMARY");
    }

    static Stream<String> ordinaryTableCreateStatements() {
        return Stream.of("", "IF NOT EXISTS ")
                .flatMap(existence -> ordinaryTableNames().flatMap(name -> Stream
                        .of("", " TABLESPACE MAIN", " STORAGE(ON MAIN,INITIAL 1,NEXT 1,MINEXTENTS 1,FILLFACTOR 80)",
                                " STORAGE(BRANCH(2,3),WITHOUT COUNTER)", " STORAGE(NOBRANCH,CLUSTERBTR,USING LONG ROW)",
                                " DISKSPACE LIMIT 100", " DISKSPACE UNLIMITED", " SEGMENT CREATION IMMEDIATE", " SEGMENT CREATION DEFERRED",
                                " COMPRESS", " COMPRESS(V)", " COMPRESS EXCEPT(ID)", " PARALLEL", " PARALLEL 2", " NOPARALLEL",
                                " TABLESPACE MAIN SEGMENT CREATION DEFERRED COMPRESS(V) PARALLEL 2 ADD LOGIC LOG")
                        .map(options -> "CREATE TABLE " + existence + name
                                + "(ID INT,V VARCHAR(30) DEFAULT 'initial' COMMENT 'value',CONSTRAINT PK_T PRIMARY KEY(ID))" + options)));
    }

    static Stream<String> ordinaryTableNames() {
        return Stream.of("T", "APP.T", "\"APP\".\"T\"");
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "DEFAULT 7 NOT NULL", "NOT NULL DEFAULT 7", "DEFAULT ON NULL 7", "CONSTRAINT NN NOT NULL ENABLE DEFAULT 7"
    })
    void shouldRetainColumnDefaultAndNullabilityInEitherOrder(String attributes) {
        final var table = applyTableDdl(new Tables(), "CREATE TABLE APP.T(ID INT " + attributes + ")", Type.CREATE);
        assertThat(table.columnWithName("ID").defaultValueExpression()).contains("7");
        assertThat(table.columnWithName("ID").isOptional()).isFalse();
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "IDENTITY PRIMARY KEY", "IDENTITY(10,2) PRIMARY KEY", "PRIMARY KEY IDENTITY(10,2)",
            "AUTO_INCREMENT PRIMARY KEY", "PRIMARY KEY AUTO_INCREMENT"
    })
    void shouldRetainIdentityWithColumnConstraintOrdering(String attributes) {
        final var table = applyTableDdl(new Tables(), "CREATE TABLE APP.T(ID INT " + attributes + ",V INT)", Type.CREATE);
        assertThat(table.columnWithName("ID").isAutoIncremented()).isTrue();
        assertThat(table.primaryKeyColumnNames()).containsExactly("ID");
    }

    @ParameterizedTest
    @MethodSource("addColumnClauses")
    void shouldAddColumnsWithOptionalColumnExistenceAndGrouping(String clause) {
        final var tables = ordinaryTableHistory();
        final var original = tables.forTable(null, "APP", "T");
        final var table = applyTableDdl(tables, "ALTER TABLE APP.T " + clause, Type.ALTER);
        assertThat(table.retrieveColumnNames()).containsExactly("ID", "V", "AMOUNT", "LABEL");
        assertThat(table.columnWithName("ID")).isEqualTo(original.columnWithName("ID"));
        assertThat(table.columnWithName("V")).isEqualTo(original.columnWithName("V"));
        assertThat(table.columnWithName("AMOUNT").jdbcType()).isEqualTo(Types.DECIMAL);
        assertThat(table.columnWithName("AMOUNT").length()).isEqualTo(10);
        assertThat(table.columnWithName("AMOUNT").scale()).contains(2);
        assertThat(table.columnWithName("AMOUNT").defaultValueExpression()).contains("1.25");
        assertThat(table.columnWithName("AMOUNT").isOptional()).isFalse();
        assertThat(table.columnWithName("LABEL").position()).isEqualTo(4);
        assertThat(table.primaryKeyColumnNames()).containsExactly("ID");
        assertThat(table.attributes()).containsExactlyElementsOf(original.attributes());
    }

    static Stream<String> addColumnClauses() {
        return Stream.of("", "COLUMN ").flatMap(column -> Stream.of("", "IF NOT EXISTS ")
                .map(existence -> "ADD " + column + existence + "(AMOUNT DECIMAL(10,2) DEFAULT 1.25 NOT NULL,LABEL VARCHAR(20))"));
    }

    @ParameterizedTest
    @ValueSource(strings = { "ADD X INT", "ADD COLUMN X INT", "ADD IF NOT EXISTS X INT", "ADD COLUMN IF NOT EXISTS X INT", "ADD (X INT)" })
    void shouldAddASingleColumn(String clause) {
        final var table = applyTableDdl(ordinaryTableHistory(), "ALTER TABLE APP.T " + clause, Type.ALTER);
        assertThat(table.retrieveColumnNames()).containsExactly("ID", "V", "X");
        assertThat(table.columnWithName("X").isOptional()).isTrue();
    }

    @Test
    void shouldApplyConditionalColumnChangesToOnlyTheColumnsThatChange() {
        final var tables = ordinaryTableHistory();
        final var original = tables.forTable(null, "APP", "T");
        final var added = applyTableDdl(tables, "ALTER TABLE APP.T ADD COLUMN IF NOT EXISTS (V BIGINT,X INT)", Type.ALTER);
        assertThat(added.retrieveColumnNames()).containsExactly("ID", "V", "X");
        assertThat(added.columnWithName("V")).isEqualTo(original.columnWithName("V"));
        applyTableDdl(tables, "ALTER TABLE APP.T DROP COLUMN IF EXISTS X", Type.ALTER);
        final var unchanged = applyTableDdl(tables, "ALTER TABLE APP.T DROP COLUMN IF EXISTS X", Type.ALTER);
        assertThat(unchanged).isEqualTo(original);
        assertThat(unchanged.attributes()).containsExactlyElementsOf(original.attributes());
    }

    @ParameterizedTest
    @MethodSource("modifyColumnClauses")
    void shouldModifyOnlySpecifiedColumnProperties(String clause, int length, String defaultValue, boolean optional) {
        final var tables = ordinaryTableHistory();
        final var original = tables.forTable(null, "APP", "T");
        final var table = applyTableDdl(tables, "ALTER TABLE APP.T " + clause, Type.ALTER);
        final var column = table.columnWithName("V");
        assertThat(column.jdbcType()).isEqualTo(Types.VARCHAR);
        assertThat(column.length()).isEqualTo(length);
        assertThat(column.defaultValueExpression()).contains(defaultValue);
        assertThat(column.isOptional()).isEqualTo(optional);
        assertThat(column.comment()).isEqualTo("original");
        assertThat(column.position()).isEqualTo(2);
        assertThat(table.columnWithName("ID")).isEqualTo(original.columnWithName("ID"));
        assertThat(table.primaryKeyColumnNames()).containsExactly("ID");
    }

    static Stream<Arguments> modifyColumnClauses() {
        return Stream.of(
                Arguments.of("MODIFY V VARCHAR(80)", 80, "'old'", false),
                Arguments.of("MODIFY (V VARCHAR(80))", 80, "'old'", false),
                Arguments.of("MODIFY V DEFAULT 'new'", 30, "'new'", false),
                Arguments.of("MODIFY V NULL", 30, "'old'", true),
                Arguments.of("MODIFY (V VARCHAR(80) DEFAULT 'new' NULL)", 80, "'new'", true),
                Arguments.of("ALTER V SET DEFAULT 'new'", 30, "'new'", false),
                Arguments.of("ALTER COLUMN V SET DEFAULT ON NULL 'new'", 30, "'new'", false),
                Arguments.of("ALTER V SET NULL", 30, "'old'", true),
                Arguments.of("ALTER COLUMN V SET NOT NULL", 30, "'old'", false));
    }

    @Test
    void shouldModifyMultipleColumnsAndDropOnlyTheDefault() {
        final var tables = ordinaryTableHistory();
        applyTableDdl(tables, "ALTER TABLE APP.T MODIFY (ID BIGINT,V VARCHAR(80))", Type.ALTER);
        final var table = applyTableDdl(tables, "ALTER TABLE APP.T ALTER COLUMN V DROP DEFAULT", Type.ALTER);
        assertThat(table.columnWithName("ID").jdbcType()).isEqualTo(Types.BIGINT);
        assertThat(table.columnWithName("V").length()).isEqualTo(80);
        assertThat(table.columnWithName("V").defaultValueExpression()).isEmpty();
        assertThat(table.columnWithName("V").isOptional()).isFalse();
        assertThat(table.primaryKeyColumnNames()).containsExactly("ID");
    }

    @ParameterizedTest
    @MethodSource("dropColumnClauses")
    void shouldDropColumnsAndUpdateTheirPositions(String clause) {
        final var tables = ordinaryTableHistory();
        applyTableDdl(tables, "ALTER TABLE APP.T ADD X INT", Type.ALTER);
        final var table = applyTableDdl(tables, "ALTER TABLE APP.T " + clause, Type.ALTER);
        assertThat(table.retrieveColumnNames()).containsExactly("ID", "X");
        assertThat(table.columnWithName("X").position()).isEqualTo(2);
        assertThat(table.primaryKeyColumnNames()).containsExactly("ID");
    }

    static Stream<String> dropColumnClauses() {
        return Stream.of("", "COLUMN ").flatMap(column -> Stream.of("", "IF EXISTS ")
                .flatMap(existence -> Stream.of("", " RESTRICT", " CASCADE").map(cascade -> "DROP " + column + existence + "V" + cascade)));
    }

    @ParameterizedTest
    @ValueSource(strings = { "RENAME COLUMN ID TO NEW_ID", "ALTER ID RENAME TO NEW_ID", "ALTER COLUMN ID RENAME TO NEW_ID" })
    void shouldRenameColumnsIncludingPrimaryKeyMembership(String clause) {
        final var table = applyTableDdl(ordinaryTableHistory(), "ALTER TABLE APP.T " + clause, Type.ALTER);
        assertThat(table.retrieveColumnNames()).containsExactly("NEW_ID", "V");
        assertThat(table.primaryKeyColumnNames()).containsExactly("NEW_ID");
        assertThat(table.columnWithName("NEW_ID").position()).isEqualTo(1);
        assertThat(table.columnWithName("NEW_ID").isOptional()).isFalse();
    }

    @ParameterizedTest
    @MethodSource("primaryKeyConstraintDefinitions")
    void shouldTrackPrimaryKeysWithClusteringIndexStorageAndValidationOptions(String definition) {
        for (final var create : new boolean[]{ true, false }) {
            final var tables = new Tables();
            if (!create) {
                applyTableDdl(tables, "CREATE TABLE APP.T(ID INT,V INT)", Type.CREATE);
            }
            final var sql = create ? "CREATE TABLE APP.T(ID INT,V INT," + definition + ")" : "ALTER TABLE APP.T ADD " + definition;
            final var table = applyTableDdl(tables, sql, create ? Type.CREATE : Type.ALTER);
            assertThat(table.primaryKeyColumnNames()).containsExactly("ID", "V");
            assertThat(table.columns()).allSatisfy(column -> assertThat(column.isOptional()).isFalse());
            assertThat(table.attributeWithName(DamengDdlParser.CONSTRAINT_PREFIX + "PK_T").value()).isEqualTo("PRIMARY");
        }
    }

    static Stream<String> primaryKeyConstraintDefinitions() {
        return Stream.of("PRIMARY KEY", "CLUSTER PRIMARY KEY", "NOT CLUSTER PRIMARY KEY")
                .flatMap(key -> Stream.of("", " USING INDEX TABLESPACE MAIN", " USING INDEX TABLESPACE DEFAULT", " GLOBAL", " USING INDEX TABLESPACE MAIN GLOBAL")
                        .flatMap(storage -> Stream.of("", " ENABLE", " ENABLE VALIDATE", " ENABLE NOVALIDATE", " VALIDATE", " NOVALIDATE")
                                .map(validation -> "CONSTRAINT PK_T " + key + "(ID,V)" + storage + validation)));
    }

    @ParameterizedTest
    @ValueSource(strings = { "PRIMARY KEY(ID)", "CONSTRAINT PRIMARY KEY(ID)", "CONSTRAINT PK_T PRIMARY KEY(ID)" })
    void shouldAcceptOptionalConstraintKeywordAndNameWhenAddingAKey(String definition) {
        final var tables = new Tables();
        applyTableDdl(tables, "CREATE TABLE APP.T(ID INT,V INT)", Type.CREATE);
        final var table = applyTableDdl(tables, "ALTER TABLE APP.T ADD " + definition, Type.ALTER);
        assertThat(table.primaryKeyColumnNames()).containsExactly("ID");
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "PRIMARY KEY USING INDEX TABLESPACE MAIN ENABLE", "NOT CLUSTER PRIMARY KEY USING INDEX TABLESPACE DEFAULT",
            "CLUSTER PRIMARY KEY ENABLE"
    })
    void shouldApplyColumnPrimaryKeyStorageOptions(String definition) {
        final var table = applyTableDdl(new Tables(), "CREATE TABLE APP.T(ID INT CONSTRAINT PK_T " + definition + ",V INT)", Type.CREATE);
        assertThat(table.primaryKeyColumnNames()).containsExactly("ID");
        assertThat(table.attributeWithName(DamengDdlParser.CONSTRAINT_PREFIX + "PK_T").value()).isEqualTo("PRIMARY");
    }

    @ParameterizedTest
    @MethodSource("nonPrimaryConstraintDefinitions")
    void shouldRetainNonPrimaryConstraintIdentityWithoutChangingTheRowSchema(String definition, String kind) {
        final var tables = ordinaryTableHistory();
        final var original = tables.forTable(null, "APP", "T");
        final var table = applyTableDdl(tables, "ALTER TABLE APP.T ADD CONSTRAINT C " + definition, Type.ALTER);
        assertThat(table.columns()).containsExactlyElementsOf(original.columns());
        assertThat(table.primaryKeyColumnNames()).containsExactly("ID");
        assertThat(table.attributeWithName(DamengDdlParser.CONSTRAINT_PREFIX + "C").value()).isEqualTo(kind);
        final var dropped = applyTableDdl(tables, "ALTER TABLE APP.T DROP CONSTRAINT C", Type.ALTER);
        assertThat(dropped.columns()).containsExactlyElementsOf(original.columns());
        assertThat(dropped.attributes()).containsExactlyElementsOf(original.attributes());
    }

    static Stream<Arguments> nonPrimaryConstraintDefinitions() {
        return Stream.concat(
                Stream.of("", " ENABLE", " VALIDATE", " NOVALIDATE", " ENABLE VALIDATE", " ENABLE NOVALIDATE")
                        .flatMap(validation -> Stream.of(Arguments.of("CHECK(ID>0)" + validation, "CHECK"),
                                Arguments.of("UNIQUE(V) USING INDEX TABLESPACE MAIN" + validation, "UNIQUE"))),
                Stream.of("", "FOREIGN KEY ").flatMap(prefix -> Stream.of("", "PENDANT ")
                        .flatMap(pendant -> Stream.of("", " MATCH FULL", " MATCH PARTIAL", " MATCH SIMPLE")
                                .flatMap(match -> Stream.of("", " ON DELETE CASCADE", " ON UPDATE SET DEFAULT", " ON DELETE NO ACTION",
                                        " ON DELETE SET NULL ON UPDATE CASCADE", " ON UPDATE NO ACTION ON DELETE SET DEFAULT")
                                        .map(action -> Arguments.of("FOREIGN KEY(V) " + prefix + "REFERENCES " + pendant
                                                + "APP.P(V)" + match + action + " WITH INDEX ENABLE", "FOREIGN"))))));
    }

    @ParameterizedTest
    @ValueSource(strings = { "CLUSTER KEY", "CLUSTER UNIQUE KEY", "UNIQUE" })
    void shouldDistinguishPhysicalClusterKeysFromUniqueConstraints(String key) {
        final var table = applyTableDdl(new Tables(), "CREATE TABLE APP.T(ID INT,V INT,CONSTRAINT C " + key + "(V))", Type.CREATE);
        assertThat(table.primaryKeyColumnNames()).isEmpty();
        assertThat(table.columnWithName("V").isOptional()).isTrue();
        if (key.equals("CLUSTER KEY")) {
            assertThat(table.attributeWithName(DamengDdlParser.CONSTRAINT_PREFIX + "C")).isNull();
        }
        else {
            assertThat(table.attributeWithName(DamengDdlParser.CONSTRAINT_PREFIX + "C").value()).isEqualTo("UNIQUE");
        }
    }

    @ParameterizedTest
    @MethodSource("dropPrimaryKeyClauses")
    void shouldDropPrimaryKeyMetadataAndRestoreNullableColumns(String clause) {
        final var tables = new Tables();
        // DM 8.1.3.162 also removes explicit NOT NULL when dropping the primary key.
        applyTableDdl(tables, "CREATE TABLE APP.T(ID INT NOT NULL,V INT NOT NULL,X INT,CONSTRAINT PK_T NOT CLUSTER PRIMARY KEY(ID,V))", Type.CREATE);
        final var table = applyTableDdl(tables, "ALTER TABLE APP.T " + clause, Type.ALTER);
        assertThat(table.primaryKeyColumnNames()).isEmpty();
        assertThat(table.columns()).allSatisfy(column -> assertThat(column.isOptional()).isTrue());
        assertThat(table.attributeWithName(DamengDdlParser.CONSTRAINT_PREFIX + "PK_T")).isNull();
        applyTableDdl(tables, "ALTER TABLE APP.T ADD CONSTRAINT PK_T PRIMARY KEY(X)", Type.ALTER);
        assertThat(tables.forTable(null, "APP", "T").primaryKeyColumnNames()).containsExactly("X");
    }

    static Stream<String> dropPrimaryKeyClauses() {
        return Stream.concat(Stream.of("", " RESTRICT", " CASCADE")
                .flatMap(cascade -> Stream.of("DROP PRIMARY KEY" + cascade, "DROP CONSTRAINT PK_T" + cascade)),
                // The manual documents INDEX / KEEP INDEX; this old DB build only executes DROP INDEX.
                Stream.of("", " CASCADE").flatMap(cascade -> Stream.of(" INDEX", " DROP INDEX", " KEEP INDEX")
                        .map(index -> "DROP CONSTRAINT PK_T" + cascade + index)));
    }

    @ParameterizedTest
    @ValueSource(strings = { "DROP ID", "DROP COLUMN ID CASCADE", "DROP COLUMN IF EXISTS ID RESTRICT" })
    void shouldRemoveTheWholeCompositePrimaryKeyWhenDroppingOneKeyColumn(String clause) {
        final var tables = new Tables();
        applyTableDdl(tables, "CREATE TABLE APP.T(ID INT,V INT,X INT,CONSTRAINT PK_T NOT CLUSTER PRIMARY KEY(ID,V))", Type.CREATE);
        final var table = applyTableDdl(tables, "ALTER TABLE APP.T " + clause, Type.ALTER);
        assertThat(table.retrieveColumnNames()).containsExactly("V", "X");
        assertThat(table.primaryKeyColumnNames()).isEmpty();
        assertThat(table.attributeWithName(DamengDdlParser.CONSTRAINT_PREFIX + "PK_T")).isNull();
        assertThat(table.columnWithName("V").isOptional()).isFalse();
    }

    @Test
    void shouldRenameAConstraintAndResolveItsNewNameOnDrop() {
        final var tables = ordinaryTableHistory();
        final var renamed = applyTableDdl(tables, "ALTER TABLE APP.T RENAME CONSTRAINT PK_T TO \"New PK\"", Type.ALTER);
        assertThat(renamed.primaryKeyColumnNames()).containsExactly("ID");
        assertThat(renamed.attributeWithName(DamengDdlParser.CONSTRAINT_PREFIX + "PK_T")).isNull();
        assertThat(renamed.attributeWithName(DamengDdlParser.CONSTRAINT_PREFIX + "New PK").value()).isEqualTo("PRIMARY");
        final var dropped = applyTableDdl(tables, "ALTER TABLE APP.T DROP CONSTRAINT \"New PK\"", Type.ALTER);
        assertThat(dropped.primaryKeyColumnNames()).isEmpty();
        assertThat(dropped.columnWithName("ID").isOptional()).isTrue();
        assertThat(dropped.attributeWithName("dameng.object.id").value()).isEqualTo("71");
    }

    @Test
    void shouldAllowConstraintNamesToBeReusedAfterTheirColumnIsDropped() {
        final var tables = ordinaryTableHistory();
        applyTableDdl(tables, "ALTER TABLE APP.T ADD CONSTRAINT UQ_V UNIQUE(V)", Type.ALTER);
        applyTableDdl(tables, "ALTER TABLE APP.T DROP COLUMN V", Type.ALTER);
        applyTableDdl(tables, "ALTER TABLE APP.T ADD X INT CONSTRAINT UQ_V UNIQUE", Type.ALTER);
        applyTableDdl(tables, "ALTER TABLE APP.T DROP COLUMN X", Type.ALTER);
        applyTableDdl(tables, "ALTER TABLE APP.T ADD Y INT CONSTRAINT UQ_Y UNIQUE", Type.ALTER);
        applyTableDdl(tables, "ALTER TABLE APP.T RENAME CONSTRAINT UQ_Y TO UQ_V", Type.ALTER);
        final var table = applyTableDdl(tables, "ALTER TABLE APP.T DROP CONSTRAINT UQ_V", Type.ALTER);
        assertThat(table.retrieveColumnNames()).containsExactly("ID", "Y");
        assertThat(table.primaryKeyColumnNames()).containsExactly("ID");
        assertThat(table.attributeWithName(DamengDdlParser.CONSTRAINT_PREFIX + "UQ_V")).isNull();
        assertThat(table.attributeWithName(DamengDdlParser.CONSTRAINT_PREFIX + "UQ_Y")).isNull();
    }

    @ParameterizedTest
    @ValueSource(strings = { "ALTER TABLE T RENAME TO U", "ALTER TABLE APP.T RENAME TO \"U\"", "RENAME T TO U", "RENAME TABLE APP.T TO APP.U" })
    void shouldMoveTableHistoryOnRename(String sql) {
        final var tables = ordinaryTableHistory();
        final var original = tables.forTable(null, "APP", "T");
        final var table = applyTableDdl(tables, sql, Type.RENAME);
        assertThat(table.id()).isEqualTo(new TableId(null, "APP", "U"));
        assertThat(tables.forTable(null, "APP", "T")).isNull();
        assertThat(table.columns()).containsExactlyElementsOf(original.columns());
        assertThat(table.primaryKeyColumnNames()).containsExactly("ID");
        assertThat(table.attributes()).containsExactlyElementsOf(original.attributes());
    }

    @ParameterizedTest
    @MethodSource("ordinaryTableDropStatements")
    void shouldRemoveOrdinaryTableHistoryOnDrop(String sql) {
        final var tables = ordinaryTableHistory();
        final var original = tables.forTable(null, "APP", "T");
        assertThat(applyTableDdl(tables, sql, Type.DROP)).isEqualTo(original);
        assertThat(tables.tableIds()).isEmpty();
    }

    static Stream<String> ordinaryTableDropStatements() {
        return Stream.of("", "IF EXISTS ").flatMap(existence -> ordinaryTableNames()
                .flatMap(name -> Stream.of("", " RESTRICT", " CASCADE").map(cascade -> "DROP TABLE " + existence + name + cascade)));
    }

    @ParameterizedTest
    @MethodSource("wholeTableTruncateStatements")
    void shouldProduceATruncateChangeWhilePreservingTableHistory(String sql) {
        final var tables = ordinaryTableHistory();
        final var original = tables.forTable(null, "APP", "T");
        final var table = applyTableDdl(tables, sql, Type.TRUNCATE);
        assertThat(table).isEqualTo(original);
        assertThat(table.attributes()).containsExactlyElementsOf(original.attributes());
        assertThat(tables.forTable(null, "APP", "T")).isEqualTo(original);
    }

    static Stream<String> wholeTableTruncateStatements() {
        return ordinaryTableNames().flatMap(name -> Stream.of("", " DROP STORAGE", " REUSE STORAGE")
                .flatMap(storage -> Stream.of("", " UPDATE GLOBAL INDEXES", " IGNORE GLOBAL INDEXES")
                        .map(indexes -> "TRUNCATE TABLE " + name + storage + indexes)));
    }

    @ParameterizedTest
    @MethodSource("purgeStatements")
    void shouldSkipRecycleBinStatementsWithoutChangingCapturedTables(String sql) {
        final var analysis = analyzer.analyze(sql, "APP").requireSupported();
        assertThat(analysis.impact()).isEqualTo(Impact.NO_TABLE_CHANGE);
        assertThat(analysis.tableId()).isNull();
        assertThat(analysis.renamedTableId()).isNull();
        assertThat(DamengParseTree.parseScope(sql, false).tree().statement(0).nonTableStatement().purgeStatement()).isNotNull();

        final var tables = ordinaryTableHistory();
        final var original = tables.forTable(null, "APP", "T");
        final var parser = new DamengDdlParser();
        assertThat(parser.plan(analysis, tables)).isEmpty();
        assertThat(parser.parse(sql, tables, "APP")).isEmpty();
        assertThat(tables.tableIds()).containsExactly(original.id());
        assertThat(tables.forTable(original.id())).isSameAs(original);
    }

    static Stream<String> purgeStatements() {
        return Stream.of(
                Stream.of("PURGE RECYCLEBIN", "PURGE DBA_RECYCLEBIN"),
                Stream.of("TABLE", "INDEX").flatMap(kind -> ordinaryTableNames().map(name -> "PURGE " + kind + " " + name)),
                Stream.of("MAIN", "\"Main Space\"").flatMap(tablespace -> Stream.of("", " USER APP", " USER \"App User\"")
                        .map(user -> "PURGE TABLESPACE " + tablespace + user)),
                Stream.of("purge /* recycle bin */ recyclebin; -- done", "purge dba_recyclebin;",
                        "PURGE TABLE \"APP\".\"T; DROP TABLE APP.T\";", "PURGE INDEX \"APP\".\"I\"\"Name\";",
                        "PURGE TABLESPACE \"TS; DROP TABLE APP.T\" USER \"U\"\"Name\";"))
                .flatMap(statements -> statements);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "PURGE RECYCLEBIN", "PURGE DBA_RECYCLEBIN", "PURGE TABLE APP.T", "PURGE INDEX APP.I",
            "PURGE TABLESPACE MAIN", "PURGE TABLESPACE MAIN USER APP", "PURGE TABLE \"APP\".\"T; DROP TABLE APP.T\""
    })
    void shouldNotSwallowTableChangesOrDmlFollowingRecycleBinStatements(String sql) {
        assertFollowingTableChangesAreNotSkipped(sql);
        for (final var separator : new String[]{ " ", "; " }) {
            assertUnsupported(sql + separator + "INSERT INTO APP.T(ID) VALUES(1)");
            assertUnsupported(sql + separator + "UPDATE APP.T SET V='changed'");
            assertUnsupported(sql + separator + "DELETE FROM APP.T");
            assertUnsupported(sql + separator + "CALL APP.P()");
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "PURGE", "PURGE UNKNOWN", "PURGE RECYCLEBIN UNKNOWN", "PURGE DBA_RECYCLEBIN UNKNOWN",
            "PURGE TABLE", "PURGE INDEX", "PURGE TABLE APP.", "PURGE INDEX APP.",
            "PURGE TABLE APP.T UNKNOWN", "PURGE TABLE APP.T CASCADE", "PURGE INDEX APP.I UNKNOWN",
            "PURGE TABLE APP.T,APP.U", "PURGE TABLE APP.T.I", "PURGE TABLE IF EXISTS APP.T",
            "PURGE TABLESPACE", "PURGE TABLESPACE MAIN USER", "PURGE TABLESPACE MAIN USER APP UNKNOWN",
            "PURGE TABLESPACE APP.MAIN", "PURGE TABLESPACE MAIN USER APP.U", "PURGE TABLESPACE MAIN CASCADE"
    })
    void shouldRejectIncompleteOrUnsupportedRecycleBinStatements(String sql) {
        assertUnsupported(sql);
    }

    @ParameterizedTest
    @ValueSource(strings = { "RECYCLEBIN", "DBA_RECYCLEBIN" })
    void shouldKeepRecycleBinKeywordsUsableAsIdentifiers(String name) {
        final var parser = new DamengDdlParser();
        final var tables = new Tables();
        parser.parse("CREATE TABLE APP." + name + "(" + name + " INT)", tables, "APP");
        assertThat(tables.forTable(null, "APP", name).retrieveColumnNames()).containsExactly(name);
        assertThat(analyzer.analyze("CREATE TABLE APP.COPY AS SELECT " + name + " FROM APP." + name, "APP").requireSupported().isCtas()).isTrue();
    }

    @ParameterizedTest
    @MethodSource("ctasStatements")
    void shouldRecognizeCtasStatements(String sql) {
        final var analysis = analyzer.analyze(sql, "APP").requireSupported();
        assertThat(analysis.impact()).isEqualTo(Impact.TABLE);
        assertThat(analysis.isCtas()).isTrue();
        assertThat(analysis.tableId()).isEqualTo(new TableId(null, "APP", "T"));
        assertThat(analysis.isTemporaryTable()).isFalse();
    }

    static Stream<String> ctasStatements() {
        return Stream.of(
                "SELECT 1 AS ID FROM DUAL", "SELECT 1 AS ID",
                "SELECT A.ID,B.V FROM APP.A A JOIN APP.B B ON A.ID=B.ID",
                "SELECT A.ID,B.V FROM APP.A A LEFT JOIN APP.B B ON A.ID=B.ID",
                "SELECT ID FROM APP.A UNION ALL SELECT ID FROM APP.B",
                "SELECT ID FROM APP.A UNION SELECT ID FROM APP.B",
                "WITH Q AS (SELECT ID FROM APP.A) SELECT ID FROM Q",
                "SELECT X.ID FROM (SELECT ID FROM APP.A) X",
                "SELECT ID,(SELECT MAX(V) FROM APP.B) AS V FROM APP.A",
                "SELECT V,COUNT(*) AS TOTAL FROM APP.A GROUP BY V",
                "SELECT ID,ROW_NUMBER() OVER(ORDER BY ID) AS RN,LAG(V) OVER(ORDER BY ID) AS PREV FROM APP.A",
                "SELECT CASE WHEN ID=1 THEN V ELSE 'other' END AS V FROM APP.A",
                "SELECT CAST(NULL AS VARCHAR(20)) AS V,ID+1 AS NEXT_ID FROM APP.A",
                "SELECT TRUNCATE(1.23,1) AS V,JSON_VALUE(DOC,'$.x' RETURNING VARCHAR NULL ON ERROR) AS J FROM APP.A",
                "SELECT q'[; DROP TABLE APP.A]' AS V FROM DUAL",
                "(SELECT 1 AS ID FROM DUAL)")
                .map(query -> "CREATE TABLE APP.T AS " + query);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "CREATE TABLE APP.T(ID INT DEFAULT CAST(1 AS INTEGER))",
            "CREATE TABLE APP.T(V VARCHAR(100) DEFAULT 'AS SELECT 1 FROM DUAL')",
            "CREATE TABLE APP.T(\"AS SELECT\" INT)",
            "CREATE TABLE APP.T LIKE APP.S"
    })
    void shouldNotMistakeOrdinaryCreatesForCtas(String sql) {
        assertThat(analyzer.analyze(sql, "APP").requireSupported().isCtas()).isFalse();
    }

    @Test
    void shouldPreserveQuotedCtasTargetAndDefaultSchema() {
        final var analysis = analyzer.analyze("create /* AS SELECT */ table \"T; AS SELECT\" as select 1 as id", "APP").requireSupported();
        assertThat(analysis.isCtas()).isTrue();
        assertThat(analysis.tableId()).isEqualTo(new TableId(null, "APP", "T; AS SELECT"));
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "CREATE TABLE APP.T AS SELECT", "CREATE TABLE APP.T AS SELECT (1 FROM DUAL",
            "CREATE TABLE APP.T AS SELECT 1) FROM DUAL",
            "CREATE TABLE APP.T AS SELECT 1 FROM DUAL; DROP TABLE APP.S",
            "CREATE TABLE APP.T AS SELECT 1 FROM DUAL DROP TABLE APP.S",
            "CREATE TABLE APP.T AS SELECT 1 FROM DUAL; ALTER TABLE APP.S ADD X INT",
            "CREATE TABLE APP.T AS SELECT 1 FROM DUAL ALTER TABLE APP.S ADD X INT",
            "CREATE TABLE APP.T AS SELECT 1 FROM DUAL INSERT INTO APP.S VALUES(1)",
            "CREATE TABLE APP.T AS SELECT 1 FROM DUAL UPDATE APP.S SET ID=1",
            "CREATE TABLE APP.T AS SELECT 1 FROM DUAL GRANT SELECT ON APP.S TO U"
    })
    void shouldRejectIncompleteOrCompoundCtas(String sql) {
        assertUnsupported(sql);
    }

    @Test
    void shouldRequireOnlineDictionaryForOfflineCtasPlanningWithoutChangingHistory() {
        final var parser = new DamengDdlParser();
        final var tables = ordinaryTableHistory();
        final var original = tables.forTable(null, "APP", "T");
        final var analysis = analyzer.analyze("CREATE TABLE APP.COPY AS SELECT * FROM APP.T", "APP").requireSupported();
        assertThatThrownBy(() -> parser.plan(analysis, tables)).hasMessageContaining("online dictionary");
        assertThat(tables.forTable(null, "APP", "COPY")).isNull();
        assertThat(tables.forTable(null, "APP", "T")).isSameAs(original);
    }

    @Test
    void shouldKeepTemporaryCtasAsNonRowHistory() {
        final var sql = "CREATE GLOBAL TEMPORARY TABLE APP.T AS SELECT ROW_NUMBER() OVER(ORDER BY ID) AS RN FROM APP.S";
        final var analysis = analyzer.analyze(sql, "APP").requireSupported();
        assertThat(analysis.isCtas()).isTrue();
        assertThat(analysis.isTemporaryTable()).isTrue();
        assertThat(DamengTableType.isNonRow(applyTableDdl(new Tables(), sql, Type.CREATE))).isTrue();
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "CREATE TABLE APP.T(ID INT, pass VARCHAR(50) ENCRYPT)",
            "CREATE TABLE APP.T(ID INT, pass VARCHAR(50) ENCRYPT DEFAULT 'initial' NOT NULL COMMENT 'secret')",
            "CREATE TABLE APP.T(ID INT, pass VARCHAR(50) DEFAULT 'initial' NOT NULL ENCRYPT COMMENT 'secret')"
    })
    void testEncyptColumn(String sql) {
        final var table = applyTableDdl(new Tables(), sql, Type.CREATE);
        assertThat(table.retrieveColumnNames()).containsExactly("ID", "PASS");
        assertThat(table.columnWithName("PASS").jdbcType()).isEqualTo(Types.VARCHAR);
        assertThat(table.columnWithName("PASS").length()).isEqualTo(50);
        assertThat(table).isEqualTo(applyTableDdl(new Tables(), sql.replace(" ENCRYPT", ""), Type.CREATE));
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "ADD PASS VARCHAR(50) ENCRYPT DEFAULT 'initial' NOT NULL COMMENT 'secret'",
            "ADD COLUMN (PASS VARCHAR(50) ENCRYPT DEFAULT 'initial' NOT NULL COMMENT 'secret')",
            "MODIFY PASS VARCHAR(50) ENCRYPT DEFAULT 'initial' NOT NULL COMMENT 'secret'"
    })
    void shouldRetainLogicalColumnDefinitionsWhenAddingOrModifyingEncryptedColumns(String clause) {
        final var tables = ordinaryTableHistory();
        final var original = tables.forTable(null, "APP", "T");
        if (clause.startsWith("MODIFY")) {
            applyTableDdl(tables, "ALTER TABLE APP.T ADD PASS VARCHAR(20)", Type.ALTER);
        }
        final var table = applyTableDdl(tables, "ALTER TABLE APP.T " + clause, Type.ALTER);
        assertThat(table.retrieveColumnNames()).containsExactly("ID", "V", "PASS");
        assertThat(table.columnWithName("PASS").jdbcType()).isEqualTo(Types.VARCHAR);
        assertThat(table.columnWithName("PASS").length()).isEqualTo(50);
        assertThat(table.columnWithName("PASS").defaultValueExpression()).contains("'initial'");
        assertThat(table.columnWithName("PASS").isOptional()).isFalse();
        assertThat(table.columnWithName("PASS").comment()).isEqualTo("secret");
        assertThat(table.columnWithName("ID")).isEqualTo(original.columnWithName("ID"));
        assertThat(table.columnWithName("V")).isEqualTo(original.columnWithName("V"));
        assertThat(table.primaryKeyColumnNames()).containsExactly("ID");
        assertThat(table.attributes()).containsExactlyElementsOf(original.attributes());
    }

    @Test
    void shouldPreserveUnspecifiedColumnPropertiesWhenOnlyEnablingTransparentEncryption() {
        final var tables = ordinaryTableHistory();
        final var original = tables.forTable(null, "APP", "T");
        final var table = applyTableDdl(tables, "ALTER TABLE APP.T MODIFY V ENCRYPT", Type.ALTER);
        assertThat(table).isEqualTo(original);
        assertThat(table.attributes()).containsExactlyElementsOf(original.attributes());
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "CREATE TABLE APP.T(ID INT, PASS VARCHAR(50) ENCRYPT WITH)",
            "CREATE TABLE APP.T(ID INT, PASS VARCHAR(50) ENCRYPT UNKNOWN)",
            "CREATE TABLE APP.T(ID INT, PASS VARCHAR(50) ENCRYPT); DROP TABLE APP.T",
            "ALTER TABLE APP.T ADD PASS VARCHAR(50) ENCRYPT; DROP TABLE APP.T",
            "ALTER TABLE APP.T MODIFY V ENCRYPT; INSERT INTO APP.T(ID) VALUES(1)"
    })
    void shouldRejectMalformedOrCompoundEncryptedColumnStatements(String sql) {
        assertUnsupported(sql);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "CREATE TABLE APP.T(ID INT PRIMARY KEY DISABLE)",
            "CREATE TABLE APP.T(ID INT,CONSTRAINT PK_T PRIMARY KEY(ID) DISABLE NOVALIDATE)",
            "ALTER TABLE APP.T ADD CONSTRAINT C UNIQUE(V) DISABLE",
            "ALTER TABLE APP.T DISABLE CONSTRAINT PK_T",
            "TRUNCATE TABLE APP.T CASCADE", "TRUNCATE TABLE APP.T REUSE STORAGE CASCADE UPDATE GLOBAL INDEXES"
    })
    void shouldRejectTableSemanticsThatCannotBeRepresentedWithoutChangingHistory(String sql) {
        final var parser = new DamengDdlParser();
        final var tables = sql.startsWith("CREATE") ? new Tables() : ordinaryTableHistory();
        final var original = tables.forTable(null, "APP", "T");
        final var analysis = analyzer.analyze(sql, "APP").requireSupported();
        assertThat(analysis.impact()).isEqualTo(Impact.TABLE);
        assertThatThrownBy(() -> parser.plan(analysis, tables)).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("Unsupported");
        assertThatThrownBy(() -> parser.parse(sql, tables, "APP")).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("Unsupported");
        assertThat(tables.forTable(null, "APP", "T")).isSameAs(original);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "ALTER TABLE APP.T ADD ID INT", "ALTER TABLE APP.T MODIFY MISSING INT", "ALTER TABLE APP.T DROP MISSING",
            "ALTER TABLE APP.T DROP CONSTRAINT MISSING", "ALTER TABLE APP.T DROP CONSTRAINT", "ALTER TABLE APP.T RENAME COLUMN ID TO V",
            "ALTER TABLE APP.T ALTER COLUMN ID RENAME TO V", "ALTER TABLE APP.T RENAME CONSTRAINT MISSING TO C",
            "ALTER TABLE APP.T ADD PRIMARY KEY(V)",
            "ALTER TABLE APP.T ADD CONSTRAINT C FOREIGN KEY(V) REFERENCES APP.P(V) ON DELETE CASCADE ON DELETE SET NULL"
    })
    void shouldRejectHistoricalTableConflictsAtomically(String sql) {
        final var parser = new DamengDdlParser();
        final var tables = ordinaryTableHistory();
        final var original = tables.forTable(null, "APP", "T");
        assertThatThrownBy(() -> parser.plan(analyzer.analyze(sql, "APP"), tables)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> parser.parse("ALTER TABLE APP.T ADD BEFORE_FAILURE INT;" + sql, tables, "APP"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(tables.forTable(null, "APP", "T")).isSameAs(original);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "CREATE TABLE IF EXISTS APP.T(ID INT)", "CREATE TABLE APP.T()", "CREATE TABLE APP.T(ID INT) UNKNOWN",
            "ALTER TABLE APP.T ADD", "ALTER TABLE APP.T ADD COLUMN IF EXISTS X INT", "ALTER TABLE APP.T ADD (X INT,)",
            "ALTER TABLE APP.T MODIFY ID", "ALTER TABLE APP.T MODIFY ()", "ALTER TABLE APP.T DROP COLUMN IF NOT EXISTS V",
            "ALTER TABLE APP.T DROP CONSTRAINT C CASCADE KEEP", "ALTER TABLE APP.T ADD CONSTRAINT", "ALTER TABLE APP.T ADD PRIMARY KEY()",
            "ALTER TABLE APP.T RENAME COLUMN ID NEW_ID", "ALTER TABLE APP.T ALTER ID RENAME NEW_ID",
            "ALTER TABLE APP.T DROP CONSTRAINT C KEEP", "DROP TABLE IF NOT EXISTS APP.T", "DROP TABLE APP.T UNKNOWN",
            "TRUNCATE TABLE APP.T DROP", "TRUNCATE TABLE APP.T REUSE STORAGE DROP STORAGE", "TRUNCATE TABLE APP.T UPDATE GLOBAL",
            "ALTER TABLE APP.T ADD X INT; DROP TABLE APP.T", "TRUNCATE TABLE APP.T; INSERT INTO APP.T(ID) VALUES(1)"
    })
    void shouldRejectMalformedOrCompoundOrdinaryTableStatements(String sql) {
        assertUnsupported(sql);
    }

    private Tables ordinaryTableHistory() {
        final var tables = new Tables();
        final var table = applyTableDdl(tables, "CREATE TABLE APP.T(ID INT,V VARCHAR(30) DEFAULT 'old' NOT NULL COMMENT 'original',"
                + "CONSTRAINT PK_T PRIMARY KEY(ID))", Type.CREATE);
        tables.overwriteTable(table.edit().addAttributes(table.attributes())
                .addAttribute(Attribute.editor().name("dameng.object.id").value("71").create()).create());
        return tables;
    }

    private Table applyTableDdl(Tables tables, String sql, Type type) {
        final var parser = new DamengDdlParser();
        final var analysis = analyzer.analyze(sql, "APP").requireSupported();
        assertThat(analysis.impact()).as(sql).isEqualTo(Impact.TABLE);
        assertThat(analysis.tableId()).isEqualTo(new TableId(null, "APP", "T"));
        final var original = tables.forTable(analysis.tableId());
        final var originalIds = Set.copyOf(tables.tableIds());
        final var plan = parser.plan(analysis, tables);
        assertThat(plan).as(sql).singleElement().satisfies(change -> assertThat(change.type()).isEqualTo(type));
        assertThat(tables.forTable(analysis.tableId())).isSameAs(original);
        assertThat(tables.tableIds()).containsExactlyInAnyOrderElementsOf(originalIds);
        final var changes = parser.parse(sql, tables, "APP");
        assertThat(changes).containsExactlyElementsOf(plan);
        return changes.get(0).table();
    }

    // DM SQL Language Manual 3.5.1.4 and 3.5.2.2, including interval, recursive
    // subpartition definitions and the four documented SPLIT productions.
    // https://eco.dameng.com/document/dm/zh-cn/pm/definition-statement.html
    @ParameterizedTest
    @MethodSource("partitionedTableDefinitions")
    void shouldCreateAlterAndDropPartitionedTablesLikeOrdinaryTables(String definition) {
        for (final var existence : new String[]{ "", "IF NOT EXISTS " }) {
            final var sql = "CREATE TABLE " + existence + "APP.T(ID INT PRIMARY KEY,V VARCHAR(30)) " + definition;
            final var analysis = analyzer.analyze(sql, "APP").requireSupported();
            final var id = new TableId(null, "APP", "T");
            assertThat(analysis.impact()).isEqualTo(Impact.TABLE);
            assertThat(analysis.tableId()).isEqualTo(id);
            final var tables = new Tables();
            final var parser = new DamengDdlParser();
            assertThat(parser.parse(sql, tables, "APP")).singleElement().satisfies(change -> {
                assertThat(change.type()).isEqualTo(DamengDdlParser.Type.CREATE);
                assertThat(change.table().retrieveColumnNames()).containsExactly("ID", "V");
                assertThat(change.table().primaryKeyColumnNames()).containsExactly("ID");
            });
            parser.parse("ALTER TABLE APP.T ADD EXTRA INT DEFAULT 9", tables, "APP");
            assertThat(tables.forTable(id).retrieveColumnNames()).containsExactly("ID", "V", "EXTRA");
            parser.parse("ALTER TABLE APP.T DROP COLUMN EXTRA", tables, "APP");
            assertThat(tables.forTable(id).retrieveColumnNames()).containsExactly("ID", "V");
            assertThat(parser.parse("DROP TABLE IF EXISTS APP.T", tables, "APP")).singleElement()
                    .satisfies(change -> assertThat(change.type()).isEqualTo(DamengDdlParser.Type.DROP));
            assertThat(tables.tableIds()).isEmpty();
        }
    }

    static Stream<String> partitionedTableDefinitions() {
        return Stream.concat(Stream.of(
                "PARTITION BY HASH(ID) PARTITIONS 4",
                "PARTITION BY HASH(ID) PARTITIONS 2 STORE IN(MAIN,TS2)",
                "PARTITION BY HASH(ID)(PARTITION P1 TABLESPACE MAIN,PARTITION P2 STORAGE(ON MAIN))",
                "PARTITION BY RANGE(ID)(PARTITION P1 VALUES LESS THAN(10),PARTITION P2 VALUES LESS THAN(MAXVALUE))",
                "PARTITION BY RANGE(ID)(PARTITION P1 VALUES EQU OR LESS THAN(10),PARTITION P2 VALUES LESS THAN(MAXVALUE))",
                "PARTITION BY RANGE(ID,V)(PARTITION P1 VALUES LESS THAN(10,'m'),PARTITION P2 VALUES LESS THAN(MAXVALUE,MAXVALUE))",
                "PARTITION BY RANGE(ID) INTERVAL(10)(PARTITION P1 VALUES LESS THAN(10))",
                "PARTITION BY LIST(V)(PARTITION P1 VALUES('a','b'),PARTITION P2 VALUES(DEFAULT))",
                "PARTITION BY HASH(ID) SUBPARTITION BY LIST(V) SUBPARTITION TEMPLATE"
                        + "(SUBPARTITION S1 VALUES('a'),SUBPARTITION SD VALUES(DEFAULT)) PARTITIONS 4",
                "PARTITION BY RANGE(ID) SUBPARTITION BY HASH(ID) SUBPARTITIONS 2"
                        + "(PARTITION P1 VALUES LESS THAN(10),PARTITION PM VALUES LESS THAN(MAXVALUE))",
                "PARTITION BY RANGE(ID) SUBPARTITION BY HASH(ID) SUBPARTITION TEMPLATE SUBPARTITIONS 2 STORE IN(MAIN)"
                        + "(PARTITION P1 VALUES LESS THAN(10),PARTITION PM VALUES LESS THAN(MAXVALUE))",
                "PARTITION BY RANGE(ID) SUBPARTITION BY HASH(ID) SUBPARTITION TEMPLATE"
                        + "(SUBPARTITION S1 TABLESPACE MAIN,SUBPARTITION S2 STORAGE(ON MAIN))"
                        + "(PARTITION P1 VALUES LESS THAN(10) SUBPARTITIONS 3 STORE IN(MAIN),PARTITION PM VALUES LESS THAN(MAXVALUE))",
                "PARTITION BY LIST(V) SUBPARTITION BY RANGE(ID) SUBPARTITION TEMPLATE"
                        + "(SUBPARTITION S1 VALUES LESS THAN(10),SUBPARTITION SM VALUES LESS THAN(MAXVALUE))"
                        + "(PARTITION P1 VALUES('a')(SUBPARTITION CUSTOM1 VALUES LESS THAN(20),SUBPARTITION CUSTOM2 VALUES LESS THAN(MAXVALUE)),"
                        + "PARTITION PD VALUES(DEFAULT))",
                "PARTITION BY RANGE(ID) SUBPARTITION BY LIST(V) SUBPARTITION TEMPLATE"
                        + "(SUBPARTITION S1 VALUES('a'),SUBPARTITION SD VALUES(DEFAULT)),"
                        + "SUBPARTITION BY HASH(ID) SUBPARTITION TEMPLATE SUBPARTITIONS 2"
                        + "(PARTITION P1 VALUES LESS THAN(10)(SUBPARTITION C1 VALUES('a') SUBPARTITIONS 3,"
                        + "SUBPARTITION CD VALUES(DEFAULT)(SUBPARTITION H1,SUBPARTITION H2)),PARTITION PM VALUES LESS THAN(MAXVALUE))",
                "PARTITION BY HASH(ID) PARTITIONS 4 STORAGE(HASHPARTMAP(2),ON MAIN) ENABLE ROW MOVEMENT",
                "PARTITION BY HASH(ID) PARTITIONS 2 DISKSPACE UNLIMITED",
                "PARTITION BY HASH(ID) PARTITIONS 2 DISKSPACE LIMIT 1024 ADD LOGIC LOG",
                "PARTITION BY HASH(ID) PARTITIONS 2 AUTO_INCREMENT 10",
                "PARTITION BY HASH(ID) PARTITIONS 2 AUTO_INCREMENT = 2147483648",
                "PARTITION BY LIST(V)(PARTITION \"分区; DROP TABLE APP.T\" VALUES(q'[a; DROP TABLE APP.T]'),PARTITION PD VALUES(DEFAULT))"),
                Stream.of("", " TABLESPACE MAIN", " STORAGE(INITIAL 1,NEXT 2,MINEXTENTS 1,FILLFACTOR 80)")
                        .flatMap(storage -> Stream.of("", " SEGMENT CREATION IMMEDIATE", " SEGMENT CREATION DEFERRED")
                                .flatMap(segment -> Stream.of("", " COMPRESS", " COMPRESS(V)", " COMPRESS EXCEPT(V)")
                                        .flatMap(compress -> Stream.of("", " PARALLEL", " PARALLEL 2", " NOPARALLEL")
                                                .flatMap(parallel -> Stream.of("", " ENABLE ROW MOVEMENT", " DISABLE ROW MOVEMENT")
                                                        .map(movement -> "PARTITION BY HASH(ID) PARTITIONS 2" + storage + segment + compress + parallel + movement))))));
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "PARTITION BY HASH(ID) PARTITIONS 2 STORAGE(HASHPARTMAP(2))",
            "PARTITION BY RANGE(ID) INTERVAL(10)(PARTITION P1 VALUES LESS THAN(10))",
            "PARTITION BY LIST(V)(PARTITION P1 VALUES('a'),PARTITION PD VALUES(DEFAULT))"
    })
    void shouldRecognizePartitionedCtasWithoutSourceHistory(String definition) {
        final var analysis = analyzer.analyze("CREATE TABLE APP.T " + definition + " AS SELECT ID,V FROM APP.S", "APP").requireSupported();
        assertThat(analysis.isCtas()).isTrue();
        assertThat(analysis.tableId()).isEqualTo(new TableId(null, "APP", "T"));
    }

    @Test
    void shouldParseDateIntervalPartitions() {
        final var parser = new DamengDdlParser();
        final var tables = new Tables();
        parser.parse("CREATE TABLE APP.T(ID INT,CREATED DATE) PARTITION BY RANGE(CREATED) INTERVAL(NUMTOYMINTERVAL(1,'MONTH'))"
                + "(PARTITION P1 VALUES LESS THAN(TO_DATE('2030-01-01','YYYY-MM-DD')))", tables, "APP");
        assertThat(tables.forTable(new TableId(null, "APP", "T")).retrieveColumnNames()).containsExactly("ID", "CREATED");
    }

    @ParameterizedTest
    @ValueSource(strings = { "AUTO_INCREMENT 10", "AUTO_INCREMENT = 2147483648" })
    void shouldPreservePartitionedAutoIncrementColumnsAndCtasTrailingOptions(String option) {
        final var parser = new DamengDdlParser();
        final var tables = new Tables();
        parser.parse("CREATE TABLE APP.T(ID BIGINT AUTO_INCREMENT PRIMARY KEY,V VARCHAR(30)) PARTITION BY HASH(ID) PARTITIONS 2 " + option,
                tables, "APP");
        assertThat(tables.forTable(new TableId(null, "APP", "T")).columnWithName("ID").isAutoIncremented()).isTrue();
        final var analysis = analyzer.analyze("CREATE TABLE APP.COPY PARTITION BY HASH(ID) PARTITIONS 2 AS SELECT ID,V FROM APP.T " + option, "APP")
                .requireSupported();
        assertThat(analysis.isCtas()).isTrue();
    }

    @ParameterizedTest
    @MethodSource("partitionMaintenanceStatements")
    void shouldSkipPartitionMaintenanceWithoutChangingCapturedTables(String sql) {
        final var tree = DamengParseTree.parseScope(sql, false).tree().statement(0).tableStatement();
        assertThat(tree.alterTable().alterTableClause().partitionMaintenanceClause()).isNotNull();
        final var analysis = analyzer.analyze(sql, "APP").requireSupported();
        assertThat(analysis.impact()).isEqualTo(Impact.NO_TABLE_CHANGE);
        final var parser = new DamengDdlParser();
        final var tables = new Tables();
        final var id = new TableId(null, "APP", "T");
        parser.parse("CREATE TABLE APP.T(ID INT PRIMARY KEY,V VARCHAR(30)) PARTITION BY HASH(ID) PARTITIONS 2", tables, "APP");
        final var original = tables.forTable(id);
        assertThat(parser.plan(analysis, tables)).isEmpty();
        assertThat(parser.parse(sql, tables, "APP")).isEmpty();
        assertThat(tables.tableIds()).containsExactly(id);
        assertThat(tables.forTable(id)).isSameAs(original);
    }

    static Stream<String> partitionMaintenanceStatements() {
        return Stream.concat(Stream.of(
                Stream.of("P1,P2", "1,2", "\"分区一\",\"分区二\"")
                        .map(partitions -> "MERGE PARTITIONS " + partitions + " INTO PARTITION P12"),
                Stream.of("SPLIT PARTITION P1 AT(10) INTO(PARTITION PA,PARTITION PB)",
                        "SPLIT PARTITION P1 AT(10,'m') INTO(PARTITION PA TABLESPACE MAIN,PARTITION PB STORAGE(ON MAIN))",
                        "SPLIT PARTITION P1 VALUES('a','b') INTO(PARTITION PA STORAGE(INITIAL 1),PARTITION PB TABLESPACE MAIN)",
                        "SPLIT PARTITION P1 INTO(PARTITION PA VALUES LESS THAN(10),PARTITION PB VALUES EQU OR LESS THAN(20),PARTITION PC)",
                        "SPLIT PARTITION P1 INTO(PARTITION PA VALUES('a'),PARTITION PB VALUES('b','c'),PARTITION PC)"))
                .flatMap(clauses -> clauses).flatMap(clause -> globalIndexClauses().map(index -> clause + index))
                .flatMap(clause -> partitionTableNames().map(name -> "ALTER TABLE " + name + " " + clause)),
                Stream.of("ADD PARTITION P3", "ADD PARTITION IF NOT EXISTS P3 TABLESPACE MAIN STORAGE(ON MAIN)",
                        "ADD PARTITION P3 VALUES LESS THAN(100)", "ADD PARTITION IF NOT EXISTS P3 VALUES('a','b')",
                        "ADD PARTITION P3 VALUES LESS THAN(100)(SUBPARTITION S1 VALUES('a'),SUBPARTITION SD VALUES(DEFAULT))",
                        "MODIFY PARTITION P1 ADD SUBPARTITION S3 VALUES LESS THAN(100)",
                        "MODIFY SUBPARTITION S1 ADD SUBPARTITION IF NOT EXISTS S3 VALUES('a')",
                        "MODIFY PARTITION P1 ADD VALUES('a','b')", "MODIFY SUBPARTITION S1 DROP VALUES('a')",
                        "SET SUBPARTITION TEMPLATE()", "SET SUBPARTITION TEMPLATE(SUBPARTITION S1,SUBPARTITION S2 TABLESPACE MAIN)",
                        "SET SUBPARTITION TEMPLATE(SUBPARTITION S1 VALUES LESS THAN(10),SUBPARTITION SM VALUES LESS THAN(MAXVALUE))",
                        "SET SUBPARTITION TEMPLATE(SUBPARTITION S1 VALUES('a'),SUBPARTITION SD VALUES(DEFAULT))",
                        "SET SUBPARTITION TEMPLATE SUBPARTITIONS 4 STORE IN(MAIN)", "SET SUBPARTITION TEMPLATE 4 STORE IN(MAIN,TS2)",
                        "ENABLE ROW MOVEMENT", "DISABLE ROW MOVEMENT", "RENAME PARTITION P1 TO PNEW", "RENAME SUBPARTITION S1 TO SNEW",
                        "MOVE PARTITION P1 TABLESPACE MAIN", "MOVE SUBPARTITION S1 TABLESPACE MAIN")
                        .flatMap(clause -> partitionTableNames().map(name -> "ALTER TABLE " + name + " " + clause)));
    }

    private static Stream<String> globalIndexClauses() {
        return Stream.of("", " UPDATE GLOBAL INDEXES", " IGNORE GLOBAL INDEXES");
    }

    private static Stream<String> partitionTableNames() {
        return Stream.of("T", "APP.T", "\"APP\".\"T\"", "\"应用\".\"分区表\"");
    }

    @ParameterizedTest
    @MethodSource("dropPartitionStatements")
    void shouldParsePartitionDropsAndRejectThemWithADedicatedError(String sql) {
        final var maintenance = DamengParseTree.parseScope(sql, false).tree().statement(0).tableStatement()
                .alterTable().alterTableClause().partitionMaintenanceClause();
        assertThat(maintenance.partitionDropClause()).isNotNull();
        assertPartitionDataRemovalRequiresOptIn(sql, maintenance.partitionDropClause().SUBPARTITION() != null ? "DROP SUBPARTITION" : "DROP PARTITION");
    }

    private void assertPartitionDataRemovalRequiresOptIn(String sql, String operation) {
        final var analysis = analyzer.analyze(sql, "APP");
        assertThat(analysis.impact()).isEqualTo(Impact.UNSUPPORTED);
        assertThat(analysis.failure()).isInstanceOf(DamengDdlStatementAnalyzer.PartitionDataRemovalException.class)
                .hasMessageContaining("Unsupported " + operation, "per-row DELETE", "partition.drop.ignore=true", "continue CDC", "downstream");
        final var parser = new DamengDdlParser();
        final var tables = new Tables();
        parser.parse("CREATE TABLE APP.T(ID INT)", tables, "APP");
        final var original = tables.forTable(new TableId(null, "APP", "T"));
        assertThatThrownBy(() -> parser.parse(sql, tables, "APP")).isInstanceOf(DamengDdlStatementAnalyzer.PartitionDataRemovalException.class);
        assertThat(tables.forTable(original.id())).isSameAs(original);
    }

    static Stream<String> dropPartitionStatements() {
        return Stream.of("PARTITION", "SUBPARTITION").flatMap(kind -> Stream.of("P1", "IF EXISTS P1", "FOR(1)", "FOR IF EXISTS(1,'a')",
                "\"分区; DROP TABLE APP.T\"").flatMap(
                        partition -> globalIndexClauses()
                                .flatMap(index -> partitionTableNames().map(name -> "ALTER TABLE " + name + " DROP " + kind + " " + partition + index))));
    }

    @ParameterizedTest
    @MethodSource("truncatePartitionStatements")
    void shouldParsePartitionTruncatesAndRequireExplicitOptIn(String sql, String operation) {
        assertPartitionDataRemovalRequiresOptIn(sql, operation);
    }

    static Stream<Arguments> truncatePartitionStatements() {
        return Stream.concat(
                Stream.of("PARTITION", "SUBPARTITION").flatMap(kind -> Stream.concat(
                        Stream.of("P1", "(P1)").flatMap(partition -> Stream.of("", " DROP STORAGE", " REUSE STORAGE")
                                .map(storage -> partition + storage)),
                        Stream.of("FOR(1,'a')"))
                        .flatMap(partition -> globalIndexClauses().flatMap(index -> partitionTableNames()
                                .map(name -> Arguments.of("ALTER TABLE " + name + " TRUNCATE " + kind + " " + partition + index, "TRUNCATE " + kind))))),
                Stream.of(" PARTITION P1", "(P1)").flatMap(partition -> Stream.of("", " DROP STORAGE", " REUSE STORAGE")
                        .flatMap(storage -> globalIndexClauses().flatMap(index -> partitionTableNames()
                                .map(name -> Arguments.of("TRUNCATE TABLE " + name + partition + storage + index, "TRUNCATE PARTITION"))))));
    }

    @ParameterizedTest
    @ValueSource(strings = { "", " DROP STORAGE", " REUSE STORAGE" })
    void shouldStillTreatWholePartitionedTableTruncateAsATableChange(String storage) {
        final var parser = new DamengDdlParser();
        final var tables = new Tables();
        parser.parse("CREATE TABLE APP.T(ID INT) PARTITION BY HASH(ID) PARTITIONS 2", tables, "APP");
        final var id = new TableId(null, "APP", "T");
        final var original = tables.forTable(id);
        final var sql = "TRUNCATE TABLE APP.T" + storage;
        assertThat(analyzer.analyze(sql, "APP").requireSupported().impact()).isEqualTo(Impact.TABLE);
        assertThat(parser.parse(sql, tables, "APP")).singleElement()
                .satisfies(change -> assertThat(change.type()).isEqualTo(DamengDdlParser.Type.TRUNCATE));
        assertThat(tables.forTable(id)).isEqualTo(original);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "ALTER TABLE APP.T EXCHANGE PARTITION P1 WITH TABLE APP.U", "ALTER TABLE APP.T EXCHANGE SUBPARTITION S1 WITH TABLE APP.U"
    })
    void shouldNotAllowPartitionExchangeUsingTheDataRemovalOptIn(String sql) {
        assertUnsupported(sql);
        assertThat(analyzer.analyze(sql, "APP").failure()).hasMessageContaining("partition")
                .isNotInstanceOf(DamengDdlStatementAnalyzer.PartitionDataRemovalException.class);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "CREATE TABLE APP.T(ID INT) PARTITION BY RANGE(ID)",
            "CREATE TABLE APP.T(ID INT) PARTITION BY HASH(ID) PARTITIONS",
            "CREATE TABLE APP.T(ID INT) PARTITION BY RANGE(ID) INTERVAL()(PARTITION P1 VALUES LESS THAN(10))",
            "CREATE TABLE APP.T(ID INT) PARTITION BY HASH(ID) SUBPARTITION BY HASH(ID) SUBPARTITION TEMPLATE PARTITIONS 2 PARTITIONS 4",
            "ALTER TABLE APP.T DROP PARTITION FOR(", "ALTER TABLE APP.T DROP PARTITION IF EXISTS", "ALTER TABLE APP.T DROP PARTITION FOR()",
            "ALTER TABLE APP.T DROP SUBPARTITION P1 UPDATE GLOBAL",
            "ALTER TABLE APP.T DROP PARTITION P1 CASCADE", "ALTER TABLE APP.T DROP SUBPARTITION P1,P2",
            "ALTER TABLE APP.T MERGE PARTITIONS P1 INTO PARTITION P2", "ALTER TABLE APP.T MERGE PARTITIONS P1,P2",
            "ALTER TABLE APP.T MERGE PARTITIONS 1,P2 INTO PARTITION P3", "ALTER TABLE APP.T MERGE PARTITIONS P1,P2 INTO PARTITION P3 UPDATE GLOBAL",
            "ALTER TABLE APP.T SPLIT PARTITION P1 AT(10)", "ALTER TABLE APP.T SPLIT PARTITION P1 AT() INTO(PARTITION PA,PARTITION PB)",
            "ALTER TABLE APP.T SPLIT PARTITION P1 AT(10) INTO(PARTITION PA)", "ALTER TABLE APP.T ENABLE ROW",
            "ALTER TABLE APP.T RENAME PARTITION P1", "ALTER TABLE APP.T MOVE PARTITION P1", "ALTER TABLE APP.T SET SUBPARTITION TEMPLATE",
            "ALTER TABLE APP.T ADD PARTITION", "ALTER TABLE APP.T MODIFY PARTITION P1 ADD VALUES()",
            "ALTER TABLE APP.T TRUNCATE PARTITION", "ALTER TABLE APP.T TRUNCATE SUBPARTITION()",
            "ALTER TABLE APP.T TRUNCATE PARTITION FOR()", "ALTER TABLE APP.T TRUNCATE PARTITION P1 REUSE",
            "ALTER TABLE APP.T TRUNCATE SUBPARTITION S1 UPDATE GLOBAL", "TRUNCATE TABLE APP.T PARTITION"
    })
    void shouldRejectIncompletePartitionStatements(String sql) {
        assertUnsupported(sql);
        assertThat(analyzer.analyze(sql, "APP").failure()).isNotInstanceOf(DamengDdlStatementAnalyzer.PartitionDataRemovalException.class);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "ALTER TABLE APP.T MERGE PARTITIONS P1,P2 INTO PARTITION P3",
            "ALTER TABLE APP.T SPLIT PARTITION P1 AT(10) INTO(PARTITION PA,PARTITION PB)",
            "ALTER TABLE APP.T DROP PARTITION P1", "ALTER TABLE APP.T ENABLE ROW MOVEMENT", "ALTER TABLE APP.T ADD PARTITION P3",
            "ALTER TABLE APP.T TRUNCATE PARTITION P1", "ALTER TABLE APP.T TRUNCATE SUBPARTITION S1", "TRUNCATE TABLE APP.T PARTITION P1"
    })
    void shouldNotIgnoreStatementsAppendedToPartitionMaintenance(String sql) {
        assertFollowingTableChangesAreNotSkipped(sql);
        for (final var suffix : new String[]{ " INSERT INTO APP.T(ID) VALUES(1)", "; DELETE FROM APP.T", " UPDATE APP.T SET ID=1" }) {
            final var analysis = analyzer.analyze(sql + suffix, "APP");
            assertThat(analysis.impact()).isEqualTo(Impact.UNSUPPORTED);
            assertThat(analysis.failure()).isNotInstanceOf(DamengDdlStatementAnalyzer.PartitionDataRemovalException.class);
        }
    }

    @ParameterizedTest
    @MethodSource("dropPartitionedTableStatements")
    void shouldDropTheWholePartitionedTableUsingOrdinaryDropOptions(String sql) {
        final var parser = new DamengDdlParser();
        final var tables = new Tables();
        parser.parse("CREATE TABLE APP.T(ID INT) PARTITION BY RANGE(ID)(PARTITION P1 VALUES LESS THAN(MAXVALUE))", tables, "APP");
        final var analysis = analyzer.analyze(sql, "APP").requireSupported();
        assertThat(analysis.impact()).isEqualTo(Impact.TABLE);
        assertThat(analysis.tableId()).isEqualTo(new TableId(null, "APP", "T"));
        assertThat(parser.parse(sql, tables, "APP")).singleElement()
                .satisfies(change -> assertThat(change.type()).isEqualTo(DamengDdlParser.Type.DROP));
        assertThat(tables.tableIds()).isEmpty();
    }

    static Stream<String> dropPartitionedTableStatements() {
        return Stream.of("", "IF EXISTS ").flatMap(exists -> Stream.of("", " RESTRICT", " CASCADE", " CASCADE CONSTRAINTS")
                .flatMap(cascade -> Stream.of("", " PURGE").map(purge -> "DROP TABLE " + exists + "APP.T" + cascade + purge)));
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "PARTITION BY HASH(UNKNOWN) PARTITIONS 2",
            "PARTITION BY RANGE(ID) SUBPARTITION BY HASH(UNKNOWN) SUBPARTITIONS 2(PARTITION P1 VALUES LESS THAN(MAXVALUE))",
            "PARTITION BY HASH(ID) PARTITIONS 0",
            "PARTITION BY RANGE(ID) SUBPARTITION BY HASH(ID) SUBPARTITIONS 0(PARTITION P1 VALUES LESS THAN(MAXVALUE))",
            "PARTITION BY RANGE(ID) SUBPARTITION BY HASH(ID)(PARTITION P1 VALUES LESS THAN(MAXVALUE) SUBPARTITIONS 0)"
    })
    void shouldValidatePartitionColumnsAndCountsWithoutChangingHistory(String definition) {
        final var parser = new DamengDdlParser();
        final var tables = new Tables();
        assertThatThrownBy(() -> parser.parse("CREATE TABLE APP.T(ID INT) " + definition, tables, "APP"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(tables.tableIds()).isEmpty();
    }

    @ParameterizedTest
    @ValueSource(strings = { "EXCHANGE", "HASHPARTMAP", "MOVEMENT", "SPLIT", "SUBPARTITIONS", "TEMPLATE" })
    void shouldKeepPartitionWordsUsableAsColumnNames(String name) {
        final var parser = new DamengDdlParser();
        final var tables = new Tables();
        parser.parse("CREATE TABLE APP.T(ID INT," + name + " INT)", tables, "APP");
        assertThat(analyzer.analyze("CREATE TABLE APP.COPY AS SELECT " + name + " FROM APP.T", "APP").requireSupported().isCtas()).isTrue();
        // A lone contextual word after DROP can still be an ordinary column name.
        parser.parse("ALTER TABLE APP.T DROP " + name, tables, "APP");
        assertThat(tables.forTable(new TableId(null, "APP", "T")).retrieveColumnNames()).containsExactly("ID");
    }

    @Test
    void shouldDistinguishDroppingAColumnNamedPartitionFromDroppingAPartition() {
        final var parser = new DamengDdlParser();
        final var tables = new Tables();
        parser.parse("CREATE TABLE APP.T(ID INT,PARTITION INT)", tables, "APP");
        final var analysis = analyzer.analyze("ALTER TABLE APP.T DROP PARTITION", "APP").requireSupported();
        assertThat(analysis.impact()).isEqualTo(Impact.TABLE);
        parser.parse(analysis.sql(), tables, "APP");
        assertThat(tables.forTable(new TableId(null, "APP", "T")).retrieveColumnNames()).containsExactly("ID");
    }

    // DM SQL Language Manual 3.15: COMMENT ON creates or replaces comments;
    // there are no CREATE/ALTER/DROP COMMENT productions.
    // https://eco.dameng.com/document/dm/zh-cn/pm/definition-statement.html
    // Native DM 8.1.3.162 also verified empty-string clearing for TABLE, VIEW
    // and their COLUMNs. SEQUENCE is documented but rejected by that build.
    @ParameterizedTest
    @MethodSource("commentStatements")
    void shouldSkipCommentStatementsWithoutChangingCapturedTables(String sql) {
        assertStatementDoesNotChangeCapturedTables(sql);
        assertThat(DamengParseTree.parseScope(sql, false).tree().statement(0).nonTableStatement().commentStatement()).isNotNull();
    }

    static Stream<String> commentStatements() {
        return commentTargets().flatMap(target -> Stream.of("'description'", "'中文注释'", "''", "'it''s; DROP TABLE APP.T'",
                "q'[comment; ALTER TABLE APP.T ADD X INT]'", "'-- literal /* text */\nsecond line'")
                .flatMap(value -> Stream.of("", ";").map(ending -> "COMMENT ON " + target + " IS " + value + ending)));
    }

    static Stream<String> commentTargets() {
        return Stream.concat(Stream.of("TABLE", "VIEW", "SEQUENCE").flatMap(kind -> Stream.of(
                "T", "APP.T", "\"APP\".\"T\"", "\"应用\".\"对象\"", "\"a.b\".\"it\"\"s; DROP TABLE APP.T\"")
                .map(name -> kind + " " + name)),
                Stream.of("T.V", "APP.T.V", "V.ID", "APP.V.ID", "\"APP\".\"T\".\"V\"",
                        "\"应用\".\"视图\".\"列\"", "\"a.b\".\"it\"\"s\".\"DROP; TABLE APP.T\"")
                        .map(name -> "COLUMN " + name));
    }

    @ParameterizedTest
    @MethodSource("commentTargets")
    void shouldCreateReplaceAndClearCommentsWithoutChangingHistoricalDescriptions(String target) {
        final var parser = new DamengDdlParser();
        final var tables = new Tables();
        final var id = new TableId(null, "APP", "T");
        parser.parse("CREATE TABLE APP.T(ID INT PRIMARY KEY,V VARCHAR(30) COMMENT 'original column')", tables, "APP");
        final var original = tables.forTable(id);
        for (final var value : new String[]{ "'created'", "'changed'", "''" }) {
            final var sql = "COMMENT ON " + target + " IS " + value;
            assertThat(analyzer.analyze(sql, "APP").requireSupported().impact()).isEqualTo(Impact.NO_TABLE_CHANGE);
            assertThat(parser.parse(sql, tables, "APP")).isEmpty();
            assertThat(tables.tableIds()).containsExactly(id);
            assertThat(tables.forTable(id)).isSameAs(original);
            assertThat(tables.forTable(id).columnWithName("V").comment()).isEqualTo("original column");
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "comment/* object */on table app.t is 'lower case'; -- end",
            "COMMENT\nON\tCOLUMN APP . T . V /* value */ IS 'column'",
            "COMMENT ON VIEW \"VIEW\" IS 'view'",
            "COMMENT ON SEQUENCE \"APP\".\"SEQUENCE\" IS ''"
    })
    void shouldParseCommentWhitespaceAndQuotedKeywordNames(String sql) {
        assertStatementDoesNotChangeCapturedTables(sql);
    }

    // These literal forms were accepted by the parser before explicit COMMENT
    // validation. Native DM 8.1.3.162 rejects them; they are not official examples.
    @ParameterizedTest
    @ValueSource(strings = { "COMMENT ON COLUMN APP.T.ID IS NULL;", "COMMENT ON COLUMN T.\"DROP\" IS n'字段'" })
    void shouldRetainExistingCommentLiteralCompatibility(String sql) {
        assertStatementDoesNotChangeCapturedTables(sql);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "COMMENT", "COMMENT ON", "COMMENT ON TABLE", "COMMENT ON TABLE APP.T", "COMMENT ON TABLE APP.T IS",
            "COMMENT TABLE APP.T IS 'x'", "COMMENT ON TABLE APP.T 'x'", "COMMENT ON TABLE APP.T = 'x'",
            "COMMENT ON TABLE T IS 42", "COMMENT ON TABLE T IS 'x' UNKNOWN", "COMMENT ON COLUMN APP.T.ID IS 'x' UNKNOWN",
            "COMMENT ON TABLE A.B.C IS 'x'", "COMMENT ON VIEW A.B.C IS 'x'", "COMMENT ON SEQUENCE A.B.C IS 'x'",
            "COMMENT ON COLUMN A IS 'x'", "COMMENT ON COLUMN A.B.C.D IS 'x'", "COMMENT ON COLUMN APP.T. IS 'x'",
            "COMMENT ON TABLE APP.T IS ('x')", "COMMENT ON TABLE APP.T IS 'x' || 'y'",
            "COMMENT ON INDEX APP.I IS 'x'", "COMMENT ON SCHEMA APP IS 'x'", "COMMENT ON PROCEDURE APP.P IS 'x'",
            "CREATE COMMENT ON TABLE APP.T IS 'x'", "ALTER COMMENT ON TABLE APP.T IS 'x'", "DROP COMMENT ON TABLE APP.T",
            "COMMENT ON TABLE IF EXISTS APP.T IS 'x'", "COMMENT ON TABLE IF NOT EXISTS APP.T IS 'x'",
            "COMMENT ON TABLE APP.T IS 'x' CASCADE"
    })
    void shouldRejectMalformedOrUndocumentedCommentStatements(String sql) {
        assertUnsupported(sql);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "COMMENT ON TABLE APP.T IS 'comment; DROP TABLE APP.T'", "COMMENT ON VIEW APP.V IS ''",
            "COMMENT ON SEQUENCE APP.S IS 'sequence'", "COMMENT ON COLUMN APP.T.V IS 'column'"
    })
    void shouldNotHideStatementsFollowingComments(String sql) {
        assertFollowingTableChangesAreNotSkipped(sql);
        for (final var separator : new String[]{ " ", "; " }) {
            assertUnsupported(sql + separator + "INSERT INTO APP.T VALUES(1,'x')");
            assertUnsupported(sql + separator + "UPDATE APP.T SET V='x'");
            assertUnsupported(sql + separator + "DELETE FROM APP.T");
        }
    }

    @Test
    void shouldSkipCommentOnlyScriptsWithoutChangingCapturedTables() {
        final var parser = new DamengDdlParser();
        final var tables = new Tables();
        final var id = new TableId(null, "APP", "T");
        parser.parse("CREATE TABLE APP.T(ID INT,V VARCHAR(30))", tables, "APP");
        final var original = tables.forTable(id);
        final var sql = "COMMENT ON TABLE APP.T IS 'created'; COMMENT ON TABLE APP.T IS 'changed';"
                + "COMMENT ON COLUMN APP.T.V IS 'column'; COMMENT ON TABLE APP.T IS ''; COMMENT ON COLUMN APP.T.V IS '';";
        assertNonTableStatement(sql);
        assertThat(parser.parse(sql, tables, "APP")).isEmpty();
        assertThat(tables.tableIds()).containsExactly(id);
        assertThat(tables.forTable(id)).isSameAs(original);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "COMMENT ON TABLE APP.T IS q'[it's; DROP TABLE T]'",
            "GRANT CREATE TABLE, CREATE VIEW TO U,V WITH ADMIN OPTION; REVOKE SELECT ON T FROM U",
            "CREATE INDEX I ON T(\"DROP\"); DROP INDEX I;",
            "DROP INDEX IF EXISTS APP.I",
            "CREATE ROLE DBZ_ISOLATED_ROLE; DROP ROLE DBZ_ISOLATED_ROLE",
            "DROP ROLE IF EXISTS DBZ_ISOLATED_ROLE",
            "CREATE OR REPLACE PROCEDURE APP.P AS BEGIN NULL; BEGIN NULL; END; END P;",
            "CREATE FUNCTION F RETURN INT AS BEGIN RETURN CASE WHEN 1=1 THEN 1 ELSE 0 END; END;",
            "CREATE TRIGGER TR BEFORE INSERT ON T BEGIN IF 1=1 THEN NULL; END IF; FOR I IN 1..2 LOOP NULL; END LOOP; END;",
            "CREATE VIEW V1 AS SELECT ID FROM APP.T1;",
            "CREATE OR REPLACE VIEW V1 AS SELECT ID FROM APP.T1;",
            "CREATE VIEW APP.V AS SELECT ROW_NUMBER() OVER(PARTITION BY DEPT ORDER BY ID) AS N FROM APP.T",
            "CREATE VIEW APP.V AS SELECT ID FROM APP.T FETCH FIRST 3 ROWS ONLY",
            "CREATE VIEW APP.V AS SELECT JSON_VALUE(DOC,'$.x' RETURNING VARCHAR NULL ON ERROR) AS V FROM APP.T",
            "ALTER INDEX APP.I RENAME TO APP.J",
            "ALTER SEQUENCE APP.S RENAME TO APP.T",
            "REVOKE SELECT ON APP.T FROM U CASCADE",
            "GRANT CREATE OPERATOR TO U; REVOKE CREATE OPERATOR FROM U",
            "CREATE VIEW APP.V AS SELECT q'[; ALTER TABLE T ADD HIDDEN INT]' AS V FROM APP.T; DROP VIEW APP.V",
            "CREATE VIEW APP.\"V; DROP TABLE T\" AS SELECT ID FROM APP.T",
            "CREATE USER U IDENTIFIED BY password",
            "ALTER USER U IDENTIFIED BY password",
            "CREATE DOMAIN D AS INT",
            "CREATE CONTEXT C USING P",
            "CREATE DIRECTORY D AS '/tmp'",
            "CREATE PROFILE P LIMIT FAILED_LOGIN_ATTEMPTS 3",
            "CREATE LIBRARY L AS '/tmp/lib.so'",
            "CREATE FORCE VIEW APP.V AS SELECT ID FROM APP.T",
            "CREATE OR REPLACE FORCE VIEW APP.V AS SELECT ID FROM APP.T",
            "CREATE FORCE VIEW IF NOT EXISTS APP.V AS SELECT ID FROM APP.T;",
            "create or replace force view if not exists app.v as select id from app.t",
            "CREATE FORCE VIEW APP.\"FORCE\" AS SELECT ID FROM APP.T",
            "CREATE FORCE VIEW APP.V (\"DROP\") AS SELECT ID FROM APP.T WITH READ ONLY",
            "CREATE VIEW APP.V AS SELECT SUM(N) OVER(ORDER BY ID ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS TOTAL FROM APP.T",
            "CREATE VIEW APP.V AS SELECT JSON_QUERY(DOC,'$.a' WITH WRAPPER ERROR ON ERROR) AS V FROM APP.T",
            "CREATE VIEW APP.V AS SELECT TRUNCATE(1.25,1) FROM DUAL",
            "CREATE VIEW APP.V AS SELECT X.ID FROM XMLTABLE('/r' PASSING XMLTYPE(DOC) COLUMNS ID INT PATH 'id') X",
            "CREATE VIEW APP.V AS SELECT q'[;DROP TABLE APP.T]' AS \"ALTER TABLE\" FROM APP.T",
            "CREATE INDEX APP.\"I;DROP TABLE T\" ON APP.T(ID)",
            "CREATE VIEW APP.\"CREATE\" AS SELECT ID FROM APP.T",
            "DROP VIEW APP.\"DROP\"",
            "GRANT CREATE OPERATOR, DROP OPERATOR TO U",
            "REVOKE CREATE OPERATOR, DROP OPERATOR FROM U",
            "CREATE PROCEDURE APP.P AS BEGIN IF 1=1 THEN NULL; ELSE NULL; END IF; END;",
            "CREATE TRIGGER APP.TR BEFORE INSERT ON APP.T BEGIN FOR I IN 1..2 LOOP NULL; END LOOP; END;",
            "CREATE FUNCTION APP.F RETURN INT AS BEGIN RETURN CASE WHEN 1=1 THEN 1 ELSE 0 END; END;",
            "CREATE OR REPLACE PROCEDURE APP.P AS BEGIN BEGIN IF 1=1 THEN FOR I IN 1..2 LOOP NULL; END LOOP; END IF; END; END P;"
    })
    void shouldProveTheCompleteInputHasNoRowSchemaChange(String sql) {
        assertNonTableStatement(sql);
    }

    @ParameterizedTest
    @MethodSource({ "createUserStatements", "alterUserStatements", "dropUserStatements", "grantUserStatements", "revokeUserStatements" })
    void shouldSkipUserManagementWithoutChangingCapturedTables(String sql) {
        assertStatementDoesNotChangeCapturedTables(sql);
    }

    // DM SQL Language Manual, sections 3.2.1-3.2.3:
    // https://eco.dameng.com/document/dm/zh-cn/pm/definition-statement.html
    static Stream<String> createUserStatements() {
        return Stream.of(
                Stream.of("", "IF NOT EXISTS ").flatMap(ifNotExists -> userAuthenticationClauses()
                        .map(authentication -> "CREATE USER " + ifNotExists + "U IDENTIFIED " + authentication)),
                userOptionClauses().map(option -> "CREATE USER U IDENTIFIED BY DMsys_123 " + option),
                Stream.of(
                        "CREATE USER IF NOT EXISTS U IDENTIFIED BY \"Dm_new_123\" HASH WITH SHA512 NO SALT"
                                + " PASSWORD_POLICY 31 ACCOUNT LOCK ENCRYPT BY \"Dm_key_123\" NOT READ ONLY"
                                + " LIMIT SESSION_PER_USER 10, CONNECT_IDLE_TIME UNLIMITED PASSWORD EXPIRE"
                                + " ALLOW_IP \"192.168.0.*\" NOT_ALLOW_IP \"192.168.0.10\""
                                + " ALLOW_DATETIME MON \"08:30:00\" TO FRI \"17:00:00\""
                                + " NOT_ALLOW_DATETIME SAT \"00:00:00\" TO SUN \"23:59:59\""
                                + " DEFAULT TABLESPACE MAIN DEFAULT INDEX TABLESPACE MAIN QUOTA 100M ON MAIN",
                        "CREATE USER U IDENTIFIED EXTERNALLY AS \"CN=U,OU=Users,DC=example,DC=com\""
                                + " ACCOUNT UNLOCK READ ONLY PROFILE USER_PROFILE DEFAULT TABLESPACE MAIN",
                        "create /* user options */ user if not exists \"U; DROP TABLE APP.T\""
                                + " identified by \"Dm;ALTER TABLE APP.T ADD X INT\";"))
                .flatMap(statements -> statements);
    }

    static Stream<String> alterUserStatements() {
        return Stream.of(
                userAuthenticationClauses().map(authentication -> "ALTER USER U IDENTIFIED " + authentication),
                userOptionClauses().map(option -> "ALTER USER U " + option),
                Stream.of(
                        "ALTER USER U IDENTIFIED BY \"Dm_new_123\" REPLACE \"Dm_old_123\"",
                        "ALTER USER U IDENTIFIED BY \"Dm_new_123\" RETAIN CURRENT PASSWORD",
                        "ALTER USER U IDENTIFIED BY \"Dm_new_123\" RETAIN CURRENT PASSWORD HASH WITH SHA512 SALT",
                        "ALTER USER U DISCARD OLD PASSWORD",
                        "ALTER USER U ON SCHEMA OTHER",
                        "ALTER USER U IDENTIFIED BY \"Dm_new_123\" HASH WITH SHA512 NO SALT REPLACE \"Dm_old_123\""
                                + " PASSWORD_POLICY 31 ACCOUNT UNLOCK ENCRYPT BY \"Dm_key_123\" NOT READ ONLY"
                                + " LIMIT SESSION_PER_USER DEFAULT CONNECT_IDLE_TIME UNLIMITED PASSWORD EXPIRE"
                                + " ALLOW_IP DROP \"192.168.0.10\", \"192.168.0.11\" NOT_ALLOW_IP ADD \"10.0.0.0/8\""
                                + " DEFAULT TABLESPACE MAIN DEFAULT INDEX TABLESPACE MAIN ON SCHEMA OTHER"
                                + " QUOTA 100M ON MAIN QUOTA UNLIMITED ON TS_DATA",
                        "ALTER USER U GRANT CONNECT THROUGH PROXY_USER",
                        "ALTER USER U REVOKE CONNECT THROUGH PROXY_USER",
                        "alter /* proxy authorization */ user \"U\" grant connect through \"PROXY; DROP TABLE APP.T\";",
                        "alter user \"U\" revoke connect through \"PROXY\";"))
                .flatMap(statements -> statements);
    }

    private static Stream<String> userAuthenticationClauses() {
        return Stream.of(
                "BY DMsys_123",
                "BY \"Dm_quoted_123\"",
                "BY DMsys_123 HASH WITH SHA512",
                "BY DMsys_123 HASH WITH SHA512 SALT",
                "BY DMsys_123 HASH WITH SHA512 NO SALT",
                "BY DMsys_123 HASH WITH MY_ENGINE.SHA512",
                "BY DMsys_123 HASH WITH MY_ENGINE.SHA512 SALT",
                "BY DMsys_123 HASH WITH MY_ENGINE.SHA512 NO SALT",
                "EXTERNALLY",
                "EXTERNALLY AS \"CN=U,OU=Users,DC=example,DC=com\"",
                "EXTERNALLY RADIUS");
    }

    // Shared CREATE/ALTER USER options; exercise each branch and representative combinations.
    private static Stream<String> userOptionClauses() {
        return Stream.concat(
                Stream.of(
                        "PASSWORD_POLICY 15",
                        "PASSWORD_POLICY 31",
                        "ACCOUNT LOCK",
                        "ACCOUNT UNLOCK",
                        "ENCRYPT BY \"Dm_key_123\"",
                        "READ ONLY",
                        "NOT READ ONLY",
                        "DROP PROFILE",
                        "PROFILE USER_PROFILE",
                        "LIMIT SESSION_PER_USER 10, CONNECT_IDLE_TIME UNLIMITED, CONNECT_TIME DEFAULT",
                        "LIMIT SESSION_PER_USER 10 CONNECT_IDLE_TIME UNLIMITED CONNECT_TIME DEFAULT",
                        "PASSWORD EXPIRE",
                        "ALLOW_IP NULL",
                        "ALLOW_IP \"192.168.0.10\"",
                        "ALLOW_IP \"192.168.0.*\", \"10.0.0.0/8\"",
                        "ALLOW_IP ADD \"192.168.0.10\"",
                        "ALLOW_IP ADD \"192.168.0.*\", \"10.0.0.0/8\"",
                        "ALLOW_IP DROP \"192.168.0.10\"",
                        "ALLOW_IP DROP \"192.168.0.*\", \"10.0.0.0/8\"",
                        "NOT_ALLOW_IP NULL",
                        "NOT_ALLOW_IP \"192.168.0.10\"",
                        "NOT_ALLOW_IP \"192.168.0.*\", \"10.0.0.0/8\"",
                        "NOT_ALLOW_IP ADD \"192.168.0.10\"",
                        "NOT_ALLOW_IP ADD \"192.168.0.*\", \"10.0.0.0/8\"",
                        "NOT_ALLOW_IP DROP \"192.168.0.10\"",
                        "NOT_ALLOW_IP DROP \"192.168.0.*\", \"10.0.0.0/8\"",
                        "ALLOW_DATETIME \"2026-01-01\" \"08:30:00\" TO \"2026-12-31\" \"17:00:00\"",
                        "ALLOW_DATETIME MON \"08:30:00\" TO FRI \"17:00:00\"",
                        "ALLOW_DATETIME MON \"08:30:00\" TO TUE \"17:00:00\", WED \"08:30:00\" TO THURS \"17:00:00\"",
                        "NOT_ALLOW_DATETIME \"2026-01-01\" \"00:00:00\" TO \"2026-01-03\" \"23:59:59\"",
                        "NOT_ALLOW_DATETIME SAT \"00:00:00\" TO SUN \"23:59:59\"",
                        "NOT_ALLOW_DATETIME MON \"00:00:00\" TO TUE \"08:00:00\", WED \"00:00:00\" TO THURS \"08:00:00\"",
                        "DEFAULT TABLESPACE MAIN",
                        "DEFAULT INDEX TABLESPACE MAIN",
                        "DEFAULT TABLESPACE MAIN DEFAULT INDEX TABLESPACE MAIN",
                        "QUOTA UNLIMITED",
                        "QUOTA 0",
                        "QUOTA UNLIMITED ON MAIN",
                        "QUOTA 0 ON MAIN",
                        "QUOTA 1024 ON MAIN",
                        "QUOTA 100K ON MAIN",
                        "QUOTA 100M ON MAIN",
                        "QUOTA 1G ON MAIN",
                        "QUOTA 1T ON MAIN",
                        "QUOTA 100M ON MAIN QUOTA UNLIMITED ON TS_DATA"),
                Stream.of(
                        "GLOBAL_SESSION_PER_USER", "SESSION_PER_USER", "CONNECT_IDLE_TIME", "CONNECT_TIME",
                        "CPU_PER_CALL", "CPU_PER_SESSION", "MEM_SPACE", "READ_PER_CALL", "READ_PER_SESSION",
                        "FAILED_LOGIN_ATTEMPTS", "PASSWORD_LIFE_TIME", "PASSWORD_REUSE_TIME", "PASSWORD_REUSE_MAX",
                        "PASSWORD_LOCK_TIME", "PASSWORD_GRACE_TIME", "INACTIVE_ACCOUNT_TIME")
                        .map(resource -> "LIMIT " + resource + " 30"));
    }

    static Stream<String> dropUserStatements() {
        return Stream.of(
                "DROP USER U",
                "DROP USER U RESTRICT",
                "DROP USER IF EXISTS U",
                "DROP USER IF EXISTS U RESTRICT",
                "drop /* user removal */ user if exists \"U; DROP TABLE APP.T\" restrict;");
    }

    // DM Security Management, section 3.3: grant/revoke database, object, role and schema privileges to/from users.
    // https://eco.dameng.com/document/dm/zh-cn/pm/discretionary-access-control.html
    static Stream<String> grantUserStatements() {
        return Stream.of(
                "GRANT CREATE USER TO U",
                "GRANT ALTER USER, DROP USER TO U, V WITH ADMIN OPTION",
                "GRANT CREATE TABLE, ALTER ANY TABLE, DROP ANY TABLE TO U WITH ADMIN OPTION",
                "GRANT RESOURCE TO U",
                "GRANT ROLE_A, ROLE_B TO U, V WITH ADMIN OPTION",
                "GRANT SELECT ON APP.T TO U",
                "GRANT SELECT ON TABLE T TO U",
                "GRANT SELECT ON TABLE APP.T TO U WITH GRANT OPTION",
                "GRANT SELECT, INSERT, DELETE, UPDATE ON APP.T TO U, V WITH GRANT OPTION",
                "GRANT UPDATE(ID, V), REFERENCES(ID) ON APP.T TO U WITH GRANT OPTION",
                "GRANT ALL ON APP.T TO U",
                "GRANT ALL PRIVILEGES ON APP.T TO U, V WITH GRANT OPTION",
                "GRANT SELECT OFF ON APP.T TO U",
                "GRANT SELECT ON SCHEMA APP TO U WITH GRANT OPTION",
                "GRANT SELECT ANY TABLE ON SCHEMA APP TO U",
                "GRANT UPDATE ANY TABLE ON SCHEMA APP TO U, V WITH ADMIN OPTION",
                "GRANT ALL ON SCHEMA APP TO U WITH ADMIN OPTION",
                "GRANT ALL PRIVILEGES ON SCHEMA APP TO U, V WITH ADMIN OPTION",
                "grant /* user privileges */ create user to \"U; DROP TABLE APP.T\" with admin option;");
    }

    static Stream<String> revokeUserStatements() {
        return Stream.of(
                "REVOKE CREATE USER FROM U",
                "REVOKE ALTER USER, DROP USER FROM U, V",
                "REVOKE ADMIN OPTION FOR CREATE USER, ALTER USER, DROP USER FROM U, V",
                "REVOKE CREATE TABLE, ALTER ANY TABLE, DROP ANY TABLE FROM U",
                "REVOKE RESOURCE FROM U",
                "REVOKE ROLE_A, ROLE_B FROM U",
                "REVOKE ADMIN OPTION FOR ROLE_A, ROLE_B FROM U",
                "REVOKE SELECT ON APP.T FROM U",
                "REVOKE SELECT ON TABLE T FROM U",
                "REVOKE SELECT ON TABLE APP.T FROM U CASCADE",
                "REVOKE SELECT, INSERT, DELETE, UPDATE ON APP.T FROM U, V RESTRICT",
                "REVOKE UPDATE, REFERENCES ON APP.T FROM U CASCADE",
                "REVOKE GRANT OPTION FOR SELECT ON APP.T FROM U CASCADE",
                "REVOKE ALL ON APP.T FROM U RESTRICT",
                "REVOKE ALL PRIVILEGES ON APP.T FROM U, V CASCADE",
                "REVOKE SELECT OFF ON APP.T FROM U",
                "REVOKE GRANT OPTION FOR SELECT OFF ON APP.T FROM U CASCADE",
                "REVOKE SELECT ON SCHEMA APP FROM U RESTRICT",
                "REVOKE SELECT ANY TABLE ON SCHEMA APP FROM U",
                "REVOKE UPDATE ANY TABLE ON SCHEMA APP FROM U, V CASCADE",
                "REVOKE ALL ON SCHEMA APP FROM U RESTRICT",
                "REVOKE ALL PRIVILEGES ON SCHEMA APP FROM U, V CASCADE",
                "REVOKE GRANT ADMIN FOR SELECT ANY TABLE ON SCHEMA APP FROM U RESTRICT",
                "revoke /* user privileges */ admin option for create user from \"U; DROP TABLE APP.T\";");
    }

    // Section 3.2.3: CASCADE can remove owned tables and referencing constraints, so it cannot be skipped.
    @ParameterizedTest
    @ValueSource(strings = {
            "DROP USER U CASCADE",
            "DROP USER IF EXISTS U CASCADE",
            "drop user \"U\" cascade;",
            "drop user if exists \"U\" cascade;"
    })
    void shouldRejectCascadingUserRemovalThatCanDropCapturedTables(String sql) {
        assertUnsupported(sql);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "CREATE USER IF NOT EXISTS U IDENTIFIED BY DMsys_123 DROP PROFILE",
            "ALTER USER U DROP PROFILE",
            "ALTER USER U ALLOW_IP DROP \"192.168.0.10\", \"192.168.0.11\"",
            "ALTER USER U NOT_ALLOW_IP DROP \"192.168.0.10\", \"192.168.0.11\"",
            "ALTER USER U GRANT CONNECT THROUGH PROXY_USER",
            "ALTER USER U REVOKE CONNECT THROUGH PROXY_USER",
            "DROP USER IF EXISTS U RESTRICT",
            "GRANT CREATE USER, ALTER USER, DROP USER TO U WITH ADMIN OPTION",
            "REVOKE ADMIN OPTION FOR CREATE USER, ALTER USER, DROP USER FROM U"
    })
    void shouldNotSwallowTableChangesFollowingUserManagement(String sql) {
        assertFollowingTableChangesAreNotSkipped(sql);
    }

    @ParameterizedTest
    @MethodSource({ "createRoleStatements", "dropRoleStatements", "grantPrivilegesToRoleStatements", "grantRoleStatements",
            "revokePrivilegesFromRoleStatements", "revokeRoleStatements" })
    void shouldSkipRoleManagementWithoutChangingCapturedTables(String sql) {
        assertStatementDoesNotChangeCapturedTables(sql);
    }

    // DM Security Management, sections 3.2.1.2-3.2.1.3: roles have no authentication or CASCADE clause.
    // https://eco.dameng.com/document/dm/zh-cn/pm/discretionary-access-control.html
    static Stream<String> createRoleStatements() {
        return Stream.of(
                "CREATE ROLE ROLE_A",
                "CREATE ROLE IF NOT EXISTS ROLE_A",
                "CREATE ROLE \"Role A\"",
                "create /* role creation */ role if not exists \"R; DROP TABLE APP.T\";");
    }

    static Stream<String> dropRoleStatements() {
        return Stream.of(
                "DROP ROLE ROLE_A",
                "DROP ROLE IF EXISTS ROLE_A",
                "DROP ROLE \"Role A\"",
                "drop /* role removal */ role if exists \"R; DROP TABLE APP.T\";");
    }

    // Sections 3.3.1, 3.3.2 and 3.3.4: the role is the recipient of database, object or schema privileges.
    static Stream<String> grantPrivilegesToRoleStatements() {
        return Stream.of(
                "GRANT CREATE TABLE TO ROLE_A",
                "GRANT CREATE TABLE TO ROLE_A, ROLE_B WITH ADMIN OPTION",
                "GRANT CREATE ROLE, DROP ROLE TO ROLE_A, ROLE_B",
                "GRANT CREATE TABLE, ALTER ANY TABLE, DROP ANY TABLE TO ROLE_A WITH ADMIN OPTION",
                "GRANT SELECT ON T TO ROLE_A",
                "GRANT SELECT ON TABLE T TO ROLE_A",
                "GRANT SELECT ON APP.T TO ROLE_A",
                "GRANT SELECT ON TABLE APP.T TO ROLE_A WITH GRANT OPTION",
                "GRANT SELECT, INSERT, DELETE, UPDATE ON APP.T TO ROLE_A, ROLE_B WITH GRANT OPTION",
                "GRANT SELECT(ID), INSERT(ID, V), UPDATE(V), REFERENCES(ID) ON APP.T TO ROLE_A WITH GRANT OPTION",
                "GRANT ALL ON APP.T TO ROLE_A",
                "GRANT ALL PRIVILEGES ON APP.T TO ROLE_A, ROLE_B WITH GRANT OPTION",
                "GRANT SELECT OFF ON APP.T TO ROLE_A",
                "GRANT SELECT OFF ON TABLE APP.T TO ROLE_A, ROLE_B WITH GRANT OPTION",
                "GRANT SELECT FOR DUMP, FLASHBACK, INDEX, ALTER ON APP.T TO ROLE_A",
                "GRANT EXECUTE ON PROCEDURE APP.P TO ROLE_A WITH GRANT OPTION",
                "GRANT READ, WRITE ON DIRECTORY DATA_DIR TO ROLE_A",
                "GRANT SELECT ON SCHEMA APP TO ROLE_A WITH GRANT OPTION",
                "GRANT SELECT ANY TABLE ON SCHEMA APP TO ROLE_A",
                "GRANT CREATE ANY TABLE ON SCHEMA APP TO ROLE_A, ROLE_B WITH ADMIN OPTION",
                "GRANT ALL ON SCHEMA APP TO ROLE_A WITH ADMIN OPTION",
                "GRANT ALL PRIVILEGES ON SCHEMA APP TO ROLE_A, ROLE_B WITH ADMIN OPTION",
                "grant /* role privileges */ select on \"APP\".\"T\" to \"R; DROP TABLE APP.T\" with grant option;");
    }

    // Section 3.3.3.1: grant one or more roles to users or other roles, optionally with administration rights.
    static Stream<String> grantRoleStatements() {
        return Stream.concat(
                Stream.of("ROLE_A", "ROLE_A, ROLE_B")
                        .flatMap(roles -> Stream.of("U", "U, V", "ROLE_C", "ROLE_C, ROLE_D", "U, ROLE_C")
                                .flatMap(principals -> Stream.of("", " WITH ADMIN OPTION")
                                        .map(option -> "GRANT " + roles + " TO " + principals + option))),
                Stream.of("grant /* role membership */ \"R; DROP TABLE APP.T\", \"Role B\" to \"U\", \"V\" with admin option;"));
    }

    static Stream<String> revokePrivilegesFromRoleStatements() {
        return Stream.of(
                "REVOKE CREATE TABLE FROM ROLE_A",
                "REVOKE CREATE ROLE, DROP ROLE FROM ROLE_A, ROLE_B",
                "REVOKE ADMIN OPTION FOR CREATE TABLE FROM ROLE_A",
                "REVOKE ADMIN OPTION FOR CREATE TABLE, ALTER ANY TABLE, DROP ANY TABLE FROM ROLE_A, ROLE_B",
                "REVOKE SELECT ON T FROM ROLE_A",
                "REVOKE SELECT ON TABLE T FROM ROLE_A",
                "REVOKE SELECT ON APP.T FROM ROLE_A",
                "REVOKE SELECT ON TABLE APP.T FROM ROLE_A RESTRICT",
                "REVOKE SELECT, INSERT, DELETE, UPDATE, REFERENCES ON APP.T FROM ROLE_A, ROLE_B CASCADE",
                "REVOKE ALL ON APP.T FROM ROLE_A RESTRICT",
                "REVOKE ALL PRIVILEGES ON APP.T FROM ROLE_A, ROLE_B CASCADE",
                "REVOKE GRANT OPTION FOR SELECT ON APP.T FROM ROLE_A",
                "REVOKE GRANT OPTION FOR SELECT ON APP.T FROM ROLE_A RESTRICT",
                "REVOKE GRANT OPTION FOR SELECT ON APP.T FROM ROLE_A, ROLE_B CASCADE",
                "REVOKE SELECT OFF ON APP.T FROM ROLE_A",
                "REVOKE SELECT OFF ON APP.T FROM ROLE_A RESTRICT",
                "REVOKE SELECT OFF ON APP.T FROM ROLE_A, ROLE_B CASCADE",
                "REVOKE GRANT OPTION FOR SELECT OFF ON APP.T FROM ROLE_A CASCADE",
                "REVOKE SELECT FOR DUMP, FLASHBACK, INDEX, ALTER ON APP.T FROM ROLE_A",
                "REVOKE EXECUTE ON PROCEDURE APP.P FROM ROLE_A CASCADE",
                "REVOKE READ, WRITE ON DIRECTORY DATA_DIR FROM ROLE_A RESTRICT",
                "REVOKE SELECT ON SCHEMA APP FROM ROLE_A RESTRICT",
                "REVOKE SELECT ANY TABLE ON SCHEMA APP FROM ROLE_A",
                "REVOKE CREATE ANY TABLE ON SCHEMA APP FROM ROLE_A, ROLE_B CASCADE",
                "REVOKE ALL ON SCHEMA APP FROM ROLE_A RESTRICT",
                "REVOKE ALL PRIVILEGES ON SCHEMA APP FROM ROLE_A, ROLE_B CASCADE",
                // Section 3.3.4.2 spells the optional prefix GRANT ADMIN FOR in its syntax definition.
                "REVOKE GRANT ADMIN FOR SELECT ANY TABLE ON SCHEMA APP FROM ROLE_A RESTRICT",
                "REVOKE GRANT ADMIN FOR ALL PRIVILEGES ON SCHEMA APP FROM ROLE_A, ROLE_B CASCADE",
                "revoke /* role privileges */ grant option for select on \"APP\".\"T\" from \"R; DROP TABLE APP.T\" cascade;");
    }

    // Section 3.3.3.2 allows multiple revoked roles but specifies one recipient, with optional ADMIN OPTION FOR.
    static Stream<String> revokeRoleStatements() {
        return Stream.concat(
                Stream.of("", "ADMIN OPTION FOR ")
                        .flatMap(option -> Stream.of("ROLE_A", "ROLE_A, ROLE_B")
                                .flatMap(roles -> Stream.of("U", "ROLE_C")
                                        .map(principal -> "REVOKE " + option + roles + " FROM " + principal))),
                Stream.of("revoke /* role membership */ admin option for \"R; DROP TABLE APP.T\", \"Role B\" from \"U\";"));
    }

    // Section 3.2 documents no ALTER ROLE statement. Section 3.2.2 uses SP_SET_ROLE for enablement instead.
    // These procedure calls are outside this analyzer's supported DDL scope, not proven skippable DDL.
    @ParameterizedTest
    @ValueSource(strings = {
            "ALTER ROLE ROLE_A",
            "ALTER ROLE ROLE_A IDENTIFIED BY \"Dm_role_123\"",
            "SP_SET_ROLE('ROLE_A', 0);",
            "SP_SET_ROLE('ROLE_A', 1);"
    })
    void shouldKeepUnsupportedRoleAlterationOutsideSkippableDdl(String sql) {
        assertUnsupported(sql);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "CREATE ROLE IF NOT EXISTS ROLE_A",
            "DROP ROLE IF EXISTS ROLE_A",
            "GRANT CREATE TABLE, ALTER ANY TABLE, DROP ANY TABLE TO ROLE_A WITH ADMIN OPTION",
            "GRANT SELECT OFF ON APP.T TO ROLE_A WITH GRANT OPTION",
            "GRANT CREATE ANY TABLE ON SCHEMA APP TO ROLE_A WITH ADMIN OPTION",
            "GRANT ROLE_A, ROLE_B TO U, V WITH ADMIN OPTION",
            "REVOKE ADMIN OPTION FOR CREATE TABLE, DROP ANY TABLE FROM ROLE_A",
            "REVOKE GRANT OPTION FOR SELECT OFF ON APP.T FROM ROLE_A CASCADE",
            "REVOKE GRANT ADMIN FOR CREATE ANY TABLE ON SCHEMA APP FROM ROLE_A RESTRICT",
            "REVOKE ADMIN OPTION FOR ROLE_A, ROLE_B FROM U"
    })
    void shouldNotSwallowTableChangesFollowingRoleManagement(String sql) {
        assertFollowingTableChangesAreNotSkipped(sql);
    }

    @ParameterizedTest
    @MethodSource({ "databasePrivilegeStatements", "objectPrivilegeStatements", "schemaPrivilegeStatements",
            "grantRoleStatements", "revokeRoleStatements", "privilegePrincipalStatements" })
    void shouldSkipGrantAndRevokeWithoutChangingCapturedTables(String sql) {
        assertStatementDoesNotChangeCapturedTables(sql);
        final var statement = DamengParseTree.parseScope(sql, false).tree().statement(0).nonTableStatement();
        assertThat(statement.grantStatement() != null || statement.revokeStatement() != null).isTrue();
    }

    // DM Security Management 3.3.1-3.3.4: database, object, role and schema privileges.
    // https://eco.dameng.com/document/dm/zh-cn/pm/discretionary-access-control.html
    // Database privilege names come from both role/privilege tables in appendices 1 and 2,
    // plus CREATE SESSION in section 3.1.1. Applicability and authorization remain DB checks.
    // https://eco.dameng.com/document/dm/zh-cn/pm/dm8-safety-appendix.html
    static Stream<String> databasePrivilegeStatements() {
        return Stream.concat(databasePrivilegeNames().flatMap(privilege -> Stream.of(
                "GRANT " + privilege + " TO U",
                "GRANT " + privilege + " TO U, ROLE_A WITH ADMIN OPTION",
                "REVOKE " + privilege + " FROM U, ROLE_A",
                "REVOKE ADMIN OPTION FOR " + privilege + " FROM U")),
                Stream.of(
                        "GRANT CREATE TABLE, ALTER ANY TABLE, DROP ANY TABLE, GRANT ANY OBJECT PRIVILEGE TO U WITH ADMIN OPTION",
                        "REVOKE ADMIN OPTION FOR CREATE TABLE, ALTER ANY TABLE, DROP ANY TABLE, GRANT ANY OBJECT PRIVILEGE FROM U, ROLE_A"));
    }

    private static Stream<String> databasePrivilegeNames() {
        return Stream.of(
                "ADMIN ANY DATABASE PRIVILEGE", "ADMIN ANY ROLE", "ADMIN JOB",
                "ALTER ANY CONTEXT INDEX", "ALTER ANY INDEX", "ALTER ANY LIBRARY",
                "ALTER ANY MATERIALIZED VIEW", "ALTER ANY SEQUENCE", "ALTER ANY TABLE",
                "ALTER ANY TRIGGER", "ALTER ANY VIEW", "ALTER CONTEXT INDEX",
                "ALTER DATABASE", "ALTER INDEX", "ALTER LIBRARY",
                "ALTER MATERIALIZED VIEW", "ALTER PROFILE", "ALTER SEQUENCE",
                "ALTER TABLE", "ALTER TABLESPACE", "ALTER TRIGGER",
                "ALTER USER", "ALTER VIEW", "AUDIT DATABASE",
                "BACKUP DATABASE", "COMMENT ANY TABLE", "CREATE ANY CONTEXT",
                "CREATE ANY CONTEXT INDEX", "CREATE ANY DIRECTORY", "CREATE ANY DOMAIN",
                "CREATE ANY INDEX", "CREATE ANY LIBRARY", "CREATE ANY LINK",
                "CREATE ANY MATERIALIZED VIEW", "CREATE ANY OPERATOR", "CREATE ANY PACKAGE",
                "CREATE ANY PARTITION GROUP", "CREATE ANY PROCEDURE", "CREATE ANY SCHEMA",
                "CREATE ANY SEQUENCE", "CREATE ANY SYNONYM", "CREATE ANY TABLE",
                "CREATE ANY TRIGGER", "CREATE ANY VIEW", "CREATE CONTEXT INDEX",
                "CREATE DOMAIN", "CREATE INDEX", "CREATE LIBRARY",
                "CREATE LINK", "CREATE MATERIALIZED VIEW", "CREATE OPERATOR",
                "CREATE PACKAGE", "CREATE PARTITION GROUP", "CREATE PROCEDURE",
                "CREATE PROFILE", "CREATE PUBLIC SYNONYM", "CREATE ROLE",
                "CREATE SCHEMA", "CREATE SEQUENCE", "CREATE SESSION",
                "CREATE SYNONYM", "CREATE TABLE", "CREATE TABLESPACE",
                "CREATE TRIGGER", "CREATE USER", "CREATE VIEW",
                "DELETE ANY TABLE", "DELETE ANY VIEW", "DELETE TABLE",
                "DELETE VIEW", "DROP ANY CONTEXT", "DROP ANY CONTEXT INDEX",
                "DROP ANY DIRECTORY", "DROP ANY DOMAIN", "DROP ANY INDEX",
                "DROP ANY LIBRARY", "DROP ANY LINK", "DROP ANY MATERIALIZED VIEW",
                "DROP ANY OPERATOR", "DROP ANY PACKAGE", "DROP ANY PARTITION GROUP",
                "DROP ANY PROCEDURE", "DROP ANY SCHEMA", "DROP ANY SEQUENCE",
                "DROP ANY SYNONYM", "DROP ANY TABLE", "DROP ANY TRIGGER",
                "DROP ANY VIEW", "DROP CONTEXT INDEX", "DROP DOMAIN",
                "DROP INDEX", "DROP LIBRARY", "DROP LINK",
                "DROP MATERIALIZED VIEW", "DROP OPERATOR", "DROP PACKAGE",
                "DROP PARTITION GROUP", "DROP PROCEDURE", "DROP PROFILE",
                "DROP PUBLIC SYNONYM", "DROP ROLE", "DROP SCHEMA",
                "DROP SEQUENCE", "DROP SYNONYM", "DROP TABLE",
                "DROP TABLESPACE", "DROP TRIGGER", "DROP USER",
                "DROP VIEW", "DUMP ANY TABLE", "DUMP TABLE",
                "EXECUTE ANY LIBRARY", "EXECUTE ANY PACKAGE", "EXECUTE ANY PROCEDURE",
                "EXECUTE LIBRARY", "EXECUTE PACKAGE", "EXECUTE PROCEDURE",
                "FLASHBACK ANY TABLE", "FLASHBACK TABLE", "GRANT ANY CONTEXT",
                "GRANT ANY DOMAIN", "GRANT ANY LIBRARY", "GRANT ANY OBJECT PRIVILEGE",
                "GRANT ANY PACKAGE", "GRANT ANY PROCEDURE", "GRANT ANY SEQUENCE",
                "GRANT ANY TABLE", "GRANT ANY VIEW", "GRANT DOMAIN",
                "GRANT LIBRARY", "GRANT PACKAGE", "GRANT PROCEDURE",
                "GRANT SEQUENCE", "GRANT TABLE", "GRANT VIEW",
                "INSERT ANY TABLE", "INSERT ANY VIEW", "INSERT TABLE",
                "INSERT VIEW", "LABEL DATABASE", "REFERENCES ANY TABLE",
                "REFERENCES TABLE", "RESTORE DATABASE", "SELECT ANY DICTIONARY",
                "SELECT ANY MATERIALIZED VIEW", "SELECT ANY SEQUENCE", "SELECT ANY TABLE",
                "SELECT ANY VIEW", "SELECT MATERIALIZED VIEW", "SELECT SEQUENCE",
                "SELECT TABLE", "SELECT VIEW", "SUBSCRIBE LOG",
                "UNLIMITED TABLESPACE", "UPDATE ANY TABLE", "UPDATE ANY VIEW",
                "UPDATE TABLE", "UPDATE VIEW", "USAGE ANY DOMAIN",
                "USAGE ANY PARTITION GROUP", "USAGE DOMAIN", "USAGE PARTITION GROUP");
    }

    static Stream<String> objectPrivilegeStatements() {
        return Stream.of(
                Stream.of(
                        "SELECT ON T", "SELECT ON APP.T", "SELECT ON TABLE T", "SELECT ON TABLE APP.T",
                        "SELECT ON TABLE \"APP\".\"T\"", "SELECT ON VIEW APP.V", "INSERT ON APP.T", "UPDATE ON APP.T",
                        "DELETE ON APP.T", "REFERENCES ON APP.T", "INDEX ON APP.T", "ALTER ON APP.T",
                        "SELECT FOR DUMP ON APP.T", "FLASHBACK ON APP.T", "SELECT OFF ON APP.T",
                        "SELECT, INSERT, UPDATE, DELETE, REFERENCES ON TABLE APP.T",
                        "SELECT, SELECT FOR DUMP, FLASHBACK, INDEX, ALTER ON APP.T",
                        "ALL ON APP.T", "ALL PRIVILEGES ON TABLE APP.T",
                        "EXECUTE ON PROCEDURE APP.P", "EXECUTE ON FUNCTION APP.F", "EXECUTE ON PACKAGE APP.PKG",
                        "EXECUTE ON CLASS APP.C", "EXECUTE ON TYPE APP.OBJ", "EXECUTE ON LIBRARY APP.LIB",
                        "SELECT ON SEQUENCE APP.SEQ", "ALTER ON SEQUENCE APP.SEQ", "USAGE ON DOMAIN APP.D",
                        "SELECT ON SYNONYM APP.S", "READ ON DIRECTORY DATA_DIR", "WRITE ON DIRECTORY DATA_DIR",
                        "READ, WRITE ON DIRECTORY \"Data Dir\"", "SELECT ON SCHEMA APP", "ALL ON SCHEMA APP",
                        "ALL PRIVILEGES ON SCHEMA \"App Schema\"")
                        .flatMap(privilege -> Stream.concat(
                                Stream.of("", " WITH GRANT OPTION").map(option -> "GRANT " + privilege + " TO U, ROLE_A" + option),
                                Stream.of("", "GRANT OPTION FOR ").flatMap(option -> Stream.of("", " RESTRICT", " CASCADE")
                                        .map(behavior -> "REVOKE " + option + privilege + " FROM U, ROLE_A" + behavior)))),
                // Only GRANT has column lists. DM 8.1.3.162 rejects column-level REVOKE.
                Stream.of("SELECT", "INSERT", "UPDATE", "REFERENCES")
                        .flatMap(action -> Stream.of("ID", "ID, V", "\"ID\", \"DROP; TABLE APP.T\"")
                                .flatMap(columns -> Stream.of("APP.T", "TABLE APP.T")
                                        .flatMap(target -> Stream.of("", " WITH GRANT OPTION")
                                                .map(option -> "GRANT " + action + "(" + columns + ") ON " + target + " TO U" + option)))),
                Stream.of(
                        "GRANT SELECT(ID), INSERT(ID,V), UPDATE(V), REFERENCES(ID) ON APP.T TO U WITH GRANT OPTION",
                        "grant /* privileges */ select on \"APP\".\"T; DROP TABLE APP.T\" to \"U\";",
                        "revoke grant option for select off on table \"APP\".\"T\" from \"U\" cascade; -- ALTER TABLE APP.T ADD X INT"))
                .flatMap(statements -> statements);
    }

    static Stream<String> schemaPrivilegeStatements() {
        // Section 3.3.4.2 literally specifies GRANT ADMIN FOR. Schema grants require
        // GRANT_SCHEMA; the 8.1.3.162 probe did not enable it. These are grammar tests.
        return schemaPrivilegeNames().flatMap(privilege -> Stream.concat(
                Stream.of("", " WITH ADMIN OPTION").map(option -> "GRANT " + privilege + " ON SCHEMA APP TO U, ROLE_A" + option),
                Stream.of("", "GRANT ADMIN FOR ").flatMap(option -> Stream.of("", " RESTRICT", " CASCADE")
                        .map(behavior -> "REVOKE " + option + privilege + " ON SCHEMA APP FROM U, ROLE_A" + behavior))));
    }

    private static Stream<String> schemaPrivilegeNames() {
        return Stream.of(
                "SELECT ANY TABLE", "INSERT ANY TABLE", "UPDATE ANY TABLE",
                "DELETE ANY TABLE", "REFERENCES ANY TABLE", "DUMP ANY TABLE",
                "FLASHBACK ANY TABLE", "CREATE ANY TABLE", "ALTER ANY TABLE",
                "DROP ANY TABLE", "SELECT ANY VIEW", "INSERT ANY VIEW",
                "UPDATE ANY VIEW", "DELETE ANY VIEW", "CREATE ANY VIEW",
                "ALTER ANY VIEW", "DROP ANY VIEW", "SELECT ANY MATERIALIZED VIEW",
                "CREATE ANY MATERIALIZED VIEW", "ALTER ANY MATERIALIZED VIEW", "DROP ANY MATERIALIZED VIEW",
                "CREATE ANY INDEX", "ALTER ANY INDEX", "DROP ANY INDEX",
                "CREATE ANY CONTEXT INDEX", "ALTER ANY CONTEXT INDEX", "DROP ANY CONTEXT INDEX",
                "SELECT ANY SEQUENCE", "CREATE ANY SEQUENCE", "ALTER ANY SEQUENCE",
                "DROP ANY SEQUENCE", "EXECUTE ANY PROCEDURE", "CREATE ANY PROCEDURE",
                "DROP ANY PROCEDURE", "EXECUTE ANY PACKAGE", "CREATE ANY PACKAGE",
                "DROP ANY PACKAGE", "USAGE ANY DOMAIN", "CREATE ANY DOMAIN",
                "DROP ANY DOMAIN", "CREATE ANY TRIGGER", "ALTER ANY TRIGGER",
                "DROP ANY TRIGGER", "CREATE ANY SYNONYM", "DROP ANY SYNONYM",
                "ALL", "ALL PRIVILEGES");
    }

    static Stream<String> privilegePrincipalStatements() {
        return Stream.of("U", "ROLE_A", "PUBLIC", "U, ROLE_A, PUBLIC", "\"User Name\"", "\"用户\"", "\"U\"\"Name\"",
                "\"U; DROP TABLE APP.T\", \"V, W\"")
                .flatMap(principals -> Stream.of(
                        "GRANT SELECT ON APP.T TO " + principals,
                        "REVOKE SELECT ON APP.T FROM " + principals + " CASCADE",
                        "GRANT CREATE TABLE TO " + principals + " WITH ADMIN OPTION",
                        "REVOKE ADMIN OPTION FOR CREATE TABLE FROM " + principals));
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "GRANT CREATE TABLE, DROP ANY TABLE TO U WITH ADMIN OPTION",
            "REVOKE ADMIN OPTION FOR GRANT ANY OBJECT PRIVILEGE FROM U",
            "GRANT SELECT(ID), REFERENCES(ID) ON TABLE APP.T TO U WITH GRANT OPTION",
            "GRANT SELECT OFF ON APP.T TO U", "GRANT READ, WRITE ON DIRECTORY DATA_DIR TO U",
            "GRANT ROLE_A, ROLE_B TO U, ROLE_C WITH ADMIN OPTION",
            "REVOKE ADMIN OPTION FOR ROLE_A FROM U",
            "REVOKE GRANT OPTION FOR SELECT OFF ON APP.T FROM U CASCADE",
            "REVOKE REFERENCES ON APP.T FROM U CASCADE",
            "GRANT SELECT ANY TABLE ON SCHEMA APP TO U WITH ADMIN OPTION",
            "REVOKE GRANT ADMIN FOR SELECT ANY TABLE ON SCHEMA APP FROM U RESTRICT"
    })
    void shouldNotSwallowTableChangesFollowingGrantOrRevoke(String sql) {
        assertFollowingTableChangesAreNotSkipped(sql);
        for (final var separator : new String[]{ " ", "; " }) {
            assertUnsupported(sql + separator + "INSERT INTO APP.T(ID) VALUES(1)");
            assertUnsupported(sql + separator + "UPDATE APP.T SET V='changed'");
            assertUnsupported(sql + separator + "DELETE FROM APP.T");
            assertUnsupported(sql + separator + "CALL APP.P()");
            assertUnsupported(sql + separator + "EXECUTE APP.P 1");
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "GRANT", "GRANT TO U", "GRANT SELECT ON APP.T", "GRANT SELECT ON APP.T TO",
            "GRANT SELECT ON TO U", "GRANT SELECT ON TABLE", "GRANT SELECT ON APP. TO U",
            "GRANT SELECT ON SCHEMA", "GRANT READ ON DIRECTORY", "GRANT READ ON DIRECTORY APP.D TO U",
            "GRANT SELECT ON APP.T TO U,", "GRANT SELECT ON APP.T TO ,U", "GRANT SELECT ON APP.T TO U V",
            "GRANT SELECT ON APP.T TO 'U'", "GRANT SELECT ON APP.T TO U WITH", "GRANT SELECT ON APP.T TO U WITH GRANT",
            "GRANT SELECT ON APP.T TO U WITH ADMIN OPTION", "GRANT CREATE TABLE TO U WITH GRANT OPTION",
            "GRANT ROLE_A TO U WITH GRANT OPTION", "GRANT SELECT ON APP.T TO U CASCADE",
            "GRANT SELECT, ON APP.T TO U", "GRANT ,SELECT ON APP.T TO U", "GRANT SELECT,,UPDATE ON APP.T TO U",
            "GRANT ALL PRIVILEGES, SELECT ON APP.T TO U", "GRANT SELECT() ON APP.T TO U",
            "GRANT UPDATE(ID,) ON APP.T TO U", "GRANT UPDATE((ID)) ON APP.T TO U", "GRANT DELETE(ID) ON APP.T TO U",
            "GRANT SELECT OFF APP.T TO U", "GRANT SELECT OFF OFF ON APP.T TO U", "GRANT SELECT ON APP.T OFF TO U",
            "GRANT SELECT FOR ON APP.T TO U", "GRANT SELECT ON APP.T, APP.U TO U",
            "GRANT CREATE TABLE APP.T(ID INT) TO U", "GRANT SELECT ON APP.T INSERT INTO APP.T VALUES(1) TO U",
            "GRANT UPDATE(SELECT ID FROM APP.T) ON APP.T TO U",
            "REVOKE", "REVOKE FROM U", "REVOKE SELECT ON APP.T", "REVOKE SELECT ON APP.T FROM",
            "REVOKE SELECT ON FROM U", "REVOKE SELECT ON APP. FROM U", "REVOKE SELECT ON SCHEMA",
            "REVOKE SELECT ON APP.T FROM U,", "REVOKE SELECT ON APP.T FROM U WITH GRANT OPTION",
            "REVOKE SELECT ON APP.T FROM U CASCADE RESTRICT", "REVOKE SELECT ON APP.T FROM U CASCADE CONSTRAINTS",
            "REVOKE CREATE TABLE FROM U CASCADE", "REVOKE CREATE TABLE FROM U RESTRICT", "REVOKE ROLE_A FROM U CASCADE",
            "REVOKE ADMIN FOR CREATE TABLE FROM U", "REVOKE ADMIN OPTION CREATE TABLE FROM U",
            "REVOKE GRANT FOR SELECT ON APP.T FROM U", "REVOKE GRANT OPTION SELECT ON APP.T FROM U",
            "REVOKE GRANT OPTION FOR ON APP.T FROM U", "REVOKE ADMIN OPTION FOR SELECT ON APP.T FROM U",
            "REVOKE GRANT ADMIN FOR SELECT ON APP.T FROM U", "REVOKE GRANT ADMIN SELECT ANY TABLE ON SCHEMA APP FROM U",
            "REVOKE SELECT(ID) ON APP.T FROM U", "REVOKE INSERT(ID) ON APP.T FROM U",
            "REVOKE UPDATE(V) ON APP.T FROM U", "REVOKE REFERENCES(ID) ON APP.T FROM U",
            "REVOKE SELECT OFF OFF ON APP.T FROM U", "REVOKE SELECT ON APP.T, APP.U FROM U",
            "REVOKE SELECT, ON APP.T FROM U", "REVOKE SELECT ON APP.T UPDATE APP.T SET V='changed' FROM U"
    })
    void shouldRejectMalformedGrantAndRevokeStatements(String sql) {
        assertUnsupported(sql);
    }

    @ParameterizedTest
    @MethodSource({ "createViewStatements", "alterViewStatements", "dropViewStatements" })
    void shouldSkipViewManagementWithoutChangingCapturedTables(String sql) {
        assertStatementDoesNotChangeCapturedTables(sql);
    }

    @ParameterizedTest
    @MethodSource({ "createMaterializedViewStatements", "alterMaterializedViewStatements", "dropMaterializedViewStatements", "refreshMaterializedViewStatements" })
    void shouldSkipMaterializedViewManagementWithoutChangingCapturedTables(String sql) {
        assertStatementDoesNotChangeCapturedTables(sql);
        assertThat(DamengParseTree.parseScope(sql, false).tree().statement(0).nonTableStatement().materializedViewStatement()).isNotNull();
    }

    // DM SQL Language Manual 7.1-7.4 and 7.9, including the refresh-option diagram.
    // https://eco.dameng.com/document/dm/zh-cn/pm/materialized-view.html
    // PARTITION/TABLESPACE/STORAGE reference 3.5.1.1 and 3.5.1.4 in:
    // https://eco.dameng.com/document/dm/zh-cn/pm/definition-statement.html
    // These tests classify DDL and preserve captured tables. DM validates query
    // semantics, option compatibility and the prerequisites for FAST refresh.
    static Stream<String> createMaterializedViewStatements() {
        return Stream.of(
                materializedViewNames().flatMap(name -> Stream.of("", "(ID, V)", "(\"Id\", \"Value\")")
                        .flatMap(columns -> Stream.of("", " BUILD IMMEDIATE", " BUILD DEFERRED")
                                .map(build -> "CREATE MATERIALIZED VIEW " + name + columns + build + " AS SELECT ID,V FROM APP.T"))),
                materializedViewRefreshClauses().flatMap(refresh -> materializedViewRewriteClauses()
                        .map(rewrite -> "CREATE MATERIALIZED VIEW APP.MV " + refresh + rewrite + " AS SELECT ID,V FROM APP.T")),
                materializedViewRewriteClauses().map(rewrite -> "CREATE MATERIALIZED VIEW APP.MV" + rewrite + " AS SELECT ID,V FROM APP.T"),
                Stream.of(
                        "TABLESPACE MAIN", "STORAGE(INITIAL 1, NEXT 2, MINEXTENTS 1, FILLFACTOR 80)",
                        "TABLESPACE MAIN STORAGE(ON MAIN, CLUSTERBTR, WITH COUNTER)",
                        "STORAGE(WITHOUT COUNTER, USING LONG ROW)", "STORAGE(DISABLE USING LONG ROW)",
                        "PARTITION BY HASH(ID) PARTITIONS 4", "PARTITION BY HASH(ID) PARTITIONS 2 STORE IN (MAIN, TS2)",
                        "PARTITION BY HASH(ID)(PARTITION P1 TABLESPACE MAIN, PARTITION P2 STORAGE(ON MAIN))",
                        "PARTITION BY RANGE(ID)(PARTITION P1 VALUES LESS THAN(10), PARTITION PMAX VALUES LESS THAN(MAXVALUE))",
                        "PARTITION BY RANGE(ID) INTERVAL(10)(PARTITION P1 VALUES EQU OR LESS THAN(10))",
                        "PARTITION BY LIST(V)(PARTITION P1 VALUES('a','b'), PARTITION POTHER VALUES(DEFAULT))",
                        "PARTITION BY RANGE(ID) SUBPARTITION BY LIST(V) SUBPARTITION TEMPLATE"
                                + "(SUBPARTITION S1 VALUES('a'), SUBPARTITION S2 VALUES(DEFAULT))"
                                + "(PARTITION P1 VALUES LESS THAN(10), PARTITION PMAX VALUES LESS THAN(MAXVALUE))",
                        "PARTITION BY HASH(ID) PARTITIONS 4 TABLESPACE MAIN STORAGE(HASHPARTMAP(2))")
                        .flatMap(physical -> Stream.of("", "BUILD IMMEDIATE ", "BUILD DEFERRED ")
                                .map(build -> "CREATE MATERIALIZED VIEW APP.MV(ID,V) " + build + physical
                                        + " REFRESH COMPLETE ON DEMAND USING TRUNCATE DISABLE QUERY REWRITE AS SELECT ID,V FROM APP.T")),
                materializedViewQueries().map(query -> "CREATE MATERIALIZED VIEW APP.MV REFRESH COMPLETE AS " + query),
                Stream.of(
                        "create /* materialized view */ materialized view \"APP\".\"MV\" build deferred never refresh disable query rewrite as select ID,V from APP.T;",
                        "CREATE MATERIALIZED VIEW APP.MV REFRESH WITH ROWID START WITH SYSDATE NEXT SYSDATE+1 AS SELECT ID,V FROM APP.T",
                        "CREATE MATERIALIZED VIEW APP.MV REFRESH FAST WITH ROWID START WITH SYSDATE NEXT SYSDATE+1 AS SELECT ID,V,ROWID AS X FROM APP.T",
                        "CREATE MATERIALIZED VIEW APP.MV AS SELECT q'[ON PREBUILT TABLE; DROP TABLE APP.T]' AS V FROM APP.T",
                        "CREATE MATERIALIZED VIEW APP.MV AS SELECT \"ON PREBUILT TABLE\", PREBUILT, REFRESH, TRUNCATE(1.25,1) FROM APP.T"))
                .flatMap(statements -> statements);
    }

    private static Stream<String> materializedViewNames() {
        // The same name as APP.T must still be classified as a view operation.
        return Stream.of("MV", "APP.MV", "APP.T", "\"APP\".\"Mv Name\"", "\"应用\".\"物化视图\"", "\"APP\".\"MV; DROP TABLE APP.T\"");
    }

    private static Stream<String> materializedViewRefreshClauses() {
        return Stream.concat(
                Stream.of("FAST", "COMPLETE", "FORCE").flatMap(method -> Stream.of("", " ON DEMAND", " ON COMMIT")
                        .flatMap(timing -> Stream.of("", " WITH PRIMARY KEY", " WITH ROWID")
                                .flatMap(rule -> Stream.of("", " USING DEFAULT", " USING TRUNCATE", " USING DELETE")
                                        .map(using -> "REFRESH " + method + timing + rule + using)))),
                Stream.of(
                        "NEVER REFRESH", "REFRESH ON DEMAND", "REFRESH ON COMMIT", "REFRESH WITH PRIMARY KEY", "REFRESH WITH ROWID",
                        "REFRESH USING DEFAULT", "REFRESH USING TRUNCATE", "REFRESH USING DELETE",
                        "REFRESH START WITH SYSDATE", "REFRESH NEXT SYSDATE+1", "REFRESH START WITH SYSDATE+1 NEXT SYSDATE+2",
                        "REFRESH START WITH DATE '2030-01-01' NEXT SYSDATE+INTERVAL '1' DAY",
                        "REFRESH COMPLETE ON DEMAND START WITH SYSDATE+1 NEXT SYSDATE+2 WITH PRIMARY KEY USING DEFAULT",
                        "REFRESH FORCE WITH ROWID ON COMMIT START WITH SYSDATE+1 NEXT SYSDATE+2 USING DELETE",
                        "REFRESH USING TRUNCATE WITH PRIMARY KEY COMPLETE ON DEMAND",
                        "REFRESH WITH ROWID START WITH SYSDATE NEXT SYSDATE+1 FAST ON DEMAND"));
    }

    private static Stream<String> materializedViewRewriteClauses() {
        return Stream.of("", " ENABLE QUERY REWRITE", " DISABLE QUERY REWRITE");
    }

    private static Stream<String> materializedViewQueries() {
        return Stream.of(
                "SELECT ID,V FROM APP.T ORDER BY ID", "SELECT ID,COUNT(*) C FROM APP.T GROUP BY ID",
                "SELECT T.ID,U.V FROM APP.T T JOIN APP.U U ON T.ID=U.ID",
                "SELECT ID FROM APP.T WHERE ID IN (SELECT ID FROM APP.U)",
                "SELECT ID,V FROM APP.T UNION ALL SELECT ID,V FROM APP.U",
                "SELECT ID,ROW_NUMBER() OVER(PARTITION BY V ORDER BY ID) RN FROM APP.T",
                "SELECT JSON_VALUE(V,'$.x' RETURNING VARCHAR NULL ON ERROR) J FROM APP.T",
                "WITH Q AS (SELECT ID,V FROM APP.T), R AS (SELECT ID,V FROM Q) SELECT ID,V FROM R",
                "(SELECT ID,V FROM APP.T)");
    }

    static Stream<String> alterMaterializedViewStatements() {
        return Stream.of(
                materializedViewNames().flatMap(name -> Stream.of("COMPILE", "NEVER REFRESH", "REFRESH COMPLETE", "REFRESH FAST ON DEMAND",
                        "ENABLE QUERY REWRITE", "DISABLE QUERY REWRITE").map(option -> "ALTER MATERIALIZED VIEW " + name + " " + option)),
                materializedViewRefreshClauses().flatMap(refresh -> materializedViewRewriteClauses()
                        .map(rewrite -> "ALTER MATERIALIZED VIEW APP.MV " + refresh + rewrite)),
                Stream.of("alter /* materialized view */ materialized view \"APP\".\"MV\" compile;",
                        "ALTER MATERIALIZED VIEW APP.MV NEVER REFRESH DISABLE QUERY REWRITE;"))
                .flatMap(statements -> statements);
    }

    static Stream<String> dropMaterializedViewStatements() {
        return Stream.concat(materializedViewNames().flatMap(name -> Stream.of("", "IF EXISTS ")
                .map(ifExists -> "DROP MATERIALIZED VIEW " + ifExists + name)),
                Stream.of("drop /* materialized view */ materialized view if exists \"APP\".\"MV\"; -- DROP TABLE APP.T"));
    }

    static Stream<String> refreshMaterializedViewStatements() {
        return Stream.concat(materializedViewNames().flatMap(name -> Stream.concat(Stream.of("", " FAST"),
                Stream.of(" COMPLETE", " FORCE").flatMap(method -> Stream.of("", " USING DEFAULT", " USING TRUNCATE", " USING DELETE")
                        .map(using -> method + using)))
                .map(options -> "REFRESH MATERIALIZED VIEW " + name + options)),
                Stream.of("refresh /* materialized view */ materialized view \"APP\".\"MV\" complete using truncate;"));
    }

    @ParameterizedTest
    @MethodSource("prebuiltMaterializedViewStatements")
    void shouldRejectPrebuiltMaterializedViewsUsingOrdinaryTables(String sql) {
        final var definition = DamengParseTree.parseScope(sql, false).tree().statement(0).nonTableStatement()
                .materializedViewStatement().materializedViewDefinition();
        assertThat(definition.materializedViewPrebuiltClause()).isNotNull();
        final var result = analyzer.analyze(sql, "APP");
        assertThat(result.impact()).isEqualTo(Impact.UNSUPPORTED);
        assertThat(result.failure().getMessage()).contains("PREBUILT", "captured business table");
        final var parser = new DamengDdlParser();
        final var tables = new Tables();
        final var id = new TableId(null, "APP", "T");
        parser.parse("CREATE TABLE APP.T(ID INT PRIMARY KEY, V VARCHAR(30))", tables, "APP");
        final var original = tables.forTable(id);
        assertThatThrownBy(() -> parser.parse(sql, tables, "APP")).isInstanceOf(IllegalArgumentException.class);
        assertThat(tables.tableIds()).containsExactly(id);
        assertThat(tables.forTable(id)).isSameAs(original);
    }

    static Stream<String> prebuiltMaterializedViewStatements() {
        return Stream.of("T", "APP.T", "\"APP\".\"T\"")
                .flatMap(table -> Stream.of("", " WITH REDUCED PRECISION", " WITHOUT REDUCED PRECISION")
                        .flatMap(precision -> Stream.of("", " REFRESH COMPLETE ON DEMAND", " NEVER REFRESH DISABLE QUERY REWRITE")
                                .map(options -> "CREATE MATERIALIZED VIEW APP.MV(ID,V) FOR " + table
                                        + " ON /* prebuilt */ PREBUILT TABLE" + precision + options + " AS SELECT ID,V FROM APP.T")));
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "CREATE MATERIALIZED VIEW APP.MV AS SELECT ID,V FROM APP.T",
            "CREATE MATERIALIZED VIEW APP.MV BUILD DEFERRED NEVER REFRESH AS SELECT ID FROM APP.T",
            "CREATE MATERIALIZED VIEW APP.MV REFRESH COMPLETE USING TRUNCATE AS SELECT ID FROM APP.T",
            "ALTER MATERIALIZED VIEW APP.MV REFRESH COMPLETE USING DELETE",
            "ALTER MATERIALIZED VIEW APP.MV ENABLE QUERY REWRITE", "ALTER MATERIALIZED VIEW APP.MV COMPILE",
            "DROP MATERIALIZED VIEW APP.MV", "DROP MATERIALIZED VIEW IF EXISTS APP.MV",
            "REFRESH MATERIALIZED VIEW APP.MV", "REFRESH MATERIALIZED VIEW APP.MV FAST",
            "REFRESH MATERIALIZED VIEW APP.MV COMPLETE USING TRUNCATE", "REFRESH MATERIALIZED VIEW APP.MV FORCE USING DELETE"
    })
    void shouldNotSwallowTableChangesFollowingMaterializedViewManagement(String sql) {
        assertFollowingTableChangesAreNotSkipped(sql);
        for (final var separator : new String[]{ " ", "; " }) {
            assertUnsupported(sql + separator + "INSERT INTO APP.T(ID) VALUES(1)");
            assertUnsupported(sql + separator + "UPDATE APP.T SET V='changed'");
            assertUnsupported(sql + separator + "DELETE FROM APP.T");
            assertUnsupported(sql + separator + "MERGE INTO APP.T USING APP.U ON (T.ID=U.ID) WHEN MATCHED THEN UPDATE SET T.V=U.V");
            assertUnsupported(sql + separator + "CALL APP.P()");
            assertUnsupported(sql + separator + "EXEC APP.P 1");
            assertUnsupported(sql + separator + "EXECUTE APP.P 1");
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "CREATE MATERIALIZED VIEW", "CREATE MATERIALIZED VIEW APP.MV", "CREATE MATERIALIZED VIEW APP.MV AS",
            "CREATE MATERIALIZED VIEW APP.MV AS SELECT", "CREATE MATERIALIZED VIEW APP.MV AS INSERT INTO APP.T VALUES(1)",
            "CREATE MATERIALIZED VIEW APP.MV() AS SELECT ID FROM APP.T", "CREATE MATERIALIZED VIEW APP.MV(ID,) AS SELECT ID FROM APP.T",
            "CREATE MATERIALIZED VIEW IF NOT EXISTS APP.MV AS SELECT ID FROM APP.T",
            "CREATE OR REPLACE MATERIALIZED VIEW APP.MV AS SELECT ID FROM APP.T",
            "CREATE FORCE MATERIALIZED VIEW APP.MV AS SELECT ID FROM APP.T",
            "CREATE MATERIALIZED VIEW APP.MV BUILD AS SELECT ID FROM APP.T",
            "CREATE MATERIALIZED VIEW APP.MV BUILD UNKNOWN AS SELECT ID FROM APP.T",
            "CREATE MATERIALIZED VIEW APP.MV TABLESPACE AS SELECT ID FROM APP.T",
            "CREATE MATERIALIZED VIEW APP.MV STORAGE() AS SELECT ID FROM APP.T",
            "CREATE MATERIALIZED VIEW APP.MV STORAGE(ON MAIN AS SELECT ID FROM APP.T",
            "CREATE MATERIALIZED VIEW APP.MV PARTITION BY HASH(ID) AS SELECT ID FROM APP.T",
            "CREATE MATERIALIZED VIEW APP.MV REFRESH AS SELECT ID FROM APP.T",
            "CREATE MATERIALIZED VIEW APP.MV REFRESH COMPLETE USING AS SELECT ID FROM APP.T",
            "CREATE MATERIALIZED VIEW APP.MV NEVER AS SELECT ID FROM APP.T",
            "CREATE MATERIALIZED VIEW APP.MV ENABLE REWRITE AS SELECT ID FROM APP.T",
            "CREATE MATERIALIZED VIEW APP.MV AS SELECT (ID FROM APP.T",
            "CREATE MATERIALIZED VIEW APP.MV AS SELECT ID INTO APP.U FROM APP.T",
            "CREATE MATERIALIZED VIEW APP.MV FOR APP.T AS SELECT ID FROM APP.T",
            "CREATE MATERIALIZED VIEW APP.MV ON PREBUILT TABLE AS SELECT ID FROM APP.T",
            "ALTER MATERIALIZED VIEW", "ALTER MATERIALIZED VIEW APP.MV", "ALTER MATERIALIZED VIEW APP.MV REFRESH",
            "ALTER MATERIALIZED VIEW APP.MV REFRESH UNKNOWN", "ALTER MATERIALIZED VIEW APP.MV REFRESH ON",
            "ALTER MATERIALIZED VIEW APP.MV REFRESH WITH", "ALTER MATERIALIZED VIEW APP.MV REFRESH WITH PRIMARY",
            "ALTER MATERIALIZED VIEW APP.MV REFRESH START WITH", "ALTER MATERIALIZED VIEW APP.MV REFRESH NEXT",
            "ALTER MATERIALIZED VIEW APP.MV REFRESH USING", "ALTER MATERIALIZED VIEW APP.MV REFRESH USING UNKNOWN",
            "ALTER MATERIALIZED VIEW APP.MV NEVER", "ALTER MATERIALIZED VIEW APP.MV ENABLE QUERY",
            "ALTER MATERIALIZED VIEW APP.MV DISABLE REWRITE", "ALTER MATERIALIZED VIEW APP.MV COMPILE DEBUG",
            "ALTER MATERIALIZED VIEW APP.MV RENAME TO APP.MV2", "ALTER MATERIALIZED VIEW APP.MV AS SELECT ID FROM APP.T",
            "DROP MATERIALIZED VIEW", "DROP MATERIALIZED VIEW IF EXISTS", "DROP MATERIALIZED VIEW IF NOT EXISTS APP.MV",
            "DROP MATERIALIZED VIEW APP.MV CASCADE", "DROP MATERIALIZED VIEW APP.MV RESTRICT", "DROP MATERIALIZED VIEW APP.MV, APP.MV2",
            "REFRESH MATERIALIZED VIEW", "REFRESH MATERIALIZED VIEW APP.MV UNKNOWN",
            "REFRESH MATERIALIZED VIEW IF EXISTS APP.MV", "REFRESH MATERIALIZED VIEW APP.MV FAST USING TRUNCATE",
            "REFRESH MATERIALIZED VIEW APP.MV USING DELETE", "REFRESH MATERIALIZED VIEW APP.MV COMPLETE USING",
            "REFRESH MATERIALIZED VIEW APP.MV FORCE USING UNKNOWN", "REFRESH MATERIALIZED VIEW APP.MV COMPLETE ON DEMAND",
            "REFRESH MATERIALIZED VIEW APP.MV NEVER REFRESH", "REFRESH MATERIALIZED VIEW APP.MV FAST COMPLETE"
    })
    void shouldRejectIncompleteOrUnsupportedMaterializedViewStatements(String sql) {
        assertUnsupported(sql);
    }

    @ParameterizedTest
    @ValueSource(strings = { "BUILD", "CALL", "COMPLETE", "DEMAND", "EXEC", "FAST", "MERGE", "NEVER", "QUERY", "REDUCED", "REWRITE" })
    void shouldKeepMaterializedViewKeywordsUsableAsColumnAndExpressionNames(String name) {
        final var parser = new DamengDdlParser();
        final var tables = new Tables();
        parser.parse("CREATE TABLE APP.T(" + name + " INT)", tables, "APP");
        assertThat(analyzer.analyze("CREATE TABLE APP.COPY AS SELECT " + name + " FROM APP.T", "APP").requireSupported().isCtas()).isTrue();
    }

    // DM SQL Language Manual, section 6.2: ordinary views, excluding materialized views.
    // https://eco.dameng.com/document/dm/zh-cn/pm/view.html
    // OR REPLACE and IF NOT EXISTS may appear together; OR REPLACE takes precedence.
    static Stream<String> createViewStatements() {
        return Stream.concat(
                viewCreationPrefixes().flatMap(prefix -> Stream.of("V", "APP.V")
                        .flatMap(name -> Stream.of("", "(VIEW_ID, VIEW_VALUE)")
                                .flatMap(columns -> Stream.of("", " WITH CHECK OPTION", " WITH LOCAL CHECK OPTION",
                                        " WITH CASCADED CHECK OPTION", " WITH READ ONLY")
                                        .map(option -> prefix + name + columns + " AS SELECT ID, V FROM APP.T WHERE ID > 0" + option)))),
                Stream.of(
                        "CREATE VIEW APP.V(VIEW_ID) AS SELECT ID FROM APP.T",
                        "CREATE VIEW \"APP\".\"视图\"(\"编号\", \"值\") AS SELECT ID, V FROM APP.T WITH LOCAL CHECK OPTION;",
                        "create /* DROP TABLE APP.T */ or replace force view if not exists app.v(id, v)"
                                + " as select id, v from app.t where id > 0 with cascaded check option;",
                        "CREATE OR REPLACE FORCE VIEW IF NOT EXISTS APP.V AS SELECT ID, V FROM APP.MISSING_T",
                        "CREATE VIEW APP.V(ID, TOTAL) AS SELECT ID, COUNT(*) FROM APP.T GROUP BY ID ORDER BY ID",
                        "CREATE VIEW APP.V(LEFT_ID, RIGHT_ID) AS SELECT L.ID, R.ID FROM APP.T L INNER JOIN APP.U R ON L.ID = R.ID",
                        "CREATE VIEW APP.V AS SELECT S.ID, S.V FROM (SELECT ID, V FROM APP.T WHERE ID > 0) S WITH READ ONLY",
                        "CREATE VIEW APP.V AS SELECT ID, V FROM APP.T UNION ALL SELECT ID, V FROM APP.U WITH READ ONLY",
                        "CREATE VIEW APP.\"V; DROP TABLE APP.T\"(\"ALTER\", \"TRUNCATE\")"
                                + " AS SELECT q'[; ALTER TABLE APP.T ADD HIDDEN INT]', TRUNCATE(1.25, 1) FROM APP.T WITH READ ONLY;"));
    }

    private static Stream<String> viewCreationPrefixes() {
        return Stream.of("", "OR REPLACE ")
                .flatMap(replacement -> Stream.of("", "FORCE ")
                        .flatMap(force -> Stream.of("", "IF NOT EXISTS ")
                                .map(ifNotExists -> "CREATE " + replacement + force + "VIEW " + ifNotExists)));
    }

    // Section 6.5: ALTER VIEW [schema.]view COMPILE recompiles the view definition.
    static Stream<String> alterViewStatements() {
        return Stream.of(
                "ALTER VIEW V COMPILE",
                "ALTER VIEW APP.V COMPILE",
                "ALTER VIEW \"APP\".\"视图\" COMPILE;",
                "alter /* view compilation */ view \"APP\".\"V; DROP TABLE APP.T\" compile;");
    }

    // Section 6.3: CASCADE may also remove dependent views, but does not drop their base tables.
    static Stream<String> dropViewStatements() {
        return Stream.concat(
                Stream.of("", "IF EXISTS ")
                        .flatMap(ifExists -> Stream.of("V", "APP.V")
                                .flatMap(name -> Stream.of("", " RESTRICT", " CASCADE")
                                        .map(option -> "DROP VIEW " + ifExists + name + option))),
                Stream.of(
                        "DROP VIEW IF EXISTS \"APP\".\"视图\" RESTRICT;",
                        "drop /* view removal */ view if exists \"APP\".\"V; DROP TABLE APP.T\" cascade;"));
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "CREATE VIEW V AS SELECT ID, V FROM APP.T",
            "CREATE OR REPLACE FORCE VIEW IF NOT EXISTS APP.V(ID, V) AS SELECT ID, V FROM APP.T WITH CHECK OPTION",
            "CREATE VIEW APP.V AS SELECT ID, V FROM APP.T WITH LOCAL CHECK OPTION",
            "CREATE VIEW APP.V AS SELECT ID, V FROM APP.T WITH CASCADED CHECK OPTION",
            "CREATE VIEW APP.V AS SELECT ID, V FROM APP.T WITH READ ONLY",
            "CREATE VIEW APP.V(ID, TOTAL) AS SELECT ID, COUNT(*) FROM APP.T GROUP BY ID ORDER BY ID",
            "CREATE VIEW APP.\"V; DROP TABLE APP.T\"(\"ALTER\", \"TRUNCATE\")"
                    + " AS SELECT q'[; ALTER TABLE APP.T ADD HIDDEN INT]', TRUNCATE(1.25, 1) FROM APP.T WITH READ ONLY",
            "ALTER VIEW V COMPILE",
            "ALTER VIEW APP.\"V; DROP TABLE APP.T\" COMPILE",
            "DROP VIEW V",
            "DROP VIEW IF EXISTS APP.V RESTRICT",
            "DROP VIEW IF EXISTS APP.V CASCADE"
    })
    void shouldNotSwallowTableChangesFollowingViewManagement(String sql) {
        assertFollowingTableChangesAreNotSkipped(sql);
    }

    @ParameterizedTest
    @MethodSource({ "createIndexStatements", "alterIndexStatements", "dropIndexStatements", "createArrayIndexStatements",
            "createContextIndexStatements", "alterContextIndexStatements", "dropContextIndexStatements" })
    void shouldSkipIndexManagementWithoutChangingCapturedTables(String sql) {
        assertStatementDoesNotChangeCapturedTables(sql);
    }

    // DM SQL Language Manual, section 3.6.1:
    // https://eco.dameng.com/document/dm/zh-cn/pm/definition-statement.html
    // Exercise documented options without combining mutually exclusive clauses such as NOSORT and ONLINE.
    static Stream<String> createIndexStatements() {
        return Stream.of(
                indexCreationPrefixes().flatMap(prefix -> Stream.of("V", "V ASC", "V DESC", "V ASC, ID DESC")
                        .map(columns -> prefix + "I ON APP.T(" + columns + ")")),
                Stream.of(
                        "LOCAL", "GLOBAL", "TABLESPACE MAIN", "STORAGE(INITIAL 1)", "STORAGE(NEXT 1)",
                        "STORAGE(MINEXTENTS 1)", "STORAGE(ON MAIN)", "STORAGE(FILLFACTOR 80)", "STORAGE(CLUSTERBTR)",
                        "NOSORT", "ONLINE", "REVERSE", "UNUSABLE", "NOPARALLEL", "PARALLEL", "PARALLEL 0", "PARALLEL 4")
                        .map(option -> "CREATE INDEX I ON APP.T(V) " + option),
                Stream.of(
                        "CREATE INDEX I ON T(V)",
                        "CREATE INDEX I ON APP.T(LOWER(V))",
                        "CREATE UNIQUE INDEX I ON APP.T(ID + 1, LOWER(V) DESC)",
                        "CREATE INDEX I ON APP.T(TRUNCATE(ID, 1))",
                        "CREATE INDEX I ON APP.T(EXTRACT(DAY FROM CREATED_AT))",
                        "CREATE INDEX I ON APP.T(APP.F(V, q'[FROM APP.U; DROP TABLE APP.T]'))",
                        "CREATE BITMAP INDEX I ON APP.T(\"FROM\")",
                        "CREATE BITMAP INDEX I ON APP.T(V) /* FROM APP.U; DROP TABLE APP.T */",
                        "CREATE CLUSTER INDEX I ON APP.T(V) STORAGE(BRANCH 32)",
                        "CREATE CLUSTER UNIQUE INDEX I ON APP.T(V) STORAGE(BRANCH(32, 16))",
                        "CREATE CLUSTER INDEX I ON APP.T(V) STORAGE(NOBRANCH)",
                        "CREATE INDEX I ON APP.HUGE_T(V) STORAGE(STAT NONE)",
                        "CREATE INDEX I ON APP.HUGE_T(V) STORAGE(SECTION(4))",
                        "CREATE INDEX I ON APP.HUGE_T(V) STORAGE(ON MAIN, STAT NONE, SECTION(4)) PARALLEL 2",
                        "CREATE INDEX I ON APP.T(ID) GLOBAL PARTITION BY RANGE(ID)"
                                + " (PARTITION P1 VALUES LESS THAN(100) STORAGE(ON MAIN), PARTITION P2 VALUES LESS THAN(MAXVALUE))",
                        "CREATE INDEX I ON APP.T(ID) GLOBAL PARTITION BY LIST(ID)"
                                + " (PARTITION P1 VALUES(1, 2), PARTITION P2 VALUES(DEFAULT))",
                        "CREATE INDEX I ON APP.T(ID) GLOBAL PARTITION BY HASH(ID) PARTITIONS 4",
                        "CREATE OR REPLACE NOT PARTIAL UNIQUE INDEX IF NOT EXISTS I ON APP.T(ID ASC, V DESC)"
                                + " LOCAL TABLESPACE MAIN STORAGE(INITIAL 1, NEXT 1, MINEXTENTS 1, FILLFACTOR 80) REVERSE PARALLEL 2",
                        "CREATE UNIQUE INDEX IF NOT EXISTS I ON APP.T(V) STORAGE(ON MAIN) NOSORT NOPARALLEL",
                        "CREATE NOT PARTIAL INDEX IF NOT EXISTS I ON APP.T(V) ONLINE REVERSE PARALLEL 2",
                        "CREATE INDEX IF NOT EXISTS I ON APP.T(V) REVERSE UNUSABLE PARALLEL 2",
                        "create /* index creation */ or replace not partial unique index if not exists \"I; DROP TABLE APP.T\""
                                + " on \"APP\".\"T\"(\"V\" desc, \"ID\" asc) storage(on main) parallel 2;"))
                .flatMap(statements -> statements);
    }

    private static Stream<String> indexCreationPrefixes() {
        // Bitmap indexes cannot be clustered. Replacement and existence checks may be specified together.
        return Stream.of("", "OR REPLACE ")
                .flatMap(replacement -> Stream.of("", "UNIQUE ", "BITMAP ", "CLUSTER ", "CLUSTER UNIQUE ",
                        "NOT PARTIAL ", "NOT PARTIAL UNIQUE ", "NOT PARTIAL BITMAP ")
                        .flatMap(kind -> Stream.of("", "IF NOT EXISTS ")
                                .map(ifNotExists -> "CREATE " + replacement + kind + "INDEX " + ifNotExists)));
    }

    // Section 3.6.2: SHARE and SHARE ASYNCHRONOUS cannot be combined with NOSORT or ONLINE.
    static Stream<String> alterIndexStatements() {
        return Stream.concat(
                Stream.of("I", "APP.I").flatMap(name -> Stream.of(
                        "RENAME TO J", "RENAME TO APP.J", "INVISIBLE", "VISIBLE", "UNUSABLE",
                        "REBUILD", "REBUILD NOSORT", "REBUILD ONLINE", "REBUILD SHARE", "REBUILD SHARE ASYNCHRONOUS",
                        "REBUILD SHARE ASYNCHRONOUS 3", "REBUILD EXCLUSIVE", "REBUILD PARALLEL", "REBUILD PARALLEL 4",
                        "REBUILD NOPARALLEL", "REBUILD NOSORT NOPARALLEL", "REBUILD ONLINE PARALLEL 2",
                        "MONITORING USAGE", "NOMONITORING USAGE")
                        .map(clause -> "ALTER INDEX " + name + " " + clause)),
                Stream.of(
                        "ALTER INDEX \"APP\".\"索引\" RENAME TO \"APP\".\"新索引\";",
                        "alter /* index maintenance */ index \"APP\".\"I; DROP TABLE APP.T\" rebuild online;"));
    }

    // Sections 3.6.3, 3.7.2 and 3.9.4 share DROP INDEX syntax; the SQL does not identify the index subtype.
    static Stream<String> dropIndexStatements() {
        return Stream.concat(
                Stream.of("", "IF EXISTS ").flatMap(ifExists -> Stream.of("I", "APP.I")
                        .map(name -> "DROP INDEX " + ifExists + name)),
                Stream.of(
                        "DROP INDEX \"APP\".\"索引\";",
                        "drop /* index removal */ index if exists \"APP\".\"I; DROP TABLE APP.T\";"));
    }

    // Section 3.9: array indexes use ALTER/DROP INDEX, without rename, NOSORT or ONLINE support.
    static Stream<String> createArrayIndexStatements() {
        return Stream.concat(
                Stream.of("", "OR REPLACE ").flatMap(replacement -> Stream.of("", "IF NOT EXISTS ")
                        .flatMap(ifNotExists -> Stream.of("", " STORAGE(ON MAIN)")
                                .flatMap(storage -> Stream.of("", " NOPARALLEL", " PARALLEL", " PARALLEL 2")
                                        .map(parallel -> "CREATE " + replacement + "ARRAY INDEX " + ifNotExists
                                                + "I ON APP.ARRAY_T(V)" + storage + parallel)))),
                Stream.of(
                        "CREATE ARRAY INDEX I ON ARRAY_T(V)",
                        "create or replace array index if not exists \"I; DROP TABLE APP.T\" on \"APP\".\"ARRAY_T\"(\"V\");"));
    }

    // Sections 3.8.1-3.8.3: full-text indexes use CONTEXT INDEX, with five documented lexer choices.
    static Stream<String> createContextIndexStatements() {
        return Stream.concat(
                Stream.of("", "IF NOT EXISTS ").flatMap(ifNotExists -> Stream.of("", " TABLESPACE MAIN", " STORAGE(ON MAIN)")
                        .flatMap(storage -> contextIndexLexerClauses().flatMap(lexer -> Stream.of("", " SYNC", " SYNC TRANSACTION")
                                .map(sync -> "CREATE CONTEXT INDEX " + ifNotExists + "I ON APP.T(V)" + storage + lexer + sync)))),
                Stream.of(
                        "CREATE CONTEXT INDEX I ON T(V)",
                        "create context index if not exists \"I; DROP TABLE APP.T\" on \"APP\".\"T\"(\"V\")"
                                + " storage(on main) lexer english_lexer sync transaction;"));
    }

    private static Stream<String> contextIndexLexerClauses() {
        return Stream.of("", " LEXER CHINESE_LEXER", " LEXER CHINESE_VGRAM_LEXER", " LEXER CHINESE_FP_LEXER",
                " LEXER ENGLISH_LEXER", " LEXER DEFAULT_LEXER");
    }

    static Stream<String> alterContextIndexStatements() {
        // LEXER is only supported with REBUILD, not INCREMENT or OPTIMIZE.
        return Stream.of(
                Stream.of("", " ONLINE").flatMap(online -> contextIndexLexerClauses()
                        .map(lexer -> "ALTER CONTEXT INDEX I ON APP.T REBUILD" + online + lexer)),
                Stream.of("INCREMENT", "OPTIMIZE").flatMap(action -> Stream.of("", " ONLINE")
                        .map(online -> "ALTER CONTEXT INDEX I ON APP.T " + action + online)),
                Stream.of(
                        "ALTER CONTEXT INDEX I ON T REBUILD",
                        "alter context index \"I; DROP TABLE APP.T\" on \"APP\".\"T\" rebuild online lexer chinese_lexer;"))
                .flatMap(statements -> statements);
    }

    static Stream<String> dropContextIndexStatements() {
        return Stream.concat(
                Stream.of("", "IF EXISTS ").flatMap(ifExists -> Stream.of("T", "APP.T")
                        .map(table -> "DROP CONTEXT INDEX " + ifExists + "I ON " + table)),
                Stream.of("drop context index if exists \"I; DROP TABLE APP.T\" on \"APP\".\"T\";"));
    }

    // Section 3.7.1 creates a BMJ$_ table, excluded by the connector's built-in internal-table filter.
    @ParameterizedTest
    @MethodSource("createBitmapJoinIndexStatements")
    void shouldSkipBitmapJoinIndexCreationWithoutChangingCapturedTables(String sql) {
        assertThat(DamengParseTree.parseScope(sql, false).tree().statement(0).nonTableStatement().createIndex()).isNotNull();
        assertStatementDoesNotChangeCapturedTables(sql);
    }

    static Stream<String> createBitmapJoinIndexStatements() {
        return Stream.concat(
                Stream.of("", "OR REPLACE ").flatMap(replacement -> Stream.of("", "IF NOT EXISTS ")
                        .flatMap(ifNotExists -> Stream.of(
                                " ON APP.T(APP.U.V) FROM APP.T, APP.U WHERE APP.T.ID = APP.U.ID",
                                " ON APP.T(U.V) FROM APP.T T, APP.U U WHERE T.ID = U.ID")
                                .flatMap(join -> Stream.of("", " TABLESPACE MAIN", " STORAGE(ON MAIN, SECTION(4))", " PARALLEL 2")
                                        .map(option -> "CREATE " + replacement + "BITMAP INDEX " + ifNotExists + "I" + join + option)))),
                Stream.of(
                        "CREATE OR REPLACE BITMAP INDEX IF NOT EXISTS I ON APP.T(U.V DESC) FROM APP.T T, APP.U U WHERE T.ID = U.ID"
                                + " TABLESPACE MAIN STORAGE(INITIAL 1, NEXT 1, MINEXTENTS 1, FILLFACTOR 80, WITH COUNTER, USING LONG ROW, SECTION(128))"
                                + " NOPARALLEL",
                        "create bitmap index \"I; DROP TABLE APP.T\" on \"APP\".\"T\"(U.V)"
                                + " from \"APP\".\"T\" T, \"APP\".\"U\" U where T.ID = U.ID;"));
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "CREATE OR REPLACE NOT PARTIAL UNIQUE INDEX IF NOT EXISTS I ON APP.T(ID ASC, V DESC) STORAGE(ON MAIN) PARALLEL 2",
            "CREATE CLUSTER INDEX I ON APP.T(V) STORAGE(BRANCH(32, 16))",
            "CREATE BITMAP INDEX I ON APP.T(V)",
            "CREATE BITMAP INDEX I ON APP.T(U.V) FROM APP.T T, APP.U U WHERE T.ID=U.ID",
            "CREATE INDEX I ON APP.T(TRUNCATE(ID, 1))",
            "CREATE INDEX I ON APP.T(APP.F(V, q'[FROM APP.U; DROP TABLE APP.T]'))",
            "CREATE INDEX I ON APP.T(ID) GLOBAL PARTITION BY RANGE(ID) (PARTITION P1 VALUES LESS THAN(MAXVALUE))",
            "CREATE INDEX I ON APP.T(V) REVERSE UNUSABLE",
            "ALTER INDEX I RENAME TO APP.J",
            "ALTER INDEX APP.\"I; DROP TABLE APP.T\" RENAME TO APP.\"J; ALTER TABLE APP.T ADD X INT\"",
            "ALTER INDEX APP.I REBUILD ONLINE PARALLEL 2",
            "ALTER INDEX APP.I REBUILD SHARE ASYNCHRONOUS 3",
            "ALTER INDEX APP.I INVISIBLE",
            "ALTER INDEX APP.I NOMONITORING USAGE",
            "DROP INDEX IF EXISTS APP.I",
            "CREATE OR REPLACE ARRAY INDEX IF NOT EXISTS I ON APP.ARRAY_T(V) STORAGE(ON MAIN) PARALLEL 2",
            "CREATE CONTEXT INDEX IF NOT EXISTS I ON APP.T(V) LEXER CHINESE_LEXER SYNC TRANSACTION",
            "ALTER CONTEXT INDEX I ON APP.T REBUILD ONLINE LEXER CHINESE_LEXER",
            "ALTER CONTEXT INDEX I ON APP.T INCREMENT ONLINE",
            "DROP CONTEXT INDEX IF EXISTS I ON APP.T"
    })
    void shouldNotSwallowTableChangesFollowingIndexManagement(String sql) {
        assertFollowingTableChangesAreNotSkipped(sql);
    }

    @ParameterizedTest
    @MethodSource({ "createSequenceStatements", "alterSequenceStatements", "dropSequenceStatements" })
    void shouldSkipSequenceManagementWithoutChangingCapturedTables(String sql) {
        assertStatementDoesNotChangeCapturedTables(sql);
    }

    // DM SQL Language Manual, sections 3.10.1-3.10.3:
    // https://eco.dameng.com/document/dm/zh-cn/pm/definition-statement.html
    static Stream<String> createSequenceStatements() {
        return Stream.of(
                Stream.of("", "IF NOT EXISTS ").flatMap(ifNotExists -> Stream.of("S", "APP.S")
                        .flatMap(name -> Stream.concat(sequenceOptionClauses(), Stream.of("", "START WITH 10", "GLOBAL", "LOCAL"))
                                .map(option -> "CREATE SEQUENCE " + ifNotExists + name + (option.isEmpty() ? "" : " " + option)))),
                sequenceOptionCombinations().flatMap(options -> Stream.of("GLOBAL", "LOCAL")
                        .map(scope -> "CREATE SEQUENCE IF NOT EXISTS APP.S START WITH 10 INCREMENT BY 2 " + options + " " + scope)),
                Stream.of(
                        "CREATE SEQUENCE APP.S INCREMENT BY -2 START WITH -10 MAXVALUE -1 MINVALUE -1000 CYCLE CACHE 10 NOORDER",
                        "CREATE SEQUENCE APP.S INCREMENT BY -1 START WITH -1 NOMAXVALUE NOMINVALUE NOCYCLE NOCACHE",
                        "CREATE SEQUENCE APP.S MINVALUE -9223372036854775808 MAXVALUE 9223372036854775807 START WITH 0 INCREMENT BY 1 NOCACHE",
                        "CREATE SEQUENCE APP.S LOCAL MINVALUE -9007199254740992 MAXVALUE 9007199254740991 START WITH 0 NOCACHE",
                        "CREATE SEQUENCE APP.S CACHE 2147483647",
                        "CREATE SEQUENCE APP.S NOORDER NOCACHE NOCYCLE NOMINVALUE NOMAXVALUE START WITH 10 INCREMENT BY 2 GLOBAL",
                        "create /* sequence creation */ sequence if not exists \"APP\".\"S; DROP TABLE APP.T\""
                                + " increment by 2 start with 10 minvalue 1 maxvalue 1000 cycle cache 10 order;"))
                .flatMap(statements -> statements);
    }

    private static Stream<String> sequenceOptionClauses() {
        return Stream.of("INCREMENT BY 1", "INCREMENT BY -1", "MAXVALUE 1000", "NOMAXVALUE", "MINVALUE 0", "NOMINVALUE",
                "CYCLE", "NOCYCLE", "CACHE 2", "CACHE 20", "NOCACHE", "ORDER", "NOORDER");
    }

    private static Stream<String> sequenceOptionCombinations() {
        return Stream.of("MAXVALUE 1000", "NOMAXVALUE")
                .flatMap(maximum -> Stream.of("MINVALUE 1", "NOMINVALUE")
                        .flatMap(minimum -> Stream.of("CYCLE", "NOCYCLE")
                                .flatMap(cycle -> Stream.of("CACHE 10", "NOCACHE")
                                        .flatMap(cache -> Stream.of("ORDER", "NOORDER")
                                                .map(order -> maximum + " " + minimum + " " + cycle + " " + cache + " " + order)))));
    }

    // ALTER does not change START WITH or GLOBAL/LOCAL; RENAME TO cannot be combined with other options.
    static Stream<String> alterSequenceStatements() {
        return Stream.of(
                Stream.of("S", "APP.S").flatMap(name -> Stream.concat(sequenceOptionClauses(),
                        Stream.of("CURRENT VALUE 100", "CURRENT VALUE -10", "RENAME TO NEW_S"))
                        .map(option -> "ALTER SEQUENCE " + name + " " + option)),
                sequenceOptionCombinations().map(options -> "ALTER SEQUENCE APP.S INCREMENT BY 2 " + options + " CURRENT VALUE 100"),
                Stream.of(
                        "ALTER SEQUENCE APP.S INCREMENT BY -2 MAXVALUE -1 MINVALUE -1000 CYCLE CACHE 10 NOORDER CURRENT VALUE -10",
                        "ALTER SEQUENCE APP.S CURRENT VALUE 0 MINVALUE -9223372036854775808 MAXVALUE 9223372036854775807 NOCACHE",
                        "ALTER SEQUENCE APP.S CACHE 2147483647",
                        "ALTER SEQUENCE APP.S CURRENT VALUE 100 NOORDER NOCACHE NOCYCLE NOMINVALUE NOMAXVALUE INCREMENT BY 2",
                        "ALTER SEQUENCE \"APP\".\"序列\" RENAME TO \"新序列\";",
                        "alter /* sequence rename */ sequence \"APP\".\"S; DROP TABLE APP.T\" rename to \"S; ALTER TABLE APP.T ADD X INT\";"))
                .flatMap(statements -> statements);
    }

    static Stream<String> dropSequenceStatements() {
        return Stream.concat(
                Stream.of("", "IF EXISTS ").flatMap(ifExists -> Stream.of("S", "APP.S")
                        .map(name -> "DROP SEQUENCE " + ifExists + name)),
                Stream.of(
                        "DROP SEQUENCE \"APP\".\"序列\";",
                        "drop /* sequence removal */ sequence if exists \"APP\".\"S; DROP TABLE APP.T\";"));
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "CREATE SEQUENCE S",
            "CREATE SEQUENCE IF NOT EXISTS APP.S START WITH 10 INCREMENT BY 2 MINVALUE 1 MAXVALUE 1000 CYCLE CACHE 10 ORDER GLOBAL",
            "CREATE SEQUENCE APP.S INCREMENT BY -1 NOMAXVALUE NOMINVALUE NOCYCLE NOCACHE NOORDER LOCAL",
            "CREATE SEQUENCE APP.\"S; DROP TABLE APP.T\" CACHE 20",
            "ALTER SEQUENCE APP.S INCREMENT BY -2 MAXVALUE -1 MINVALUE -1000 NOCYCLE NOCACHE NOORDER CURRENT VALUE -10",
            "ALTER SEQUENCE APP.S CURRENT VALUE 100",
            "ALTER SEQUENCE S RENAME TO NEW_S",
            "ALTER SEQUENCE APP.\"S; DROP TABLE APP.T\" RENAME TO \"S; ALTER TABLE APP.T ADD X INT\"",
            "DROP SEQUENCE S",
            "DROP SEQUENCE IF EXISTS APP.S"
    })
    void shouldNotSwallowTableChangesFollowingSequenceManagement(String sql) {
        assertFollowingTableChangesAreNotSkipped(sql);
    }

    // NEXTVAL/CURRVAL are expression pseudocolumns, not DDL commands. SEQ MODIFY (37) redo is
    // discarded by LogMinerTransactionBuffer; row changes using sequence values still follow the DML path.
    @ParameterizedTest
    @MethodSource("sequenceValueQueriesAndDml")
    void shouldKeepSequenceValueQueriesAndDmlOutsideSkippableDdl(String sql) {
        assertUnsupported(sql);
    }

    static Stream<String> sequenceValueQueriesAndDml() {
        return Stream.of("NEXTVAL", "CURRVAL").flatMap(pseudocolumn -> Stream.of("S", "APP.S", "\"APP\".\"S; DROP TABLE APP.T\"")
                .flatMap(name -> Stream.of(
                        "SELECT " + name + "." + pseudocolumn + " FROM DUAL",
                        "INSERT INTO APP.T(ID) VALUES(" + name + "." + pseudocolumn + ")",
                        "UPDATE APP.T SET ID = " + name + "." + pseudocolumn + " WHERE ID = 1")));
    }

    @ParameterizedTest
    @ValueSource(strings = { "NEXTVAL", "CURRVAL" })
    void shouldKeepTableCreationUsingSequenceValuesInTableScope(String pseudocolumn) {
        final var sql = "CREATE TABLE APP.T AS SELECT APP.S." + pseudocolumn + " AS ID FROM DUAL";
        final var analysis = analyzer.analyze(sql, "APP").requireSupported();

        assertThat(analysis.impact()).isEqualTo(Impact.TABLE);
        assertThat(analysis.tableId()).isEqualTo(new TableId(null, "APP", "T"));
        assertThat(analysis.tableStatement().createTable()).isNotNull();
    }

    @ParameterizedTest
    @MethodSource({ "createSynonymStatements", "dropSynonymStatements" })
    void shouldSkipSynonymManagementWithoutChangingCapturedTables(String sql) {
        assertStatementDoesNotChangeCapturedTables(sql);
    }

    // DM SQL Language Manual, sections 15.1-15.2:
    // https://eco.dameng.com/document/dm/zh-cn/pm/synonym.html
    // A PUBLIC synonym cannot have a schema-qualified name; its target may be schema-qualified.
    // Section 15.1 permits remote targets; object@link notation follows the external-link chapter:
    // https://eco.dameng.com/document/dm/zh-cn/pm/external-link.html
    static Stream<String> createSynonymStatements() {
        return Stream.concat(
                Stream.of("", "OR REPLACE ").flatMap(replacement -> Stream.of("", "PUBLIC ")
                        .flatMap(visibility -> Stream.of("", "IF NOT EXISTS ")
                                .flatMap(ifNotExists -> (visibility.isEmpty() ? Stream.of("S", "APP.S") : Stream.of("S"))
                                        .flatMap(name -> Stream.of("T", "APP.T", "T@REMOTE_LINK", "OTHER.T@REMOTE_LINK")
                                                .map(target -> "CREATE " + replacement + visibility + "SYNONYM " + ifNotExists + name + " FOR " + target))))),
                Stream.of(
                        "CREATE SYNONYM APP.S FOR OTHER.SOURCE_SYNONYM",
                        "CREATE SYNONYM APP.S FOR APP.MISSING_OBJECT",
                        "CREATE OR REPLACE SYNONYM APP.S FOR APP.U",
                        "CREATE OR REPLACE PUBLIC SYNONYM S FOR OTHER.U",
                        "CREATE SYNONYM \"APP\".\"同义词\" FOR \"OTHER\".\"目标表\";",
                        "CREATE PUBLIC SYNONYM \"公共同义词\" FOR \"APP\".\"T\";",
                        "CREATE OR REPLACE SYNONYM APP.\"DROP\" FOR APP.\"ALTER\"",
                        "create /* synonym creation */ or replace synonym if not exists \"APP\".\"S; DROP TABLE APP.T\""
                                + " for \"OTHER\".\"T; ALTER TABLE APP.T ADD X INT\";",
                        "create or replace public synonym if not exists \"S; DROP TABLE APP.T\""
                                + " for \"OTHER\".\"T\"@\"LINK; TRUNCATE TABLE APP.T\";"));
    }

    static Stream<String> dropSynonymStatements() {
        return Stream.concat(
                Stream.of("", "PUBLIC ").flatMap(visibility -> Stream.of("", "IF EXISTS ")
                        .flatMap(ifExists -> (visibility.isEmpty() ? Stream.of("S", "APP.S") : Stream.of("S"))
                                .map(name -> "DROP " + visibility + "SYNONYM " + ifExists + name))),
                Stream.of(
                        "DROP SYNONYM \"APP\".\"同义词\";",
                        "DROP PUBLIC SYNONYM \"公共同义词\";",
                        "drop /* synonym removal */ synonym if exists \"APP\".\"S; DROP TABLE APP.T\";",
                        "drop public synonym if exists \"S; ALTER TABLE APP.T ADD X INT\";"));
    }

    // Chapter 15 documents replacement through CREATE OR REPLACE, not a standalone ALTER SYNONYM.
    @ParameterizedTest
    @ValueSource(strings = {
            "ALTER SYNONYM APP.S COMPILE",
            "ALTER PUBLIC SYNONYM S COMPILE",
            "ALTER SYNONYM APP.S FOR APP.U",
            "ALTER SYNONYM APP.S RENAME TO NEW_S"
    })
    void shouldKeepUndocumentedSynonymAlterationOutsideSkippableDdl(String sql) {
        assertUnsupported(sql);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "CREATE SYNONYM S FOR T",
            "CREATE SYNONYM IF NOT EXISTS APP.S FOR APP.T",
            "CREATE OR REPLACE SYNONYM IF NOT EXISTS APP.S FOR APP.U",
            "CREATE OR REPLACE PUBLIC SYNONYM IF NOT EXISTS S FOR APP.T",
            "CREATE SYNONYM APP.S FOR OTHER.T@REMOTE_LINK",
            "CREATE PUBLIC SYNONYM S FOR T@REMOTE_LINK",
            "CREATE OR REPLACE SYNONYM APP.\"S; DROP TABLE APP.T\" FOR OTHER.\"T; ALTER TABLE APP.T ADD X INT\"",
            "CREATE PUBLIC SYNONYM S FOR OTHER.T@\"LINK; TRUNCATE TABLE APP.T\"",
            "DROP SYNONYM S",
            "DROP SYNONYM IF EXISTS APP.S",
            "DROP PUBLIC SYNONYM IF EXISTS S",
            "DROP PUBLIC SYNONYM IF EXISTS \"S; DROP TABLE APP.T\""
    })
    void shouldNotSwallowTableChangesFollowingSynonymManagement(String sql) {
        assertFollowingTableChangesAreNotSkipped(sql);
    }

    @ParameterizedTest
    @MethodSource({ "createFunctionStatements", "createPipelinedFunctionStatements", "createAggregateFunctionStatements",
            "createExternalFunctionStatements", "alterFunctionStatements", "dropFunctionStatements" })
    void shouldSkipFunctionManagementWithoutChangingCapturedTables(String sql) {
        assertStatementDoesNotChangeCapturedTables(sql);
    }

    // DM SQL Programming Manual, sections 3.2, 3.4 and 3.6-3.8:
    // https://eco.dameng.com/document/dm/zh-cn/pm/dm8_sql-definition-deletion.html
    // Replacement uses CREATE OR REPLACE; it may coexist with IF NOT EXISTS and takes precedence.
    static Stream<String> createFunctionStatements() {
        return Stream.of(
                functionCreationPrefixes().flatMap(prefix -> Stream.of("F", "APP.F")
                        .flatMap(name -> Stream.of("", " WITH ENCRYPTION")
                                .flatMap(encryption -> Stream.of("", " FOR CALCULATE")
                                        .flatMap(calculate -> functionCallOptionClauses()
                                                .flatMap(options -> Stream.of("AS", "IS")
                                                        .map(bodyKeyword -> prefix + name + encryption + calculate + " RETURN INT" + options
                                                                + " " + bodyKeyword + " BEGIN RETURN 1; END;")))))),
                Stream.of("", "()", "(A INT)", "(A IN INT)", "(A OUT INT)", "(A IN OUT INT)", "(A OUT IN INT)",
                        "(A INT DEFAULT 1)", "(A IN INT := 1)", "(A VARCHAR(30) DEFAULT 'END; DROP TABLE APP.T', B INT := 2)")
                        .flatMap(parameters -> Stream.of("AS", "IS")
                                .flatMap(bodyKeyword -> Stream.of("", "DECLARE ")
                                        .map(declaration -> "CREATE OR REPLACE FUNCTION IF NOT EXISTS APP.F" + parameters
                                                + " RETURN INT " + bodyKeyword + " " + declaration + "N INT := 1; BEGIN RETURN N; END F;"))),
                Stream.of(
                        "CREATE FUNCTION APP.F(A INT, B INT) RETURN INT AS N INT;"
                                + " BEGIN N := A + B; RETURN N; EXCEPTION WHEN OTHERS THEN RETURN NULL; END F;",
                        "CREATE FUNCTION APP.F RETURN INT IS V APP.T.ID%TYPE; BEGIN RETURN V; END;",
                        "CREATE FUNCTION APP.F(C OUT CURSOR) RETURN INT AS BEGIN OPEN C FOR SELECT ID FROM APP.T; RETURN 1; END;",
                        "CREATE FUNCTION APP.F RETURN INT AS FUNCTION G RETURN INT IS BEGIN RETURN 1; END G;"
                                + " BEGIN RETURN G(); END F;",
                        "CREATE FUNCTION APP.F RETURN INT AS BEGIN BEGIN NULL; END;"
                                + " IF 1=1 THEN FOR I IN 1..2 LOOP NULL; END LOOP; END IF;"
                                + " RETURN CASE WHEN 1=1 THEN 1 ELSE 0 END; END F;",
                        // Creating a function stores its body; the DML and dynamic DDL execute only on invocation.
                        "CREATE FUNCTION APP.F RETURN INT AS BEGIN INSERT INTO APP.T(ID) VALUES(2);"
                                + " UPDATE APP.T SET V='changed' WHERE ID=2; DELETE FROM APP.T WHERE ID=2; RETURN 1; END;",
                        "CREATE OR REPLACE FUNCTION APP.F RETURN INT AS BEGIN"
                                + " EXECUTE IMMEDIATE q'[ALTER TABLE APP.T ADD X INT]'; RETURN 1; END F;",
                        "create /* function definition */ or replace function if not exists \"APP\".\"函数; DROP TABLE APP.T\""
                                + " with encryption return varchar(50) authid current_user deterministic is"
                                + " begin return q'[END; ALTER TABLE APP.T ADD X INT]'; end \"函数; DROP TABLE APP.T\";"))
                .flatMap(statements -> statements);
    }

    private static Stream<String> functionCreationPrefixes() {
        return Stream.of("", "OR REPLACE ").flatMap(replacement -> Stream.of("", "IF NOT EXISTS ")
                .map(ifNotExists -> "CREATE " + replacement + "FUNCTION " + ifNotExists));
    }

    private static Stream<String> functionCallOptionClauses() {
        return Stream.of("", " DETERMINISTIC", " AUTHID DEFINER", " AUTHID CURRENT_USER",
                " AUTHID DEFINER DETERMINISTIC", " AUTHID CURRENT_USER DETERMINISTIC",
                " DETERMINISTIC AUTHID DEFINER", " DETERMINISTIC AUTHID CURRENT_USER");
    }

    // PIPELINED returns a collection and emits rows through PIPE ROW, rather than returning a scalar.
    static Stream<String> createPipelinedFunctionStatements() {
        return functionCreationPrefixes().flatMap(prefix -> functionCallOptionClauses()
                .flatMap(options -> Stream.of("AS", "IS")
                        .map(bodyKeyword -> prefix + "APP.F RETURN APP.INT_LIST" + options + " PIPELINED " + bodyKeyword
                                + " BEGIN PIPE ROW(1); RETURN; END F;")));
    }

    // DM SQL Language Manual, section 21.1.2. Aggregate functions do not support IF NOT EXISTS.
    // https://eco.dameng.com/document/dm/zh-cn/pm/Custom-functions.html
    static Stream<String> createAggregateFunctionStatements() {
        return Stream.of("", "OR REPLACE ").flatMap(replacement -> Stream.of("F", "APP.F")
                .flatMap(name -> Stream.of("", "PARALLEL_ENABLE ", "DETERMINISTIC ",
                        "PARALLEL_ENABLE DETERMINISTIC ", "DETERMINISTIC PARALLEL_ENABLE ")
                        .flatMap(options -> Stream.of("MY_MAX_IMPL", "APP.MY_MAX_IMPL")
                                .map(implementation -> "CREATE " + replacement + "FUNCTION " + name
                                        + "(INPUT NUMBER) RETURN NUMBER " + options + "AGGREGATE USING " + implementation))));
    }

    // DM SQL Programming Manual, sections 10.1.2 and 10.2.2:
    // https://eco.dameng.com/document/dm/zh-cn/pm/external-function.html
    static Stream<String> createExternalFunctionStatements() {
        return functionCreationPrefixes().flatMap(prefix -> Stream.of(
                Stream.of("C", "CS").flatMap(language -> Stream.of("", "AND ")
                        .flatMap(and -> Stream.of("", "NATIVE_F ", "\"native_f\" ")
                                .map(reference -> prefix + "APP.F(A INT) RETURN INT EXTERNAL '/tmp/libfunction.so' "
                                        + and + reference + "USING " + language))),
                Stream.of("C", "CS").flatMap(language -> Stream.of("CODE", "APP.CODE")
                        .flatMap(library -> Stream.of("", " NATIVE_F", " \"native_f\"")
                                .map(reference -> prefix + "APP.F(A INT) RETURN INT AS LANGUAGE " + language
                                        + " LIBRARY " + library + " NAME" + reference))),
                Stream.of("", "AND ").flatMap(and -> Stream.of("\"com.example.Functions.run\"", "\"com.example.Functions.run(int)\"")
                        .map(reference -> prefix + "APP.F(A INT) RETURN INT EXTERNAL '/tmp/function.jar' "
                                + and + reference + " USING JAVA")),
                Stream.of(
                        prefix + "APP.F RETURN INT EXTERNAL '/tmp/lib; DROP TABLE APP.T.so' AND 'native_f' USING C;",
                        prefix + "APP.F(A INT) RETURN INT EXTERNAL '/tmp/function.jar' AND 'com.example.Functions.run(int)' USING JAVA;"))
                .flatMap(statements -> statements));
    }

    // Section 3.8.2 describes CASCADE in prose but omits it from the displayed syntax.
    // These cases verify its DDL scope; they do not establish database-version support.
    static Stream<String> alterFunctionStatements() {
        return Stream.concat(
                Stream.of("F", "APP.F").flatMap(name -> Stream.of("", " CASCADE")
                        .flatMap(cascade -> Stream.of("", " DEBUG")
                                .map(debug -> "ALTER FUNCTION " + name + " COMPILE" + cascade + debug))),
                Stream.of(
                        "ALTER FUNCTION \"APP\".\"函数\" COMPILE;",
                        "alter /* function recompilation */ function \"APP\".\"F; DROP TABLE APP.T\" compile debug;"));
    }

    static Stream<String> dropFunctionStatements() {
        return Stream.concat(
                Stream.of("", "IF EXISTS ").flatMap(ifExists -> Stream.of("F", "APP.F")
                        .map(name -> "DROP FUNCTION " + ifExists + name)),
                Stream.of(
                        "DROP FUNCTION \"APP\".\"函数\";",
                        "drop /* function removal */ function if exists \"APP\".\"F; DROP TABLE APP.T\";"));
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "CREATE FUNCTION F RETURN INT AS BEGIN RETURN 1; END",
            "CREATE OR REPLACE FUNCTION IF NOT EXISTS APP.F WITH ENCRYPTION FOR CALCULATE RETURN INT"
                    + " AUTHID CURRENT_USER DETERMINISTIC IS BEGIN RETURN 1; END F",
            "CREATE FUNCTION APP.F RETURN APP.INT_LIST PIPELINED AS BEGIN PIPE ROW(1); RETURN; END",
            "CREATE FUNCTION APP.F RETURN INT AS BEGIN EXECUTE IMMEDIATE q'[ALTER TABLE APP.T ADD X INT]'; RETURN 1; END",
            "CREATE FUNCTION APP.F RETURN INT AS FUNCTION G RETURN INT AS BEGIN RETURN 1; END; BEGIN RETURN G(); END F",
            "CREATE FUNCTION APP.\"F; DROP TABLE APP.T\" RETURN VARCHAR(50) AS BEGIN RETURN 'END; DROP TABLE APP.T'; END",
            "CREATE OR REPLACE FUNCTION APP.F(INPUT NUMBER) RETURN NUMBER DETERMINISTIC PARALLEL_ENABLE AGGREGATE USING APP.MY_MAX_IMPL",
            "CREATE FUNCTION APP.F(A INT) RETURN INT EXTERNAL '/tmp/function.so' AND 'native_f' USING C",
            "CREATE FUNCTION APP.F(A INT) RETURN INT AS LANGUAGE CS LIBRARY APP.CODE NAME \"native_f\"",
            "CREATE FUNCTION APP.F(A INT) RETURN INT EXTERNAL '/tmp/function.jar' AND 'com.example.Functions.run(int)' USING JAVA",
            "ALTER FUNCTION F COMPILE",
            "ALTER FUNCTION APP.F COMPILE DEBUG",
            "ALTER FUNCTION APP.F COMPILE CASCADE DEBUG",
            "DROP FUNCTION F",
            "DROP FUNCTION IF EXISTS APP.F"
    })
    void shouldNotSwallowTableChangesFollowingFunctionManagement(String sql) {
        assertFollowingTableChangesAreNotSkipped(sql);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "REPLACE FUNCTION APP.F RETURN INT AS BEGIN RETURN 1; END;",
            "CREATE FUNCTION APP.F RETURN INT",
            "CREATE FUNCTION APP.F RETURN INT AS BEGIN RETURN 1;",
            "CREATE FUNCTION APP.F RETURN INT AS FUNCTION G RETURN INT AS BEGIN RETURN 1; END;",
            "CREATE FUNCTION APP.F RETURN NUMBER AGGREGATE USING",
            "CREATE FUNCTION APP.F RETURN INT EXTERNAL '/tmp/function.so' USING",
            "CREATE FUNCTION APP.F RETURN INT AS LANGUAGE CS LIBRARY",
            "CREATE FUNCTION APP.F RETURN INT AS BEGIN RETURN 1; END; CALL APP.F()",
            "CREATE FUNCTION APP.F RETURN INT AS BEGIN RETURN 1; END; SELECT APP.F() FROM DUAL"
    })
    void shouldRejectIncompleteOrCompoundFunctionDefinitions(String sql) {
        assertUnsupported(sql);
    }

    // Section 3.8.1: CALL, direct invocation and SELECT execute the function body. They are not
    // skippable DDL; any row changes produced by invocation must still follow the CDC DML path.
    @ParameterizedTest
    @MethodSource("functionInvocationsAndDml")
    void shouldKeepFunctionInvocationsAndDmlOutsideSkippableDdl(String sql) {
        assertUnsupported(sql);
    }

    static Stream<String> functionInvocationsAndDml() {
        return Stream.of("F", "APP.F", "\"APP\".\"F; DROP TABLE APP.T\"").flatMap(name -> Stream.of(
                "CALL " + name,
                "CALL " + name + "()",
                "CALL " + name + "(1)",
                name + "(1)",
                "SELECT " + name + "(1)",
                "SELECT " + name + "(1) FROM DUAL",
                "INSERT INTO APP.T(ID) VALUES(" + name + "(1))",
                "UPDATE APP.T SET ID = " + name + "(1) WHERE ID = 1"));
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "CREATE TABLE APP.T(ID INT DEFAULT APP.F())",
            "CREATE TABLE APP.T AS SELECT APP.F(1) AS ID FROM DUAL"
    })
    void shouldKeepTableCreationUsingFunctionsInTableScope(String sql) {
        final var analysis = analyzer.analyze(sql, "APP").requireSupported();

        assertThat(analysis.impact()).isEqualTo(Impact.TABLE);
        assertThat(analysis.tableId()).isEqualTo(new TableId(null, "APP", "T"));
        assertThat(analysis.tableStatement().createTable()).isNotNull();
    }

    @ParameterizedTest
    @MethodSource({ "createProcedureStatements", "alterProcedureStatements", "dropProcedureStatements" })
    void shouldSkipProcedureManagementWithoutChangingCapturedTables(String sql) {
        assertStatementDoesNotChangeCapturedTables(sql);
    }

    // DM SQL Programming Manual, sections 3.1 and 3.4-3.8:
    // https://eco.dameng.com/document/dm/zh-cn/pm/dm8_sql-definition-deletion.html
    // Replacement uses CREATE OR REPLACE; it takes precedence when combined with IF NOT EXISTS.
    static Stream<String> createProcedureStatements() {
        return Stream.of(
                Stream.of("", "OR REPLACE ").flatMap(replacement -> Stream.of("", "IF NOT EXISTS ")
                        .flatMap(ifNotExists -> Stream.of("P", "APP.P")
                                .flatMap(name -> Stream.of("", " WITH ENCRYPTION")
                                        .flatMap(encryption -> Stream.of("", " AUTHID DEFINER", " AUTHID CURRENT_USER")
                                                .flatMap(authid -> Stream.of("AS", "IS")
                                                        .map(bodyKeyword -> "CREATE " + replacement + "PROCEDURE " + ifNotExists
                                                                + name + encryption + authid + " " + bodyKeyword + " BEGIN NULL; END;")))))),
                Stream.of("", "()", "(A INT)", "(A IN INT)", "(A OUT INT)", "(A IN OUT INT)", "(A OUT IN INT)",
                        "(A INT DEFAULT 1)", "(A IN INT DEFAULT 1)", "(A INT := 1)", "(A IN INT := 1)",
                        "(A VARCHAR(30) DEFAULT 'END; DROP TABLE APP.T', B INT := 2)",
                        "(A IN INT, B OUT INT, C IN OUT INT, D OUT IN INT)")
                        .flatMap(parameters -> Stream.of("AS", "IS")
                                .flatMap(bodyKeyword -> Stream.of("", "DECLARE ")
                                        .flatMap(declaration -> Stream.of("", " P")
                                                .map(endName -> "CREATE OR REPLACE PROCEDURE IF NOT EXISTS APP.P WITH ENCRYPTION" + parameters
                                                        + " AUTHID CURRENT_USER " + bodyKeyword + " " + declaration
                                                        + "N INT := 1; BEGIN PRINT N; END" + endName + ";")))),
                // Section 3.5 allows DEFAULT, ASSIGN and := for variable initialization.
                Stream.of("DEFAULT", "ASSIGN", ":=").flatMap(initializer -> Stream.of("", "DECLARE ")
                        .map(declaration -> "CREATE PROCEDURE APP.P IS " + declaration + "N CONSTANT INT NOT NULL "
                                + initializer + " 1; BEGIN PRINT N; END P;")),
                Stream.of(
                        "CREATE PROCEDURE APP.P(A IN OUT INT) AS N INT := 10;"
                                + " BEGIN A := A + N; PRINT A; EXCEPTION WHEN OTHERS THEN NULL; END P;",
                        "CREATE PROCEDURE APP.P AS V APP.T.ID%TYPE; R APP.T%ROWTYPE;"
                                + " BEGIN SELECT ID INTO V FROM APP.T WHERE ID=1; END;",
                        "CREATE PROCEDURE APP.P AS CURSOR C FOR SELECT ID FROM APP.T; V INT;"
                                + " BEGIN OPEN C; LOOP FETCH C INTO V; EXIT WHEN C%NOTFOUND; END LOOP; CLOSE C; END P;",
                        "CREATE PROCEDURE APP.P(C OUT CURSOR) AS BEGIN OPEN C FOR SELECT ID FROM APP.T; END;",
                        "CREATE PROCEDURE APP.P AS PROCEDURE NESTED_P IS BEGIN NULL; END NESTED_P;"
                                + " FUNCTION NESTED_F RETURN INT AS BEGIN RETURN 1; END NESTED_F;"
                                + " BEGIN CALL NESTED_P(); PRINT NESTED_F(); END P;",
                        "CREATE PROCEDURE APP.P IS BEGIN BEGIN NULL; END;"
                                + " IF 1=1 THEN FOR I IN 1..2 LOOP NULL; END LOOP; ELSE NULL; END IF;"
                                + " CASE WHEN 1=1 THEN NULL; ELSE NULL; END CASE; END P;",
                        // Creating a procedure stores its body; the body is not executed during DDL analysis.
                        "CREATE PROCEDURE APP.P AS BEGIN INSERT INTO APP.T(ID) VALUES(2);"
                                + " UPDATE APP.T SET V='changed' WHERE ID=2; DELETE FROM APP.T WHERE ID=2; COMMIT; END;",
                        "CREATE PROCEDURE APP.P AS BEGIN SAVEPOINT S; UPDATE APP.T SET V='changed'; ROLLBACK TO S; END;",
                        // Dynamic SQL syntax: section 5.3, pm/dm8_sql-sql-statement.html.
                        "CREATE PROCEDURE APP.P(A IN INT) AS BEGIN"
                                + " EXECUTE IMMEDIATE 'INSERT INTO APP.T(ID) VALUES(?)' USING A; END;",
                        "CREATE OR REPLACE PROCEDURE APP.P AS BEGIN"
                                + " EXECUTE IMMEDIATE q'[ALTER TABLE APP.T ADD X INT]'; END P;",
                        "CREATE PROCEDURE APP.P AS BEGIN CALL APP.OTHER_P(); END P;",
                        "CREATE PROCEDURE \"APP\".\"过程\" AS BEGIN NULL; END \"过程\";",
                        "create /* procedure definition */ or replace procedure if not exists \"APP\".\"P; DROP TABLE APP.T\""
                                + " with encryption(a varchar(50) default q'[END; ALTER TABLE APP.T ADD X INT]') authid definer is"
                                + " begin print a; end \"P; DROP TABLE APP.T\";"))
                .flatMap(statements -> statements);
    }

    // Section 3.8.2 describes CASCADE in prose but omits it from the displayed syntax.
    // These cases verify its DDL scope; they do not establish database-version support.
    static Stream<String> alterProcedureStatements() {
        return Stream.concat(
                Stream.of("P", "APP.P").flatMap(name -> Stream.of("", " CASCADE")
                        .flatMap(cascade -> Stream.of("", " DEBUG")
                                .map(debug -> "ALTER PROCEDURE " + name + " COMPILE" + cascade + debug))),
                Stream.of(
                        "ALTER PROCEDURE \"APP\".\"过程\" COMPILE;",
                        "alter /* procedure recompilation */ procedure \"APP\".\"P; DROP TABLE APP.T\" compile debug;"));
    }

    static Stream<String> dropProcedureStatements() {
        return Stream.concat(
                Stream.of("", "IF EXISTS ").flatMap(ifExists -> Stream.of("P", "APP.P")
                        .map(name -> "DROP PROCEDURE " + ifExists + name)),
                Stream.of(
                        "DROP PROCEDURE \"APP\".\"过程\";",
                        "drop /* procedure removal */ procedure if exists \"APP\".\"P; DROP TABLE APP.T\";"));
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "CREATE PROCEDURE P AS BEGIN NULL; END",
            "CREATE PROCEDURE APP.P() IS BEGIN NULL; END P",
            "CREATE OR REPLACE PROCEDURE IF NOT EXISTS APP.P WITH ENCRYPTION(A IN INT DEFAULT 1, B OUT INT)"
                    + " AUTHID CURRENT_USER IS DECLARE N INT := 1; BEGIN B := A + N; END P",
            "CREATE PROCEDURE APP.P AS PROCEDURE NESTED_P AS BEGIN NULL; END; BEGIN NESTED_P(); END P",
            "CREATE PROCEDURE APP.P AS BEGIN IF 1=1 THEN FOR I IN 1..2 LOOP NULL; END LOOP; END IF; END P",
            "CREATE PROCEDURE APP.P AS BEGIN EXECUTE IMMEDIATE q'[ALTER TABLE APP.T ADD X INT]'; END",
            "CREATE PROCEDURE APP.\"P; DROP TABLE APP.T\" AS BEGIN PRINT 'END; DROP TABLE APP.T'; END",
            "ALTER PROCEDURE P COMPILE",
            "ALTER PROCEDURE APP.P COMPILE DEBUG",
            "ALTER PROCEDURE APP.P COMPILE CASCADE DEBUG",
            "DROP PROCEDURE P",
            "DROP PROCEDURE IF EXISTS APP.P"
    })
    void shouldNotSwallowTableChangesFollowingProcedureManagement(String sql) {
        assertFollowingTableChangesAreNotSkipped(sql);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "REPLACE PROCEDURE APP.P AS BEGIN NULL; END;",
            "CREATE PROCEDURE APP.P",
            "CREATE PROCEDURE APP.P AS BEGIN NULL;",
            "CREATE PROCEDURE APP.P(A IN) AS BEGIN NULL; END;",
            "CREATE PROCEDURE APP.P AUTHID AS BEGIN NULL; END;",
            "CREATE PROCEDURE APP.P AS N INT;",
            "CREATE PROCEDURE APP.P AS PROCEDURE NESTED_P AS BEGIN NULL; END;",
            "CREATE PROCEDURE APP.P AS BEGIN NULL; END; CALL APP.P()",
            "CREATE PROCEDURE APP.P AS BEGIN NULL; END; EXEC APP.P(1)",
            "CREATE PROCEDURE APP.P AS BEGIN NULL; END; EXECUTE APP.P(1)",
            "CREATE PROCEDURE APP.P AS BEGIN NULL; END; INSERT INTO APP.T(ID) VALUES(1)",
            "CREATE PROCEDURE APP.P AS BEGIN NULL; END; UPDATE APP.T SET V='changed'",
            "CREATE PROCEDURE APP.P AS BEGIN NULL; END; DELETE FROM APP.T"
    })
    void shouldRejectIncompleteOrCompoundProcedureDefinitions(String sql) {
        assertUnsupported(sql);
    }

    // Section 3.8.1 documents CALL and direct invocation. Execution is not management DDL;
    // its actual row changes follow the CDC DML path. EXEC/EXECUTE inputs also stay outside
    // skippable DDL, without asserting that every spelling is valid native server syntax.
    @ParameterizedTest
    @MethodSource("procedureInvocations")
    void shouldKeepProcedureInvocationsOutsideSkippableDdl(String sql) {
        assertUnsupported(sql);
    }

    static Stream<String> procedureInvocations() {
        return Stream.concat(
                Stream.of("CALL ", "", "EXEC ", "EXECUTE ").flatMap(invocation -> Stream.of("P", "APP.P", "\"APP\".\"P; DROP TABLE APP.T\"")
                        .flatMap(name -> Stream.of("", "()", "(1)")
                                .map(arguments -> invocation + name + arguments))),
                Stream.of(
                        "BEGIN APP.P(1); END;",
                        "BEGIN CALL APP.P(1); END;",
                        "DECLARE N INT := 1; BEGIN APP.P(N); END;",
                        "BEGIN EXECUTE IMMEDIATE 'CALL APP.P(?)' USING 1; END;"));
    }

    @ParameterizedTest
    @MethodSource({ "createTableTriggerStatements", "createInsteadOfTriggerStatements", "createEventTriggerStatements",
            "createTimerTriggerStatements", "alterTriggerStatements", "dropTriggerStatements" })
    void shouldSkipTriggerManagementWithoutChangingCapturedTables(String sql) {
        assertStatementDoesNotChangeCapturedTables(sql);
    }

    // DM SQL Language Manual, sections 14.1-14.6:
    // https://eco.dameng.com/document/dm/zh-cn/pm/trigger.html
    // Replacement uses CREATE OR REPLACE. CREATE TRIGGER does not document IF NOT EXISTS.
    private static Stream<String> triggerCreationPrefixes() {
        return Stream.of("", "OR REPLACE ").flatMap(replacement -> Stream.of("TR", "APP.TR")
                .flatMap(name -> Stream.of("", " WITH ENCRYPTION")
                        .map(encryption -> "CREATE " + replacement + "TRIGGER " + name + encryption + " ")));
    }

    static Stream<String> createTableTriggerStatements() {
        return Stream.of(
                triggerCreationPrefixes().flatMap(prefix -> Stream.of("BEFORE", "AFTER")
                        .flatMap(timing -> Stream.of("INSERT", "DELETE", "UPDATE", "UPDATE OF ID", "UPDATE OF ID,V",
                                "INSERT OR DELETE", "INSERT OR UPDATE OR DELETE", "UPDATE OF ID,V OR INSERT OR DELETE")
                                .flatMap(events -> Stream.of("", " LOCAL")
                                        .flatMap(local -> Stream.of("", " FOR EACH ROW", " FOR EACH STATEMENT")
                                                .map(level -> prefix + timing + " " + events + local + " ON APP.T" + level + " BEGIN NULL; END;"))))),
                triggerReferencingClauses().flatMap(referencing -> Stream.of("BEFORE", "AFTER")
                        .map(timing -> "CREATE TRIGGER APP.TR " + timing + " UPDATE ON APP.T " + referencing
                                + " FOR EACH ROW WHEN (1=1) BEGIN NULL; END;")),
                Stream.of("FOLLOWS", "PRECEDES").flatMap(ordering -> Stream.of("OTHER_TR", "APP.OTHER_TR", "APP.OTHER_TR,APP.THIRD_TR")
                        .flatMap(targets -> Stream.of("", " WHEN (:NEW.ID > 0)")
                                .map(condition -> "CREATE OR REPLACE TRIGGER APP.TR BEFORE INSERT ON APP.T FOR EACH ROW "
                                        + ordering + " " + targets + condition + " BEGIN :NEW.V := 'default'; END;"))),
                Stream.of("NEW.ID > 0", "OLD.ID <> NEW.ID", ":NEW.ID > 0", ":OLD.ID <> :NEW.ID", ":NEW.V IS NOT NULL AND :OLD.ID BETWEEN 1 AND 10",
                        ":NEW.ID IN (1,2) OR :OLD.V LIKE 'x%'", "COALESCE(:NEW.ID,0) > 0")
                        .map(condition -> "CREATE TRIGGER APP.TR BEFORE UPDATE ON APP.T FOR EACH ROW WHEN ("
                                + condition + ") BEGIN NULL; END;"),
                Stream.of(
                        "CREATE TRIGGER TR BEFORE INSERT ON T FOR EACH ROW BEGIN :NEW.ID := 1; END;",
                        "CREATE TRIGGER APP.TR BEFORE UPDATE ON APP.T REFERENCING OLD AS O NEW AS N FOR EACH ROW"
                                + " WHEN (:N.ID > :O.ID) BEGIN :N.V := :O.V; END;",
                        "CREATE TRIGGER APP.TR AFTER INSERT OR UPDATE OR DELETE ON APP.T FOR EACH ROW"
                                + " DECLARE N INT := 0; BEGIN IF INSERTING THEN N:=1; ELSIF UPDATING THEN N:=2;"
                                + " ELSIF DELETING THEN N:=3; END IF; PRINT N; EXCEPTION WHEN OTHERS THEN NULL; END;",
                        "CREATE TRIGGER APP.TR AFTER INSERT ON APP.T BEGIN BEGIN NULL; END;"
                                + " FOR I IN 1..2 LOOP NULL; END LOOP; CASE WHEN 1=1 THEN NULL; ELSE NULL; END CASE; END;",
                        // Definition analysis must not execute or apply the stored body to captured tables.
                        "CREATE TRIGGER APP.TR AFTER INSERT ON APP.SOURCE_T FOR EACH ROW BEGIN"
                                + " INSERT INTO APP.T(ID,V) VALUES(:NEW.ID,'inserted'); UPDATE APP.T SET V='changed' WHERE ID=:NEW.ID;"
                                + " DELETE FROM APP.T WHERE ID=:NEW.ID; END;",
                        "CREATE TRIGGER APP.TR AFTER INSERT ON APP.SOURCE_T BEGIN"
                                + " EXECUTE IMMEDIATE q'[ALTER TABLE APP.T ADD X INT]'; END;",
                        "CREATE TRIGGER APP.TR AFTER INSERT ON APP.T DECLARE PROCEDURE P AS BEGIN NULL; END; BEGIN CALL P(); END;",
                        "create /* trigger definition */ or replace trigger \"APP\".\"触发器; DROP TABLE APP.T\" with encryption"
                                + " before update of \"ID\",\"V\" on \"APP\".\"T\" referencing old row as o new row as n"
                                + " for each row follows \"APP\".\"TR; ALTER TABLE APP.T ADD X INT\" when (:n.id > 0)"
                                + " begin print q'[END; DROP TABLE APP.T]'; end;"))
                .flatMap(statements -> statements);
    }

    // Section 14.1.1.1's syntax diagram has one REFERENCING followed by OLD and/or NEW.
    private static Stream<String> triggerReferencingClauses() {
        return Stream.of(
                triggerReferenceAliases("OLD", "O").map(old -> "REFERENCING " + old),
                triggerReferenceAliases("NEW", "N").map(newValue -> "REFERENCING " + newValue),
                triggerReferenceAliases("OLD", "O").flatMap(old -> triggerReferenceAliases("NEW", "N")
                        .map(newValue -> "REFERENCING " + old + " " + newValue)))
                .flatMap(clauses -> clauses);
    }

    private static Stream<String> triggerReferenceAliases(String version, String alias) {
        return Stream.of("", " ROW").flatMap(row -> Stream.of("", " AS")
                .map(as -> version + row + as + " " + alias));
    }

    // INSTEAD OF applies to views and only supports row triggers; UPDATE OF is not allowed.
    static Stream<String> createInsteadOfTriggerStatements() {
        return Stream.concat(
                triggerCreationPrefixes().flatMap(prefix -> Stream.of("INSERT", "UPDATE", "DELETE", "INSERT OR UPDATE OR DELETE")
                        .flatMap(events -> Stream.of("", " LOCAL")
                                .map(local -> prefix + "INSTEAD OF " + events + local + " ON APP.V FOR EACH ROW BEGIN NULL; END;"))),
                Stream.of(
                        "CREATE TRIGGER APP.TR INSTEAD OF INSERT ON APP.V REFERENCING NEW ROW AS N"
                                + " FOR EACH ROW BEGIN INSERT INTO APP.T(ID,V) VALUES(:N.ID,:N.V); END;",
                        "CREATE OR REPLACE TRIGGER APP.TR WITH ENCRYPTION INSTEAD OF UPDATE ON APP.V REFERENCING OLD AS O NEW AS N"
                                + " FOR EACH ROW WHEN (:N.ID > 0) BEGIN UPDATE APP.T SET V=:N.V WHERE ID=:O.ID; END;"));
    }

    static Stream<String> createEventTriggerStatements() {
        return Stream.of(
                triggerCreationPrefixes().flatMap(prefix -> Stream.of("BEFORE", "AFTER")
                        .flatMap(timing -> Stream.of("DDL", "CREATE", "ALTER", "DROP", "GRANT", "REVOKE", "TRUNCATE", "COMMENT",
                                "CREATE OR ALTER OR DROP OR TRUNCATE OR COMMENT", "GRANT OR REVOKE")
                                .flatMap(events -> Stream.of("SCHEMA", "APP.SCHEMA", "DATABASE")
                                        .map(scope -> prefix + timing + " " + events + " ON " + scope + " BEGIN NULL; END;")))),
                // System events have timing and scope restrictions; only compatible event families are combined.
                triggerCreationPrefixes().flatMap(prefix -> Stream.of("AFTER LOGIN", "AFTER LOGON", "BEFORE LOGOUT", "BEFORE LOGOFF",
                        "AFTER SERERR", "AFTER BACKUP DATABASE", "AFTER RESTORE DATABASE", "AFTER BACKUP DATABASE OR RESTORE DATABASE",
                        "AFTER STARTUP", "BEFORE SHUTDOWN", "AFTER CHECKPOINT", "AFTER AUDIT", "AFTER NOAUDIT", "AFTER AUDIT OR NOAUDIT")
                        .map(event -> prefix + event + " ON DATABASE BEGIN NULL; END;")),
                // Section 14.1.2: EXECUTE AT takes a RAFT group OR a DMDSC node (CHECKPOINT only).
                // Confirmed on DM 8.1.3.162: neither RAFT1 0 nor two EXECUTE AT clauses is valid here.
                triggerCreationPrefixes().flatMap(prefix -> Stream.of("AFTER CREATE", "AFTER LOGIN", "AFTER CHECKPOINT")
                        .flatMap(event -> Stream.of("RAFT1", "RAFT_AT_ALL_SP$", "\"RAFT1\"")
                                .flatMap(raft -> Stream.of("", " WHEN (1=1)")
                                        .map(condition -> prefix + event + " ON DATABASE EXECUTE AT " + raft + condition + " BEGIN NULL; END;")))),
                triggerCreationPrefixes().flatMap(prefix -> Stream.of("0", "1")
                        .map(node -> prefix + "AFTER CHECKPOINT ON DATABASE EXECUTE AT " + node + " BEGIN NULL; END;")),
                Stream.of(
                        "CREATE TRIGGER APP.TR BEFORE CREATE ON DATABASE WHEN (1=1)"
                                + " BEGIN INSERT INTO APP.T(ID,V) VALUES(1,:EVENTINFO.OBJECTNAME); END;",
                        "CREATE OR REPLACE TRIGGER APP.TR AFTER DDL ON APP.SCHEMA WHEN (1=1)"
                                + " DECLARE N INT := 1; BEGIN PRINT :EVENTINFO.OBJECTTYPE; END;",
                        "CREATE TRIGGER APP.TR AFTER CREATE ON DATABASE EXECUTE AT RAFT1 WHEN (:EVENTINFO.OBJECTTYPE = 'TABLE')"
                                + " BEGIN PRINT :EVENTINFO.OBJECTNAME; END;",
                        "create trigger \"APP\".\"事件触发器; DROP TABLE APP.T\" after create on \"APP\".schema"
                                + " begin print q'[END; ALTER TABLE APP.T ADD X INT]'; end;"))
                .flatMap(statements -> statements);
    }

    // Section 14.1.3: monthly/weekly/daily, time/date windows, one-shot and node placement options.
    static Stream<String> createTimerTriggerStatements() {
        return Stream.of(
                triggerCreationPrefixes().flatMap(prefix -> Stream.of(
                        "FOR ONCE AT DATETIME '2030-01-01 12:00:00'",
                        "FOR ONCE AT DATETIME (TO_DATE('2030-01-01','YYYY-MM-DD'))",
                        "FOR EACH 1 MONTH DAY 15 AT TIME '12:00:00'",
                        "FOR EACH 1 MONTH DAY 2 OF WEEK 1 AT TIME '12:00:00'",
                        "FOR EACH 1 MONTH DAY 2 OF WEEK LAST AT TIME '12:00:00'",
                        "FOR EACH 1 WEEK 1 AT TIME '12:00:00'",
                        "FOR EACH 1 WEEK 1,3,5 AT TIME '12:00:00'",
                        "FOR EACH 1 DAY AT TIME '12:00:00'",
                        "FOR EACH 1 DAY FOR EACH 1 MINUTE",
                        "FOR EACH 1 DAY FOR EACH 30 SECOND")
                        .map(schedule -> prefix + "AFTER TIMER ON DATABASE " + schedule + " BEGIN NULL; END;")),
                Stream.of("FOR EACH 1 MONTH DAY 15", "FOR EACH 1 WEEK 1,3,5", "FOR EACH 1 DAY")
                        .flatMap(rate -> Stream.of("", "FROM TIME '09:00:00' ", "FROM TIME '09:00:00' TO TIME '18:00:00' ")
                                .flatMap(timeWindow -> Stream.of("MINUTE", "SECOND")
                                        .flatMap(unit -> Stream.of("", " FROM DATETIME '2030-01-01'", " FROM DATETIME '2030-01-01' TO DATETIME '2030-12-31'")
                                                .map(dateWindow -> "CREATE TRIGGER APP.TR AFTER TIMER ON DATABASE " + rate + " " + timeWindow
                                                        + "FOR EACH 1 " + unit + dateWindow + " BEGIN NULL; END;")))),
                Stream.of("", " EXECUTE AT RAFT1", " EXECUTE AT RAFT_AT_ALL_SP$")
                        .flatMap(raft -> Stream.of("", " EXECUTE AT 0")
                                .flatMap(node -> Stream.of("", " WHEN (1=1)")
                                        .map(condition -> "CREATE OR REPLACE TRIGGER APP.TR AFTER TIMER ON DATABASE" + raft
                                                + " FOR EACH 1 DAY FOR EACH 1 MINUTE" + node + condition
                                                + " DECLARE N INT := 1; BEGIN INSERT INTO APP.T(ID) VALUES(N); END;"))),
                Stream.of("CREATE TRIGGER APP.TR AFTER TIMER ON DATABASE FOR ONCE AT DATETIME '2030-01-01 12:00:00'"
                        + " BEGIN EXECUTE IMMEDIATE q'[ALTER TABLE APP.T ADD X INT]'; END;"))
                .flatMap(statements -> statements);
    }

    static Stream<String> alterTriggerStatements() {
        return Stream.concat(
                Stream.of("TR", "APP.TR").flatMap(name -> Stream.of("ENABLE", "DISABLE", "COMPILE", "COMPILE DEBUG")
                        .map(action -> "ALTER TRIGGER " + name + " " + action)),
                Stream.of(
                        "ALTER TRIGGER \"APP\".\"触发器\" ENABLE;",
                        "alter /* trigger status */ trigger \"APP\".\"TR; DROP TABLE APP.T\" disable;"));
    }

    static Stream<String> dropTriggerStatements() {
        return Stream.concat(
                Stream.of("", "IF EXISTS ").flatMap(ifExists -> Stream.of("TR", "APP.TR")
                        .map(name -> "DROP TRIGGER " + ifExists + name)),
                Stream.of(
                        "DROP TRIGGER \"APP\".\"触发器\";",
                        "drop /* trigger removal */ trigger if exists \"APP\".\"TR; DROP TABLE APP.T\";"));
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "CREATE TRIGGER TR BEFORE INSERT ON APP.T BEGIN NULL; END",
            "CREATE OR REPLACE TRIGGER APP.TR WITH ENCRYPTION AFTER UPDATE OF ID,V LOCAL ON APP.T REFERENCING OLD ROW AS O NEW ROW AS N"
                    + " FOR EACH ROW FOLLOWS APP.OTHER_TR WHEN (:N.ID > :O.ID) BEGIN PRINT :N.ID; END",
            "CREATE TRIGGER APP.TR INSTEAD OF INSERT ON APP.V FOR EACH ROW BEGIN INSERT INTO APP.T(ID) VALUES(:NEW.ID); END",
            "CREATE TRIGGER APP.TR AFTER INSERT ON APP.T DECLARE PROCEDURE P AS BEGIN NULL; END; BEGIN P(); END",
            "CREATE TRIGGER APP.TR AFTER INSERT ON APP.T BEGIN IF 1=1 THEN FOR I IN 1..2 LOOP NULL; END LOOP; END IF; END",
            "CREATE TRIGGER APP.TR AFTER INSERT ON APP.T BEGIN EXECUTE IMMEDIATE q'[ALTER TABLE APP.T ADD X INT]'; END",
            "CREATE TRIGGER APP.TR AFTER CREATE OR ALTER OR DROP ON APP.SCHEMA BEGIN NULL; END",
            "CREATE TRIGGER APP.TR AFTER CREATE ON DATABASE EXECUTE AT RAFT1 WHEN (:EVENTINFO.OBJECTTYPE = 'TABLE') BEGIN NULL; END",
            "CREATE TRIGGER APP.TR AFTER CHECKPOINT ON DATABASE EXECUTE AT 0 BEGIN NULL; END",
            "CREATE TRIGGER APP.TR BEFORE SHUTDOWN ON DATABASE BEGIN NULL; END",
            "CREATE TRIGGER APP.TR AFTER TIMER ON DATABASE FOR EACH 1 DAY FOR EACH 1 MINUTE BEGIN NULL; END",
            "CREATE TRIGGER APP.TR AFTER TIMER ON DATABASE EXECUTE AT RAFT1 FOR ONCE AT DATETIME '2030-01-01 12:00:00'"
                    + " EXECUTE AT 0 WHEN (1=1) BEGIN NULL; END",
            "CREATE TRIGGER APP.\"TR; DROP TABLE APP.T\" BEFORE UPDATE ON APP.T BEGIN PRINT 'END; ALTER TABLE APP.T ADD X INT'; END",
            "ALTER TRIGGER APP.TR ENABLE",
            "ALTER TRIGGER APP.TR DISABLE",
            "ALTER TRIGGER APP.TR COMPILE",
            "ALTER TRIGGER APP.TR COMPILE DEBUG",
            "DROP TRIGGER TR",
            "DROP TRIGGER IF EXISTS APP.TR"
    })
    void shouldNotSwallowTableChangesFollowingTriggerManagement(String sql) {
        assertFollowingTableChangesAreNotSkipped(sql);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "REPLACE TRIGGER APP.TR BEFORE INSERT ON APP.T BEGIN NULL; END;",
            "CREATE TRIGGER IF NOT EXISTS APP.TR BEFORE INSERT ON APP.T BEGIN NULL; END;",
            "CREATE TRIGGER APP.TR BEFORE INSERT ON APP.T",
            "CREATE TRIGGER APP.TR BEFORE INSERT ON APP.T BEGIN NULL;",
            "CREATE TRIGGER APP.TR BEFORE UPDATE ON APP.T REFERENCING OLD AS O NEW FOR EACH ROW BEGIN NULL; END;",
            "CREATE TRIGGER APP.TR BEFORE UPDATE ON APP.T REFERENCING FOR EACH ROW BEGIN NULL; END;",
            "CREATE TRIGGER APP.TR BEFORE UPDATE ON APP.T REFERENCING REFERENCING OLD AS O FOR EACH ROW BEGIN NULL; END;",
            "CREATE TRIGGER APP.TR BEFORE INSERT ON APP.T FOR EACH ROW WHEN (1=1 BEGIN NULL; END;",
            "CREATE TRIGGER APP.TR BEFORE INSERT ON APP.T FOR EACH ROW WHEN (:NEW. > 0) BEGIN NULL; END;",
            "CREATE TRIGGER APP.TR AFTER TIMER ON DATABASE FOR EACH 1 DAY; BEGIN NULL; END;",
            "CREATE TRIGGER APP.TR AFTER TIMER ON DATABASE FOR ONCE AT DATETIME (TO_DATE('2030-01-01','YYYY-MM-DD') BEGIN NULL; END;",
            "CREATE TRIGGER APP.TR AFTER CREATE ON DATABASE EXECUTE AT BEGIN NULL; END;",
            "CREATE TRIGGER APP.TR AFTER CREATE ON DATABASE EXECUTE AT RAFT1 0 BEGIN NULL; END;",
            "CREATE TRIGGER APP.TR AFTER CHECKPOINT ON DATABASE EXECUTE AT RAFT1 EXECUTE AT 0 BEGIN NULL; END;",
            "CREATE TRIGGER APP.TR AFTER INSERT ON APP.T BEGIN NULL; END; INSERT INTO APP.T(ID) VALUES(1)",
            "CREATE TRIGGER APP.TR AFTER INSERT ON APP.T BEGIN NULL; END; UPDATE APP.T SET V='changed'",
            "CREATE TRIGGER APP.TR AFTER INSERT ON APP.T BEGIN NULL; END; DELETE FROM APP.T",
            "CREATE TRIGGER APP.TR AFTER INSERT ON APP.T BEGIN NULL; END; CALL APP.P()"
    })
    void shouldRejectIncompleteOrCompoundTriggerDefinitions(String sql) {
        assertUnsupported(sql);
    }

    // Triggers execute implicitly on their events, not through standalone trigger-management commands.
    // Triggering DML and any row changes produced by the body must still follow the CDC DML path.
    @ParameterizedTest
    @ValueSource(strings = {
            "EXEC APP.TR",
            "EXECUTE APP.TR",
            "EXECUTE TRIGGER APP.TR",
            "CALL APP.TR()",
            "BEGIN APP.TR(); END;",
            "INSERT INTO APP.T(ID) VALUES(1)",
            "UPDATE APP.T SET V='changed' WHERE ID=1",
            "DELETE FROM APP.T WHERE ID=1",
            "INSERT INTO APP.V(ID) VALUES(1)",
            "UPDATE APP.V SET V='changed' WHERE ID=1",
            "DELETE FROM APP.V WHERE ID=1"
    })
    void shouldKeepTriggerExecutionOutsideSkippableDdl(String sql) {
        assertUnsupported(sql);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "CREATE TABLE APP.T(ID INT)",
            "ALTER TABLE APP.T ADD V VARCHAR(30)",
            "DROP TABLE APP.T",
            "TRUNCATE TABLE APP.T"
    })
    void shouldKeepTableDdlThatCanFireEventTriggersInTableScope(String sql) {
        final var analysis = analyzer.analyze(sql, "APP").requireSupported();

        assertThat(analysis.impact()).isEqualTo(Impact.TABLE);
        assertThat(analysis.tableId()).isEqualTo(new TableId(null, "APP", "T"));
        assertThat(analysis.tableStatement()).isNotNull();
    }

    @ParameterizedTest
    @MethodSource({ "createPackageStatements", "createPackageBodyStatements", "alterPackageStatements", "dropPackageStatements" })
    void shouldSkipPackageManagementWithoutChangingCapturedTables(String sql) {
        assertStatementDoesNotChangeCapturedTables(sql);
    }

    // DM SQL Language Manual, sections 11.1-11.3:
    // https://eco.dameng.com/document/dm/zh-cn/pm/package.html
    // IF NOT EXISTS and AUTHID belong to the specification, not CREATE PACKAGE BODY.
    static Stream<String> createPackageStatements() {
        return Stream.of(
                Stream.of("", "OR REPLACE ").flatMap(replacement -> Stream.of("", "IF NOT EXISTS ")
                        .flatMap(ifNotExists -> Stream.of("PKG", "APP.PKG")
                                .flatMap(name -> Stream.of("", " WITH ENCRYPTION")
                                        .flatMap(encryption -> Stream.of("", " AUTHID DEFINER", " AUTHID CURRENT_USER")
                                                .flatMap(authid -> Stream.of("AS", "IS")
                                                        .flatMap(keyword -> Stream.of("", " PKG")
                                                                .map(endName -> "CREATE " + replacement + "PACKAGE " + ifNotExists + name
                                                                        + encryption + authid + " " + keyword
                                                                        + " N INT; PROCEDURE RUN; FUNCTION GET_VALUE RETURN INT; END" + endName + ";"))))))),
                Stream.of("DEFAULT", "ASSIGN", ":=")
                        .map(initializer -> "CREATE PACKAGE APP.PKG AS N CONSTANT INT NOT NULL " + initializer + " 1; END PKG;"),
                Stream.of("N INT;", "N,M INT;", "V APP.T.ID%TYPE; R APP.T%ROWTYPE;", "C CURSOR;",
                        "CURSOR C;", "CURSOR C FOR SELECT ID,V FROM APP.T WHERE ID > 0;",
                        "E EXCEPTION;", "E EXCEPTION FOR -20001;", "E EXCEPTION FOR -20001,'missing';",
                        "PROCEDURE RUN;", "PROCEDURE RUN();", "PROCEDURE RUN(A IN INT, B OUT INT, C IN OUT INT);",
                        "PROCEDURE RUN(A INT DEFAULT 1); PROCEDURE RUN(A VARCHAR(30));",
                        "FUNCTION GET_VALUE RETURN INT;", "FUNCTION GET_VALUE(A INT := 1) RETURN INT;")
                        .map(declaration -> "CREATE PACKAGE APP.PKG IS " + declaration + " END;"),
                Stream.of("", " RESULT_CACHE").flatMap(cache -> Stream.of("", " DETERMINISTIC")
                        .flatMap(deterministic -> Stream.of("", " PIPELINED")
                                .map(pipelined -> "CREATE PACKAGE APP.PKG AS FUNCTION GET_ROWS RETURN APP.INT_LIST"
                                        + cache + deterministic + pipelined + "; END PKG;"))),
                Stream.of(
                        "CREATE PACKAGE \"APP\".\"程序包\" AS PROCEDURE RUN; END \"程序包\";",
                        "create /* package specification */ or replace package if not exists \"APP\".\"PKG; DROP TABLE APP.T\""
                                + " with encryption authid current_user is v varchar(50) default q'[END; ALTER TABLE APP.T ADD X INT]';"
                                + " procedure run; end \"PKG; DROP TABLE APP.T\";"))
                .flatMap(statements -> statements);
    }

    static Stream<String> createPackageBodyStatements() {
        return Stream.of(
                Stream.of("", "OR REPLACE ").flatMap(replacement -> Stream.of("PKG", "APP.PKG")
                        .flatMap(name -> Stream.of("", " WITH ENCRYPTION")
                                .flatMap(encryption -> Stream.of("AS", "IS")
                                        .flatMap(keyword -> Stream.of("", " PKG")
                                                .flatMap(endName -> Stream.of("", " BEGIN NULL;", " BEGIN PRINT 'initialized';")
                                                        .map(initializer -> "CREATE " + replacement + "PACKAGE BODY " + name + encryption + " " + keyword
                                                                + " PROCEDURE RUN AS BEGIN NULL; END RUN;"
                                                                + " FUNCTION GET_VALUE RETURN INT IS BEGIN RETURN 1; END GET_VALUE;"
                                                                + initializer + " END" + endName + ";")))))),
                Stream.of("AS", "IS").flatMap(keyword -> Stream.of("", " RUN")
                        .flatMap(routineEndName -> Stream.of("", "DECLARE ")
                                .map(declare -> "CREATE PACKAGE BODY APP.PKG " + keyword + " N INT := 1;"
                                        + " PROCEDURE RUN " + keyword + " BEGIN PRINT N; END" + routineEndName + ";"
                                        + " " + declare + "M INT := 2; BEGIN PRINT M; EXCEPTION WHEN OTHERS THEN NULL; END PKG;"))),
                Stream.of("", " DETERMINISTIC").map(deterministic -> "CREATE PACKAGE BODY APP.PKG AS"
                        + " FUNCTION GET_ROWS RETURN APP.INT_LIST" + deterministic
                        + " PIPELINED AS BEGIN PIPE ROW(1); RETURN; END GET_ROWS; END PKG;"),
                Stream.of(
                        "CREATE PACKAGE BODY APP.PKG AS N INT DEFAULT 1; CURSOR C FOR SELECT ID FROM APP.T; E EXCEPTION;"
                                + " PROCEDURE RUN AS BEGIN OPEN C; CLOSE C; END; END;",
                        "CREATE PACKAGE BODY APP.PKG AS PROCEDURE RUN(A INT DEFAULT 1);"
                                + " PROCEDURE RUN(A INT DEFAULT 1) AS BEGIN PRINT A; END; END PKG;",
                        "CREATE PACKAGE BODY APP.PKG AS PROCEDURE RUN(A INT) AS BEGIN PRINT A; END;"
                                + " PROCEDURE RUN(A VARCHAR(30)) AS BEGIN PRINT A; END; END PKG;",
                        "CREATE PACKAGE BODY APP.PKG AS PROCEDURE RUN AS PROCEDURE LOCAL_P IS BEGIN NULL; END;"
                                + " FUNCTION LOCAL_F RETURN INT AS BEGIN RETURN 1; END;"
                                + " BEGIN CALL LOCAL_P(); PRINT LOCAL_F(); END RUN; END PKG;",
                        "CREATE PACKAGE BODY APP.PKG AS PROCEDURE RUN AS BEGIN BEGIN NULL; END;"
                                + " IF 1=1 THEN FOR I IN 1..2 LOOP NULL; END LOOP; ELSE NULL; END IF;"
                                + " CASE WHEN 1=1 THEN NULL; ELSE NULL; END CASE; END; END PKG;",
                        // Stored methods and initialization code must not be applied to the captured table model.
                        "CREATE PACKAGE BODY APP.PKG AS PROCEDURE RUN AS BEGIN INSERT INTO APP.T(ID,V) VALUES(1,'inserted');"
                                + " UPDATE APP.T SET V='changed' WHERE ID=1; DELETE FROM APP.T WHERE ID=1; END RUN; END PKG;",
                        // Section 11.1.2: initialization runs on first access to the package in a session.
                        "CREATE PACKAGE BODY APP.PKG AS N INT; PROCEDURE RUN AS BEGIN PRINT N; END;"
                                + " BEGIN SELECT COUNT(*) INTO N FROM APP.T; INSERT INTO APP.T(ID) VALUES(N); END PKG;",
                        "CREATE PACKAGE BODY APP.PKG AS PROCEDURE RUN AS BEGIN"
                                + " EXECUTE IMMEDIATE q'[ALTER TABLE APP.T ADD X INT]'; END;"
                                + " BEGIN EXECUTE IMMEDIATE q'[DELETE FROM APP.T]'; END PKG;",
                        "CREATE PACKAGE BODY APP.PKG AS PROCEDURE RUN AS BEGIN NULL; END;"
                                + " BEGIN BEGIN NULL; END; EXCEPTION WHEN OTHERS THEN NULL; END PKG;",
                        "CREATE PACKAGE BODY \"APP\".\"程序包\" AS PROCEDURE RUN AS BEGIN NULL; END RUN; END \"程序包\";",
                        "create /* package body */ or replace package body \"APP\".\"PKG; DROP TABLE APP.T\" with encryption is"
                                + " procedure run as begin print q'[END; ALTER TABLE APP.T ADD X INT]'; end run;"
                                + " begin print 'initialized; END'; end \"PKG; DROP TABLE APP.T\";"))
                .flatMap(statements -> statements);
    }

    // CASCADE recompiles dependent objects; it is not a cascading removal of captured tables.
    static Stream<String> alterPackageStatements() {
        return Stream.concat(
                Stream.of("PKG", "APP.PKG").flatMap(name -> Stream.of("", " CASCADE")
                        .flatMap(cascade -> Stream.of("", " DEBUG")
                                .map(debug -> "ALTER PACKAGE " + name + " COMPILE" + cascade + debug))),
                Stream.of(
                        "ALTER PACKAGE \"APP\".\"程序包\" COMPILE;",
                        "alter /* package recompilation */ package \"APP\".\"PKG; DROP TABLE APP.T\" compile cascade debug;"));
    }

    // Dropping the specification also drops its body; dropping BODY preserves the specification.
    static Stream<String> dropPackageStatements() {
        return Stream.concat(
                Stream.of("", "BODY ").flatMap(body -> Stream.of("", "IF EXISTS ")
                        .flatMap(ifExists -> Stream.of("PKG", "APP.PKG")
                                .map(name -> "DROP PACKAGE " + body + ifExists + name))),
                Stream.of(
                        "DROP PACKAGE \"APP\".\"程序包\";",
                        "DROP PACKAGE BODY IF EXISTS \"APP\".\"程序包\";",
                        "drop /* package removal */ package if exists \"APP\".\"PKG; DROP TABLE APP.T\";",
                        "drop /* body removal */ package body if exists \"APP\".\"PKG; DROP TABLE APP.T\";"));
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "CREATE PACKAGE PKG AS PROCEDURE RUN; END",
            "CREATE OR REPLACE PACKAGE IF NOT EXISTS APP.PKG WITH ENCRYPTION AUTHID CURRENT_USER IS PROCEDURE RUN; END PKG",
            "CREATE PACKAGE APP.PKG AS CURSOR C FOR SELECT ID FROM APP.T; END PKG",
            "CREATE PACKAGE BODY APP.PKG AS PROCEDURE RUN AS BEGIN NULL; END; END",
            "CREATE PACKAGE BODY APP.PKG AS FUNCTION F RETURN INT AS BEGIN RETURN CASE WHEN 1=1 THEN 1 ELSE 0 END; END; END PKG",
            "CREATE PACKAGE BODY APP.PKG AS PROCEDURE RUN AS BEGIN NULL; END; BEGIN INSERT INTO APP.T(ID) VALUES(1); END PKG",
            "CREATE PACKAGE BODY APP.PKG AS PROCEDURE RUN AS BEGIN NULL; END; DECLARE N INT; BEGIN N:=1; END PKG",
            "CREATE PACKAGE BODY APP.PKG AS PROCEDURE RUN AS BEGIN EXECUTE IMMEDIATE q'[ALTER TABLE APP.T ADD X INT]'; END; END PKG",
            "CREATE PACKAGE BODY APP.\"PKG; DROP TABLE APP.T\" AS PROCEDURE RUN AS BEGIN NULL; END; END \"PKG; DROP TABLE APP.T\"",
            "ALTER PACKAGE APP.PKG COMPILE",
            "ALTER PACKAGE APP.PKG COMPILE CASCADE",
            "ALTER PACKAGE APP.PKG COMPILE DEBUG",
            "ALTER PACKAGE APP.PKG COMPILE CASCADE DEBUG",
            "DROP PACKAGE APP.PKG",
            "DROP PACKAGE IF EXISTS APP.PKG",
            "DROP PACKAGE BODY APP.PKG",
            "DROP PACKAGE BODY IF EXISTS APP.PKG"
    })
    void shouldNotSwallowTableChangesFollowingPackageManagement(String sql) {
        assertFollowingTableChangesAreNotSkipped(sql);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "CREATE PACKAGE APP.PKG AS PROCEDURE RUN;",
            "CREATE PACKAGE BODY APP.PKG AS PROCEDURE RUN AS BEGIN NULL; END RUN;",
            "CREATE PACKAGE BODY APP.PKG AS PROCEDURE RUN AS BEGIN NULL; END; BEGIN NULL;",
            "CREATE PACKAGE BODY APP.PKG AS PROCEDURE RUN AS BEGIN NULL; END; DECLARE N INT; END PKG;",
            "CREATE PACKAGE APP.PKG AS PROCEDURE RUN; END; END;",
            "CREATE PACKAGE BODY IF NOT EXISTS APP.PKG AS PROCEDURE RUN AS BEGIN NULL; END; END PKG;",
            "CREATE PACKAGE BODY APP.PKG AUTHID CURRENT_USER AS PROCEDURE RUN AS BEGIN NULL; END; END PKG;",
            "ALTER PACKAGE BODY APP.PKG COMPILE",
            "ALTER PACKAGE APP.PKG COMPILE BODY",
            "ALTER PACKAGE APP.PKG COMPILE SPECIFICATION",
            "ALTER PACKAGE APP.PKG COMPILE DEBUG CASCADE",
            "ALTER PACKAGE APP.PKG RENAME TO OTHER_PKG",
            "DROP PACKAGE IF EXISTS BODY APP.PKG",
            "DROP PACKAGE APP.PKG CASCADE",
            "DROP PACKAGE BODY APP.PKG CASCADE",
            "CREATE PACKAGE APP.PKG AS PROCEDURE RUN; END; INSERT INTO APP.T(ID) VALUES(1)",
            "CREATE PACKAGE BODY APP.PKG AS PROCEDURE RUN AS BEGIN NULL; END; END; UPDATE APP.T SET V='changed'",
            "CREATE PACKAGE BODY APP.PKG AS PROCEDURE RUN AS BEGIN NULL; END; END; DELETE FROM APP.T",
            "CREATE PACKAGE BODY APP.PKG AS PROCEDURE RUN AS BEGIN NULL; END; END; CALL APP.PKG.RUN()"
    })
    void shouldRejectIncompleteOrCompoundPackageDefinitions(String sql) {
        assertUnsupported(sql);
    }

    // First access can run package initialization; actual invocation and DML stay outside skippable DDL.
    @ParameterizedTest
    @ValueSource(strings = {
            "CALL APP.PKG.RUN()",
            "APP.PKG.RUN()",
            "EXEC APP.PKG.RUN 1",
            "EXECUTE APP.PKG.RUN 1",
            "BEGIN APP.PKG.RUN(); END;",
            "BEGIN PRINT APP.PKG.N; END;",
            "SELECT APP.PKG.GET_VALUE() FROM DUAL",
            "INSERT INTO APP.T(ID) VALUES(APP.PKG.GET_VALUE())",
            "UPDATE APP.T SET ID=APP.PKG.GET_VALUE()",
            "DELETE FROM APP.T WHERE ID=APP.PKG.GET_VALUE()"
    })
    void shouldKeepPackageAccessOutsideSkippableDdl(String sql) {
        assertUnsupported(sql);
    }

    @ParameterizedTest
    @MethodSource({ "createTypeStatements", "createTypeBodyStatements", "alterTypeStatements", "dropTypeStatements" })
    void shouldSkipTypeManagementWithoutChangingCapturedTables(String sql) {
        assertStatementDoesNotChangeCapturedTables(sql);
    }

    // DM SQL Language Manual, sections 13.1-13.4:
    // https://eco.dameng.com/document/dm/zh-cn/pm/custom-type.html
    // Replacement uses CREATE OR REPLACE; neither TYPE nor TYPE BODY documents IF NOT EXISTS.
    private static Stream<String> typeCreationPrefixes() {
        return Stream.of("", "OR REPLACE ").flatMap(replacement -> Stream.of("OBJ", "APP.OBJ")
                .flatMap(name -> Stream.of("", " WITH ENCRYPTION")
                        .flatMap(encryption -> Stream.of("", " AUTHID DEFINER", " AUTHID CURRENT_USER")
                                .map(authid -> "CREATE " + replacement + "TYPE " + name + encryption + authid + " "))));
    }

    static Stream<String> createTypeStatements() {
        return Stream.of(
                typeCreationPrefixes().flatMap(prefix -> Stream.of("AS", "IS")
                        .flatMap(keyword -> Stream.of("OBJECT(ID INT)", "RECORD(ID INT,V VARCHAR(30))",
                                "ARRAY INT[3]", "ARRAY INT[2,3]", "ARRAY INT[]", "ARRAY INT[,]",
                                "VARRAY(10) OF INT", "VARRAY(10) OF INT NOT NULL", "VARRAY(10) OF APP.ELEMENT_TYPE",
                                "TABLE OF INT", "TABLE OF INT NOT NULL", "TABLE OF APP.ELEMENT_TYPE",
                                "TABLE OF INT INDEX BY INT", "TABLE OF INT INDEX BY INTEGER", "TABLE OF INT INDEX BY VARCHAR(30)")
                                .map(definition -> prefix + keyword + " " + definition + ";"))),
                // NOT INSTANTIABLE objects must also be NOT FINAL (class-type.html, section 12.1.1).
                Stream.of("", " FINAL", " NOT FINAL", " INSTANTIABLE", " NOT FINAL INSTANTIABLE", " NOT FINAL NOT INSTANTIABLE")
                        .flatMap(modifiers -> Stream.of("", " PERSISTABLE", " NOT PERSISTABLE")
                                .flatMap(persistence -> Stream.of("AS", "IS")
                                        .map(keyword -> "CREATE TYPE APP.OBJ " + keyword + " OBJECT(ID INT)" + modifiers + persistence))),
                typeCreationPrefixes().flatMap(prefix -> Stream.of("BASE_OBJ", "APP.BASE_OBJ")
                        .map(parent -> prefix + "UNDER " + parent + "(V VARCHAR(30)) NOT FINAL;")),
                Stream.of("", "MEMBER ", "STATIC ", "FINAL MEMBER ", "NOT FINAL MEMBER ", "INSTANTIABLE MEMBER ",
                        "NOT OVERRIDING MEMBER ", "OVERRIDING MEMBER ", "OVERRIDING FINAL MEMBER ",
                        "OVERRIDING NOT FINAL INSTANTIABLE MEMBER ")
                        .flatMap(modifier -> Stream.of("PROCEDURE RUN(A IN INT, B OUT INT)", "FUNCTION GET_VALUE RETURN INT",
                                "FUNCTION GET_VALUE(A INT DEFAULT 1) RETURN INT DETERMINISTIC")
                                .map(method -> "CREATE TYPE APP.OBJ UNDER APP.BASE_OBJ(V INT," + modifier + method + ");")),
                Stream.of(
                        "CREATE TYPE APP.OBJ AS OBJECT(ID INT,MAP FUNCTION GET_VALUE RETURN INT)",
                        "CREATE TYPE APP.OBJ AS OBJECT(ID INT,MAP MEMBER FUNCTION GET_VALUE RETURN INT)",
                        "CREATE TYPE APP.OBJ AS OBJECT(ID INT,NOT INSTANTIABLE MEMBER FUNCTION GET_VALUE RETURN INT) NOT FINAL NOT INSTANTIABLE",
                        "CREATE TYPE APP.OBJ AS OBJECT(ID INT,CONSTRUCTOR FUNCTION OBJ(ID INT) RETURN SELF AS RESULT)",
                        "CREATE TYPE APP.OBJ AS OBJECT(ID INT,CONSTRUCTOR FUNCTION OBJ(SELF IN OUT APP.OBJ,ID INT) RETURN SELF AS RESULT)",
                        "CREATE TYPE APP.OBJ AS OBJECT(ID INT,MEMBER FUNCTION GET_ROWS RETURN APP.INT_LIST DETERMINISTIC PIPELINED)",
                        "CREATE TYPE APP.OBJ AS OBJECT(ID INT,PROCEDURE RUN(A INT),PROCEDURE RUN(A VARCHAR(30)),FUNCTION GET_VALUE RETURN INT)",
                        "CREATE TYPE \"APP\".\"对象类型\" AS OBJECT(\"ID\" INT)",
                        "CREATE TYPE \"APP\".\"BODY\" AS RECORD(ID INT)",
                        "create /* object definition */ or replace type \"APP\".\"OBJ; DROP TABLE APP.T\" with encryption authid current_user is"
                                + " object(\"ID\" int,\"V; ALTER TABLE APP.T ADD X INT\" varchar(30)) not final not persistable;"))
                .flatMap(statements -> statements);
    }

    static Stream<String> createTypeBodyStatements() {
        return Stream.of(
                Stream.of("", "OR REPLACE ").flatMap(replacement -> Stream.of("OBJ", "APP.OBJ")
                        .flatMap(name -> Stream.of("", " WITH ENCRYPTION")
                                .flatMap(encryption -> Stream.of("AS", "IS")
                                        .map(keyword -> "CREATE " + replacement + "TYPE BODY " + name + encryption + " " + keyword
                                                + " MEMBER PROCEDURE RUN AS BEGIN NULL; END RUN;"
                                                + " MEMBER FUNCTION GET_VALUE RETURN INT IS BEGIN RETURN 1; END GET_VALUE; END;")))),
                Stream.of("", "MEMBER ", "STATIC ", "FINAL MEMBER ", "NOT FINAL MEMBER ", "INSTANTIABLE MEMBER ",
                        "NOT OVERRIDING MEMBER ", "OVERRIDING MEMBER ", "OVERRIDING FINAL MEMBER ",
                        "OVERRIDING NOT FINAL INSTANTIABLE MEMBER ", "MAP ", "MAP MEMBER ")
                        .flatMap(modifier -> Stream.of("AS", "IS")
                                .flatMap(keyword -> Stream.of("", " GET_VALUE")
                                        .map(endName -> "CREATE TYPE BODY APP.OBJ AS " + modifier + "FUNCTION GET_VALUE RETURN INT " + keyword
                                                + " BEGIN RETURN 1; END" + endName + "; END;"))),
                Stream.of("AS", "IS").flatMap(keyword -> Stream.of("", "SELF IN OUT APP.OBJ,")
                        .map(self -> "CREATE OR REPLACE TYPE BODY APP.OBJ WITH ENCRYPTION " + keyword
                                + " CONSTRUCTOR FUNCTION OBJ(" + self + "ID INT) RETURN SELF AS RESULT " + keyword
                                + " BEGIN SELF.ID:=ID; RETURN; END OBJ; END;")),
                Stream.of("", " DETERMINISTIC").map(deterministic -> "CREATE TYPE BODY APP.OBJ AS"
                        + " MEMBER FUNCTION GET_ROWS RETURN APP.INT_LIST" + deterministic
                        + " PIPELINED AS BEGIN PIPE ROW(1); RETURN; END GET_ROWS; END;"),
                // Section 13.5.2 and DM 8.1.3.162 use semicolons; the comma in 13.2's syntax text is not accepted.
                Stream.of(
                        "CREATE TYPE BODY APP.OBJ AS MEMBER PROCEDURE RUN AS N INT:=0; BEGIN PRINT N; END;"
                                + " MEMBER FUNCTION GET_VALUE RETURN INT AS DECLARE N INT:=1; BEGIN RETURN N; END; END;",
                        "CREATE TYPE BODY APP.OBJ AS MEMBER FUNCTION GET_VALUE RETURN INT AS"
                                + " FUNCTION LOCAL_F RETURN INT AS BEGIN RETURN 1; END; BEGIN RETURN LOCAL_F(); END; END;",
                        "CREATE TYPE BODY APP.OBJ AS MEMBER PROCEDURE RUN AS BEGIN BEGIN NULL; END;"
                                + " IF 1=1 THEN FOR I IN 1..2 LOOP NULL; END LOOP; ELSE NULL; END IF;"
                                + " CASE WHEN 1=1 THEN NULL; ELSE NULL; END CASE; EXCEPTION WHEN OTHERS THEN NULL; END; END;",
                        // Analysis stores no member implementation in the captured table model and never executes it.
                        "CREATE TYPE BODY APP.OBJ AS MEMBER PROCEDURE RUN AS BEGIN INSERT INTO APP.T(ID,V) VALUES(1,'inserted');"
                                + " UPDATE APP.T SET V='changed' WHERE ID=1; DELETE FROM APP.T WHERE ID=1; END; END;",
                        "CREATE TYPE BODY APP.OBJ AS MEMBER PROCEDURE RUN AS BEGIN"
                                + " EXECUTE IMMEDIATE q'[ALTER TABLE APP.T ADD X INT]'; END; END;",
                        "CREATE TYPE BODY \"APP\".\"对象类型\" AS MEMBER FUNCTION GET_VALUE RETURN INT AS BEGIN RETURN 1; END; END;",
                        "create /* type body */ or replace type body \"APP\".\"OBJ; DROP TABLE APP.T\" with encryption is"
                                + " member procedure run as begin print q'[END; ALTER TABLE APP.T ADD X INT]'; end run; end;"))
                .flatMap(statements -> statements);
    }

    static Stream<String> alterTypeStatements() {
        return Stream.concat(
                Stream.of("OBJ", "APP.OBJ").flatMap(name -> Stream.of("", " DEBUG")
                        .map(debug -> "ALTER TYPE " + name + " COMPILE" + debug)),
                Stream.of(
                        "ALTER TYPE \"APP\".\"对象类型\" COMPILE;",
                        "alter /* type recompilation */ type \"APP\".\"OBJ; DROP TABLE APP.T\" compile debug;"));
    }

    static Stream<String> dropTypeStatements() {
        return Stream.of(
                Stream.of("", "BODY ").flatMap(body -> Stream.of("", "IF EXISTS ")
                        .flatMap(ifExists -> Stream.of("OBJ", "APP.OBJ")
                                .flatMap(name -> Stream.of("", " RESTRICT")
                                        .map(restrict -> "DROP TYPE " + body + ifExists + name + restrict)))),
                // TYPE BODY CASCADE removes implementations, preserving the type, table columns and stored rows.
                // Object-column reads may fail until a required implementation is recreated; this is not a schema change.
                Stream.of("", "IF EXISTS ").flatMap(ifExists -> Stream.of("OBJ", "APP.OBJ")
                        .map(name -> "DROP TYPE BODY " + ifExists + name + " CASCADE")),
                Stream.of(
                        "DROP TYPE \"APP\".\"对象类型\";",
                        "DROP TYPE BODY IF EXISTS \"APP\".\"对象类型\" RESTRICT;",
                        "DROP TYPE BODY IF EXISTS \"APP\".\"对象类型\" CASCADE;",
                        "drop /* type removal */ type if exists \"APP\".\"OBJ; DROP TABLE APP.T\" restrict;",
                        "drop /* body removal */ type body if exists \"APP\".\"OBJ; DROP TABLE APP.T\";"))
                .flatMap(statements -> statements);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "CREATE TYPE APP.OBJ AS OBJECT(ID INT)",
            "CREATE OR REPLACE TYPE APP.OBJ WITH ENCRYPTION AUTHID CURRENT_USER IS OBJECT(ID INT) NOT FINAL NOT PERSISTABLE",
            "CREATE TYPE APP.OBJ UNDER APP.BASE_OBJ(V INT)",
            "CREATE TYPE APP.OBJ AS RECORD(ID INT)",
            "CREATE TYPE APP.OBJ AS ARRAY INT[2,3]",
            "CREATE TYPE APP.OBJ AS VARRAY(10) OF INT NOT NULL",
            "CREATE TYPE APP.OBJ AS TABLE OF INT INDEX BY VARCHAR(30)",
            "CREATE TYPE BODY APP.OBJ AS MEMBER FUNCTION GET_VALUE RETURN INT AS BEGIN RETURN 1; END; END",
            "CREATE TYPE BODY APP.OBJ AS CONSTRUCTOR FUNCTION OBJ(ID INT) RETURN SELF AS RESULT AS BEGIN SELF.ID:=ID; RETURN; END; END",
            "CREATE TYPE BODY APP.OBJ AS MEMBER PROCEDURE RUN AS BEGIN EXECUTE IMMEDIATE q'[ALTER TABLE APP.T ADD X INT]'; END; END",
            "CREATE TYPE APP.\"OBJ; DROP TABLE APP.T\" AS OBJECT(ID INT)",
            "ALTER TYPE APP.OBJ COMPILE",
            "ALTER TYPE APP.OBJ COMPILE DEBUG",
            "DROP TYPE APP.OBJ",
            "DROP TYPE IF EXISTS APP.OBJ RESTRICT",
            "DROP TYPE BODY APP.OBJ",
            "DROP TYPE BODY IF EXISTS APP.OBJ RESTRICT",
            "DROP TYPE BODY IF EXISTS APP.OBJ CASCADE"
    })
    void shouldNotSwallowTableChangesFollowingTypeManagement(String sql) {
        assertFollowingTableChangesAreNotSkipped(sql);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "REPLACE TYPE APP.OBJ AS OBJECT(ID INT)",
            "CREATE TYPE IF NOT EXISTS APP.OBJ AS OBJECT(ID INT)",
            "CREATE TYPE BODY IF NOT EXISTS APP.OBJ AS MEMBER PROCEDURE RUN AS BEGIN NULL; END; END;",
            "CREATE TYPE BODY APP.OBJ AUTHID CURRENT_USER AS MEMBER PROCEDURE RUN AS BEGIN NULL; END; END;",
            "CREATE TYPE APP.OBJ AS OBJECT(ID INT",
            "CREATE TYPE APP.OBJ UNDER APP.BASE_OBJ(ID INT",
            "CREATE TYPE BODY APP.OBJ AS MEMBER PROCEDURE RUN AS BEGIN NULL; END;",
            "CREATE TYPE BODY APP.OBJ AS MEMBER PROCEDURE RUN AS BEGIN NULL; END RUN;",
            "CREATE TYPE BODY APP.OBJ AS MEMBER PROCEDURE RUN AS BEGIN NULL; END, MEMBER PROCEDURE OTHER_P AS BEGIN NULL; END; END;",
            "CREATE TYPE BODY APP.OBJ AS MEMBER PROCEDURE RUN AS BEGIN NULL; END;, MEMBER PROCEDURE OTHER_P AS BEGIN NULL; END; END;",
            "CREATE TYPE BODY APP.OBJ AS END;",
            "ALTER TYPE BODY APP.OBJ COMPILE",
            "ALTER TYPE APP.OBJ COMPILE BODY",
            "ALTER TYPE APP.OBJ COMPILE SPECIFICATION",
            "ALTER TYPE APP.OBJ COMPILE CASCADE",
            "ALTER TYPE APP.OBJ ADD ATTRIBUTE V INT",
            "ALTER TYPE APP.OBJ DROP ATTRIBUTE V",
            "DROP TYPE IF EXISTS BODY APP.OBJ",
            "DROP TYPE APP.OBJ FORCE",
            "DROP TYPE BODY APP.OBJ CASCADE RESTRICT",
            "CREATE TYPE APP.OBJ AS OBJECT(ID INT); INSERT INTO APP.T(ID) VALUES(1)",
            "CREATE TYPE BODY APP.OBJ AS MEMBER PROCEDURE RUN AS BEGIN NULL; END; END; UPDATE APP.T SET V='changed'",
            "CREATE TYPE BODY APP.OBJ AS MEMBER PROCEDURE RUN AS BEGIN NULL; END; END; DELETE FROM APP.T",
            "CREATE TYPE BODY APP.OBJ AS MEMBER PROCEDURE RUN AS BEGIN NULL; END; END; CALL APP.P()"
    })
    void shouldRejectIncompleteOrCompoundTypeDefinitions(String sql) {
        assertUnsupported(sql);
    }

    // custom-type.html 13.5.1 adopts class-type.html 12.3's table-column dependency rules.
    // DM 8.1.3.162 removes dependent tables for OBJECT, VARRAY and TABLE OF types with CASCADE.
    @ParameterizedTest
    @ValueSource(strings = {
            "DROP TYPE APP.OBJ CASCADE",
            "DROP TYPE IF EXISTS APP.OBJ CASCADE",
            "DROP TYPE APP.INT_LIST CASCADE",
            "DROP TYPE APP.NESTED_TABLE CASCADE",
            "DROP TYPE \"APP\".\"对象类型\" CASCADE"
    })
    void shouldKeepCascadingTypeRemovalOutsideSkippableDdl(String sql) {
        assertUnsupported(sql);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "CREATE TABLE APP.T(ID APP.OBJ)",
            "CREATE TABLE APP.T(ID APP.INT_LIST)",
            "ALTER TABLE APP.T ADD V APP.OBJ",
            "CREATE TABLE APP.T AS SELECT APP.OBJ(1) AS ID FROM DUAL"
    })
    void shouldKeepTablesUsingCustomTypesInTableScope(String sql) {
        final var analysis = analyzer.analyze(sql, "APP").requireSupported();

        assertThat(analysis.impact()).isEqualTo(Impact.TABLE);
        assertThat(analysis.tableId()).isEqualTo(new TableId(null, "APP", "T"));
        assertThat(analysis.tableStatement()).isNotNull();
    }

    @ParameterizedTest
    @MethodSource({ "createDirectoryStatements", "dropDirectoryStatements" })
    void shouldSkipDirectoryManagementWithoutChangingCapturedTables(String sql) {
        assertStatementDoesNotChangeCapturedTables(sql);
    }

    // DM SQL Language Manual, sections 3.13.1-3.13.2:
    // https://eco.dameng.com/document/dm/zh-cn/pm/definition-statement.html
    // CREATE [OR REPLACE] DIRECTORY [IF NOT EXISTS] name AS 'dir_path';
    // DROP DIRECTORY [IF EXISTS] name;
    // Directory names have no schema qualifier; paths may be absolute or relative.
    static Stream<String> createDirectoryStatements() {
        return Stream.concat(
                Stream.of("", "OR REPLACE ").flatMap(replacement -> Stream.of("", "IF NOT EXISTS ")
                        .flatMap(ifNotExists -> directoryNames().flatMap(name -> Stream.of("'/var/lib/dm/export'", "'E:\\test\\path'", "'../doc'")
                                .map(path -> "CREATE " + replacement + "DIRECTORY " + ifNotExists + name + " AS " + path)))),
                Stream.of(
                        "CREATE OR REPLACE DIRECTORY \"TESTDIR\" AS 'E:\\test\\path';",
                        "create /* directory options */ or replace directory if not exists \"Data Dir\" as '../doc';",
                        "CREATE DIRECTORY \"D\"\"IR\" AS '/tmp/it''s a directory';",
                        "CREATE DIRECTORY DATA_DIR AS '/tmp/; DROP TABLE APP.T; -- /* directory */';",
                        "CREATE DIRECTORY DATA_DIR AS q'[/tmp/it's; ALTER TABLE APP.T ADD V INT]';",
                        "CREATE DIRECTORY DATA_DIR AS n'/tmp/目录';"));
    }

    static Stream<String> dropDirectoryStatements() {
        return Stream.concat(
                Stream.of("", "IF EXISTS ").flatMap(ifExists -> directoryNames().map(name -> "DROP DIRECTORY " + ifExists + name)),
                Stream.of(
                        "DROP DIRECTORY \"D\"\"IR\";",
                        "drop /* directory options */ directory if exists \"Data Dir\";",
                        "DROP DIRECTORY IF EXISTS DATA_DIR; -- DROP TABLE APP.T"));
    }

    private static Stream<String> directoryNames() {
        // T also names the captured table in the schema-preservation assertion.
        return Stream.of("DATA_DIR", "T", "\"Data Dir\"", "\"目录对象\"", "\"DROP; TABLE APP.T\"");
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "CREATE DIRECTORY DATA_DIR AS '/tmp'",
            "CREATE DIRECTORY IF NOT EXISTS DATA_DIR AS '../doc'",
            "CREATE OR REPLACE DIRECTORY DATA_DIR AS '/tmp/new'",
            "CREATE OR REPLACE DIRECTORY IF NOT EXISTS \"T\" AS '/tmp/; DROP TABLE APP.T'",
            "DROP DIRECTORY DATA_DIR",
            "DROP DIRECTORY IF EXISTS \"T\""
    })
    void shouldNotSwallowTableChangesFollowingDirectoryManagement(String sql) {
        assertFollowingTableChangesAreNotSkipped(sql);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "CREATE DIRECTORY",
            "CREATE OR REPLACE DIRECTORY",
            "CREATE DIRECTORY IF NOT EXISTS",
            "CREATE DIRECTORY DATA_DIR",
            "CREATE DIRECTORY DIR AS",
            "CREATE DIRECTORY DATA_DIR '/tmp'",
            "CREATE DIRECTORY AS '/tmp'",
            "CREATE DIRECTORY DATA_DIR AS NULL",
            "CREATE DIRECTORY DATA_DIR AS 42",
            "CREATE DIRECTORY DATA_DIR AS \"/tmp\"",
            "CREATE DIRECTORY DATA_DIR AS ('/tmp')",
            "CREATE DIRECTORY DATA_DIR AS '/tmp' || '/export'",
            "CREATE DIRECTORY DATA_DIR AS '/tmp' UNKNOWN",
            "CREATE DIRECTORY APP.DATA_DIR AS '/tmp'",
            "CREATE DIRECTORY IF EXISTS DATA_DIR AS '/tmp'",
            "CREATE DIRECTORY DATA_DIR IF NOT EXISTS AS '/tmp'",
            "CREATE DIRECTORY OR REPLACE DATA_DIR AS '/tmp'",
            "CREATE OR REPLACE IF NOT EXISTS DIRECTORY DATA_DIR AS '/tmp'",
            "CREATE DIRECTORY DATA_DIR AS '/tmp' CASCADE",
            "DROP DIRECTORY",
            "DROP DIRECTORY IF EXISTS",
            "DROP DIRECTORY IF NOT EXISTS DATA_DIR",
            "DROP DIRECTORY APP.DATA_DIR",
            "DROP DIRECTORY DATA_DIR, OTHER_DIR",
            "DROP DIRECTORY DATA_DIR CASCADE",
            "DROP DIRECTORY DATA_DIR RESTRICT",
            "DROP DIRECTORY DATA_DIR AS '/tmp'",
            "ALTER DIRECTORY DATA_DIR AS '/tmp/new'",
            "REPLACE DIRECTORY DATA_DIR AS '/tmp/new'",
            "CREATE DIRECTORY DATA_DIR AS '/tmp' INSERT INTO APP.T(ID) VALUES(1)",
            "CREATE DIRECTORY DATA_DIR AS '/tmp'; UPDATE APP.T SET V='changed'",
            "CREATE DIRECTORY DATA_DIR AS '/tmp' DELETE FROM APP.T",
            "CREATE DIRECTORY DATA_DIR AS '/tmp' CALL APP.P()",
            "CREATE DIRECTORY DATA_DIR AS '/tmp' EXECUTE APP.P 1",
            "DROP DIRECTORY DATA_DIR; DELETE FROM APP.T"
    })
    void shouldRejectIncompleteOrCompoundDirectoryStatements(String sql) {
        assertUnsupported(sql);
    }

    @ParameterizedTest
    @MethodSource({ "createTablespaceStatements", "alterTablespaceStatements", "dropTablespaceStatements" })
    void shouldSkipTablespaceManagementWithoutChangingCapturedTables(String sql) {
        assertStatementDoesNotChangeCapturedTables(sql);
        assertThat(DamengParseTree.parseScope(sql, false).tree().statement(0).nonTableStatement().tablespaceStatement()).isNotNull();
    }

    // DM SQL Language Manual, sections 3.4.1-3.4.3, including the syntax diagrams:
    // https://eco.dameng.com/document/dm/zh-cn/pm/definition-statement.html
    // Database Management, sections 14.1.3 and 14.2.2: DROP TABLESPACE requires
    // an unused tablespace, and DROP DATAFILE requires its objects to be removed first.
    // https://eco.dameng.com/document/dm/zh-cn/pm/database-management.html
    // ASM/DSC/DPC options are grammar coverage; they require the corresponding deployment.
    // The current manual spells out bare UNLIMITED; DM 8.1.3.162 accepts MAXSIZE UNLIMITED.
    // That version rejects bare KEEP and FILE LIMIT, but accepts quoted "KEEP".
    static Stream<String> createTablespaceStatements() {
        return Stream.of(
                Stream.of("", "IF NOT EXISTS ").flatMap(ifNotExists -> tablespaceNames()
                        .flatMap(name -> tablespaceDataFiles().map(files -> "CREATE TABLESPACE " + ifNotExists + name + " DATAFILE " + files))),
                Stream.of("", "IF NOT EXISTS ").flatMap(ifNotExists -> tablespaceCreateOptions()
                        .map(options -> "CREATE TABLESPACE " + ifNotExists + "TS DATAFILE '/tmp/ts.dbf' SIZE 128 " + options)),
                Stream.of(
                        "create /* tablespace */ tablespace if not exists \"TS Space\" datafile './data/ts.dbf' size 128;",
                        "CREATE TABLESPACE \"TS\"\"Name\" DATAFILE 'it''s; DROP TABLE APP.T.dbf' SIZE 128;",
                        "CREATE TABLESPACE TS DATAFILE q'[/tmp/it's; ALTER TABLE APP.T ADD V INT.dbf]' SIZE 128;",
                        "CREATE TABLESPACE TS DATAFILE 'd:\\ts.dbf' SIZE 128 WITH HUGE PATH 'd:\\huge';",
                        "CREATE TABLESPACE IF NOT EXISTS TS DATAFILE '/tmp/ts.dbf' MIRROR '/tmp/mirror.dbf' SIZE 128"
                                + " AUTOEXTEND ON NEXT 64 MAXSIZE 1024, '/tmp/ts2.dbf' SIZE 64 AUTOEXTEND OFF"
                                + " CACHE = KEEP ENCRYPT WITH AES256_CFB BY \"Abcd1234\" STRIPING 128 HIGH"
                                + " WITH HUGE PATH '/tmp/huge' FILE LIMIT 256 STORAGE(ON RAFT1)",
                        // DBMS_METADATA.GET_DDL example in 3.4.1 includes COPY and MICRO.
                        "CREATE TABLESPACE \"TS03\" DATAFILE '/data/ts03.dbf' SIZE 32 AUTOEXTEND ON NEXT 64 MAXSIZE 16777215"
                                + " CACHE = \"NORMAL\" ENCRYPT WITH OPENSSL_SM4_OFB_V1 BY WRAPPED '0xDEFBACCB9F0D15FB'"
                                + " COPY 0 MICRO WITH HUGE PATH '/data/ts03' MICRO;",
                        // Exported definitions observed on DM 8.1.3.162.
                        "CREATE TABLESPACE \"TS\" DATAFILE '/data/ts.dbf' SIZE 64 AUTOEXTEND OFF CACHE = \"NORMAL\" COPY 0 MICRO;",
                        "CREATE TABLESPACE \"TS\" DATAFILE '/data/ts.dbf' SIZE 32 AUTOEXTEND ON NEXT 0 MAXSIZE 0"
                                + " CACHE = \"NORMAL\" COPY 0 MICRO WITH HUGE PATH '/data/huge' MICRO;"))
                .flatMap(statements -> statements);
    }

    private static Stream<String> tablespaceNames() {
        // T is also the captured table name; tablespaces have no schema qualifier.
        return Stream.of("TS", "T", "\"TS Space\"", "\"表空间\"", "\"DROP; TABLE APP.T\"");
    }

    private static Stream<String> tablespaceAutoextendClauses() {
        return Stream.of("", " AUTOEXTEND OFF", " AUTOEXTEND ON", " AUTOEXTEND ON NEXT 64",
                " AUTOEXTEND ON MAXSIZE 1024", " AUTOEXTEND ON NEXT 64 MAXSIZE 1024",
                " AUTOEXTEND ON UNLIMITED", " AUTOEXTEND ON NEXT 64 UNLIMITED",
                " AUTOEXTEND ON MAXSIZE UNLIMITED", " AUTOEXTEND ON NEXT 64 MAXSIZE UNLIMITED");
    }

    private static Stream<String> tablespaceDataFiles() {
        return Stream.concat(
                Stream.of("", " MIRROR '/tmp/ts_mirror.dbf'").flatMap(mirror -> tablespaceAutoextendClauses()
                        .map(autoextend -> "'/tmp/ts.dbf'" + mirror + " SIZE 128" + autoextend)),
                Stream.of("'/tmp/ts1.dbf' SIZE 128, '/tmp/ts2.dbf' SIZE 64 AUTOEXTEND ON",
                        "'+DATA/ts1.dbf' SIZE 128 AUTOEXTEND ON NEXT 32 MAXSIZE 1024, '+DATA/ts2.dbf' SIZE 128 AUTOEXTEND OFF"));
    }

    private static Stream<String> tablespaceCreateOptions() {
        return Stream.of(
                Stream.of("CACHE = NORMAL", "CACHE = KEEP", "CACHE = \"NORMAL\"", "CACHE = \"KEEP\"",
                        "ENCRYPT WITH AES256_CFB", "ENCRYPT WITH AES256_CFB BY Abcd1234",
                        "ENCRYPT WITH AES256_CFB BY \"Abcd1234\"", "ENCRYPT WITH OPENSSL_SM4_OFB_V1 BY WRAPPED '0x1234'",
                        "WITH HUGE PATH '/tmp/huge'", "FILE LIMIT 64", "FILE LIMIT 8191",
                        "STORAGE(ON RAFT1)", "STORAGE(ON BP1)", "STORAGE(ON \"Raft Group\")",
                        "WITH HUGE PATH '+DATA/huge' FILE LIMIT 256 STORAGE(ON BP1)"),
                Stream.of("", "STRIPING 0 ", "STRIPING 32 ", "STRIPING 64 ", "STRIPING 128 ", "STRIPING 256 ")
                        .flatMap(striping -> Stream.of("", "HIGH", "NORMAL", "EXTERNAL").map(mirror -> (striping + mirror).trim()))
                        .filter(option -> !option.isEmpty()))
                .flatMap(options -> options);
    }

    static Stream<String> alterTablespaceStatements() {
        return Stream.concat(tablespaceNames().flatMap(name -> tablespaceAlterClauses().map(clause -> "ALTER TABLESPACE " + name + " " + clause)),
                Stream.of(
                        "alter /* tablespace */ tablespace \"TS Space\" rename to \"TS\"\"Renamed\";",
                        "ALTER TABLESPACE TS RENAME DATAFILE 'old; DROP TABLE APP.T.dbf' TO 'new; ALTER TABLE APP.T ADD V INT.dbf';",
                        "ALTER TABLESPACE TS DROP DATAFILE q'[/tmp/it's; DROP TABLE APP.T.dbf]';"));
    }

    private static Stream<String> tablespaceAlterClauses() {
        return Stream.of(
                Stream.of("ONLINE", "OFFLINE", "CORRUPT", "RENAME TO TS_NEW", "RENAME TO \"TS New\"",
                        "RENAME DATAFILE '/tmp/old.dbf' TO '/tmp/new.dbf'",
                        "RENAME DATAFILE '/tmp/a.dbf', '/tmp/b.dbf' TO '/tmp/c.dbf', '/tmp/d.dbf'",
                        "RESIZE DATAFILE '/tmp/ts.dbf' TO 256", "RESIZE DATAFILE 'TEMP.DBF' TO 512 ON RAFT1",
                        "RESIZE DATAFILE 'TEMP.DBF' TO 512 ON \"Raft Group\"",
                        "CACHE = NORMAL", "CACHE = KEEP", "CACHE = \"NORMAL\"", "CACHE = \"KEEP\"",
                        "OPTIMIZE 0", "OPTIMIZE 1", "ADD HUGE PATH '/tmp/huge'", "ADD HUGE PATH '+DATA/huge'",
                        "DROP DATAFILE '/tmp/ts2.dbf'", "FILE LIMIT 64", "FILE LIMIT 8191"),
                tablespaceDataFiles().map(files -> "ADD DATAFILE " + files),
                Stream.of("'/tmp/ts.dbf'", "'/tmp/a.dbf', '/tmp/b.dbf'").flatMap(files -> tablespaceAutoextendClauses()
                        .map(autoextend -> "DATAFILE " + files + autoextend)))
                .flatMap(clauses -> clauses);
    }

    static Stream<String> dropTablespaceStatements() {
        return Stream.concat(Stream.of("", "IF EXISTS ").flatMap(ifExists -> tablespaceNames().map(name -> "DROP TABLESPACE " + ifExists + name)),
                Stream.of("drop /* tablespace */ tablespace if exists \"TS Space\";", "DROP TABLESPACE \"TS\"\"Name\";",
                        "DROP TABLESPACE IF EXISTS TS; -- DROP TABLE APP.T"));
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "CREATE TABLESPACE TS DATAFILE '/tmp/ts.dbf' SIZE 128",
            "CREATE TABLESPACE IF NOT EXISTS TS DATAFILE '/tmp/ts.dbf' SIZE 128 WITH HUGE PATH '/tmp/huge'",
            "CREATE TABLESPACE TS DATAFILE '/tmp/ts.dbf' SIZE 128 ENCRYPT WITH AES256_CFB BY \"Abcd1234\"",
            "ALTER TABLESPACE TS ADD DATAFILE '/tmp/ts2.dbf' SIZE 64",
            "ALTER TABLESPACE TS DROP DATAFILE '/tmp/ts2.dbf'",
            "ALTER TABLESPACE TS RENAME DATAFILE '/tmp/old.dbf' TO '/tmp/new.dbf'",
            "ALTER TABLESPACE TS RENAME TO TS_NEW",
            "ALTER TABLESPACE TS DATAFILE '/tmp/ts.dbf' AUTOEXTEND ON NEXT 64 MAXSIZE 1024",
            "ALTER TABLESPACE TS RESIZE DATAFILE '/tmp/ts.dbf' TO 256 ON RAFT1",
            "ALTER TABLESPACE TS ONLINE",
            "DROP TABLESPACE TS", "DROP TABLESPACE IF EXISTS TS"
    })
    void shouldNotSwallowTableChangesFollowingTablespaceManagement(String sql) {
        assertFollowingTableChangesAreNotSkipped(sql);
        for (final var separator : new String[]{ " ", "; " }) {
            assertUnsupported(sql + separator + "INSERT INTO APP.T(ID) VALUES(1)");
            assertUnsupported(sql + separator + "UPDATE APP.T SET V='changed'");
            assertUnsupported(sql + separator + "DELETE FROM APP.T");
            assertUnsupported(sql + separator + "CALL APP.P()");
            assertUnsupported(sql + separator + "EXECUTE APP.P 1");
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "CREATE TABLESPACE", "CREATE TABLESPACE TS", "CREATE TABLESPACE IF NOT EXISTS",
            "CREATE TABLESPACE TS DATAFILE", "CREATE TABLESPACE TS DATAFILE '/tmp/ts.dbf'",
            "CREATE TABLESPACE TS DATAFILE '/tmp/ts.dbf' SIZE", "CREATE TABLESPACE TS DATAFILE /tmp/ts.dbf SIZE 128",
            "CREATE TABLESPACE TS DATAFILE '/tmp/ts.dbf' MIRROR SIZE 128",
            "CREATE TABLESPACE TS DATAFILE '/tmp/ts.dbf' SIZE 128,",
            "CREATE TABLESPACE TS DATAFILE '/tmp/ts.dbf' SIZE 128 AUTOEXTEND",
            "CREATE TABLESPACE TS DATAFILE '/tmp/ts.dbf' SIZE 128 AUTOEXTEND ON NEXT",
            "CREATE TABLESPACE TS DATAFILE '/tmp/ts.dbf' SIZE 128 AUTOEXTEND OFF NEXT 64",
            "CREATE TABLESPACE TS DATAFILE '/tmp/ts.dbf' SIZE 128 AUTOEXTEND ON MAXSIZE",
            "CREATE TABLESPACE TS DATAFILE '/tmp/ts.dbf' SIZE 128 CACHE NORMAL",
            "CREATE TABLESPACE TS DATAFILE '/tmp/ts.dbf' SIZE 128 CACHE EQU NORMAL",
            "CREATE TABLESPACE TS DATAFILE '/tmp/ts.dbf' SIZE 128 CACHE =",
            "CREATE TABLESPACE TS DATAFILE '/tmp/ts.dbf' SIZE 128 ENCRYPT WITH",
            "CREATE TABLESPACE TS DATAFILE '/tmp/ts.dbf' SIZE 128 ENCRYPT WITH AES256_ECB BY",
            "CREATE TABLESPACE TS DATAFILE '/tmp/ts.dbf' SIZE 128 ENCRYPT WITH AES256_ECB BY WRAPPED",
            "CREATE TABLESPACE TS DATAFILE '/tmp/ts.dbf' SIZE 128 ENCRYPT WITH AES256_CFB BY 'Abcd1234'",
            "CREATE TABLESPACE TS DATAFILE '/tmp/ts.dbf' SIZE 128 ENCRYPT WITH AES256_CFB BY WRAPPED \"0x1234\"",
            "CREATE TABLESPACE TS DATAFILE '/tmp/ts.dbf' SIZE 128 STRIPING",
            "CREATE TABLESPACE TS DATAFILE '/tmp/ts.dbf' SIZE 128 WITH HUGE PATH",
            "CREATE TABLESPACE TS DATAFILE '/tmp/ts.dbf' SIZE 128 FILE LIMIT",
            "CREATE TABLESPACE TS DATAFILE '/tmp/ts.dbf' SIZE 128 STORAGE(ON)",
            "CREATE TABLESPACE TS DATAFILE '/tmp/ts.dbf' SIZE 128 STORAGE(ON RAFT1",
            "CREATE TABLESPACE TS DATAFILE '/tmp/ts.dbf' SIZE 128 UNKNOWN",
            "CREATE TABLESPACE APP.TS DATAFILE '/tmp/ts.dbf' SIZE 128",
            "CREATE OR REPLACE TABLESPACE TS DATAFILE '/tmp/ts.dbf' SIZE 128",
            "CREATE TABLESPACE IF EXISTS TS DATAFILE '/tmp/ts.dbf' SIZE 128",
            "ALTER TABLESPACE", "ALTER TABLESPACE TS", "ALTER TABLESPACE TS UNKNOWN",
            "ALTER TABLESPACE TS ONLINE OFFLINE", "ALTER TABLESPACE TS RENAME", "ALTER TABLESPACE TS RENAME TO",
            "ALTER TABLESPACE TS RENAME DATAFILE '/tmp/old.dbf'", "ALTER TABLESPACE TS RENAME DATAFILE '/tmp/old.dbf' TO",
            "ALTER TABLESPACE TS ADD DATAFILE", "ALTER TABLESPACE TS ADD DATAFILE '/tmp/ts.dbf' SIZE",
            "ALTER TABLESPACE TS RESIZE DATAFILE '/tmp/ts.dbf' TO", "ALTER TABLESPACE TS RESIZE DATAFILE '/tmp/ts.dbf' TO 256 ON",
            "ALTER TABLESPACE TS DATAFILE", "ALTER TABLESPACE TS DATAFILE '/tmp/ts.dbf' AUTOEXTEND OFF MAXSIZE 512",
            "ALTER TABLESPACE TS CACHE =", "ALTER TABLESPACE TS OPTIMIZE", "ALTER TABLESPACE TS ADD HUGE PATH",
            "ALTER TABLESPACE TS DROP DATAFILE", "ALTER TABLESPACE TS DROP DATAFILE '/tmp/ts.dbf', '/tmp/ts2.dbf'",
            "ALTER TABLESPACE TS FILE LIMIT", "ALTER TABLESPACE IF EXISTS TS ONLINE",
            "ALTER TABLESPACE APP.TS ONLINE", "ALTER TABLESPACE TS INCLUDING CONTENTS",
            "DROP TABLESPACE", "DROP TABLESPACE IF EXISTS", "DROP TABLESPACE IF NOT EXISTS TS", "DROP TABLESPACE APP.TS",
            "DROP TABLESPACE TS, TS2", "DROP TABLESPACE TS CASCADE", "DROP TABLESPACE TS RESTRICT",
            "DROP TABLESPACE TS INCLUDING CONTENTS", "DROP TABLESPACE TS INCLUDING CONTENTS AND DATAFILES CASCADE CONSTRAINTS"
    })
    void shouldRejectIncompleteOrUnsupportedTablespaceStatements(String sql) {
        assertUnsupported(sql);
    }

    @ParameterizedTest
    @ValueSource(strings = { "AUTOEXTEND", "COPY", "CORRUPT", "ENCRYPT", "FILE", "HIGH", "KEEP", "MAXSIZE", "MICRO", "MIRROR",
            "NORMAL", "OFFLINE", "OPTIMIZE", "PATH", "RESIZE", "STRIPING", "WRAPPED" })
    void shouldKeepTablespaceKeywordsUsableAsColumnAndExpressionNames(String name) {
        final var parser = new DamengDdlParser();
        final var tables = new Tables();
        parser.parse("CREATE TABLE APP.T(" + name + " INT)", tables, "APP");
        assertThat(analyzer.analyze("CREATE TABLE APP.COPY AS SELECT " + name + " FROM APP.T", "APP").requireSupported().isCtas()).isTrue();
    }

    @ParameterizedTest
    @MethodSource("createExternalTableStatements")
    void shouldTrackExternalTableDefinitionsWithoutChangingCapturedRows(String sql) {
        final var analysis = analyzer.analyze(sql, "APP").requireSupported();
        assertThat(analysis.impact()).isEqualTo(Impact.TABLE);
        assertThat(analysis.tableId()).isEqualTo(new TableId(null, "APP", "EXT"));
        assertThat(analysis.renamedTableId()).isNull();
        assertThat(analysis.tableStatement().createExternalTable()).isNotNull();

        final var parser = new DamengDdlParser();
        final var tables = new Tables();
        final var captured = new TableId(null, "APP", "T");
        parser.parse("CREATE TABLE APP.T(ID INT PRIMARY KEY)", tables, "APP");
        final var original = tables.forTable(captured);

        assertThat(parser.plan(analysis, tables)).singleElement().satisfies(change -> {
            assertThat(change.type()).isEqualTo(DamengDdlParser.Type.CREATE);
            assertThat(DamengTableType.isExternal(change.table())).isTrue();
            assertThat(change.table().columns()).isEmpty();
        });
        assertThat(tables.tableIds()).containsExactly(captured);
        assertThat(parser.parse(sql, tables, "APP")).hasSize(1);
        assertThat(DamengTableType.isExternal(tables.forTable(analysis.tableId()))).isTrue();
        assertThat(tables.forTable(captured)).isSameAs(original);
    }

    // DM SQL Language Manual, sections 3.5.1.2 and 3.5.3:
    // https://eco.dameng.com/document/dm/zh-cn/pm/definition-statement.html
    // External tables are read-only, but DROP TABLE carries no EXTERNAL discriminator.
    // Keep table identity and durable type metadata; do not classify these as NO_TABLE_CHANGE.
    // ON raft and MISSING FIELD VALUES ARE NULL follow the current manual; DM 8.1.3.162
    // rejects these options. These cases verify grammar coverage, not server acceptance.
    static Stream<String> createExternalTableStatements() {
        return Stream.of(
                Stream.of("", "IF NOT EXISTS ").flatMap(ifNotExists -> Stream.of("EXT", "APP.EXT", "\"APP\".\"EXT\"")
                        .flatMap(name -> Stream.of("", " ON RAFT1").flatMap(raft -> Stream.of("", "DATAFILE ")
                                .map(file -> "CREATE EXTERNAL TABLE " + ifNotExists + name + "(ID INT, V VARCHAR(30) COMMENT 'value') FROM "
                                        + file + "DEFAULT DIRECTORY DATA_DIR LOCATION ('ext.dat'" + raft + ")")))),
                Stream.of("", "IF NOT EXISTS ").flatMap(ifNotExists -> externalTableParameterClauses()
                        .map(parameters -> "CREATE EXTERNAL TABLE " + ifNotExists + "APP.EXT(ID INT, V VARCHAR(30))"
                                + " FROM DATAFILE DEFAULT DIRECTORY DATA_DIR LOCATION ('ext.csv') PARMS(" + parameters + ")")),
                Stream.of(
                        "create /* external table */ external table if not exists \"APP\".\"EXT\"(\"ID\" int comment 'key')"
                                + " from datafile default directory \"Data Dir\" location ('ext.csv' on \"Raft Group\");",
                        "CREATE EXTERNAL TABLE APP.EXT(\"DROP; TABLE APP.T\" INT COMMENT 'column; DROP TABLE APP.T')"
                                + " FROM DEFAULT DIRECTORY \"D; DROP TABLE APP.T\" LOCATION ('ext; DROP TABLE APP.T.ctl')",
                        "CREATE EXTERNAL TABLE APP.EXT(ID INT) FROM DATAFILE DEFAULT DIRECTORY DATA_DIR"
                                + " LOCATION ('it''s.csv') PARMS(NULL_STR q'[it's; DROP TABLE APP.T]')",
                        "CREATE EXTERNAL TABLE APP.EXT(ID INT,V CLOB,B BLOB,N DECIMAL(12,2),D TIMESTAMP(6),I INTERVAL DAY(3) TO SECOND(6))"
                                + " FROM DEFAULT DIRECTORY DATA_DIR LOCATION ('ext.ctl');"))
                .flatMap(statements -> statements);
    }

    private static Stream<String> externalTableParameterClauses() {
        return Stream.of(
                "FIELDS DELIMITED BY '|'",
                "FIELDS DELIMITED BY 0x7c",
                "FIELDS DELIMITED BY '|' || '|'",
                "RECORDS DELIMITED BY 0x0a",
                "RECORDS DELIMITED BY 0x0d0a",
                "RECORDS DELIMITED BY '\n'",
                "ERRORS 10",
                "ERRORS 4294967294",
                "BADFILE 'ext.bad'",
                "LOG 'ext.log'",
                "NULL_STR 'NULL'",
                "SKIP 1",
                "CHARACTER_CODE 'GBK'",
                "CHARACTER_CODE 'UTF-8'",
                "CHARACTER_CODE 'SINGLE_BYTE'",
                "CHARACTER_CODE 'EUC-KR'",
                "MISSING FIELD VALUES ARE NULL",
                "FIELDS DELIMITED BY '|', RECORDS DELIMITED BY 0x0a, ERRORS 10, BADFILE 'ext.bad', LOG 'ext.log',"
                        + " NULL_STR 'NULL', SKIP 1, CHARACTER_CODE 'UTF-8', MISSING FIELD VALUES ARE NULL");
    }

    @ParameterizedTest
    @MethodSource("externalTableDropScenarios")
    void shouldDropExternalTableWithoutChangingCapturedTables(String createSql, String dropSql) {
        final var parser = new DamengDdlParser();
        final var tables = new Tables();
        final var capturedId = new TableId(null, "APP", "T");
        final var externalId = new TableId(null, "APP", "EXT");
        parser.parse("CREATE TABLE APP.T(ID INT PRIMARY KEY)", tables, "APP");
        parser.parse(createSql, tables, "APP");
        final var captured = tables.forTable(capturedId);
        final var external = tables.forTable(externalId);
        assertThat(DamengTableType.isExternal(external)).isTrue();
        assertThat(external.columns()).isEmpty();

        final var analysis = analyzer.analyze(dropSql, "APP").requireSupported();
        assertThat(analysis.impact()).isEqualTo(Impact.TABLE);
        assertThat(analysis.tableId()).isEqualTo(externalId);
        assertThat(analysis.tableStatement().dropTable()).isNotNull();
        final var planned = parser.plan(analysis, tables);
        assertThat(planned).singleElement().satisfies(change -> {
            assertThat(change.type()).isEqualTo(DamengDdlParser.Type.DROP);
            assertThat(change.tableId()).isEqualTo(externalId);
            assertThat(change.table()).isEqualTo(external);
            assertThat(DamengTableType.isExternal(change.table())).isTrue();
        });
        assertThat(tables.tableIds()).containsExactlyInAnyOrder(capturedId, externalId);
        assertThat(tables.forTable(externalId)).isSameAs(external);
        assertThat(tables.forTable(capturedId)).isSameAs(captured);

        assertThat(parser.parse(dropSql, tables, "APP")).isEqualTo(planned);
        assertThat(tables.forTable(externalId)).isNull();
        assertThat(tables.tableIds()).containsExactly(capturedId);
        assertThat(tables.forTable(capturedId)).isSameAs(captured);
    }

    static Stream<Arguments> externalTableDropScenarios() {
        return Stream.of(
                "CREATE EXTERNAL TABLE APP.EXT(ID INT) FROM DEFAULT DIRECTORY DATA_DIR LOCATION ('ext.ctl')",
                "CREATE EXTERNAL TABLE APP.EXT(ID INT) FROM DATAFILE DEFAULT DIRECTORY DATA_DIR LOCATION ('ext.csv')")
                .flatMap(createSql -> dropExternalTableStatements().map(dropSql -> Arguments.of(createSql, dropSql)));
    }

    @ParameterizedTest
    @MethodSource("dropExternalTableStatements")
    void shouldApplyDropTableToOrdinaryTableMetadata(String sql) {
        final var analysis = analyzer.analyze(sql, "APP").requireSupported();
        final var tableId = new TableId(null, "APP", "EXT");
        assertThat(analysis.impact()).isEqualTo(Impact.TABLE);
        assertThat(analysis.tableId()).isEqualTo(tableId);
        assertThat(analysis.tableStatement().dropTable()).isNotNull();

        // The same SQL can drop an ordinary captured table and must update its history.
        final var parser = new DamengDdlParser();
        final var tables = new Tables();
        parser.parse("CREATE TABLE APP.EXT(ID INT PRIMARY KEY)", tables, "APP");
        final var original = tables.forTable(tableId);
        assertThat(DamengTableType.isExternal(original)).isFalse();
        assertThat(parser.plan(analysis, tables)).singleElement()
                .satisfies(change -> assertThat(change.type()).isEqualTo(DamengDdlParser.Type.DROP));
        assertThat(tables.forTable(tableId)).isSameAs(original);
        assertThat(parser.parse(sql, tables, "APP")).singleElement().satisfies(change -> {
            assertThat(change.type()).isEqualTo(DamengDdlParser.Type.DROP);
            assertThat(change.table()).isEqualTo(original);
            assertThat(DamengTableType.isExternal(change.table())).isFalse();
        });
        assertThat(tables.tableIds()).isEmpty();
    }

    static Stream<String> dropExternalTableStatements() {
        // Section 3.5.3 says external tables do not require RESTRICT or CASCADE.
        return Stream.of("", "IF EXISTS ").flatMap(ifExists -> Stream.of("EXT", "APP.EXT", "\"APP\".\"EXT\"")
                .flatMap(name -> Stream.of("", " RESTRICT", " CASCADE").map(option -> "DROP TABLE " + ifExists + name + option)));
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "CREATE EXTERNAL TABLE APP.EXT(ID INT) FROM DEFAULT DIRECTORY D LOCATION ('ext.ctl')",
            "CREATE EXTERNAL TABLE IF NOT EXISTS APP.EXT(ID INT) FROM DATAFILE DEFAULT DIRECTORY D LOCATION ('ext.csv' ON RAFT1)",
            "CREATE EXTERNAL TABLE APP.EXT(ID INT COMMENT 'DROP TABLE APP.T') FROM DATAFILE DEFAULT DIRECTORY D"
                    + " LOCATION ('ext.csv') PARMS(FIELDS DELIMITED BY '|', RECORDS DELIMITED BY 0x0a)",
            "DROP TABLE APP.EXT",
            "DROP TABLE IF EXISTS APP.EXT"
    })
    void shouldNotSwallowTableChangesFollowingExternalTableManagement(String sql) {
        assertFollowingTableChangesAreNotSkipped(sql);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "CREATE EXTERNAL TABLE",
            "CREATE EXTERNAL TABLE IF NOT EXISTS",
            "CREATE EXTERNAL TABLE APP.EXT(ID INT)",
            "CREATE EXTERNAL TABLE APP.EXT() FROM DEFAULT DIRECTORY D LOCATION ('ext.ctl')",
            "CREATE EXTERNAL TABLE APP.EXT(ID INT) FROM",
            "CREATE EXTERNAL TABLE APP.EXT(ID INT) FROM DEFAULT DIRECTORY",
            "CREATE EXTERNAL TABLE APP.EXT(ID INT) FROM DEFAULT DIRECTORY D",
            "CREATE EXTERNAL TABLE APP.EXT(ID INT) FROM DEFAULT DIRECTORY D LOCATION ()",
            "CREATE EXTERNAL TABLE APP.EXT(ID INT) FROM DEFAULT DIRECTORY D LOCATION ('ext.ctl' ON)",
            "CREATE EXTERNAL TABLE APP.EXT(ID INT) FROM DEFAULT DIRECTORY D LOCATION ('ext.ctl') PARMS(ERRORS 10)",
            "CREATE EXTERNAL TABLE APP.EXT(ID INT) FROM DATAFILE DEFAULT DIRECTORY D LOCATION ('ext.csv') PARMS()",
            "CREATE EXTERNAL TABLE APP.EXT(ID INT) FROM DATAFILE DEFAULT DIRECTORY D LOCATION ('ext.csv') PARMS(FIELDS DELIMITED BY)",
            "CREATE EXTERNAL TABLE APP.EXT(ID INT) FROM DATAFILE DEFAULT DIRECTORY D LOCATION ('ext.csv') PARMS(ERRORS)",
            "CREATE EXTERNAL TABLE APP.EXT(ID INT) FROM DATAFILE DEFAULT DIRECTORY D LOCATION ('ext.csv') PARMS(CHARACTER_CODE)",
            "CREATE EXTERNAL TABLE APP.EXT(ID INT) FROM DATAFILE DEFAULT DIRECTORY D LOCATION ('ext.csv') PARMS(CHARACTER_CODE GBK)",
            "CREATE EXTERNAL TABLE APP.EXT(ID INT) FROM DATAFILE DEFAULT DIRECTORY D LOCATION ('ext.csv') PARMS(CHARACTER_CODE UTF-8)",
            "CREATE EXTERNAL TABLE APP.EXT(ID INT) FROM DATAFILE DEFAULT DIRECTORY D LOCATION ('ext.csv') PARMS(CHARACTER_CODE \"UTF-8\")",
            "CREATE EXTERNAL TABLE APP.EXT(ID INT COMMENT) FROM DEFAULT DIRECTORY D LOCATION ('ext.ctl')",
            "CREATE EXTERNAL TABLE APP.EXT(ID INT PRIMARY KEY) FROM DEFAULT DIRECTORY D LOCATION ('ext.ctl')",
            "CREATE EXTERNAL TABLE APP.EXT(ID INT NOT NULL) FROM DEFAULT DIRECTORY D LOCATION ('ext.ctl')",
            "CREATE EXTERNAL TABLE APP.EXT(ID INT) AS SELECT ID FROM APP.T",
            "CREATE OR REPLACE EXTERNAL TABLE APP.EXT(ID INT) FROM DEFAULT DIRECTORY D LOCATION ('ext.ctl')",
            "CREATE EXTERNAL TABLE IF EXISTS APP.EXT(ID INT) FROM DEFAULT DIRECTORY D LOCATION ('ext.ctl')",
            "CREATE EXTERNAL TABLE APP.EXT(ID INT) FROM DEFAULT DIRECTORY D LOCATION ('ext.ctl') PARTITION BY HASH(ID) PARTITIONS 2",
            "CREATE EXTERNAL TABLE APP.EXT(ID INT) FROM DEFAULT DIRECTORY D LOCATION ('ext.ctl'); INSERT INTO APP.T VALUES(1)",
            "DROP EXTERNAL TABLE APP.EXT",
            "DROP EXTERNAL TABLE IF EXISTS APP.EXT",
            "DROP TABLE IF NOT EXISTS APP.EXT"
    })
    void shouldRejectIncompleteOrCompoundExternalTableDefinitions(String sql) {
        assertUnsupported(sql);
    }

    private void assertFollowingTableChangesAreNotSkipped(String sql) {
        for (final var separator : new String[]{ " ", "; " }) {
            assertUnsupported(sql + separator + "CREATE TABLE APP.T(ID INT)");
            assertUnsupported(sql + separator + "ALTER TABLE APP.T ADD V INT");
            assertUnsupported(sql + separator + "DROP TABLE APP.T");
            assertUnsupported(sql + separator + "TRUNCATE TABLE APP.T");
            assertUnsupported(sql + separator + "RENAME TABLE APP.T TO APP.U");
        }
    }

    @ParameterizedTest
    @MethodSource("additionalNonTableFamilies")
    void shouldRecognizeConfirmedNonTableFamiliesThroughTheSameParseTreeBranch(String sql) {
        assertNonTableStatement(sql);
    }

    static Stream<String> additionalNonTableFamilies() {
        return Stream.of(
                "CREATE ARRAY INDEX APP.I ON APP.T(V)",
                "CREATE OR REPLACE ARRAY INDEX IF NOT EXISTS APP.I ON APP.T(V)",
                "CREATE LINK L CONNECT 'DPI' WITH USER01 IDENTIFIED BY DMsys_123 USING '192.168.0.31:5369'",
                "CREATE PUBLIC LINK L CONNECT 'DPI' WITH USER01 IDENTIFIED BY DMsys_123 USING '192.168.0.31:5369'",
                "CREATE OR REPLACE LINK L CONNECT 'DPI' WITH USER01 IDENTIFIED BY DMsys_123 USING '192.168.0.31:5369'",
                "CREATE OR REPLACE PUBLIC LINK IF NOT EXISTS L CONNECT 'DAMENG' WITH USER01 IDENTIFIED BY DMsys_123 USING 'server1:5236;server2:5236'",
                "DROP LINK L",
                "DROP PUBLIC LINK IF EXISTS APP.L",
                "CREATE OPERATOR @^ (FUNCTION FUNC_OP_TEST, LEFTARG INT, RIGHTARG INT)",
                "DROP OPERATOR IF EXISTS @^(INT,INT)",
                "CREATE JAVA CLASS APP.J { int a; }",
                "CREATE OR REPLACE JAVA CLASS APP.J { public static int add(int a,int b) { return a+b; } }",
                "CREATE JAVA PUBLIC CLASS APP.J { public int a; }",
                "CREATE JAVA ABSTRACT CLASS APP.J { public abstract int f(); }",
                "CREATE JAVA FINAL CLASS APP.J { public final int a=1; }",
                "CREATE OR REPLACE JAVA PUBLIC ABSTRACT CLASS APP.J { public abstract int f(); }",
                "CREATE JAVA PUBLIC FINAL CLASS APP.J { public final int a=1; }",
                "CREATE JAVA CLASS APP.J EXTENDS APP.BASE { public int a; }",
                "ALTER JAVA CLASS APP.J COMPILE",
                "ALTER JAVA CLASS APP.J COMPILE DEBUG",
                "ALTER DATABASE ADD LOGFILE 'file.log'",
                "ALTER DATABASE DROP LOGFILE 'file.log'",
                "ALTER DATABASE RENAME LOGFILE 'old.log' TO 'new.log'",
                "ALTER SYSTEM SWITCH LOGFILE",
                "ALTER SESSION SET NLS_DATE_FORMAT='YYYY-MM-DD'",
                "CREATE TABLESPACE TS DATAFILE 'file.dbf' SIZE 128",
                "ALTER TABLESPACE TS DROP DATAFILE 'file.dbf'",
                "ALTER TABLESPACE TS RENAME DATAFILE 'old.dbf' TO 'new.dbf'",
                "DROP TABLESPACE TS",
                "SET SCHEMA APP",
                "SET IDENTITY_INSERT APP.T ON",
                "DROP TYPE APP.O",
                "DROP TYPE BODY IF EXISTS APP.O",
                "DROP CLASS IF EXISTS APP.C",
                "DROP CLASS BODY APP.C",
                "CREATE CONTEXT CTX USING",
                "CREATE LIBRARY APP.L AS",
                "CREATE DOMAIN APP.D AS",
                "CREATE PROFILE P LIMIT",
                "ALTER LIBRARY APP.L",
                "ALTER PROFILE P");
    }

    // DM SQL Language Manual, section 3.8: full-text indexes are distinct from application contexts.
    @ParameterizedTest
    @ValueSource(strings = {
            "CREATE CONTEXT INDEX I ON APP.T(V)",
            "CREATE CONTEXT INDEX IF NOT EXISTS \"I; DROP TABLE APP.T\" ON \"APP\".\"T\"(\"V\") STORAGE(ON MAIN) LEXER CHINESE_LEXER SYNC TRANSACTION",
            "ALTER CONTEXT INDEX I ON APP.T REBUILD",
            "ALTER CONTEXT INDEX I ON APP.T REBUILD ONLINE LEXER CHINESE_LEXER",
            "ALTER CONTEXT INDEX I ON APP.T OPTIMIZE",
            "DROP CONTEXT INDEX I ON APP.T",
            "drop context /* index maintenance */ index if exists \"I\" on \"APP\".\"T\";"
    })
    void shouldRecognizeFullTextIndexesWithoutTreatingIndexAsAContextName(String sql) {
        assertNonTableStatement(sql);

        final var statement = DamengParseTree.parseScope(sql, false).tree().statement(0).nonTableStatement();
        assertThat(statement.contextIndexStatement()).isNotNull();
        assertThat(statement.createContext()).isNull();
        assertThat(statement.dropContext()).isNull();
    }

    @Test
    void shouldPreserveAQuotedApplicationContextNamedIndex() {
        final var sql = "CREATE CONTEXT \"INDEX\" USING APP.P";
        assertNonTableStatement(sql);

        final var statement = DamengParseTree.parseScope(sql, false).tree().statement(0).nonTableStatement();
        assertThat(statement.createContext()).isNotNull();
        assertThat(statement.contextIndexStatement()).isNull();
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "CREATE ARRAY INDEX APP.I ON APP.T(V)",
            "CREATE CONTEXT INDEX I ON APP.T(V) LEXER CHINESE_LEXER SYNC TRANSACTION",
            "ALTER CONTEXT INDEX I ON APP.T REBUILD ONLINE",
            "DROP CONTEXT INDEX IF EXISTS I ON APP.T",
            "DROP TYPE IF EXISTS APP.O",
            "DROP CLASS BODY APP.C",
            "CREATE PUBLIC LINK L CONNECT 'DPI' WITH USER01 IDENTIFIED BY DMsys_123 USING '192.168.0.31:5369'",
            "CREATE OPERATOR @^ (FUNCTION FUNC_OP_TEST, LEFTARG INT, RIGHTARG INT)",
            "CREATE JAVA CLASS APP.J { public int f() { return 1; } }",
            "ALTER DATABASE DROP LOGFILE 'file.log'",
            "ALTER DATABASE RENAME LOGFILE 'old.log' TO 'new.log'",
            "ALTER TABLESPACE TS DROP DATAFILE 'file.dbf'",
            "ALTER TABLESPACE TS RENAME DATAFILE 'old.dbf' TO 'new.dbf'",
            "SET SCHEMA APP"
    })
    void shouldKeepFollowingTableChangesVisibleForTheNewNonTableFamilies(String sql) {
        assertUnsupported(sql + " ALTER TABLE APP.T ADD V INT");
        assertUnsupported(sql + "; DROP TABLE APP.T");
        assertUnsupported(sql + "; CREATE TABLE APP.U(ID INT)");
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "CREATE INDEX I ON T(ID); ALTER TABLE APP.T ADD X INT",
            "CREATE VIEW V AS SELECT ID FROM T; DROP TABLE T",
            "CREATE PROCEDURE P AS BEGIN NULL; END; ALTER TABLE T ADD X INT",
            "CREATE FUNCTION F RETURN INT AS BEGIN RETURN 1; END; UNKNOWN STATEMENT",
            "CREATE INDEX I ON T(ID) ALTER TABLE T ADD X INT",
            "DROP VIEW DROP TABLE APP.T",
            "ALTER INDEX ALTER TABLE APP.T ADD V INT",
            "CREATE VIEW CREATE TABLE APP.T(ID INT)",
            "COMMENT ON TABLE DROP TABLE APP.T",
            "CREATE VIEW V AS SELECT q'[;ALTER TABLE T ADD HIDDEN INT]' FROM T; ALTER TABLE T ADD REAL INT",
            "CREATE VIEW \"V;DROP TABLE T\" AS SELECT ID FROM T; DROP TABLE T",
            "CREATE VIEW V AS SELECT (SELECT ID FROM T); UNKNOWN STATEMENT",
            "CREATE VIEW V AS SELECT ID FROM T; CREATE TABLE U(ID INT)",
            "CREATE VIEW V AS SELECT ID FROM T; TRUNCATE TABLE T",
            "CREATE VIEW V AS SELECT ID FROM T; RENAME TABLE T TO U",
            "CREATE TABLE T(ID INT) CREATE TABLE U(ID INT)",
            "GRANT SELECT TO U; UNKNOWN STATEMENT",
            "GRANT SELECT TO U ALTER TABLE T ADD X INT",
            "GRANT TO U",
            "GRANT SELECT",
            "GRANT SELECT TO",
            "GRANT SELECT TO U WITH WRONG OPTION",
            "REVOKE SELECT TO U",
            "COMMENT ON INDEX I IS 'x'",
            "CREATE SCHEMA S AUTHORIZATION SYSDBA CREATE TABLE A(ID INT) CREATE TABLE B(ID INT)",
            "DROP SCHEMA S CASCADE",
            "DROP USER U CASCADE",
            "CREATE FULLTEXT INDEX I ON T(V)",
            "CREATE EXTENSION PGCRYPTO",
            "CONSTRUCTOR FUNCTION APP.O(ID INT) RETURN SELF AS RESULT",
            "DROP PARTITION P1",
            "DROP SUBPARTITION SP1",
            "DROP DATAFILE 'file.dbf'",
            "DROP DOMAIN APP.D CASCADE",
            "DROP TYPE APP.O CASCADE",
            "DROP CLASS APP.C CASCADE",
            "DROP CLASS BODY APP.C CASCADE",
            "DROP TYPE BODY IF EXISTS",
            "DROP CLASS BODY IF EXISTS",
            "CHECK TABLE APP.T",
            "CALL APP.P(1)",
            "EXEC APP.P(1)",
            "DECLARE N INT; BEGIN N:=1; END;",
            "RESTORE DATABASE FROM BACKUPSET '/tmp/dbz-backup'",
            "CREATE ARRAY INDEX",
            "CREATE PUBLIC LINK",
            "DROP PUBLIC LINK",
            "CREATE OPERATOR",
            "DROP OPERATOR",
            "CREATE JAVA CLASS APP.J { public int f() { return 1; }",
            "CREATE JAVA CLASS APP.J { public int f() { return 1; } }}",
            "ALTER JAVA CLASS",
            "ALTER CONTEXT",
            "ALTER CONTEXT CTX USING APP.P",
            "CREATE CONTEXT INDEX",
            "CREATE CONTEXT INDEX I",
            "CREATE CONTEXT INDEX I ON",
            "ALTER CONTEXT INDEX I ON",
            "DROP CONTEXT INDEX",
            "DROP CONTEXT INDEX IF EXISTS I",
            "DROP CONTEXT INDEX I ON",
            "ALTER TABLESPACE",
            "DROP TABLESPACE",
            "\"CREATE\" TABLE T(ID INT)",
            "CREATE \"INDEX\" I",
            "CREATE OR WRONG VIEW V AS SELECT 1",
            "CREATE PROCEDURE P AS BEGIN NULL;",
            "CREATE PROCEDURE P AS X; BEGIN NULL; END;",
            "/* outer /* inner */ residual */ ALTER TABLE T ADD X INT",
            "ALTER TABLE",
            "RENAME TABLE T TO",
            "RENAME TABLE T TO U UNKNOWN",
            "DROP TABLE 'T'",
            "CREATE",
            "",
            ";",
            "CREATE FORCE TABLE APP.T(ID INT)",
            "CREATE OR REPLACE FORCE INDEX APP.I ON APP.T(ID)",
            "CREATE MATERIALIZED VIEW APP.V FOR APP.T ON PREBUILT TABLE AS SELECT ID FROM APP.T",
            "CREATE FORCE MATERIALIZED VIEW APP.V AS SELECT ID FROM APP.T",
            "CREATE FORCE VIEW APP.V AS SELECT ID FROM APP.T; DROP TABLE APP.T",
            "CREATE VIEW APP.V AS SELECT q'[;DROP TABLE APP.T]' FROM APP.T; ALTER TABLE APP.T ADD V INT",
            "CREATE VIEW APP.\"V;DROP TABLE T\" AS SELECT ID FROM APP.T; TRUNCATE TABLE APP.T",
            "CREATE INDEX APP.I ON APP.T(ID); CREATE TABLE APP.U(ID INT)",
            "CREATE SEQUENCE APP.S START WITH 1; RENAME TABLE APP.T TO APP.U",
            "CREATE SYNONYM APP.S FOR APP.T; UNKNOWN STATEMENT",
            "CREATE FORCE VIEW APP.V AS SELECT ID FROM APP.T CREATE TABLE APP.U(ID INT)",
            "ALTER SEQUENCE APP.S INCREMENT BY 1 DROP TABLE APP.T",
            "CREATE VIEW APP.V AS SELECT ID FROM APP.T) ",
            "CREATE VIEW APP.V AS SELECT ID FROM APP.T; CREATE \"TABLE\" APP.U(ID INT)",
            "CREATE INDEX APP.I ON APP.T(ID) ALTER TABLE APP.T ADD V INT",
            "COMMENT ON INDEX APP.I IS 'description'",
            "CREATE TABLE APP.T(ID INT) CREATE TABLE APP.U(ID INT)",
            "GRANT SELECT ON APP.T TO U; UNKNOWN STATEMENT",
            "GRANT UPDATE(ID ON APP.T TO U",
            "REVOKE UPDATE ID) ON APP.T FROM U",
            "CREATE FORCE VIEW APP.V AS SELECT (ID FROM APP.T",
            "ALTER TABLE APP.T RENAME TO APP.U UNKNOWN"
    })
    void shouldFailClosedForUnknownOrCompoundScope(String sql) {
        assertUnsupported(sql);
    }

    @Test
    void shouldResolveQuotedTargetsAndRenameWithoutTheHistoricalModel() {
        final var result = analyzer.analyze("ALTER/*comment*/TABLE \"a\"\"b\".$t RENAME TO \"DROP\";", "OTHER").requireSupported();
        assertThat(result.tableId()).isEqualTo(new TableId(null, "a\"b", "$T"));
        assertThat(result.renamedTableId()).isEqualTo(new TableId(null, "a\"b", "DROP"));
        assertThat(analyzer.analyze("RENAME T TO OTHER.U", "APP").requireSupported().renamedTableId()).isEqualTo(new TableId(null, "OTHER", "U"));
    }

    @Test
    void shouldResolveDropTableIfExistsToTheActualTable() {
        final var result = analyzer.analyze("DROP TABLE IF EXISTS DB.XXX;", "OTHER").requireSupported();
        assertThat(result.impact()).isEqualTo(Impact.TABLE);
        assertThat(result.tableId()).isEqualTo(new TableId(null, "DB", "XXX"));
        assertThat(analyzer.analyze("DROP TABLE IF EXISTS XXX", "DB").requireSupported().tableId())
                .isEqualTo(new TableId(null, "DB", "XXX"));
        assertThat(analyzer.analyze("DROP TABLE \"IF\"", "DB").requireSupported().tableId())
                .isEqualTo(new TableId(null, "DB", "IF"));
        assertThat(analyzer.analyze("DROP TABLE IF XXX", "DB").impact()).isEqualTo(Impact.UNSUPPORTED);
    }

    @Test
    void shouldResolveCreateTableIfNotExistsToTheActualTable() {
        final var result = analyzer.analyze("CREATE TABLE IF NOT EXISTS DB.XXX (ID INT);", "OTHER").requireSupported();
        assertThat(result.impact()).isEqualTo(Impact.TABLE);
        assertThat(result.tableId()).isEqualTo(new TableId(null, "DB", "XXX"));
        assertThat(analyzer.analyze("CREATE TABLE IF NOT EXISTS XXX (ID INT)", "DB").requireSupported().tableId())
                .isEqualTo(new TableId(null, "DB", "XXX"));
        assertThat(analyzer.analyze("CREATE TABLE \"IF\" (ID INT)", "DB").requireSupported().tableId())
                .isEqualTo(new TableId(null, "DB", "IF"));
        assertThat(analyzer.analyze("CREATE TABLE IF EXISTS XXX (ID INT)", "DB").impact()).isEqualTo(Impact.UNSUPPORTED);
    }

    @Test
    void shouldPlanOnACopyAndRetainOriginalDefaultExpressions() {
        final var parser = new DamengDdlParser();
        final var tables = new Tables();
        parser.parse("CREATE TABLE APP.$T (ID INT PRIMARY KEY, \"NOT\" VARCHAR(30))", tables, "APP");
        final var id = new TableId(null, "APP", "$T");
        final var original = tables.forTable(id);
        final var sql = "ALTER TABLE APP.$T ADD AMOUNT DECIMAL(10,2) DEFAULT -1.25";
        final var plan = parser.plan(parser.analyze(sql, "APP"), tables);
        assertThat(tables.forTable(id)).isSameAs(original);
        assertThat(plan).hasSize(1);
        assertThat(plan.get(0).table().columnWithName("AMOUNT").defaultValueExpression()).contains("-1.25");
        final var unicodePlan = parser.plan(analyzer.analyze("ALTER TABLE APP.$T ADD V VARCHAR(30) DEFAULT '😀'", "APP").requireSupported(), tables);
        assertThat(unicodePlan).singleElement().satisfies(change -> assertThat(change.table().columnWithName("V").defaultValueExpression()).contains("'😀'"));
        assertThat(original.retrieveColumnNames()).containsExactly("ID", "NOT");
        assertThat(parser.plan(parser.analyze("GRANT SELECT ON APP.$T TO U", "APP"), tables)).isEmpty();
        assertThatThrownBy(() -> parser.plan(parser.analyze("ALTER TABLE APP.$T MOVE", "APP"), tables)).hasMessageContaining("Unsupported ALTER TABLE");
        assertThat(tables.forTable(id)).isSameAs(original);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "CREATE TABLE APP.T(ID INT)",
            "ALTER TABLE APP.T ADD V INT",
            "DROP TABLE APP.T",
            "GRANT SELECT ON APP.T TO U"
    })
    void shouldRejectStatementsFollowingACompleteStoredProgram(String trailingSql) {
        final var program = "CREATE OR REPLACE PROCEDURE APP.P AS BEGIN NULL; END";

        assertUnsupported(program + "; " + trailingSql);
        assertUnsupported(program + " " + trailingSql);
    }

    @ParameterizedTest
    @ValueSource(strings = { "APP.VIEW", "APP.\"CREATE\"", "\"app\".\"a\"\"b\"" })
    void shouldKeepKeywordAndQuotedIdentifiersAsTableTargets(String name) {
        final var result = analyzer.analyze("ALTER TABLE " + name + " ADD V INT;", "OTHER").requireSupported();
        final var expected = switch (name) {
            case "APP.VIEW" -> new TableId(null, "APP", "VIEW");
            case "APP.\"CREATE\"" -> new TableId(null, "APP", "CREATE");
            default -> new TableId(null, "app", "a\"b");
        };

        assertThat(result.impact()).isEqualTo(Impact.TABLE);
        assertThat(result.tableId()).isEqualTo(expected);
    }

    @Test
    void shouldAnalyzeOnlyTheSuppliedTokenSliceOfTheOriginalSql() {
        final var sql = "COMMENT ON TABLE APP.T IS '😀'; ALTER TABLE APP.T ADD V INT; UNKNOWN STATEMENT";
        final var tokens = new DamengAntlrSqlLexer().tokenize(sql);
        final var statements = DamengSqlTestUtil.split(tokens, ";");
        final var selected = statements.get(1);
        final var result = analyzer.analyzeTokens(sql, selected, "OTHER").requireSupported();

        assertThat(result.impact()).isEqualTo(Impact.TABLE);
        assertThat(result.tableId()).isEqualTo(new TableId(null, "APP", "T"));
        assertThat(result.sql()).isEqualTo(sql);
        assertThat(result.tokens()).hasSameSizeAs(selected);
        for (int index = 0; index < selected.size(); index++) {
            assertThat(result.tokens().get(index)).isSameAs(selected.get(index));
        }
        assertThat(analyzer.analyzeTokens(sql, statements.get(0), "OTHER").requireSupported().impact()).isEqualTo(Impact.NO_TABLE_CHANGE);
        assertThat(analyzer.analyze(sql, "OTHER").impact()).isEqualTo(Impact.UNSUPPORTED);
    }

    @Test
    void shouldReturnOriginalTableTokensWithoutStatementDelimiters() {
        final var sql = "/*😀*/\r\nALTER TABLE APP.\"DROP\" ADD V VARCHAR(30) DEFAULT q'[😀;--/*]';";
        final var tokens = new DamengAntlrSqlLexer().tokenize(sql);
        final var expected = tokens.subList(0, tokens.size() - 1);
        final var result = analyzer.analyzeTokens(sql, tokens, "OTHER").requireSupported();

        assertThat(result.impact()).isEqualTo(Impact.TABLE);
        assertThat(result.tokens()).hasSameSizeAs(expected);
        for (int index = 0; index < expected.size(); index++) {
            final var token = result.tokens().get(index);
            assertThat(token).isSameAs(expected.get(index));
            assertThat(token.source()).isEqualTo(sql);
            assertThat(sql.substring(token.start(), token.end())).isEqualTo(token.text());
        }
        assertThat(result.tokens().get(0).start()).isEqualTo(sql.indexOf("ALTER"));
        assertThat(result.tokens().get(result.tokens().size() - 1).text()).isEqualTo("q'[😀;--/*]'");
    }

    @Test
    void shouldReportTheOriginalUtf16LocationAfterSupplementaryCharacters() {
        final var sql = "/*😀*/\r\nCREATE \"INDEX\" I";
        final var result = analyzer.analyze(sql, "APP");

        assertThat(result.impact()).isEqualTo(Impact.UNSUPPORTED);
        assertThatThrownBy(result::requireSupported).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("character " + sql.indexOf("\"INDEX\""), "line 2, column 8");
    }

    @Test
    void shouldRespectTheConfiguredNestedCommentRules() {
        final var sql = "CREATE /* outer /* inner */ residual */ FORCE VIEW APP.V AS SELECT ID FROM APP.T";
        final var nested = new DamengDdlStatementAnalyzer(true, new DamengIdentifierRules(false));
        final var ordinary = new DamengDdlStatementAnalyzer(false, new DamengIdentifierRules(false));

        assertThat(nested.analyze(sql, "APP").requireSupported().impact()).isEqualTo(Impact.NO_TABLE_CHANGE);
        assertThat(ordinary.analyze(sql, "APP").impact()).isEqualTo(Impact.UNSUPPORTED);
    }

    private void assertUnsupported(String sql) {
        final var result = analyzer.analyze(sql, "APP");

        assertThat(result.impact()).isEqualTo(Impact.UNSUPPORTED);
        assertThat(result.tableId()).isNull();
        assertThat(result.renamedTableId()).isNull();
        assertThatThrownBy(result::requireSupported).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("character", "line", "column");
    }

    private void assertStatementDoesNotChangeCapturedTables(String sql) {
        assertNonTableStatement(sql);
        assertThat(DamengParseTree.parseScope(sql, false).tree().statement()).hasSize(1);

        final var parser = new DamengDdlParser();
        final var tables = new Tables();
        final var tableId = new TableId(null, "APP", "T");
        parser.parse("CREATE TABLE APP.T(ID INT PRIMARY KEY, V VARCHAR(30))", tables, "APP");
        final var original = tables.forTable(tableId);

        final var analysis = analyzer.analyze(sql, "APP").requireSupported();
        assertThat(analysis.defaultSchema()).isEqualTo("APP");
        assertThat(parser.plan(analysis, tables)).isEmpty();
        assertThat(parser.parse(sql, tables, "APP")).isEmpty();
        assertThat(tables.tableIds()).containsExactly(tableId);
        assertThat(tables.forTable(tableId)).isSameAs(original);
    }

    private void assertNonTableStatement(String sql) {
        final var scope = DamengParseTree.parseScope(sql, false).tree();
        assertThat(scope.statement()).as("non-table statements share one parse-tree branch").isNotEmpty()
                .allSatisfy(statement -> {
                    assertThat(statement.nonTableStatement()).isNotNull();
                    assertThat(statement.tableStatement()).isNull();
                });
        final var result = analyzer.analyze(sql, "APP").requireSupported();
        assertThat(result.impact()).isEqualTo(Impact.NO_TABLE_CHANGE);
        assertThat(result.tableId()).isNull();
        assertThat(result.renamedTableId()).isNull();
    }
}
