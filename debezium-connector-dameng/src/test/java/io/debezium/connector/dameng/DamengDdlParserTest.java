/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
import java.sql.Types;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import io.debezium.relational.Attribute;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;

class DamengDdlParserTest {
    @Test
    void appliesMultipleDdlStatementsAgainstHistoricalModel() {
        Tables tables = new Tables();
        DamengDdlParser parser = new DamengDdlParser();
        var changes = parser.parse("CREATE TABLE APP.T (ID INT PRIMARY KEY, NAME VARCHAR(20));"
                + "ALTER TABLE APP.T ADD COLUMN AGE INT;"
                + "ALTER TABLE APP.T RENAME COLUMN NAME TO LABEL;"
                + "ALTER TABLE APP.T DROP COLUMN AGE;", tables, "APP");
        Table table = tables.forTable(new TableId(null, "APP", "T"));
        assertThat(changes).extracting(DamengDdlParser.SchemaChange::type)
                .containsExactly(DamengDdlParser.Type.CREATE, DamengDdlParser.Type.ALTER, DamengDdlParser.Type.ALTER, DamengDdlParser.Type.ALTER);
        assertThat(table.retrieveColumnNames()).containsExactly("ID", "LABEL");
        assertThat(table.primaryKeyColumnNames()).containsExactly("ID");
    }

    @Test
    void renamesAndDropsHistoricalTable() {
        Tables tables = new Tables();
        new DamengDdlParser().parse("CREATE TABLE T (ID INT); RENAME TABLE T TO T2; DROP TABLE T2;", tables, "APP");
        assertThat(tables.forTable(new TableId(null, "APP", "T2"))).isNull();
    }

    @Test
    void preservesObjectIdentityAndNamedConstraintsAcrossTableRename() {
        final var tables = new Tables();
        final var parser = new DamengDdlParser();
        final var originalId = new TableId(null, "APP", "T");
        parser.parse("CREATE TABLE APP.T (ID INT, CONSTRAINT PK_T PRIMARY KEY (ID))", tables, "APP");
        final var original = tables.forTable(originalId);
        tables.overwriteTable(original.edit().addAttributes(original.attributes())
                .addAttribute(Attribute.editor().name("dameng.object.id").value("12345").create()).create());

        parser.parse("ALTER TABLE APP.T RENAME TO T2", tables, "APP");
        final var renamed = tables.forTable(new TableId(null, "APP", "T2"));
        assertThat(renamed.attributeWithName("dameng.object.id").asLong()).isEqualTo(12345L);
        assertThat(renamed.attributeWithName("dameng.constraint.PK_T").value()).isEqualTo("PRIMARY");
        parser.parse("ALTER TABLE APP.T2 DROP CONSTRAINT PK_T", tables, "APP");
        assertThat(tables.forTable(renamed.id()).primaryKeyColumnNames()).isEmpty();
        assertThat(tables.forTable(renamed.id()).attributeWithName("dameng.object.id").asLong()).isEqualTo(12345L);
    }

    @Test
    void rejectsUnknownSyntax() {
        assertThatThrownBy(() -> new DamengDdlParser().parse("ALTER SYSTEM SWITCH LOGFILE", new Tables(), "APP"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void preservesLocalTimezoneAsDistinctHistoricalTimestampType() {
        final var tables = new Tables();
        final var parser = new DamengDdlParser();
        final var id = new TableId(null, "APP", "T");
        parser.parse("CREATE TABLE APP.T (ID INT PRIMARY KEY, LTZ TIMESTAMP(6) WITH LOCAL TIME ZONE, "
                + "PLAIN TIMESTAMP(6), TZ TIMESTAMP(6) WITH TIME ZONE)", tables, "APP");
        assertThat(tables.forTable(id).columnWithName("LTZ").jdbcType()).isEqualTo(Types.TIMESTAMP);
        assertThat(tables.forTable(id).columnWithName("LTZ").typeName()).isEqualTo("TIMESTAMP WITH LOCAL TIME ZONE");
        assertThat(tables.forTable(id).columnWithName("PLAIN").typeName()).isEqualTo("TIMESTAMP");
        assertThat(tables.forTable(id).columnWithName("TZ").jdbcType()).isEqualTo(Types.TIMESTAMP_WITH_TIMEZONE);
        parser.parse("ALTER TABLE APP.T ADD SECOND_LTZ TIMESTAMP(3) WITH LOCAL TIME ZONE", tables, "APP");
        assertThat(tables.forTable(id).columnWithName("SECOND_LTZ").typeName()).isEqualTo("TIMESTAMP WITH LOCAL TIME ZONE");
    }

    @Test
    void preservesQuotedIdentifiersAndDelimitersInsideStringDefaults() {
        final var tables = new Tables();
        final var id = new TableId(null, "Mixed.Schema", "Order\"Detail");
        new DamengDdlParser().parse("CREATE TABLE \"Mixed.Schema\".\"Order\"\"Detail\" ("
                + "\"select\" INT PRIMARY KEY, \"display name\" VARCHAR(80) DEFAULT 'it''s,; (fine)')", tables, "APP");

        final var table = tables.forTable(id);
        assertThat(table.retrieveColumnNames()).containsExactly("select", "display name");
        assertThat(table.primaryKeyColumnNames()).containsExactly("select");
        assertThat(table.columnWithName("display name").defaultValueExpression()).contains("'it''s,; (fine)'");
    }

    @Test
    void resolvesQualifiedTargetWhenLogMinerDoesNotSupplyOwner() {
        final var parser = new DamengDdlParser();
        assertThat(parser.resolveTableId("/* DDL */ ALTER TABLE \"Sales\".\"Order\" ADD TOTAL DECIMAL(20,6)", "SYSDBA"))
                .isEqualTo(new TableId(null, "Sales", "Order"));
        assertThat(parser.resolveTableId("ALTER TABLE ORDERS ADD NOTE VARCHAR(30)", "APP"))
                .isEqualTo(new TableId(null, "APP", "ORDERS"));
    }

    @Test
    void resolvesBothDamengTableRenameFormsBeforeApplyingHistory() {
        final var parser = new DamengDdlParser();
        assertThat(parser.resolveTableId("RENAME TABLE APP.OLD_NAME TO NEW_NAME", "OTHER"))
                .isEqualTo(new TableId(null, "APP", "OLD_NAME"));
        assertThat(parser.resolveRenamedTableId("ALTER TABLE APP.OLD_NAME RENAME TO NEW_NAME", "OTHER"))
                .isEqualTo(new TableId(null, "APP", "NEW_NAME"));
        assertThat(parser.resolveRenamedTableId("RENAME TABLE OLD_NAME TO APP.NEW_NAME", "APP"))
                .isEqualTo(new TableId(null, "APP", "NEW_NAME"));
        assertThat(parser.resolveRenamedTableId("ALTER TABLE APP.OLD_NAME ADD NAME VARCHAR(20)", "APP")).isNull();
        assertThat(parser.resolveRenamedTableId("CREATE TABLE APP.NEW_NAME (ID INT)", "APP")).isNull();
    }

    @Test
    void rejectsUnexpectedTableDdlWithoutChangingTheHistoricalDefinition() {
        final var parser = new DamengDdlParser();
        final var tables = new Tables();
        final var id = new TableId(null, "APP", "T");
        parser.parse("CREATE TABLE APP.T (ID INT PRIMARY KEY, NAME VARCHAR(20))", tables, "APP");
        final var original = tables.forTable(id);

        assertThatThrownBy(() -> parser.parse("CREATE TABLE APP.T (ID INT)", tables, "APP"))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("existing table");
        assertThatThrownBy(() -> parser.parse("ALTER TABLE APP.T RENAME COLUMN ID TO NAME", tables, "APP"))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("Column already exists");
        assertThatThrownBy(() -> parser.parse("ALTER TABLE APP.T DROP CONSTRAINT MISSING", tables, "APP"))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("historical constraint");
        assertThatThrownBy(() -> parser.parse("ALTER TABLE APP.T ALTER COLUMN ID RENAME", tables, "APP"))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("Unsupported ALTER COLUMN");
        assertThatThrownBy(() -> parser.parse("ALTER TABLE APP.T MOVE", tables, "APP"))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("Unsupported ALTER TABLE");
        assertThat(parser.parse("COMMENT ON TABLE APP.T IS 'metadata'", tables, "APP")).isEmpty();
        assertThat(parser.resolveTableId("COMMENT ON TABLE APP.T IS 'metadata'", "APP")).isNull();
        assertThat(tables.forTable(id)).isEqualTo(original);
    }

    @Test
    void appliesCompositePrimaryKeyDeclaredBeforeItsColumns() {
        final var tables = new Tables();
        final var parser = new DamengDdlParser();
        parser.parse("CREATE TABLE APP.T (CONSTRAINT PK_T PRIMARY KEY (TENANT, ID), TENANT INT, ID BIGINT, NAME VARCHAR(20))", tables, "APP");
        final var table = tables.forTable(new TableId(null, "APP", "T"));

        assertThat(table.primaryKeyColumnNames()).containsExactly("TENANT", "ID");
        assertThat(table.columnWithName("TENANT").isOptional()).isFalse();
        assertThat(table.columnWithName("ID").isOptional()).isFalse();
        assertThat(table.columnWithName("NAME").isOptional()).isTrue();

        parser.parse("ALTER TABLE APP.T DROP CONSTRAINT PK_T", tables, "APP");
        assertThat(tables.forTable(table.id()).primaryKeyColumnNames()).isEmpty();
    }

    @Test
    void renamesPrimaryKeyColumnWithoutLosingKeyIdentity() {
        final var tables = new Tables();
        new DamengDdlParser().parse("CREATE TABLE APP.T (ID INT PRIMARY KEY, NAME VARCHAR(20));"
                + "ALTER TABLE APP.T RENAME COLUMN ID TO RECORD_ID", tables, "APP");
        final var table = tables.forTable(new TableId(null, "APP", "T"));

        assertThat(table.primaryKeyColumnNames()).containsExactly("RECORD_ID");
        assertThat(table.columnWithName("ID")).isNull();
        assertThat(table.columnWithName("RECORD_ID").isOptional()).isFalse();
    }

    @Test
    void capturesEachIntermediateTableDefinition() {
        final var tables = new Tables();
        final var changes = new DamengDdlParser().parse("CREATE TABLE APP.T (ID INT PRIMARY KEY);"
                + "ALTER TABLE APP.T ADD NAME VARCHAR(20);"
                + "ALTER TABLE APP.T RENAME COLUMN NAME TO LABEL;"
                + "ALTER TABLE APP.T DROP COLUMN LABEL", tables, "APP");

        assertThat(changes.get(0).table().retrieveColumnNames()).containsExactly("ID");
        assertThat(changes.get(1).table().retrieveColumnNames()).containsExactly("ID", "NAME");
        assertThat(changes.get(2).table().retrieveColumnNames()).containsExactly("ID", "LABEL");
        assertThat(changes.get(3).table().retrieveColumnNames()).containsExactly("ID");
    }

    @Test
    void doesNotPartiallyApplyABatchWhenALaterStatementIsInvalid() {
        final var tables = new Tables();
        final var parser = new DamengDdlParser();
        final var id = new TableId(null, "APP", "T");
        parser.parse("CREATE TABLE APP.T (ID INT PRIMARY KEY, NAME VARCHAR(20))", tables, "APP");
        final var original = tables.forTable(id);

        assertThatThrownBy(() -> parser.parse("ALTER TABLE APP.T ADD AGE INT;"
                + "ALTER TABLE APP.T MODIFY MISSING INT", tables, "APP"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(tables.forTable(id)).isEqualTo(original);
        assertThat(tables.forTable(id).columnWithName("AGE")).isNull();
    }

    @Test
    void doesNotRemoveHistoricalTableWhenRenameFails() {
        final var tables = new Tables();
        final var parser = new DamengDdlParser();
        parser.parse("CREATE TABLE APP.T (ID INT); CREATE TABLE APP.T2 (ID INT)", tables, "APP");

        assertThatThrownBy(() -> parser.parse("ALTER TABLE APP.T RENAME TO T2", tables, "APP"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(tables.forTable(new TableId(null, "APP", "T"))).isNotNull();
        assertThat(tables.forTable(new TableId(null, "APP", "T2"))).isNotNull();
    }

    @Test
    void modifiesColumnTypeWhilePreservingItsNullabilityAndDefault() {
        final var tables = new Tables();
        final var parser = new DamengDdlParser();
        parser.parse("CREATE TABLE APP.T (ID INT PRIMARY KEY, NAME VARCHAR(20) DEFAULT 'unknown' NOT NULL);"
                + "ALTER TABLE APP.T MODIFY NAME VARCHAR(80)", tables, "APP");
        final var column = tables.forTable(new TableId(null, "APP", "T")).columnWithName("NAME");

        assertThat(column.jdbcType()).isEqualTo(Types.VARCHAR);
        assertThat(column.length()).isEqualTo(80);
        assertThat(column.isOptional()).isFalse();
        assertThat(column.defaultValueExpression()).contains("'unknown'");
    }

    @Test
    void updatesAndRemovesDefaultWithoutChangingColumnType() {
        final var tables = new Tables();
        final var parser = new DamengDdlParser();
        final var id = new TableId(null, "APP", "T");
        parser.parse("CREATE TABLE APP.T (ID INT, AMOUNT DECIMAL(20,6));"
                + "ALTER TABLE APP.T ALTER COLUMN AMOUNT SET DEFAULT 1.250000", tables, "APP");
        assertThat(tables.forTable(id).columnWithName("AMOUNT").defaultValueExpression()).contains("1.250000");

        parser.parse("ALTER TABLE APP.T ALTER COLUMN AMOUNT DROP DEFAULT", tables, "APP");
        final var column = tables.forTable(id).columnWithName("AMOUNT");
        assertThat(column.defaultValueExpression()).isEmpty();
        assertThat(column.jdbcType()).isEqualTo(Types.DECIMAL);
        assertThat(column.length()).isEqualTo(20);
        assertThat(column.scale()).contains(6);
    }

    @Test
    void truncationPreservesHistoricalTableDefinition() {
        final var tables = new Tables();
        final var parser = new DamengDdlParser();
        final var id = new TableId(null, "APP", "T");
        parser.parse("CREATE TABLE APP.T (ID INT PRIMARY KEY, NAME VARCHAR(20))", tables, "APP");
        final var original = tables.forTable(id);
        final var changes = parser.parse("TRUNCATE TABLE APP.T REUSE STORAGE", tables, "APP");

        assertThat(changes).singleElement().satisfies(change -> {
            assertThat(change.type()).isEqualTo(DamengDdlParser.Type.TRUNCATE);
            assertThat(change.table()).isEqualTo(original);
        });
        assertThat(tables.forTable(id)).isEqualTo(original);
    }

    @Test
    void acceptsStorageAndCascadeClausesWithoutChangingHistoricalColumns() {
        final var parser = new DamengDdlParser();
        final var tables = new Tables();
        final var id = new TableId(null, "APP", "T");
        parser.parse("CREATE TABLE APP.T (ID INT PRIMARY KEY, NAME VARCHAR(20))", tables, "APP");
        final var original = tables.forTable(id);

        parser.parse("TRUNCATE TABLE APP.T DROP STORAGE", tables, "APP");
        assertThat(tables.forTable(id)).isEqualTo(original);
        final var drop = parser.parse("DROP TABLE APP.T CASCADE CONSTRAINTS PURGE", tables, "APP");
        assertThat(drop).singleElement().satisfies(change -> assertThat(change.table()).isEqualTo(original));
        assertThat(tables.forTable(id)).isNull();
    }

    @Test
    void preservesCharacterLengthSemanticsWithHistoricalColumnDefinition() {
        final var tables = new Tables();
        final var parser = new DamengDdlParser(StandardCharsets.UTF_8, true, false, true);
        parser.parse("CREATE TABLE APP.T (EXPLICIT_CHAR CHAR(4 CHAR), EXPLICIT_BYTE CHAR(4 BYTE), NATIONAL NCHAR(4))", tables, "APP");
        final var table = tables.forTable(new TableId(null, "APP", "T"));

        assertThat(table.columnWithName("EXPLICIT_CHAR").typeExpression()).isEqualTo("CHAR(4 CHAR)");
        assertThat(table.columnWithName("EXPLICIT_BYTE").typeExpression()).isEqualTo("CHAR(4 BYTE)");
        assertThat(table.columnWithName("NATIONAL").typeExpression()).isEqualTo("NCHAR(4 CHAR)");
        assertThat(table.columnWithName("NATIONAL").charsetName()).isEqualTo("UTF-8");
    }

    @Test
    void updatesConstraintsAndColumnNullabilityAcrossAlterStatements() {
        final var parser = new DamengDdlParser();
        final var tables = new Tables();
        final var id = new TableId(null, "APP", "T");
        parser.parse("CREATE TABLE APP.T (ID INT PRIMARY KEY, PARENT_ID INT CONSTRAINT FK_PARENT REFERENCES APP.P(ID) ON DELETE CASCADE, "
                + "NAME VARCHAR(20) CONSTRAINT UQ_NAME UNIQUE, CONSTRAINT CK_PARENT CHECK (PARENT_ID > 0))", tables, "APP");
        var table = tables.forTable(id);
        assertThat(table.attributeWithName("dameng.constraint.FK_PARENT").value()).isEqualTo("FOREIGN");
        assertThat(table.attributeWithName("dameng.constraint.UQ_NAME").value()).isEqualTo("UNIQUE");
        assertThat(table.attributeWithName("dameng.constraint.CK_PARENT").value()).isEqualTo("CHECK");

        parser.parse("ALTER TABLE APP.T DROP PRIMARY KEY; ALTER TABLE APP.T ALTER COLUMN ID DROP NOT NULL; "
                + "ALTER TABLE APP.T DROP CONSTRAINT FK_PARENT CASCADE CONSTRAINTS", tables, "APP");
        table = tables.forTable(id);
        assertThat(table.primaryKeyColumnNames()).isEmpty();
        assertThat(table.columnWithName("ID").isOptional()).isTrue();
        assertThat(table.attributeWithName("dameng.constraint.FK_PARENT")).isNull();
    }

    @Test
    void recordsTableConstraintsAndSetNullForeignKeyPolicy() {
        final var tables = new Tables();
        new DamengDdlParser().parse("CREATE TABLE APP.T (ID INT, PARENT_ID INT, CONSTRAINT UQ_T UNIQUE(ID), "
                + "CONSTRAINT FK_T FOREIGN KEY(PARENT_ID) REFERENCES APP.P(ID) ON DELETE SET NULL)", tables, "APP");
        final var table = tables.forTable(new TableId(null, "APP", "T"));

        assertThat(table.attributeWithName("dameng.constraint.UQ_T").value()).isEqualTo("UNIQUE");
        assertThat(table.attributeWithName("dameng.constraint.FK_T").value()).isEqualTo("FOREIGN");
        assertThat(table.retrieveColumnNames()).containsExactly("ID", "PARENT_ID");
    }

    @Test
    void recordsIdentityAndInlineCheckWithoutChangingNullableColumns() {
        final var tables = new Tables();
        new DamengDdlParser().parse("CREATE TABLE APP.T (ID INT IDENTITY(1,1), VALUE INT CONSTRAINT CK_VALUE CHECK(VALUE > 0), "
                + "DESCRIPTION VARCHAR(20) NULL)", tables, "APP");
        final var table = tables.forTable(new TableId(null, "APP", "T"));

        assertThat(table.columnWithName("ID").isAutoIncremented()).isTrue();
        assertThat(table.attributeWithName("dameng.constraint.CK_VALUE").value()).isEqualTo("CHECK");
        assertThat(table.columnWithName("DESCRIPTION").isOptional()).isTrue();
    }

    @Test
    void keepsGroupedNumericDefaultAcrossCommentsAndColumnClauses() {
        final var tables = new Tables();
        new DamengDdlParser().parse("-- DM redo comment\nCREATE TABLE APP.T (AMOUNT DECIMAL(12,2) DEFAULT (1.2e+3) NOT NULL)"
                + " /* trailing redo comment */", tables, "APP");
        final var column = tables.forTable(new TableId(null, "APP", "T")).columnWithName("AMOUNT");

        assertThat(column.defaultValueExpression()).contains("(1.2e+3)");
        assertThat(column.isOptional()).isFalse();
    }

    @ParameterizedTest
    @MethodSource("types")
    void mapsDmColumnTypeAndPrecision(String type, int jdbcType, int length, Integer scale) {
        final var tables = new Tables();
        new DamengDdlParser().parse("CREATE TABLE APP.T (VALUE " + type + ")", tables, "APP");
        final var column = tables.forTable(new TableId(null, "APP", "T")).columnWithName("VALUE");

        assertThat(column.jdbcType()).isEqualTo(jdbcType);
        assertThat(column.length()).isEqualTo(length);
        assertThat(column.scale().orElse(null)).isEqualTo(scale);
    }

    static Stream<Arguments> types() {
        // DatabaseMetaData.getColumns() values observed on the DM8 test image.
        // The driver uses display sizes for temporal columns and DECIMAL for NUMBER.
        return Stream.of(
                Arguments.of("TINYINT", Types.TINYINT, 3, 0),
                Arguments.of("SMALLINT", Types.SMALLINT, 5, 0),
                Arguments.of("INTEGER", Types.INTEGER, 10, 0),
                Arguments.of("BIGINT", Types.BIGINT, 19, 0),
                Arguments.of("DECIMAL(38,6)", Types.DECIMAL, 38, 6),
                Arguments.of("NUMBER", Types.DECIMAL, 0, 0),
                Arguments.of("NUMBER(20)", Types.DECIMAL, 20, 0),
                Arguments.of("CHARACTER VARYING(120)", Types.VARCHAR, 120, null),
                Arguments.of("VARBINARY(80)", Types.VARBINARY, 80, null),
                Arguments.of("TIMESTAMP(0)", Types.TIMESTAMP, 29, 0),
                Arguments.of("TIMESTAMP(6)", Types.TIMESTAMP, 36, 6),
                Arguments.of("TIMESTAMP(6) WITH TIME ZONE", Types.TIMESTAMP_WITH_TIMEZONE, 43, 6),
                Arguments.of("TIME(3)", Types.TIME, 19, 3),
                Arguments.of("TIME", Types.TIME, 15, 0),
                Arguments.of("TIME WITH TIME ZONE", Types.TIME_WITH_TIMEZONE, 22, 0),
                Arguments.of("FLOAT(24)", Types.REAL, 24, null),
                Arguments.of("FLOAT(53)", Types.FLOAT, 53, null),
                Arguments.of("DOUBLE PRECISION", Types.DOUBLE, 53, null),
                Arguments.of("REAL", Types.REAL, 24, null),
                Arguments.of("BIT", Types.BIT, 1, 0),
                Arguments.of("BOOLEAN", Types.BOOLEAN, 1, 0),
                Arguments.of("BINARY", Types.BINARY, 1, null));
    }

    @ParameterizedTest
    @ValueSource(strings = { "STORAGE(USING LONG ROW)", "STORAGE(DISABLE USING LONG ROW)" })
    void shouldParseLongRowStorageWithoutChangingTheLogicalColumnDefinition(String storage) {
        final var parser = new DamengDdlParser();
        final var tables = new Tables();
        parser.parse("CREATE TABLE APP.T (VALUE VARCHAR(32767)) " + storage, tables, "APP");
        assertThat(tables.forTable(new TableId(null, "APP", "T")).columnWithName("VALUE").length()).isEqualTo(32767);
        assertThatThrownBy(() -> parser.parse("CREATE TABLE APP.U (VALUE INT) STORAGE(USING UNKNOWN ROW)", tables, "APP"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(tables.forTable(new TableId(null, "APP", "U"))).isNull();
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "CREATE TABLE APP.T (ID INT, ID INT)",
            "CREATE TABLE APP.T (ID INT, PRIMARY KEY(MISSING))",
            "CREATE TABLE APP.T (ID INT PRIMARY KEY, OTHER INT PRIMARY KEY)",
            "CREATE TABLE APP.T (VALUE UNKNOWN_TYPE)",
            "CREATE TABLE APP.T (VALUE VARCHAR(20) DEFAULT 'unfinished)",
            "CREATE TABLE APP.T (VALUE DECIMAL(20,6)",
            "CREATE TABLE APP.T (VALUE)",
            "CREATE TABLE APP.T (VALUE INT, CONSTRAINT BAD_IDENTITY IDENTITY (VALUE))",
            "CREATE TABLE APP.T (VALUE INT IDENTITY(x,1))",
            "CREATE TABLE APP.T (VALUE INT IDENTITY(1,))",
            "CREATE TABLE APP.T (VALUE INT))",
            "CREATE TABLE APP.T (VALUE INT) /* unfinished comment",
            "CREATE TABLE APP.T (VALUE INT) UNSUPPORTED_TRAILER"
    })
    void rejectsInvalidOrUnimplementedDdlWithoutCreatingATable(String ddl) {
        final var tables = new Tables();
        assertThatThrownBy(() -> new DamengDdlParser().parse(ddl, tables, "APP"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(tables.forTable(new TableId(null, "APP", "T"))).isNull();
    }

    @Test
    void preservesQuotedIdentifiersAndDefaultText() {
        final var tables = new Tables();
        final var parser = new DamengDdlParser();
        parser.parse("CREATE TABLE \"Other.Schema\".\"a table\" (\"a\"\"b\" INT PRIMARY KEY, label VARCHAR(30) DEFAULT 'not null, it''s; fine' NOT NULL)", tables, "APP");
        final var id = new TableId(null, "Other.Schema", "a table");
        assertThat(parser.resolveTableId("ALTER TABLE \"Other.Schema\".\"a table\" ADD x INT", "APP")).isEqualTo(id);
        final var table = tables.forTable(id);
        assertThat(table.primaryKeyColumnNames()).containsExactly("a\"b");
        assertThat(table.columnWithName("LABEL").defaultValueExpression()).contains("'not null, it''s; fine'");
        assertThat(table.columnWithName("LABEL").isOptional()).isFalse();
    }

    @Test
    void followsNativeDamengJdbcTypes() {
        final var tables = new Tables();
        new DamengDdlParser().parse("CREATE TABLE T (i INT, b BIGINT, s SMALLINT, n NUMBER(12,4), d DATE, tm TIME(3), ts TIMESTAMP(6), "
                + "tz TIMESTAMP(6) WITH TIME ZONE, f FLOAT, f24 FLOAT(24), nc NCHAR(12), nv NVARCHAR2(30), r RAW(16), c CLOB, bl BLOB)", tables, "APP");
        final var table = tables.forTable(new TableId(null, "APP", "T"));
        assertThat(table.columns()).extracting(column -> column.jdbcType()).containsExactly(Types.INTEGER, Types.BIGINT, Types.SMALLINT,
                Types.DECIMAL, Types.DATE, Types.TIME, Types.TIMESTAMP, Types.TIMESTAMP_WITH_TIMEZONE, Types.FLOAT,
                Types.REAL, Types.CHAR, Types.VARCHAR, Types.VARBINARY, Types.CLOB, Types.BLOB);
        assertThat(table.columnWithName("I").length()).isEqualTo(10);
        assertThat(table.columnWithName("N").scale()).contains(4);
        assertThat(table.columnWithName("TM").scale()).contains(3);
        assertThat(table.columnWithName("TS").length()).isEqualTo(36);
        assertThat(table.columnWithName("TZ").length()).isEqualTo(43);
    }

    @Test
    void supportsGroupedAlterAndPreservesUnspecifiedColumnProperties() {
        final var tables = new Tables();
        final var parser = new DamengDdlParser();
        parser.parse("CREATE TABLE T (ID INT PRIMARY KEY, VALUE VARCHAR(20) DEFAULT 'x' NOT NULL)", tables, "APP");
        parser.parse("ALTER TABLE T ADD (AGE INT, AMOUNT DECIMAL(12,2)); ALTER TABLE T MODIFY (VALUE VARCHAR(80), AGE INT NOT NULL)", tables, "APP");
        final var table = tables.forTable(new TableId(null, "APP", "T"));
        assertThat(table.columnWithName("VALUE").isOptional()).isFalse();
        assertThat(table.columnWithName("VALUE").defaultValueExpression()).contains("'x'");
        assertThat(table.columnWithName("VALUE").length()).isEqualTo(80);
        assertThat(table.columnWithName("AGE").isOptional()).isFalse();
        parser.parse("ALTER TABLE T DROP (AGE, AMOUNT); ALTER TABLE T RENAME TO NEW_T", tables, "APP");
        assertThat(tables.forTable(new TableId(null, "APP", "T"))).isNull();
        assertThat(tables.forTable(new TableId(null, "APP", "NEW_T")).retrieveColumnNames()).containsExactly("ID", "VALUE");
    }

    @Test
    void tracksNamedConstraintsAcrossStatements() {
        final var tables = new Tables();
        final var parser = new DamengDdlParser();
        parser.parse("CREATE TABLE T (CONSTRAINT PK_T PRIMARY KEY (A,B), A INT, B INT, CONSTRAINT CK_T CHECK (A > 0))", tables, "APP");
        parser.parse("ALTER TABLE T DROP CONSTRAINT PK_T; ALTER TABLE T ADD CONSTRAINT PK_NEW PRIMARY KEY (B)", tables, "APP");
        final var table = tables.forTable(new TableId(null, "APP", "T"));
        assertThat(table.primaryKeyColumnNames()).containsExactly("B");
        assertThat(table.columnWithName("B").isOptional()).isFalse();
    }

    @Test
    void acceptsDecimalDefaultsAndColumnDefaultChanges() {
        final var tables = new Tables();
        final var parser = new DamengDdlParser();
        parser.parse("CREATE TABLE T (A DECIMAL(10,2) DEFAULT 1.25)", tables, "APP");
        assertThat(tables.forTable(null, "APP", "T").columnWithName("A").defaultValueExpression()).contains("1.25");
        parser.parse("ALTER TABLE T ALTER COLUMN A SET DEFAULT 2.5; ALTER TABLE T ALTER COLUMN A SET NOT NULL", tables, "APP");
        assertThat(tables.forTable(null, "APP", "T").columnWithName("A").defaultValueExpression()).contains("2.5");
        assertThat(tables.forTable(null, "APP", "T").columnWithName("A").isOptional()).isFalse();
        parser.parse("ALTER TABLE T ALTER COLUMN A DROP DEFAULT", tables, "APP");
        assertThat(tables.forTable(null, "APP", "T").columnWithName("A").hasDefaultValue()).isFalse();
    }

    @Test
    void rejectsUnrecognizedTypesAndTrailingClausesWithoutPartialMutation() {
        final var tables = new Tables();
        final var parser = new DamengDdlParser();
        assertThatThrownBy(() -> parser.parse("CREATE TABLE OK (ID INT); CREATE TABLE BAD (ID CUSTOM_TYPE)", tables, "APP"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(tables.size()).isZero();
        parser.parse("CREATE TABLE T (ID INT)", tables, "APP");
        assertThatThrownBy(() -> parser.parse("DROP TABLE T garbage", tables, "APP")).isInstanceOf(IllegalArgumentException.class);
        assertThat(tables.forTable(null, "APP", "T")).isNotNull();
        assertThatThrownBy(() -> parser.parse("ALTER TABLE T ADD V INT nonsense", tables, "APP")).isInstanceOf(IllegalArgumentException.class);
        assertThat(tables.forTable(null, "APP", "T").retrieveColumnNames()).containsExactly("ID");
    }

    @Test
    void returnsRenamedTableAndHistoricalDropDefinition() {
        final var tables = new Tables();
        final var changes = new DamengDdlParser().parse("CREATE TABLE T (ID INT); ALTER TABLE T RENAME TO T2; TRUNCATE TABLE T2; DROP TABLE T2", tables, "APP");
        assertThat(changes.get(1).table().id()).isEqualTo(new TableId(null, "APP", "T2"));
        assertThat(changes.get(1).previousTableId()).isEqualTo(new TableId(null, "APP", "T"));
        assertThat(changes.get(2).type()).isEqualTo(DamengDdlParser.Type.TRUNCATE);
        assertThat(changes.get(3).table().retrieveColumnNames()).containsExactly("ID");
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "CREATE INDEX IX_T ON APP.T(ID)",
            "CREATE UNIQUE INDEX IX_T ON APP.T(ID)",
            "ALTER INDEX IX_T REBUILD",
            "DROP INDEX IX_T",
            "CREATE SEQUENCE SEQ_T START WITH 1",
            "ALTER SEQUENCE SEQ_T INCREMENT BY 2",
            "DROP SEQUENCE SEQ_T",
            "CREATE VIEW V_T AS SELECT ID FROM APP.T",
            "CREATE OR REPLACE VIEW V_T AS SELECT ID FROM APP.T",
            "DROP VIEW V_T",
            "CREATE OR REPLACE PROCEDURE P_T AS BEGIN NULL; END;",
            "DROP PROCEDURE P_T",
            "CREATE SYNONYM S_T FOR APP.T",
            "CREATE OR REPLACE FUNCTION F_T RETURN INT AS BEGIN RETURN 1; END;"
    })
    void shouldIdentifyNonTableObjectDdlWithoutResolvingACapturedTable(String ddl) {
        final var parser = new DamengDdlParser();
        assertThat(parser.isNonTableDdl(ddl)).isTrue();
        assertThat(parser.resolveTableId(ddl, "APP")).isNull();
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "GRANT SELECT ON APP.T TO CDC_USER",
            "REVOKE SELECT ON APP.T FROM CDC_USER",
            "GRANT CREATE TABLE, CREATE VIEW TO CDC_USER",
            "GRANT SOI TO CDC_USER WITH ADMIN OPTION",
            "GRANT SELECT ON APP.T TO \"User;ALTER TABLE\" WITH GRANT OPTION",
            "// 授予要采集表的 select 权限\nGRANT SELECT ON BIZ.CDC_PROBE TO DBZ_USER1",
            "// 授予权限\r\n/* 对象权限 */ grant select on APP.T to CDC_USER;",
            "-- 回收权限\rREVOKE SELECT ON APP.T FROM CDC_USER",
            "GRANT SELECT ON APP.T TO CDC_USER; // 回收权限\nREVOKE SELECT ON APP.T FROM CDC_USER;",
            "GRANT SELECT ON APP.T TO CDC_USER // trailing comment without newline"
    })
    void shouldIdentifyCommentedPrivilegeStatementsWithoutChangingTableDefinitions(String ddl) {
        final var parser = new DamengDdlParser();
        assertThat(parser.isNonTableDdl(ddl)).isTrue();
        assertThat(parser.resolveTableId(ddl, "APP")).isNull();
    }

    @ParameterizedTest
    @ValueSource(strings = { "\n", "\r\n", "\r" })
    void shouldParseTableDdlWithSlashCommentsAndPreserveQuotedCommentMarkers(String newline) {
        final var parser = new DamengDdlParser();
        final var tables = new Tables();
        final var ddl = "// 注释中的 DROP TABLE APP.T;" + newline
                + "CREATE TABLE APP.\"a//b\" (ID INT PRIMARY KEY, "
                + "VALUE VARCHAR(100) DEFAULT 'https://example.test/--/*;') // trailing comment";

        assertThat(parser.resolveTableId(ddl, "APP")).isEqualTo(new TableId(null, "APP", "a//b"));
        parser.parse(ddl, tables, "APP");

        assertThat(tables.forTable(new TableId(null, "APP", "a//b")).columnWithName("VALUE").defaultValueExpression())
                .contains("'https://example.test/--/*;'");
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "GRANT SELECT ON APP.T TO CDC_USER; ALTER TABLE APP.T ADD EXTRA INT",
            "REVOKE SELECT ON APP.T FROM CDC_USER; CREATE TABLE APP.U (ID INT)",
            "GRANT SELECT ON APP.T TO CDC_USER; // 表结构变更\nDROP TABLE APP.T",
            "GRANT SELECT ON APP.T TO CDC_USER; UNKNOWN STATEMENT"
    })
    void shouldRejectMixedPrivilegeAndTableOrUnknownStatements(String ddl) {
        final var parser = new DamengDdlParser();
        assertThat(parser.isNonTableDdl(ddl)).isFalse();
        assertThatThrownBy(() -> parser.resolveTableId(ddl, "APP"))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("Unsupported table DDL");
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "CREATE TABLE APP.T (ID INT)",
            "ALTER TABLE APP.T ADD NAME VARCHAR(20)",
            "DROP TABLE APP.T",
            "TRUNCATE TABLE APP.T",
            "RENAME TABLE APP.T TO APP.T2",
            "CREATE TABLE APP.\"INDEX\" (ID INT)",
            "ALTER SYSTEM SWITCH LOGFILE",
            "CREATE SOMETHING UNKNOWN"
    })
    void shouldNotClassifyTableOrUnknownDdlAsHarmless(String ddl) {
        assertThat(new DamengDdlParser().isNonTableDdl(ddl)).isFalse();
    }
}
