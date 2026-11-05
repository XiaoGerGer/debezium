/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.sql.Types;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import io.debezium.connector.dameng.antlr.DamengDdlParser;
import io.debezium.connector.dameng.logminer.parser.LogMinerDmlParser;
import io.debezium.connector.dameng.logminer.parser.LogMinerDmlParser.SqlExpression;
import io.debezium.data.Envelope.Operation;
import io.debezium.relational.Attribute;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;

class DamengAntlrDdlParserTest {
    private final DamengDdlParser parser = new DamengDdlParser();
    private final Tables tables = new Tables();

    @ParameterizedTest
    @CsvSource({ "GLOBAL TEMPORARY,DELETE", "GLOBAL TEMPORARY,PRESERVE", "TEMPORARY,DELETE", "TEMPORARY,PRESERVE" })
    void shouldRetainTemporaryIdentityWithoutBuildingARowSchema(String prefix, String policy) {
        final var id = new TableId(null, "APP", "TMP");
        parser.parse("CREATE " + prefix + " TABLE APP.TMP(ID INT,V VARCHAR(30)) ON COMMIT " + policy + " ROWS", tables, "APP");
        assertThat(DamengTableType.isTemporary(tables.forTable(id))).isTrue();
        assertThat(tables.forTable(id).columns()).isEmpty();
        tables.overwriteTable(DamengTableType.temporaryTable(id, 71));

        final var changes = parser.parse("ALTER TABLE APP.TMP ADD EXTRA INT; ALTER TABLE APP.TMP RENAME COLUMN V TO VALUE;"
                + "TRUNCATE TABLE APP.TMP; ALTER TABLE APP.TMP RENAME TO RENAMED; DROP TABLE APP.RENAMED", tables, "APP");

        assertThat(changes).extracting(DamengDdlParser.SchemaChange::type).containsExactly(DamengDdlParser.Type.ALTER,
                DamengDdlParser.Type.ALTER, DamengDdlParser.Type.TRUNCATE, DamengDdlParser.Type.RENAME, DamengDdlParser.Type.DROP);
        for (var change : changes) {
            assertThat(DamengTableType.isTemporary(change.table())).isTrue();
            assertThat(change.table().columns()).isEmpty();
            assertThat(change.table().attributeWithName("dameng.object.id").asLong()).isEqualTo(71);
        }
        assertThat(tables.tableIds()).isEmpty();
    }

    @Test
    void shouldNotDeriveLikeColumnsFromATemporaryMarker() {
        final var id = new TableId(null, "APP", "TMP");
        tables.overwriteTable(DamengTableType.temporaryTable(id, 71));
        assertThatThrownBy(() -> parser.parse("CREATE TABLE APP.COPY LIKE APP.TMP", tables, "APP"))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("temporary");
        assertThat(tables.tableIds()).containsExactly(id);
        assertThat(DamengTableType.isTemporary(tables.forTable(id))).isTrue();
    }

    @Test
    void shouldPreserveExternalIdentityThroughRenameAndRemoveItOnDrop() {
        final var originalId = new TableId(null, "APP", "EXT");
        final var renamedId = new TableId(null, "APP", "RENAMED");
        final var external = DamengTableType.externalTable(originalId, 71);
        tables.overwriteTable(external);
        final var changes = parser.parse("ALTER TABLE APP.EXT RENAME TO RENAMED; DROP TABLE APP.RENAMED CASCADE", tables, "APP");
        assertThat(changes).extracting(DamengDdlParser.SchemaChange::type)
                .containsExactly(DamengDdlParser.Type.RENAME, DamengDdlParser.Type.DROP);
        assertThat(changes.get(0).table()).isEqualTo(DamengTableType.externalTable(renamedId, 71));
        assertThat(changes.get(1).table()).isEqualTo(changes.get(0).table());
        assertThat(tables.tableIds()).isEmpty();
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "CREATE TABLE APP.COPY LIKE APP.EXT",
            "ALTER TABLE APP.EXT ADD LOST INT",
            "TRUNCATE TABLE APP.EXT"
    })
    void shouldRejectRowSchemaOperationsOnExternalMarkersWithoutMutatingHistory(String sql) {
        final var external = DamengTableType.externalTable(new TableId(null, "APP", "EXT"), 71);
        tables.overwriteTable(external);
        assertThatThrownBy(() -> parser.parse(sql, tables, "APP")).isInstanceOf(IllegalArgumentException.class);
        assertThat(tables.tableIds()).containsExactly(external.id());
        assertThat(tables.forTable(external.id())).isEqualTo(external);
    }

    @Test
    void shouldRetainEachHistoricalDefinitionThroughRenameTruncateAndDrop() {
        final var originalId = new TableId(null, "app.😀", "T");
        final var renamedId = new TableId(null, "app.😀", "Renamed");
        parser.parse("CREATE TABLE \"app.😀\".T(ID INT, V VARCHAR(20), CONSTRAINT PK_T PRIMARY KEY(ID))", tables, "OTHER");
        final var original = tables.forTable(originalId);
        tables.overwriteTable(original.edit().addAttributes(original.attributes())
                .addAttribute(Attribute.editor().name("dameng.object.id").value("101").create()).create());

        final var changes = parser.parse("ALTER TABLE \"app.😀\".T ADD AMOUNT DECIMAL(12,4) DEFAULT 1.2500;"
                + "ALTER TABLE \"app.😀\".T RENAME TO \"Renamed\";"
                + "TRUNCATE TABLE \"app.😀\".\"Renamed\" REUSE STORAGE;"
                + "DROP TABLE \"app.😀\".\"Renamed\" CASCADE CONSTRAINTS PURGE", tables, "OTHER");

        assertThat(changes).extracting(DamengDdlParser.SchemaChange::type)
                .containsExactly(DamengDdlParser.Type.ALTER, DamengDdlParser.Type.RENAME,
                        DamengDdlParser.Type.TRUNCATE, DamengDdlParser.Type.DROP);
        assertThat(changes.get(0).table().id()).isEqualTo(originalId);
        assertThat(changes.get(1).previousTableId()).isEqualTo(originalId);
        assertThat(changes.get(1).tableId()).isEqualTo(renamedId);
        assertThat(changes.get(1).table().id()).isEqualTo(renamedId);
        assertThat(changes.get(2).table()).isEqualTo(changes.get(1).table());
        assertThat(changes.get(3).table()).isEqualTo(changes.get(2).table());
        for (var change : changes) {
            assertThat(change.table().primaryKeyColumnNames()).containsExactly("ID");
            assertThat(change.table().attributeWithName("dameng.object.id").asLong()).isEqualTo(101L);
            assertThat(change.table().attributeWithName("dameng.constraint.PK_T").value()).isEqualTo("PRIMARY");
            assertThat(change.table().columnWithName("AMOUNT").defaultValueExpression()).contains("1.2500");
        }
        assertThat(original.retrieveColumnNames()).containsExactly("ID", "V");
        assertThat(tables.tableIds()).isEmpty();
    }

    @Test
    void shouldBuildLikeCopiesFromTheSourceDefinitionAtTheirStatementPositions() {
        parser.parse("CREATE TABLE APP.S(ID INT IDENTITY(1,1) PRIMARY KEY, V VARCHAR(20) DEFAULT 'old' NOT NULL COMMENT 'source')", tables, "APP");
        final var sourceId = new TableId(null, "APP", "S");
        final var source = tables.forTable(sourceId);
        tables.overwriteTable(source.edit().addAttributes(source.attributes())
                .addAttribute(Attribute.editor().name("dameng.object.id").value("202").create()).create());

        final var changes = parser.parse("CREATE TABLE APP.SNAPSHOT LIKE APP.S;"
                + "ALTER TABLE APP.S MODIFY V VARCHAR(90); CREATE TABLE APP.LATER LIKE APP.S", tables, "APP");

        final var snapshot = tables.forTable(null, "APP", "SNAPSHOT");
        final var later = tables.forTable(null, "APP", "LATER");
        assertThat(changes).extracting(DamengDdlParser.SchemaChange::type)
                .containsExactly(DamengDdlParser.Type.CREATE, DamengDdlParser.Type.ALTER, DamengDdlParser.Type.CREATE);
        assertThat(snapshot.retrieveColumnNames()).containsExactly("ID", "V");
        assertThat(snapshot.columnWithName("V").length()).isEqualTo(20);
        assertThat(snapshot.columnWithName("V").isOptional()).isFalse();
        assertThat(snapshot.columnWithName("ID").isOptional()).isFalse();
        assertThat(snapshot.columnWithName("ID").isAutoIncremented()).isFalse();
        assertThat(later.columnWithName("V").length()).isEqualTo(90);
        for (var copied : List.of(snapshot, later)) {
            assertThat(copied.primaryKeyColumnNames()).isEmpty();
            assertThat(copied.attributes()).isEmpty();
            for (var column : copied.columns()) {
                assertThat(column.defaultValueExpression()).isEmpty();
                assertThat(column.comment()).isNull();
                assertThat(column.isAutoIncremented()).isFalse();
            }
        }
        assertThat(tables.forTable(sourceId).columnWithName("V").defaultValueExpression()).contains("'old'");
        assertThat(source.columnWithName("V").length()).isEqualTo(20);
    }

    @Test
    void shouldResolveUnqualifiedLikeSourcesInTheSessionSchema() {
        parser.parse("CREATE TABLE APP.S(ID INT); CREATE TABLE OTHER.S(ID BIGINT)", tables, "APP");
        final var analysis = parser.analyze("CREATE TABLE OTHER.PLANNED LIKE S", "APP");

        final var plan = parser.plan(analysis, tables);

        parser.parse("CREATE TABLE OTHER.COPY LIKE S", tables, "APP");

        assertThat(plan).singleElement().satisfies(change -> {
            assertThat(change.tableId()).isEqualTo(new TableId(null, "OTHER", "PLANNED"));
            assertThat(change.table().columnWithName("ID").jdbcType()).isEqualTo(Types.INTEGER);
        });
        assertThat(tables.forTable(null, "OTHER", "PLANNED")).isNull();
        assertThat(tables.forTable(null, "OTHER", "COPY").columnWithName("ID").jdbcType()).isEqualTo(Types.INTEGER);
    }

    @Test
    void shouldKeepFractionalPartitionBoundariesSeparateFromIntegerStorageParameters() {
        parser.parse("CREATE TABLE APP.T(ID DECIMAL(10,2)) PARTITION BY RANGE(ID)"
                + " (PARTITION P0 VALUES LESS THAN(1.25) STORAGE(INITIAL 1),"
                + " PARTITION P1 VALUES LESS THAN(1.2e+3) STORAGE(ON MAIN))", tables, "APP");

        assertThat(tables.forTable(null, "APP", "T").columnWithName("ID").scale()).contains(2);
    }

    @Test
    void shouldKeepDefaultTokenSpellingAndLiteralContentsAcrossTreeBasedChanges() {
        final var textDefault = "(q'[😀;--/* it''s */]' || N'尾')";
        final var numberDefault = "(.5e+2 + 1.2500)";
        parser.parse("/*😀*/ CREATE TABLE APP.T(ID INT, V VARCHAR(80) DEFAULT " + textDefault
                + " NOT NULL, N DECIMAL(12,4) DEFAULT (.5e+2 /* redo comment */ + 1.2500))", tables, "APP");
        final var id = new TableId(null, "APP", "T");
        assertThat(tables.forTable(id).columnWithName("V").defaultValueExpression()).contains(textDefault);
        assertThat(tables.forTable(id).columnWithName("N").defaultValueExpression()).contains(numberDefault);

        final var analysis = parser.analyze("ALTER TABLE APP.T MODIFY V VARCHAR(160)", "APP");
        final var original = tables.forTable(id);
        final var plan = parser.plan(analysis, tables);

        assertThat(tables.forTable(id)).isSameAs(original);
        assertThat(plan).singleElement().satisfies(change -> {
            final var modified = change.table().columnWithName("V");
            assertThat(modified.length()).isEqualTo(160);
            assertThat(modified.isOptional()).isFalse();
            assertThat(modified.defaultValueExpression()).contains(textDefault);
        });
        parser.parse("ALTER TABLE APP.T ALTER COLUMN V SET DEFAULT q'!changed;/*😀*/!';"
                + "ALTER TABLE APP.T ALTER COLUMN N DROP DEFAULT", tables, "APP");
        assertThat(tables.forTable(id).columnWithName("V").defaultValueExpression()).contains("q'!changed;/*😀*/!'");
        assertThat(tables.forTable(id).columnWithName("N").defaultValueExpression()).isEmpty();
    }

    @Test
    void shouldRetainHexadecimalAndKeywordFunctionDefaultsAcrossHistoricalChanges() {
        parser.parse("CREATE TABLE APP.T(ID INT PRIMARY KEY, PAYLOAD VARBINARY(4) DEFAULT 0xF,"
                + " N DOUBLE DEFAULT LOG(10,100), C VARCHAR(4) DEFAULT CHAR(65), F INT DEFAULT FIELD(2,1,2,3))", tables, "APP");
        final var id = new TableId(null, "APP", "T");
        final var original = tables.forTable(id);
        assertThat(original.columnWithName("PAYLOAD").defaultValueExpression()).contains("0xF");
        assertThat(original.columnWithName("N").defaultValueExpression()).contains("LOG(10,100)");
        assertThat(original.columnWithName("C").defaultValueExpression()).contains("CHAR(65)");
        assertThat(original.columnWithName("F").defaultValueExpression()).contains("FIELD(2,1,2,3)");

        final var changes = parser.parse("ALTER TABLE APP.T ALTER COLUMN PAYLOAD SET DEFAULT 0X123;"
                + "ALTER TABLE APP.T ALTER COLUMN C SET DEFAULT CHAR(66); ALTER TABLE APP.T MODIFY PAYLOAD VARBINARY(8)", tables, "APP");

        assertThat(changes).hasSize(3);
        assertThat(changes.get(0).table().columnWithName("PAYLOAD").length()).isEqualTo(4);
        assertThat(changes.get(0).table().columnWithName("PAYLOAD").defaultValueExpression()).contains("0X123");
        assertThat(changes.get(0).table().columnWithName("C").defaultValueExpression()).contains("CHAR(65)");
        final var changed = tables.forTable(id);
        assertThat(changed.columnWithName("PAYLOAD").jdbcType()).isEqualTo(Types.VARBINARY);
        assertThat(changed.columnWithName("PAYLOAD").length()).isEqualTo(8);
        assertThat(changed.columnWithName("PAYLOAD").defaultValueExpression()).contains("0X123");
        assertThat(changed.columnWithName("C").defaultValueExpression()).contains("CHAR(66)");
        assertThat(original.columnWithName("PAYLOAD").defaultValueExpression()).contains("0xF");
    }

    @Test
    void shouldApplyJsonCheckConstraintsWithoutReplacingTheHistoricalColumnTypes() {
        parser.parse("CREATE TABLE APP.T(ID INT PRIMARY KEY, DOC CLOB CONSTRAINT CK_JSON CHECK(DOC IS JSON (STRICT) WITH UNIQUE KEYS))",
                tables, "APP");
        final var id = new TableId(null, "APP", "T");
        final var original = tables.forTable(id);
        assertThat(original.attributeWithName("dameng.constraint.CK_JSON").value()).isEqualTo("CHECK");
        assertThat(original.columnWithName("DOC").jdbcType()).isEqualTo(Types.CLOB);

        parser.parse("ALTER TABLE APP.T DROP CONSTRAINT CK_JSON;"
                + "ALTER TABLE APP.T ADD CONSTRAINT CK_JSON_LAX CHECK(DOC IS JSON LAX WITHOUT UNIQUE KEYS)", tables, "APP");

        final var changed = tables.forTable(id);
        assertThat(changed.attributeWithName("dameng.constraint.CK_JSON")).isNull();
        assertThat(changed.attributeWithName("dameng.constraint.CK_JSON_LAX").value()).isEqualTo("CHECK");
        assertThat(changed.columnWithName("DOC").jdbcType()).isEqualTo(Types.CLOB);
        assertThat(changed.primaryKeyColumnNames()).containsExactly("ID");
        assertThat(original.attributeWithName("dameng.constraint.CK_JSON").value()).isEqualTo("CHECK");
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "ALTER TABLE APP.T ALTER COLUMN PAYLOAD SET DEFAULT 0x",
            "ALTER TABLE APP.T ALTER COLUMN PAYLOAD SET DEFAULT 0xG1",
            "ALTER TABLE APP.T ALTER COLUMN PAYLOAD SET DEFAULT 0xF1G",
            "ALTER TABLE APP.T ADD CONSTRAINT CK_JSON CHECK(DOC IS JSON WITH UNIQUE)",
            "ALTER TABLE APP.T ADD CONSTRAINT CK_JSON CHECK(DOC IS JSON (STRICT LAX))",
            "ALTER TABLE APP.T ALTER COLUMN PAYLOAD SET DEFAULT 0xF DROP TABLE APP.T"
    })
    void shouldLeaveHistoricalTablesUnchangedWhenNewLiteralOrCheckSyntaxIsIncomplete(String lastStatement) {
        parser.parse("CREATE TABLE APP.T(ID INT PRIMARY KEY, PAYLOAD VARBINARY(4) DEFAULT 0x01, DOC CLOB)", tables, "APP");
        final var id = new TableId(null, "APP", "T");
        final var original = tables.forTable(id);

        assertThatThrownBy(() -> parser.parse("ALTER TABLE APP.T ADD PENDING INT;" + lastStatement, tables, "APP"))
                .isInstanceOf(IllegalArgumentException.class);

        assertThat(tables.forTable(id)).isSameAs(original);
        assertThat(original.retrieveColumnNames()).containsExactly("ID", "PAYLOAD", "DOC");
        assertThat(original.columnWithName("PAYLOAD").defaultValueExpression()).contains("0x01");
    }

    @Test
    void shouldPreserveTheStreamingCreatedIntervalDefaultAndItsHistoricalPrecision() {
        final var expression = "INTERVAL '999999999-11' YEAR TO MONTH";
        parser.parse("CREATE TABLE APP.T(ID INT PRIMARY KEY, CALENDAR INTERVAL YEAR(9) TO MONTH DEFAULT " + expression + ")", tables, "APP");

        final var column = tables.forTable(null, "APP", "T").columnWithName("CALENDAR");
        assertThat(column.jdbcType()).isEqualTo(Types.OTHER);
        assertThat(column.typeName()).isEqualTo("INTERVAL YEAR TO MONTH");
        assertThat(column.typeExpression()).isEqualTo("INTERVAL YEAR(9) TO MONTH");
        assertThat(column.length()).isEqualTo(9);
        assertThat(column.scale()).contains(0);
        assertThat(column.defaultValueExpression()).contains(expression);
        final var defaults = new DamengDefaultValueConverter(new DamengValueConverters(
                new DamengConnectorConfig(DamengTestConfiguration.configuration().build())));
        assertThat(defaults.parseDefaultValue(column, column.defaultValueExpression().orElseThrow()))
                .contains("P999999999Y11M0DT0H0M0S");
    }

    @Test
    void shouldPreserveSignedIntervalDefaultQualifiersAcrossAnAlter() {
        final var expression = "INTERVAL -'999999999 23:59:59.123456' DAY(9) TO SECOND(6)";
        parser.parse("CREATE TABLE APP.T(ID INT, SPAN INTERVAL DAY(9) TO SECOND(6) DEFAULT " + expression + ")", tables, "APP");
        final var id = new TableId(null, "APP", "T");
        final var original = tables.forTable(id).columnWithName("SPAN");
        assertThat(original.jdbcType()).isEqualTo(Types.OTHER);
        assertThat(original.typeName()).isEqualTo("INTERVAL DAY TO SECOND");
        assertThat(original.typeExpression()).isEqualTo("INTERVAL DAY(9) TO SECOND(6)");
        assertThat(original.length()).isEqualTo(9);
        assertThat(original.scale()).contains(6);
        assertThat(original.defaultValueExpression()).contains(expression);

        final var replacement = "INTERVAL +'001 02:03:04.000005' DAY(3) TO SECOND(6)";
        parser.parse("ALTER TABLE APP.T ALTER COLUMN SPAN SET DEFAULT " + replacement, tables, "APP");

        final var changed = tables.forTable(id).columnWithName("SPAN");
        assertThat(changed.typeExpression()).isEqualTo(original.typeExpression());
        assertThat(changed.length()).isEqualTo(original.length());
        assertThat(changed.scale()).isEqualTo(original.scale());
        assertThat(changed.defaultValueExpression()).contains(replacement);
        final var defaults = new DamengDefaultValueConverter(new DamengValueConverters(
                new DamengConnectorConfig(DamengTestConfiguration.configuration().build())));
        assertThat(defaults.parseDefaultValue(changed, changed.defaultValueExpression().orElseThrow()))
                .contains("P0Y0M1DT2H3M4.000005S");
    }

    @Test
    void shouldKeepTypedTemporalLiteralSpellingWithoutChangingColumnTypesOrPrecision() {
        final var date = "DATE '2000-02-29'";
        final var timestamp = "TIMESTAMP'2000-02-29 12:34:56.123456'";
        final var datetime = "datetime '2018-04-01 00:00:00.000001'";
        parser.parse("CREATE TABLE APP.T(D DATE DEFAULT " + date + ", TS TIMESTAMP(6) DEFAULT " + timestamp
                + ", DT DATETIME(6) DEFAULT " + datetime + ")", tables, "APP");

        final var table = tables.forTable(null, "APP", "T");
        assertThat(table.columnWithName("D").jdbcType()).isEqualTo(Types.DATE);
        assertThat(table.columnWithName("D").typeName()).isEqualTo("DATE");
        assertThat(table.columnWithName("D").length()).isEqualTo(13);
        assertThat(table.columnWithName("D").scale()).contains(0);
        assertThat(table.columnWithName("D").defaultValueExpression()).contains(date);
        assertThat(table.columnWithName("TS").typeName()).isEqualTo("TIMESTAMP");
        assertThat(table.columnWithName("DT").typeName()).isEqualTo("DATETIME");
        for (var name : List.of("TS", "DT")) {
            assertThat(table.columnWithName(name).jdbcType()).isEqualTo(Types.TIMESTAMP);
            assertThat(table.columnWithName(name).length()).isEqualTo(36);
            assertThat(table.columnWithName(name).scale()).contains(6);
        }
        assertThat(table.columnWithName("TS").typeExpression()).isEqualTo("TIMESTAMP(6)");
        assertThat(table.columnWithName("DT").typeExpression()).isEqualTo("DATETIME(6)");
        assertThat(table.columnWithName("TS").defaultValueExpression()).contains(timestamp);
        assertThat(table.columnWithName("DT").defaultValueExpression()).contains(datetime);
    }

    @Test
    void shouldModifyOnlyColumnAttributesWithoutReplacingItsHistoricalType() {
        parser.parse("CREATE TABLE APP.T(ID INT, VALUE DECIMAL(12,4) DEFAULT 1.2500)", tables, "APP");
        final var id = new TableId(null, "APP", "T");

        parser.parse("ALTER TABLE APP.T MODIFY VALUE NOT NULL;"
                + "ALTER TABLE APP.T MODIFY VALUE DEFAULT 2.5000", tables, "APP");

        final var column = tables.forTable(id).columnWithName("VALUE");
        assertThat(column.jdbcType()).isEqualTo(Types.DECIMAL);
        assertThat(column.length()).isEqualTo(12);
        assertThat(column.scale()).contains(4);
        assertThat(column.defaultValueExpression()).contains("2.5000");
        assertThat(column.isOptional()).isFalse();
    }

    @Test
    void shouldResolveGroupedAlterConstraintsAfterAddingTheirColumns() {
        parser.parse("CREATE TABLE APP.T(VALUE VARCHAR(30))", tables, "APP");

        parser.parse("ALTER TABLE APP.T ADD (CONSTRAINT PK_NEW PRIMARY KEY(NEW_ID), NEW_ID INT)", tables, "APP");

        final var table = tables.forTable(null, "APP", "T");
        assertThat(table.retrieveColumnNames()).containsExactly("VALUE", "NEW_ID");
        assertThat(table.primaryKeyColumnNames()).containsExactly("NEW_ID");
        assertThat(table.columnWithName("NEW_ID").isOptional()).isFalse();
        assertThat(table.attributeWithName("dameng.constraint.PK_NEW").value()).isEqualTo("PRIMARY");
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "ALTER TABLE APP.RENAMED ADD BAD UNKNOWN_TYPE",
            "CREATE TABLE APP.COPY AS SELECT ID+1 FROM APP.RENAMED",
            "CREATE TABLE APP.COPY AS SELECT ID FROM APP.RENAMED UNION SELECT ID FROM APP.RENAMED",
            "ALTER TABLE APP.RENAMED ADD V INT GENERATED ALWAYS AS(ID+1) VIRTUAL",
            "CREATE HUGE TABLE APP.COPY(ID INT)"
    })
    void shouldRollBackTheWholeScriptWhenALaterTreeIsValidButSemanticsAreUnsupported(String lastStatement) {
        parser.parse("CREATE TABLE APP.T(ID INT PRIMARY KEY); CREATE TABLE APP.KEEP(ID INT)", tables, "APP");
        final var id = new TableId(null, "APP", "T");
        final var keptId = new TableId(null, "APP", "KEEP");
        final var original = tables.forTable(id);
        final var kept = tables.forTable(keptId);

        assertThatThrownBy(() -> parser.parse("ALTER TABLE APP.T ADD VALUE VARCHAR(30);"
                + "ALTER TABLE APP.T RENAME TO APP.RENAMED; DROP TABLE APP.KEEP;" + lastStatement, tables, "APP"))
                .isInstanceOf(IllegalArgumentException.class);

        assertThat(tables.tableIds()).containsExactlyInAnyOrder(id, keptId);
        assertThat(tables.forTable(id)).isSameAs(original);
        assertThat(tables.forTable(keptId)).isSameAs(kept);
        assertThat(original.retrieveColumnNames()).containsExactly("ID");
    }

    @Test
    void shouldUseProgramBoundariesWhenParsingAScriptWithoutWeakeningRecordAnalysis() {
        final var sql = "CREATE TABLE APP.T(ID INT);"
                + "CREATE PROCEDURE APP.P AS V INT := 1; CURSOR C IS SELECT ID FROM APP.T; BEGIN BEGIN NULL; END; END P;"
                + "ALTER TABLE APP.T ADD NAME VARCHAR(30);"
                + "CREATE FUNCTION APP.F RETURN INT AS V INT DEFAULT 1; BEGIN RETURN CASE WHEN V=1 THEN 1 ELSE 0 END; END F;"
                + "COMMENT ON TABLE APP.T IS 'not a table change';";

        final var changes = parser.parse(sql, tables, "APP");

        assertThat(changes).extracting(DamengDdlParser.SchemaChange::type)
                .containsExactly(DamengDdlParser.Type.CREATE, DamengDdlParser.Type.ALTER);
        assertThat(changes.get(0).table().retrieveColumnNames()).containsExactly("ID");
        assertThat(tables.forTable(null, "APP", "T").retrieveColumnNames()).containsExactly("ID", "NAME");
        assertThatThrownBy(() -> parser.analyze(sql, "APP")).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void shouldNotApplyEarlierTableChangesWhenALaterProgramIsIncomplete() {
        parser.parse("CREATE TABLE APP.T(ID INT)", tables, "APP");
        final var id = new TableId(null, "APP", "T");
        final var original = tables.forTable(id);

        assertThatThrownBy(() -> parser.parse("ALTER TABLE APP.T ADD NAME VARCHAR(30);"
                + "CREATE PROCEDURE APP.P AS V INT BEGIN NULL; END;", tables, "APP"))
                .isInstanceOf(IllegalArgumentException.class);

        assertThat(tables.forTable(id)).isSameAs(original);
        assertThat(original.retrieveColumnNames()).containsExactly("ID");
    }

    @Test
    void shouldContinueDecodingDmlAgainstTheEvolvedHistoricalColumnOrder() {
        parser.parse("CREATE TABLE APP.T(ID INT PRIMARY KEY, NAME VARCHAR(80), D DATE);"
                + "ALTER TABLE APP.T RENAME COLUMN NAME TO LABEL;"
                + "ALTER TABLE APP.T ADD ELAPSED INTERVAL DAY(4) TO SECOND(6)", tables, "APP");
        final var table = tables.forTable(null, "APP", "T");
        final var dml = new LogMinerDmlParser().parse("INSERT INTO APP.T(ELAPSED,LABEL,ID,D)"
                + " VALUES(INTERVAL -'2 03:04:05.000006' DAY(4) TO SECOND(6), 'TO_DATE(''literal'')', 7, TO_DATE('2026-10-29','YYYY-MM-DD'))", table);

        assertThat(table.columnWithName("D").jdbcType()).isEqualTo(Types.DATE);
        assertThat(dml.operation()).isEqualTo(Operation.CREATE);
        assertThat(dml.oldValues()).isEmpty();
        assertThat(dml.newValues()).containsExactly("7", "TO_DATE('literal')",
                new SqlExpression("TO_DATE('2026-10-29','YYYY-MM-DD')"),
                new SqlExpression("INTERVAL -'2 03:04:05.000006' DAY(4) TO SECOND(6)"));
        assertThat(dml.newPresent().cardinality()).isEqualTo(4);
    }
}
