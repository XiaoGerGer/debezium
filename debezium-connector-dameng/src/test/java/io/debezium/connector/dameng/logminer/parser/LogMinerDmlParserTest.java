/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng.logminer.parser;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.BitSet;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import io.debezium.connector.dameng.logminer.parser.LogMinerDmlParser.SqlExpression;
import io.debezium.data.Envelope.Operation;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;

class LogMinerDmlParserTest {
    private final LogMinerDmlParser parser = new LogMinerDmlParser();
    private final Table table = table("ID", "NAME", "AMOUNT", "TS", "BIN", "FLAG");

    @Test
    void shouldParseInsertInColumnOrder() {
        final var entry = parser.parse(
                "insert into \"APP\".\"EVENTS\"(\"NAME\",\"ID\",\"TS\",\"AMOUNT\",\"BIN\",\"FLAG\")"
                        + " values ('Acme','1',TO_TIMESTAMP('2026-09-24 12:34:56.123456','YYYY-MM-DD HH24:MI:SS.FF6'),"
                        + "-12345678901234567890.123456789,HEXTORAW('00ABCDEF'),NULL);",
                table);

        assertThat(entry.operation()).isEqualTo(Operation.CREATE);
        assertThat(entry.oldValues()).isEmpty();
        assertThat(entry.newValues()).containsExactly("1", "Acme", "-12345678901234567890.123456789",
                new SqlExpression("TO_TIMESTAMP('2026-09-24 12:34:56.123456','YYYY-MM-DD HH24:MI:SS.FF6')"),
                new SqlExpression("HEXTORAW('00ABCDEF')"), null);
        assertThat(entry.newPresent().cardinality()).isEqualTo(6);
    }

    @Test
    void shouldMergeOnlyUnchangedColumnsIntoUpdate() {
        final var entry = parser.parse(
                "update \"APP\".\"EVENTS\" set \"NAME\" = NULL, \"AMOUNT\" = 2.5 "
                        + "where \"ID\" = 7 and \"NAME\" = 'before' and \"AMOUNT\" = 1.5 and \"FLAG\" IS NULL;",
                table);

        assertThat(entry.operation()).isEqualTo(Operation.UPDATE);
        assertThat(entry.oldValues()).containsExactly("7", "before", "1.5", null, null, null);
        assertThat(entry.newValues()).containsExactly("7", null, "2.5", null, null, null);
        assertThat(entry.oldPresent()).isEqualTo(bits(0, 1, 2, 5));
        assertThat(entry.newPresent()).isEqualTo(bits(0, 1, 2, 5));
    }

    @Test
    void shouldPreserveBeforeValuesOnPrimaryKeyChange() {
        final var entry = parser.parse("UPDATE APP.EVENTS SET ID=2 WHERE ID=1 AND NAME='unchanged'", table);

        assertThat(entry.oldValues()).startsWith("1", "unchanged");
        assertThat(entry.newValues()).startsWith("2", "unchanged");
    }

    @Test
    void shouldParseRealDamengInsertRedo() {
        final var entry = parser.parse(
                "INSERT INTO \"SYSDBA\".\"DBZ_PROBE\"(\"ID\", \"NAME\", \"AMOUNT\") VALUES(1, 'one', 123.123)",
                table(new TableId(null, "SYSDBA", "DBZ_PROBE"), "ID", "NAME", "AMOUNT", "TS", "BIN", "FLAG"));

        assertThat(entry.operation()).isEqualTo(Operation.CREATE);
        assertThat(entry.newValues()).startsWith("1", "one", "123.123");
        assertThat(entry.newPresent()).isEqualTo(bits(0, 1, 2));
    }

    @Test
    void shouldParseRealDamengUpdateRedo() {
        final var entry = parser.parse(
                "UPDATE \"SYSDBA\".\"DBZ_PROBE\" SET \"NAME\" = 'two' WHERE \"ID\" = 1 AND \"NAME\" = 'one' AND \"AMOUNT\" = 123.123",
                table(new TableId(null, "SYSDBA", "DBZ_PROBE"), "ID", "NAME", "AMOUNT"));

        assertThat(entry.operation()).isEqualTo(Operation.UPDATE);
        assertThat(entry.oldValues()).containsExactly("1", "one", "123.123");
        assertThat(entry.newValues()).containsExactly("1", "two", "123.123");
    }

    @Test
    void shouldReadDeleteBeforeImage() {
        final var entry = parser.parse(
                "DELETE FROM APP.EVENTS WHERE (ID=7 AND (NAME='before' AND FLAG IS NULL)) AND AMOUNT=-1.2e-8;", table);

        assertThat(entry.operation()).isEqualTo(Operation.DELETE);
        assertThat(entry.oldValues()).containsExactly("7", "before", "-1.2e-8", null, null, null);
        assertThat(entry.newValues()).isEmpty();
        assertThat(entry.oldPresent()).isEqualTo(bits(0, 1, 2, 5));
    }

    @ParameterizedTest
    @MethodSource("strings")
    void shouldDecodeStringsWithoutInterpretingTheirContents(String literal, String expected) {
        final var entry = parser.parse("INSERT INTO APP.EVENTS(ID,NAME) VALUES(1," + literal + ")", table);

        assertThat(entry.newValues()[1]).isEqualTo(expected);
    }

    static Stream<Arguments> strings() {
        return Stream.of(
                Arguments.of("'null'", "null"),
                Arguments.of("'NULL'", "NULL"),
                Arguments.of("''", ""),
                Arguments.of("'a''b''''c'", "a'b''c"),
                Arguments.of("'where a=1 and b=2 OR c=3; -- /* */'", "where a=1 and b=2 OR c=3; -- /* */"),
                Arguments.of("'a,b(c)=d'", "a,b(c)=d"),
                Arguments.of("'HEXTORAW(''ABCD'')'", "HEXTORAW('ABCD')"),
                Arguments.of("'0x00FF'", "0x00FF"),
                Arguments.of("'OUT_CLOB'", "OUT_CLOB"),
                Arguments.of("'OUT_BLOB'", "OUT_BLOB"),
                Arguments.of("N'达梦😀数据库'", "达梦😀数据库"),
                Arguments.of("'line1\nline2\\folder'", "line1\nline2\\folder"));
    }

    @ParameterizedTest
    @ValueSource(strings = { "0", "-1", "+1", ".5", "-.5", "1.", "1.2e10", "-7.2E-100", "18446744073709551615",
            "99999999999999999999999999999999999999.99999999999999999999" })
    void shouldPreserveNumericPrecision(String number) {
        final var entry = parser.parse("INSERT INTO APP.EVENTS(AMOUNT) VALUES(" + number + ")", table);

        assertThat(entry.newValues()[2]).isEqualTo(number);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "TO_DATE('2026-09-24','YYYY-MM-DD')",
            "TO_TIMESTAMP('2026-09-24 12:34:56.999999','YYYY-MM-DD HH24:MI:SS.FF6')",
            "TO_TIMESTAMP_TZ('2026-09-24 12:34:56 +08:00','YYYY-MM-DD HH24:MI:SS TZH:TZM')",
            "HEXTORAW('00ff')", "UNISTR('\\4F60\\597D')", "TO_NUMBER('1.234E+20')",
            "EMPTY_BLOB()", "EMPTY_CLOB()", "DATE '2026-09-24'", "TIMESTAMP '2026-09-24 12:34:56.123456'",
            "TIME'12:34:56.123456'", "TIME '12:34:56'", "DATE'2026-09-24'", "TIMESTAMP'2026-09-24 12:34:56.123456'",
            "OUT_CLOB", "OUT_BLOB",
            "INTERVAL '12-3' YEAR(3) TO MONTH", "INTERVAL '2 3:4:5.123456' DAY(3) TO SECOND(6)",
            "INTERVAL '12.123456' SECOND(2,6)",
            "TO_NUMBER(UNISTR('\\0031'))"
    })
    void shouldKeepTypedExpressionsSeparateFromStrings(String expression) {
        final var entry = parser.parse("INSERT INTO APP.EVENTS(TS) VALUES(" + expression + ")", table);

        assertThat(entry.newValues()[3]).isEqualTo(new SqlExpression(expression));
    }

    @ParameterizedTest
    @ValueSource(strings = { "0x000180ff", "0X000180FF" })
    void shouldDecodeNativeDamengBinaryLiteral(String literal) {
        final var entry = parser.parse("INSERT INTO APP.EVENTS(ID,BIN) VALUES(2," + literal + ");", table);

        assertThat((byte[]) entry.newValues()[4]).containsExactly((byte) 0, (byte) 1, (byte) 0x80, (byte) 0xff);
    }

    @Test
    void shouldDecodeBinaryBeforeAndAfterImages() {
        final var entry = parser.parse("UPDATE APP.EVENTS SET BIN=0xABCD WHERE ID=2 AND BIN=0x0123;", table);

        assertThat((byte[]) entry.oldValues()[4]).containsExactly((byte) 0x01, (byte) 0x23);
        assertThat((byte[]) entry.newValues()[4]).containsExactly((byte) 0xab, (byte) 0xcd);
    }

    @Test
    void shouldDistinguishEmptyBinaryLiteralFromNull() {
        final var entry = parser.parse("UPDATE APP.EVENTS SET BIN=0x WHERE ID=2 AND BIN IS NULL", table);

        assertThat(entry.oldValues()[4]).isNull();
        assertThat((byte[]) entry.newValues()[4]).isEmpty();
    }

    @ParameterizedTest
    @ValueSource(strings = { "0xF", "0x00F", "0xGG", "0x00FFtail", "+0x01", "-0x01", "0x00 11" })
    void shouldRejectMalformedBinaryLiterals(String literal) {
        assertThatThrownBy(() -> parser.parse("INSERT INTO APP.EVENTS(BIN) VALUES(" + literal + ")", table))
                .isInstanceOf(DmlParserException.class);
    }

    @Test
    void shouldDistinguishMissingColumnsFromNull() {
        final var entry = parser.parse("INSERT INTO APP.EVENTS(ID,NAME) VALUES(1,NULL)", table);

        assertThat(entry.newValues()[1]).isNull();
        assertThat(entry.newValues()[2]).isNull();
        assertThat(entry.newPresent()).isEqualTo(bits(0, 1));
    }

    @Test
    void shouldDecodeQuotedIdentifiers() {
        final var quotedTable = Table.editor()
                .tableId(new TableId(null, "a\"b", "order.items"))
                .addColumn(Column.editor().name("set\"where").create())
                .addColumn(Column.editor().name("x.y").create())
                .create();

        final var entry = parser.parse(
                "UPDATE \"a\"\"b\".\"order.items\" SET \"set\"\"where\"='after' WHERE \"set\"\"where\"='before' AND \"x.y\"=2",
                quotedTable);

        assertThat(entry.oldValues()).containsExactly("before", "2");
        assertThat(entry.newValues()).containsExactly("after", "2");
    }

    @Test
    void shouldParseWhitespaceAndCommentsOutsideLiterals() {
        final var entry = parser.parse(
                "/* redo */\nInSeRt\tInTo APP.EVENTS\n(ID,/* comment */NAME)\nVaLuEs (1,'/* literal */'); -- trailing",
                table);

        assertThat(entry.newValues()).startsWith("1", "/* literal */");
    }

    @Test
    void shouldIgnoreRowLocatorWithoutLosingOtherPredicates() {
        final var entry = parser.parse(
                "UPDATE APP.EVENTS SET NAME='after' WHERE ROWID='00000001' AND ID=1 AND NAME='before'", table);

        assertThat(entry.oldValues()).startsWith("1", "before");
        assertThat(entry.newValues()).startsWith("1", "after");
        assertThat(entry.oldPresent()).isEqualTo(bits(0, 1));
    }

    @Test
    void shouldParseLongMultibyteSqlWithoutTruncation() {
        final String value = "达梦'a,b() AND \"OR\" \n".repeat(5000);
        final var entry = parser.parse(
                "UPDATE APP.EVENTS SET NAME='" + value.replace("'", "''") + "' WHERE ID=1 AND NAME='before'", table);

        assertThat(entry.newValues()[1]).isEqualTo(value);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "DELETE FROM APP.EVENTS WHERE ID=1 OR NAME='other'",
            "DELETE FROM APP.EVENTS WHERE (ID=1 OR ID=2)",
            "DELETE FROM APP.EVENTS WHERE ID>1",
            "DELETE FROM APP.EVENTS WHERE NAME IS NOT NULL",
            "DELETE FROM APP.EVENTS WHERE ID IN (1,2)",
            "DELETE FROM APP.EVENTS WHERE ID=1 AND ID=2",
            "DELETE FROM APP.EVENTS",
            "UPDATE APP.EVENTS SET NAME='x'",
            "UPDATE APP.EVENTS SET AMOUNT=AMOUNT+1 WHERE ID=1",
            "UPDATE APP.EVENTS SET ID=2,ID=3 WHERE ID=1",
            "INSERT INTO APP.EVENTS(ID) VALUES(Unsupported Type)",
            "INSERT INTO APP.EVENTS(ID) VALUES(Unsupported)",
            "INSERT INTO APP.EVENTS(ID) VALUES(SYSDATE)",
            "INSERT INTO APP.EVENTS(ID) VALUES(OUT_CLOB_OTHER)",
            "INSERT INTO APP.EVENTS(ID) VALUES(OUT_BLOB())",
            "INSERT INTO APP.EVENTS(ID) VALUES(N 1)",
            "INSERT INTO APP.EVENTS(ID) VALUES(DATE 123)",
            "INSERT INTO APP.EVENTS(ID) VALUES(INTERVAL 1 DAY)",
            "INSERT INTO APP.EVENTS(ID) VALUES(+)",
            "INSERT INTO APP.EVENTS(1) VALUES(1)",
            "INSERT INTO APP.EVENTS(ID) VALUES(INTERVAL '1' UNKNOWN)",
            "INSERT INTO APP.EVENTS(ID) VALUES(INTERVAL '1' DAY(-1))",
            "INSERT INTO APP.EVENTS(ID) VALUES(INTERVAL '1' DAY TO)",
            "INSERT INTO APP.EVENTS(ID) VALUES(MYSTERY('x'))",
            "INSERT INTO APP.EVENTS(ID) VALUES(TO_NUMBER(ID))",
            "INSERT INTO APP.EVENTS(ID) VALUES(1+2)",
            "INSERT INTO APP.EVENTS(ID) VALUES(1),(2)",
            "INSERT INTO APP.EVENTS(ID) VALUES(1,2)",
            "INSERT INTO APP.EVENTS(ID,NAME) VALUES(1)",
            "INSERT INTO APP.EVENTS(ID,ID) VALUES(1,2)",
            "INSERT INTO APP.EVENTS(UNKNOWN_COLUMN) VALUES(1)",
            "INSERT INTO APP.EVENTS(ROWID) VALUES('1')",
            "INSERT INTO APP.EVENTS(ID) VALUES(1); DELETE FROM APP.EVENTS WHERE ID=2",
            "INSERT INTO APP.EVENTS(NAME) VALUES('unterminated)",
            "INSERT INTO APP.EVENTS(ID) VALUES(1); /* unterminated",
            "MERGE INTO APP.EVENTS USING OTHER ON (1=1)"
    })
    void shouldRejectAmbiguousOrUnsupportedRedo(String sql) {
        assertThatThrownBy(() -> parser.parse(sql, table)).isInstanceOf(DmlParserException.class);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "INSERT INTO APP.WRONG_TABLE(ID) VALUES(1)",
            "INSERT INTO WRONG_SCHEMA.EVENTS(ID) VALUES(1)",
            "INSERT INTO APP.EVENTS(\"id\") VALUES(1)"
    })
    void shouldRejectMismatchedMetadata(String sql) {
        assertThatThrownBy(() -> parser.parse(sql, table)).isInstanceOf(DmlParserException.class);
    }

    @Test
    void shouldValidateCatalogWhenRedoUsesThreePartTableName() {
        final var catalogTable = table(new TableId("CATALOG", "APP", "EVENTS"), "ID");
        assertThat(parser.parse("INSERT INTO CATALOG.APP.EVENTS(ID) VALUES(1)", catalogTable).newValues()).containsExactly("1");
        assertThatThrownBy(() -> parser.parse("INSERT INTO OTHER.APP.EVENTS(ID) VALUES(1)", catalogTable))
                .isInstanceOf(DmlParserException.class).hasMessageContaining("catalog");
    }

    @Test
    void shouldRejectMissingInputs() {
        assertThatThrownBy(() -> parser.parse(null, table)).isInstanceOf(DmlParserException.class);
        assertThatThrownBy(() -> parser.parse(" ", table)).isInstanceOf(DmlParserException.class);
        assertThatThrownBy(() -> parser.parse("INSERT INTO APP.EVENTS(ID) VALUES(1)", null)).isInstanceOf(DmlParserException.class);
    }

    private static Table table(String... columns) {
        return table(new TableId(null, "APP", "EVENTS"), columns);
    }

    private static Table table(TableId id, String... columns) {
        final var editor = Table.editor().tableId(id);
        for (String column : columns) {
            editor.addColumn(Column.editor().name(column).create());
        }
        return editor.create();
    }

    private static BitSet bits(int... positions) {
        final var result = new BitSet();
        for (int position : positions) {
            result.set(position);
        }
        return result;
    }
}
