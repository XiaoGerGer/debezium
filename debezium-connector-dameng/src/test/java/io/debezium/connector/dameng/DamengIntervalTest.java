/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.ResultSet;
import java.sql.Types;
import java.util.List;
import java.util.stream.Stream;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.DataException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import io.debezium.connector.dameng.logminer.parser.LogMinerDmlParser;
import io.debezium.connector.dameng.logminer.parser.LogMinerDmlParser.SqlExpression;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;

class DamengIntervalTest {
    private final DamengValueConverters converters = new DamengValueConverters(
            new DamengConnectorConfig(DamengConnectorConfigTest.configuration().build()));

    static Stream<Arguments> qualifiers() {
        return Stream.of(
                Arguments.of("YEAR(9)", "999999999", "P999999999Y0M0DT0H0M0S"),
                Arguments.of("YEAR(9) TO MONTH", "999999999-11", "P999999999Y11M0DT0H0M0S"),
                Arguments.of("MONTH(9)", "999999999", "P0Y999999999M0DT0H0M0S"),
                Arguments.of("DAY(9)", "999999999", "P0Y0M999999999DT0H0M0S"),
                Arguments.of("DAY(9) TO HOUR", "999999999 23", "P0Y0M999999999DT23H0M0S"),
                Arguments.of("DAY(9) TO MINUTE", "999999999 23:59", "P0Y0M999999999DT23H59M0S"),
                Arguments.of("DAY(9) TO SECOND(6)", "999999999 23:59:59.999999", "P0Y0M999999999DT23H59M59.999999S"),
                Arguments.of("HOUR(9)", "999999999", "P0Y0M0DT999999999H0M0S"),
                Arguments.of("HOUR(9) TO MINUTE", "999999999:59", "P0Y0M0DT999999999H59M0S"),
                Arguments.of("HOUR(9) TO SECOND(6)", "999999999:59:59.999999", "P0Y0M0DT999999999H59M59.999999S"),
                Arguments.of("MINUTE(9)", "999999999", "P0Y0M0DT0H999999999M0S"),
                Arguments.of("MINUTE(9) TO SECOND(6)", "999999999:59.999999", "P0Y0M0DT0H999999999M59.999999S"),
                Arguments.of("SECOND(9,6)", "999999999.999999", "P0Y0M0DT0H0M999999999.999999S"));
    }

    @ParameterizedTest
    @MethodSource("qualifiers")
    void shouldDecodeEveryQualifierWithoutFloatingPointOrDurationOverflow(String qualifier, String body, String expected) {
        final var column = column("INTERVAL " + qualifier);
        final var schema = converters.schemaBuilder(column).optional().build();
        assertThat(schema.type()).isEqualTo(Schema.Type.STRING);
        assertThat(schema.name()).isEqualTo("io.debezium.time.Interval");
        assertThat(schema.version()).isEqualTo(1);
        final var converter = converters.converter(column, new Field("VALUE", 0, schema));
        assertThat(converter.convert(null)).isNull();
        assertThat(converter.convert("INTERVAL '" + body + "' " + qualifier)).isEqualTo(expected);
        assertThat(converter.convert(new SqlExpression("INTERVAL +'" + body + "' " + qualifier))).isEqualTo(expected);
        final var negative = expected.replaceAll("(?<=[PTYMH])([1-9][0-9]*(?:\\.[0-9]+)?)", "-$1");
        assertThat(converter.convert("INTERVAL -'" + body + "' " + qualifier)).isEqualTo(negative);
        assertThat(converter.convert(new SqlExpression("INTERVAL '-" + body + "' " + qualifier))).isEqualTo(negative);
    }

    @Test
    void shouldPreserveSmallNegativeFractionAndCanonicalizeZeroAndTrailingZeros() {
        final var column = column("INTERVAL SECOND");
        assertThat(DamengInterval.convert("interval -'0.000001' second(1,6)", column)).isEqualTo("P0Y0M0DT0H0M-0.000001S");
        assertThat(DamengInterval.convert("INTERVAL '-0.000000' SECOND", column)).isEqualTo("P0Y0M0DT0H0M0S");
        assertThat(DamengInterval.convert("INTERVAL '01.230000' SECOND", column)).isEqualTo("P0Y0M0DT0H0M1.23S");
        assertThat(DamengInterval.convert("INTERVAL '999999999' SECOND", column)).isEqualTo("P0Y0M0DT0H0M999999999S");
        assertThat(DamengInterval.convert("  INTERVAL '001 02:03:04.000005' DAY(3) TO SECOND(6)  ", column("INTERVAL DAY TO SECOND")))
                .isEqualTo("P0Y0M1DT2H3M4.000005S");
    }

    @ParameterizedTest
    @MethodSource("declarations")
    void shouldRetainAllBoundaryDeclarationPrecisions(DamengDatatypeCases.TypeCase type) {
        final var tables = new Tables();
        final var parser = new DamengDdlParser();
        parser.parse("CREATE TABLE APP.T (ID INT PRIMARY KEY, VALUE " + type.declaration() + ")", tables, "APP");
        final var id = new TableId(null, "APP", "T");
        final var column = tables.forTable(id).columnWithName("VALUE");
        final var parsed = DamengInterval.type(type.declaration());
        assertThat(column.jdbcType()).isEqualTo(Types.OTHER);
        assertThat(column.typeName()).isEqualTo(parsed.typeName());
        assertThat(column.length()).isEqualTo(parsed.leadingPrecision());
        assertThat(column.scale()).contains(parsed.fractionalPrecision());
        for (var value : type.values()) {
            assertThat(DamengInterval.convert(value.sql(), column)).startsWith("P").endsWith("S");
        }
        parser.parse("ALTER TABLE APP.T ADD EXTRA " + type.declaration(), tables, "APP");
        assertThat(tables.forTable(id).columnWithName("EXTRA").typeName()).isEqualTo(column.typeName());
        assertThat(tables.forTable(id).columnWithName("EXTRA").scale()).isEqualTo(column.scale());
    }

    static Stream<DamengDatatypeCases.TypeCase> declarations() {
        return DamengDatatypeCases.intervals();
    }

    @ParameterizedTest
    @ValueSource(strings = { "INTERVAL", "INTERVAL WEEK", "INTERVAL YEAR(0)", "INTERVAL YEAR(10)", "INTERVAL YEAR(2147483648)",
            "INTERVAL SECOND(2,7)", "INTERVAL SECOND(2,2147483648)", "INTERVAL YEAR(2,3)", "INTERVAL DAY TO HOUR(2)",
            "INTERVAL MONTH TO YEAR", "INTERVAL YEAR TO DAY", "INTERVAL DAY TO MONTH", "INTERVAL DAY TO DAY", "INTERVAL SECOND TO SECOND",
            "INTERVAL SECOND(2,)", "INTERVAL DAY(3) TO SECOND(6) EXTRA" })
    void shouldRejectInvalidDeclarationsWithoutChangingHistory(String declaration) {
        assertThatThrownBy(() -> DamengInterval.type(declaration)).isInstanceOf(IllegalArgumentException.class);
        final var tables = new Tables();
        final var parser = new DamengDdlParser();
        parser.parse("CREATE TABLE APP.T (ID INT PRIMARY KEY)", tables, "APP");
        final var id = new TableId(null, "APP", "T");
        final var before = tables.forTable(id);
        assertThatThrownBy(() -> parser.parse("ALTER TABLE APP.T ADD VALUE " + declaration, tables, "APP"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(tables.forTable(id)).isEqualTo(before);
    }

    @ParameterizedTest
    @ValueSource(strings = { "12-3", "INTERVAL '1-2' YEAR TO DAY", "INTERVAL '1-2' YEAR TO MONTH EXTRA", "INTERVAL -'-1-2' YEAR TO MONTH",
            "INTERVAL '1--2' YEAR TO MONTH", "INTERVAL '1-12' YEAR TO MONTH", "INTERVAL '100-0' YEAR(2) TO MONTH",
            "INTERVAL '1' YEAR TO MONTH", "INTERVAL '1 2' YEAR TO MONTH", "INTERVAL '' YEAR TO MONTH" })
    void shouldRejectCorruptYearMonthValues(String value) {
        assertThatThrownBy(() -> DamengInterval.convert(value, column("INTERVAL YEAR TO MONTH")))
                .isInstanceOf(DataException.class).hasMessageContaining("VALUE").hasMessageContaining(value);
    }

    @ParameterizedTest
    @ValueSource(strings = { "INTERVAL '100 0:0:0' DAY(2) TO SECOND", "INTERVAL '1 24:0:0' DAY TO SECOND", "INTERVAL '1 0:60:0' DAY TO SECOND",
            "INTERVAL '1 0:0:60' DAY TO SECOND", "INTERVAL '1 0:0:0.0000001' DAY TO SECOND", "INTERVAL '1 0:0:-1' DAY TO SECOND",
            "INTERVAL '1:0:0:1' DAY TO SECOND", "INTERVAL '1 0 0 1' DAY TO SECOND", "INTERVAL '1 0:0:1E0' DAY TO SECOND",
            "INTERVAL '1 0:0:0.1' DAY TO SECOND(0)", "INTERVAL '1 0:0:0' DAY TO SECOND(7)", "INTERVAL '1:0:0' HOUR TO SECOND" })
    void shouldRejectCorruptDaySecondValues(String value) {
        assertThatThrownBy(() -> DamengInterval.convert(value, column("INTERVAL DAY TO SECOND")))
                .isInstanceOf(DataException.class).hasMessageContaining("VALUE");
    }

    @Test
    void shouldOnlyRecognizePhysicalIntervalColumnsAndKeepTextLiteral() {
        assertThat(DamengInterval.isInterval(column("INTERVAL DAY"))).isTrue();
        assertThat(DamengInterval.isInterval(Column.editor().name("VALUE").jdbcType(Types.OTHER).create())).isFalse();
        final var column = Column.editor().name("VALUE").jdbcType(Types.VARCHAR).type("INTERVAL DAY").optional(true).create();
        assertThat(DamengInterval.isInterval(column)).isFalse();
        final var literal = "INTERVAL -'12' DAY";
        assertThat(converters.converter(column, new Field("VALUE", 0, Schema.OPTIONAL_STRING_SCHEMA)).convert(literal)).isEqualTo(literal);
        assertThatThrownBy(() -> DamengInterval.convert(12, column("INTERVAL DAY"))).isInstanceOf(DataException.class);
    }

    @Test
    void shouldReadIntervalsThroughStandardJdbcStringInterfaceIncludingNull() throws Exception {
        final var result = mock(ResultSet.class);
        final var connection = new DamengConnection(JdbcConfiguration.create().build());
        final var column = column("INTERVAL DAY TO SECOND");
        when(result.getString(2)).thenReturn("INTERVAL -'2 3:4:5.000001' DAY TO SECOND", null);
        assertThat(connection.getColumnValue(result, 2, column, null)).isEqualTo("INTERVAL -'2 3:4:5.000001' DAY TO SECOND");
        assertThat(connection.getColumnValue(result, 2, column, null)).isNull();
        verify(result, never()).getObject(2);
        connection.close();
    }

    @ParameterizedTest
    @MethodSource("qualifiers")
    void shouldNormalizeDriverSpecificColumnMetadata(String qualifier, String body, String expected) throws Exception {
        final var metadata = mock(ResultSet.class);
        when(metadata.getString(4)).thenReturn("VALUE");
        when(metadata.getString(6)).thenReturn("INTERVAL " + qualifier.replaceAll("\\([^)]*\\)", ""));
        when(metadata.getInt(5)).thenReturn(107);
        when(metadata.getInt(7)).thenReturn(25);
        when(metadata.getInt(11)).thenReturn(java.sql.DatabaseMetaData.columnNullable);
        when(metadata.getInt(17)).thenReturn(2);
        final var connection = spy(new DamengConnection(JdbcConfiguration.create().build()));
        doReturn("1").when(connection).getCaseSensitiveFlag();
        final var column = connection.readTableColumn(metadata, new TableId(null, "APP", "T"), null).orElseThrow().create();
        assertThat(column.jdbcType()).isEqualTo(Types.OTHER);
        assertThat(DamengInterval.isInterval(column)).isTrue();
        assertThat(DamengInterval.convert("INTERVAL '" + body + "' " + qualifier, column)).isEqualTo(expected);
        connection.close();
    }

    @Test
    void shouldDecodeConstantIntervalDefaultsWithoutExecutingExpressions() {
        final var defaults = new DamengDefaultValueConverter(converters);
        final var column = column("INTERVAL YEAR TO MONTH");
        assertThat(defaults.parseDefaultValue(column, "INTERVAL -'12-3' YEAR TO MONTH")).contains("P-12Y-3M0DT0H0M0S");
        assertThat(defaults.parseDefaultValue(column, "NULL")).isEmpty();
        assertThat(defaults.parseDefaultValue(column, "NUMTOYMINTERVAL(1,'YEAR')")).isEmpty();
        assertThatThrownBy(() -> defaults.parseDefaultValue(column, "INTERVAL '12-3' YEAR TO MONTH + INTERVAL '1' YEAR"))
                .isInstanceOf(DataException.class);
    }

    @ParameterizedTest
    @MethodSource("qualifiers")
    void shouldPreserveCompleteIntervalRedoInAllCrudPositions(String qualifier, String body, String expected) {
        final var table = Table.editor().tableId(new TableId(null, "APP", "T"))
                .addColumn(Column.editor().name("ID").jdbcType(Types.INTEGER).create())
                .addColumn(column("INTERVAL " + qualifier)).create();
        final var parser = new LogMinerDmlParser();
        final var positive = "INTERVAL '" + body + "' " + qualifier;
        final var negative = "INTERVAL -'" + body + "' " + qualifier;
        assertThat(parser.parse("INSERT INTO APP.T(ID,VALUE) VALUES(1," + negative + ")", table).newValues())
                .containsExactly("1", new SqlExpression(negative));
        final var update = parser.parse("UPDATE APP.T SET VALUE=" + positive + " WHERE ID=1 AND VALUE=" + negative, table);
        assertThat(update.oldValues()).containsExactly("1", new SqlExpression(negative));
        assertThat(update.newValues()).containsExactly("1", new SqlExpression(positive));
        assertThat(parser.parse("DELETE FROM APP.T WHERE ID=1 AND VALUE=" + negative, table).oldValues())
                .containsExactly("1", new SqlExpression(negative));
        for (var badSign : List.of("-+", "+-", "+ +")) {
            assertThatThrownBy(() -> parser.parse("INSERT INTO APP.T(ID,VALUE) VALUES(1,INTERVAL " + badSign + "'" + body + "' " + qualifier + ")", table))
                    .isInstanceOf(RuntimeException.class);
        }
    }

    private static Column column(String declaration) {
        return Column.editor().name("VALUE").jdbcType(Types.OTHER).type(declaration).optional(true).create();
    }
}
