/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng.antlr;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.tuple;

import java.util.List;

import org.assertj.core.groups.Tuple;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import io.debezium.connector.dameng.antlr.DamengSqlToken.Kind;

class DamengAntlrSqlLexerTest {
    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void shouldTokenizeDdlWithExplicitKindsAndOriginalSpelling(boolean nestedComments) {
        final var sql = "CREATE TABLE \"app\".\"a\"\"b\" ($表_1# INT PRIMARY KEY, \"DROP\" VARCHAR(10) DEFAULT N'中文', "
                + "AMOUNT DECIMAL(12,2) DEFAULT (-1.2e+3))";

        assertTokens(sql, nestedComments,
                word("CREATE"), word("TABLE"), identifier("\"app\""), symbol("."), identifier("\"a\"\"b\""), symbol("("),
                word("$表_1#"), word("INT"), word("PRIMARY"), word("KEY"), symbol(","), identifier("\"DROP\""), word("VARCHAR"),
                symbol("("), number("10"), symbol(")"), word("DEFAULT"), string("N'中文'"), symbol(","), word("AMOUNT"),
                word("DECIMAL"), symbol("("), number("12"), symbol(","), number("2"), symbol(")"), word("DEFAULT"),
                symbol("("), symbol("-"), number("1.2e+3"), symbol(")"), symbol(")"));
    }

    @Test
    void shouldKeepKeywordsAsWordsAndSeparateQuotedIdentifiers() {
        final var tokens = assertTokens("create or replace force view APP.\"VIEW\" as select ID from APP.T", false,
                word("create"), word("or"), word("replace"), word("force"), word("view"), word("APP"), symbol("."),
                identifier("\"VIEW\""), word("as"), word("select"), word("ID"), word("from"), word("APP"), symbol("."), word("T"));

        assertThat(tokens.get(0).keyword("CREATE")).isTrue();
        assertThat(tokens.get(4).keyword("VIEW")).isTrue();
        assertThat(tokens.get(6).keyword(".")).isTrue();
        assertThat(tokens.get(7).keyword("VIEW")).isFalse();
    }

    @Test
    void shouldTokenizePrivilegeAndExpressionPunctuationWithoutJoiningComments() {
        assertTokens("GRANT SELECT, UPDATE(ID) ON APP.T TO U WITH GRANT OPTION;", false,
                word("GRANT"), word("SELECT"), symbol(","), word("UPDATE"), symbol("("), word("ID"), symbol(")"),
                word("ON"), word("APP"), symbol("."), word("T"), word("TO"), word("U"), word("WITH"), word("GRANT"),
                word("OPTION"), symbol(";"));
        assertTokens("ALTER/* gap */TABLE APP.T ADD V VARCHAR(20) DEFAULT 'x' /* operator follows */ || 'y' NOT NULL", false,
                word("ALTER"), word("TABLE"), word("APP"), symbol("."), word("T"), word("ADD"), word("V"), word("VARCHAR"),
                symbol("("), number("20"), symbol(")"), word("DEFAULT"), string("'x'"), symbol("|"), symbol("|"), string("'y'"),
                word("NOT"), word("NULL"));
    }

    @Test
    void shouldKeepNumericSpellingAndSingleCharacterOperators() {
        assertTokens("1.2e+3 4E-2 5. 6 .5 +1 -2 1..2 <> != >= <= || := % ? @ :", false,
                number("1.2e+3"), number("4E-2"), number("5."), number("6"), symbol("."), number("5"),
                symbol("+"), number("1"), symbol("-"), number("2"), number("1."), symbol("."), number("2"),
                symbol("<"), symbol(">"), symbol("!"), symbol("="), symbol(">"), symbol("="), symbol("<"), symbol("="),
                symbol("|"), symbol("|"), symbol(":"), symbol("="), symbol("%"), symbol("?"), symbol("@"), symbol(":"));
        assertTokens("1.e3 1e2e+", false, number("1.e3"), number("1e2"), word("e"), symbol("+"));
    }

    @Test
    void shouldKeepHexBinaryLiteralsDistinctFromNumbersAndStrings() {
        final var tokens = assertTokens("0x00aF 0X80fF 0xF 0x123 '0x00aF'", false,
                binary("0x00aF"), binary("0X80fF"), binary("0xF"), binary("0x123"), string("'0x00aF'"));

        assertThat(tokens.get(0).keyword("0x00aF")).isFalse();
        assertThatThrownBy(() -> tokens.get(0).stringValue()).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Expected SQL string literal");
        assertThat(tokens.get(4).stringValue()).isEqualTo("0x00aF");
    }

    @Test
    void shouldPreserveBinaryLiteralLocationsAndReconstructTokenSlices() {
        final var sql = "COMMENT ON TABLE APP.T IS '😀'; /*😀*/ CREATE TABLE APP.U(V VARBINARY(4) DEFAULT 0x00fF)";
        final var tokens = new DamengAntlrSqlLexer().tokenize(sql);
        final var slice = DamengSqlTestUtil.split(tokens, ";").get(1);
        final var parsed = DamengParseTree.parseTokens(sql, slice);
        final var table = parsed.tree().statement(0).tableStatement().createTable();
        final var expression = table.tableDefinition().tableElement(0).columnDefinition().columnAttribute(0).expression();
        final var binary = DamengParseTree.tokens(expression).get(0);

        assertThat(binary.kind()).isEqualTo(Kind.BINARY);
        assertThat(binary.text()).isEqualTo("0x00fF");
        assertThat(binary.start()).isEqualTo(sql.indexOf("0x00fF"));
        assertThat(binary.end()).isEqualTo(sql.indexOf("0x00fF") + "0x00fF".length());
        assertThat(binary.source()).isSameAs(sql);
        assertThat(binary).isSameAs(slice.get(slice.size() - 2));
        assertThat(DamengParseTree.source(expression)).isEqualTo("0x00fF");
        assertThat(DamengParseTree.defaultExpression(expression)).isEqualTo("0x00fF");
        assertThat(DamengParseTree.source(table)).isEqualTo(sql.substring(sql.indexOf("CREATE TABLE")));
    }

    @ParameterizedTest
    @ValueSource(strings = { "\n", "\r", "\r\n" })
    void shouldPreserveLocationsAndCommentBoundaries(String newline) {
        final var sql = "// ignored; CREATE TABLE T" + newline + "ALTER/*gap*/TABLE $表_1# (\"DROP\" INT) -- tail";
        final var tokens = assertTokens(sql, false, word("ALTER"), word("TABLE"), word("$表_1#"), symbol("("),
                identifier("\"DROP\""), word("INT"), symbol(")"));

        assertThat(tokens.get(0).start()).isEqualTo(sql.indexOf("ALTER"));
        assertThat(tokens.get(0).error("bad")).hasMessageContaining("line 2, column 1");
        assertThat(tokens.get(4).keyword("DROP")).isFalse();
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "q'[it's;--/*//]'", "Q'{it's;--/*//}'", "q'(it's;--/*//)'", "q'<it's;--/*//>'", "q'!it's;--/*//!'",
            "q'|it's;--/*//|'", "q'§it's;--/*//§'", "q'界it's;--/*//界'", "q'\"it's;--/*//\"'", "q'7it's;--/*//7'",
            "q']it's;--/*//]'", "q'\\it's;--/*//\\'"
    })
    void shouldKeepAlternativeQuotesAndTheirDecodedContents(String sql) {
        final var tokens = assertTokens(sql, false, string(sql));

        assertThat(tokens.get(0).stringValue()).isEqualTo("it's;--/*//");
        assertThat(tokens.get(0).keyword(";")).isFalse();
    }

    @Test
    void shouldDecodeOrdinaryAndNationalStringsWithoutInterpretingBackslashes() {
        final var tokens = assertTokens("'it''s' n'中文' N'C:\\tmp' 'A\\nB' \"a\"\"b\"", false,
                string("'it''s'"), string("n'中文'"), string("N'C:\\tmp'"), string("'A\\nB'"), identifier("\"a\"\"b\""));

        assertThat(tokens.subList(0, 4)).extracting(DamengSqlToken::stringValue).containsExactly("it's", "中文", "C:\\tmp", "A\\nB");
        assertThatThrownBy(() -> tokens.get(4).stringValue()).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Expected SQL string literal", "character", "line", "column");
    }

    @Test
    void shouldRetainEmptyQuotedTokensAndDecodeEmptyStrings() {
        final var tokens = assertTokens("'' \"\" q'[]' q'!!' N''", false,
                string("''"), identifier("\"\""), string("q'[]'"), string("q'!!'"), string("N''"));

        assertThat(List.of(tokens.get(0), tokens.get(2), tokens.get(3), tokens.get(4)))
                .extracting(DamengSqlToken::stringValue).containsExactly("", "", "", "");
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void shouldKeepUtf16OffsetsAfterSupplementaryCharacters(boolean nestedComments) {
        final var sql = "'汉😀字' \"名😀\" N'😀' q'[😀;/*]' /*😀*/ NEXT";
        final var tokens = assertTokens(sql, nestedComments, string("'汉😀字'"), identifier("\"名😀\""),
                string("N'😀'"), string("q'[😀;/*]'"), word("NEXT"));

        assertThat(tokens).extracting(DamengSqlToken::start).containsExactly(0, 7, 13, 19, 37);
        assertThat(tokens).extracting(DamengSqlToken::end).containsExactly(6, 12, 18, 29, 41);
        assertThat(tokens.get(0).stringValue()).isEqualTo("汉😀字");
        assertThat(tokens.get(2).stringValue()).isEqualTo("😀");
        assertThat(tokens.get(3).stringValue()).isEqualTo("😀;/*");
    }

    @Test
    void shouldRetainTheJavaCharacterContractForUnquotedSupplementarySymbols() {
        final var tokens = assertTokens("A😀𐐀 B", false, word("A"), symbol("\uD83D"), symbol("\uDE00"),
                symbol("\uD801"), symbol("\uDC00"), word("B"));

        assertThat(tokens).extracting(DamengSqlToken::start).containsExactly(0, 1, 2, 3, 4, 6);
        assertThat(tokens).extracting(DamengSqlToken::end).containsExactly(1, 2, 3, 4, 5, 7);
    }

    @Test
    void shouldReportUtf16ErrorOffsetsAfterSupplementaryCharacters() {
        final var sql = "'😀' 1e+";

        assertThatThrownBy(() -> new DamengAntlrSqlLexer().tokenize(sql))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("character 5", "line 1, column 6");
    }

    @Test
    void shouldDefaultToNonNestedCommentsAndUseTheFirstClosingMarker() {
        final var sql = "/* outer /* inner */ residual */ ALTER TABLE T ADD C INT";
        final var tokens = new DamengAntlrSqlLexer().tokenize(sql);

        assertThat(tokens).extracting(DamengSqlToken::kind, DamengSqlToken::text)
                .containsExactly(word("residual"), symbol("*"), symbol("/"), word("ALTER"), word("TABLE"), word("T"),
                        word("ADD"), word("C"), word("INT"));
        assertTokens("ALTER/* describe /* syntax */TABLE T ADD X INT", false,
                word("ALTER"), word("TABLE"), word("T"), word("ADD"), word("X"), word("INT"));
    }

    @Test
    void shouldUseTheConfiguredCommentNesting() {
        final var sql = "ALTER/* outer /* inner */ residual */TABLE T ADD C INT";

        assertTokens(sql, false, word("ALTER"), word("residual"), symbol("*"), symbol("/"), word("TABLE"), word("T"), word("ADD"), word("C"), word("INT"));
        assertTokens(sql, true, word("ALTER"), word("TABLE"), word("T"), word("ADD"), word("C"), word("INT"));
        assertTokens("ALTER/* outer ' \" -- // ; /* inner /* third */ DROP TABLE T */ tail */TABLE T ADD C INT", true,
                word("ALTER"), word("TABLE"), word("T"), word("ADD"), word("C"), word("INT"));
        assertTokens("/*/**/*/X", true, word("X"));
        assertTokens("/*/**/*/X", false, symbol("*"), symbol("/"), word("X"));
    }

    @Test
    void shouldKeepCommentMarkersInsideQuotedValuesAndSkipHints() {
        final var sql = "/*+ hint /* nested */ tail */ '/* -- // */' q'[/*nested*/]' \"/*identifier*/\"";
        final var tokens = assertTokens(sql, true, string("'/* -- // */'"), string("q'[/*nested*/]'"), identifier("\"/*identifier*/\""));

        assertThat(tokens.subList(0, 2)).extracting(DamengSqlToken::stringValue).containsExactly("/* -- // */", "/*nested*/");
    }

    @Test
    void shouldHandleDeeplyNestedCommentsWithoutRecursion() {
        final var comments = "/*".repeat(10_000) + "*/".repeat(10_000);

        assertTokens(comments, true);
        assertTokens(comments + " SELECT", true, word("SELECT"));
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "/* open", "/*! ALTER TABLE T ADD X INT */", "'open", "\"open", "N'open", "q'", "q'[]", "q' abc'", "q'''", "1e", "1E+", "1e-",
            "0x", "0xG", "0x12ZZ", "0XFF_", "0x12$", "0x12表"
    })
    void shouldRejectMalformedInputWithoutReturningPartialTokens(String sql) {
        for (var nestedComments : List.of(false, true)) {
            final var input = "\r\nCREATE TABLE T (C INT); " + sql;

            assertThatThrownBy(() -> new DamengAntlrSqlLexer(nestedComments).tokenize(input)).isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("character " + input.indexOf(sql), "line 2, column 25");
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "/*", "/* outer /* inner */", "/* outer /* inner /* third */ */", "/*/", "/* outer /*/ */" })
    void shouldReportTheOpeningLocationOfUnclosedNestedComments(String sql) {
        assertThatThrownBy(() -> new DamengAntlrSqlLexer(true).tokenize("\r\n" + sql)).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unclosed SQL comment", "character 2", "line 2, column 1");
    }

    @Test
    void shouldSplitOnlyUnquotedDelimitersOutsideParentheses() {
        final var sql = "CREATE TABLE T(A DECIMAL(5,2), B VARCHAR(10) DEFAULT ';'); DROP TABLE T;";
        final var tokens = new DamengAntlrSqlLexer().tokenize(sql);
        final var statements = DamengSqlTestUtil.split(tokens, ";");

        assertThat(statements).hasSize(3);
        assertThat(statements.get(0)).extracting(DamengSqlToken::text).containsExactly("CREATE", "TABLE", "T", "(", "A", "DECIMAL", "(",
                "5", ",", "2", ")", ",", "B", "VARCHAR", "(", "10", ")", "DEFAULT", "';'", ")");
        assertThat(statements.get(1)).extracting(DamengSqlToken::text).containsExactly("DROP", "TABLE", "T");
        assertThat(statements.get(2)).isEmpty();
        assertThat(statements.get(0).get(0)).isSameAs(tokens.get(0));
        final var arguments = DamengSqlTestUtil.split(new DamengAntlrSqlLexer().tokenize("A(1,2), B"), ",");
        assertThat(arguments).hasSize(2);
        assertThat(arguments.get(0)).extracting(DamengSqlToken::text).containsExactly("A", "(", "1", ",", "2", ")");
        assertThat(arguments.get(1)).extracting(DamengSqlToken::text).containsExactly("B");
        assertThatThrownBy(() -> DamengSqlTestUtil.split(new DamengAntlrSqlLexer().tokenize("A)"), ";"))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("Unexpected closing", "character 1");
        assertThatThrownBy(() -> DamengSqlTestUtil.split(new DamengAntlrSqlLexer().tokenize("A("), ";"))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("Unbalanced parentheses");
    }

    @ParameterizedTest
    @ValueSource(strings = { "", " \t\r\n", "/* comment */ -- tail", "// tail" })
    void shouldReturnNoTokensForEmptyOrCommentOnlyInput(String sql) {
        assertTokens(sql, false);
        assertTokens(sql, true);
    }

    @Test
    void shouldRejectNullInput() {
        assertThatThrownBy(() -> new DamengAntlrSqlLexer().tokenize(null)).isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new DamengAntlrSqlLexer(true).tokenize(null)).isInstanceOf(NullPointerException.class);
    }

    private List<DamengSqlToken> assertTokens(String sql, boolean nestedComments, Tuple... expected) {
        final var actual = new DamengAntlrSqlLexer(nestedComments).tokenize(sql);

        assertThat(actual).extracting(DamengSqlToken::kind, DamengSqlToken::text).containsExactly(expected);
        for (var token : actual) {
            assertThat(token.source()).isEqualTo(sql);
            assertThat(sql.substring(token.start(), token.end())).isEqualTo(token.text());
        }
        return actual;
    }

    private static Tuple word(String text) {
        return tuple(Kind.WORD, text);
    }

    private static Tuple identifier(String text) {
        return tuple(Kind.IDENTIFIER, text);
    }

    private static Tuple string(String text) {
        return tuple(Kind.STRING, text);
    }

    private static Tuple binary(String text) {
        return tuple(Kind.BINARY, text);
    }

    private static Tuple number(String text) {
        return tuple(Kind.NUMBER, text);
    }

    private static Tuple symbol(String text) {
        return tuple(Kind.SYMBOL, text);
    }
}
