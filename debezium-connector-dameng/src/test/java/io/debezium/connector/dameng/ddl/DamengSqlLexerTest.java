/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng.ddl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import io.debezium.connector.dameng.ddl.DamengSqlLexer.Kind;

class DamengSqlLexerTest {
    private final DamengSqlLexer lexer = new DamengSqlLexer();

    @ParameterizedTest
    @ValueSource(strings = { "\n", "\r", "\r\n" })
    void shouldKeepLocationsAndCommentBoundaries(String newline) {
        final var sql = "// ignored; CREATE TABLE T" + newline + "ALTER/*gap*/TABLE $表_1# (\"DROP\" INT) -- tail";
        final var tokens = lexer.tokenize(sql);
        assertThat(tokens).extracting(DamengSqlLexer.Token::text).containsExactly("ALTER", "TABLE", "$表_1#", "(", "\"DROP\"", "INT", ")");
        for (var token : tokens) {
            assertThat(sql.substring(token.start(), token.end())).isEqualTo(token.text());
        }
        assertThat(tokens.get(4).keyword("DROP")).isFalse();
        assertThat(tokens.get(4).kind()).isEqualTo(Kind.IDENTIFIER);
        assertThat(tokens.get(0).error("bad")).hasMessageContaining("line 2, column 1");
    }

    @ParameterizedTest
    @ValueSource(strings = { "q'[it's;--/*//]'", "Q'{it's;--/*//}'", "q'(it's;--/*//)'", "q'<it's;--/*//>'", "q'!it's;--/*//!'" })
    void shouldTreatAlternativeQuotedLiteralsAsOneToken(String sql) {
        final var tokens = lexer.tokenize(sql);
        assertThat(tokens).hasSize(1);
        assertThat(tokens.get(0).kind()).isEqualTo(Kind.STRING);
        assertThat(tokens.get(0).stringValue()).isEqualTo("it's;--/*//");
        assertThat(tokens.get(0).keyword(";")).isFalse();
    }

    @Test
    void shouldDecodeEscapedAndNationalStringsWithoutInterpretingBackslashes() {
        final var tokens = lexer.tokenize("'it''s' n'中文' N'C:\\tmp' \"a\"\"b\" 1.2e+3 4E-2 5. 6");
        assertThat(tokens.get(0).stringValue()).isEqualTo("it's");
        assertThat(tokens.get(1).stringValue()).isEqualTo("中文");
        assertThat(tokens.get(2).stringValue()).isEqualTo("C:\\tmp");
        assertThat(tokens.get(3).kind()).isEqualTo(Kind.IDENTIFIER);
        assertThat(tokens.subList(4, 8)).extracting(DamengSqlLexer.Token::kind).containsOnly(Kind.NUMBER);
        assertThatThrownBy(() -> tokens.get(3).stringValue()).hasMessageContaining("Expected SQL string", "character");
    }

    @ParameterizedTest
    @ValueSource(strings = { "/* open", "/*! ALTER TABLE T ADD X INT */", "'open", "\"open", "q'", "q'[]", "q' abc'", "q'''", "1e", "1E+",
            "1e-" })
    void shouldRejectAmbiguousOrIncompleteTokensWithLocation(String sql) {
        assertThatThrownBy(() -> lexer.tokenize(sql)).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("character", "line", "column");
    }

    @Test
    void shouldUseTheFirstClosingMarkerInNativeNonNestedComments() {
        assertThat(lexer.tokenize("ALTER/* describe /* syntax */TABLE T ADD X INT"))
                .extracting(DamengSqlLexer.Token::text).containsExactly("ALTER", "TABLE", "T", "ADD", "X", "INT");
        assertThat(lexer.tokenize("/* outer /* inner */ residual */"))
                .extracting(DamengSqlLexer.Token::text).containsExactly("residual", "*", "/");
    }

    @Test
    void shouldSkipBalancedNestedCommentsWithoutTreatingTheirContentsAsSql() {
        final var nested = new DamengSqlLexer(true);
        final var sql = "ALTER/* outer ' \" -- // ; /* inner /* third */ DROP TABLE T */ tail */TABLE T ADD C INT";
        assertThat(nested.tokenize(sql)).extracting(DamengSqlLexer.Token::text)
                .containsExactly("ALTER", "TABLE", "T", "ADD", "C", "INT");
        assertThat(nested.tokenize("/*+ hint /* nested */ outer */ SELECT"))
                .extracting(DamengSqlLexer.Token::text).containsExactly("SELECT");
        assertThat(nested.tokenize("/*".repeat(10_000) + "*/".repeat(10_000))).isEmpty();
        assertThat(nested.tokenize("'/*not a comment*/' q'[/*nested*/]' \"/*identifier*/\""))
                .extracting(DamengSqlLexer.Token::kind).containsExactly(Kind.STRING, Kind.STRING, Kind.IDENTIFIER);
    }

    @ParameterizedTest
    @ValueSource(strings = { "/*", "/* outer /* inner */", "/* outer /* inner /* third */ */", "/*/", "/* outer /*/ */" })
    void shouldRejectUnclosedNestedCommentsAtTheirOpeningLocation(String sql) {
        assertThatThrownBy(() -> new DamengSqlLexer(true).tokenize("\r\n" + sql))
                .hasMessageContaining("Unclosed SQL comment", "character 2", "line 2, column 1");
    }

    @Test
    void shouldSplitOnlyUnquotedDelimitersOutsideParentheses() {
        final var tokens = lexer.tokenize("CREATE TABLE T(A DECIMAL(5,2), B VARCHAR(10) DEFAULT ';'); DROP TABLE T;");
        assertThat(DamengSqlLexer.split(tokens, ";")).hasSize(3);
        assertThat(DamengSqlLexer.split(lexer.tokenize("A(1,2), B"), ",")).hasSize(2);
        assertThatThrownBy(() -> DamengSqlLexer.split(lexer.tokenize("A)"), ";")).hasMessageContaining("Unexpected closing", "character 1");
        assertThatThrownBy(() -> DamengSqlLexer.split(lexer.tokenize("A("), ";")).hasMessageContaining("Unbalanced parentheses");
        assertThat(lexer.tokenize("/* comment */ -- tail")).isEmpty();
        assertThatThrownBy(() -> lexer.tokenize(null)).isInstanceOf(NullPointerException.class);
    }
}
