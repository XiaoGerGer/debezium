/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng.antlr;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonToken;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;

import io.debezium.connector.dameng.antlr.DamengSqlToken.Kind;

/** ANTLR lexer retaining the native token and UTF-16 source-location contract. */
public final class DamengAntlrSqlLexer {
    private final boolean nestedComments;

    public DamengAntlrSqlLexer() {
        this(false);
    }

    public DamengAntlrSqlLexer(boolean nestedComments) {
        this.nestedComments = nestedComments;
    }

    public List<DamengSqlToken> tokenize(String input) {
        return lex(input, nestedComments).tokens();
    }

    static Lexed lex(String input, boolean nestedComments) {
        Objects.requireNonNull(input, "SQL");
        final var offsets = utf16Offsets(input);
        final var lexer = new io.debezium.ddl.parser.dameng.generated.DamengSqlLexer(CharStreams.fromString(input));
        lexer.setNestedComments(nestedComments);
        lexer.removeErrorListeners();
        lexer.addErrorListener(new BaseErrorListener() {
            @Override
            public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol, int line, int charPositionInLine, String message,
                                    RecognitionException exception) {
                throw DamengSqlToken.error(input, offsets[lexer._tokenStartCharIndex], message);
            }
        });

        final List<DamengSqlToken> result = new ArrayList<>();
        final List<CommonToken> parserTokens = new ArrayList<>();
        for (var token = lexer.nextToken();; token = lexer.nextToken()) {
            final var adapted = new CommonToken(token.getType(), token.getText());
            adapted.setLine(token.getLine());
            adapted.setCharPositionInLine(token.getCharPositionInLine());
            if (token.getType() == org.antlr.v4.runtime.Token.EOF) {
                adapted.setStartIndex(input.length());
                adapted.setStopIndex(input.length() - 1);
                parserTokens.add(adapted);
                break;
            }
            final var kind = Vocabulary.kind(token.getType());
            final int start = offsets[token.getStartIndex()];
            final int end = offsets[token.getStopIndex() + 1];
            adapted.setStartIndex(start);
            adapted.setStopIndex(end - 1);
            parserTokens.add(adapted);
            // Symbol offsets address individual UTF-16 code units, including surrogate halves.
            if (kind == Kind.SYMBOL) {
                for (int index = start; index < end; index++) {
                    result.add(new DamengSqlToken(kind, input.substring(index, index + 1), index, index + 1, input));
                }
            }
            else {
                result.add(new DamengSqlToken(kind, input.substring(start, end), start, end, input));
            }
        }
        return new Lexed(List.copyOf(result), List.copyOf(parserTokens));
    }

    record Lexed(List<DamengSqlToken> tokens, List<CommonToken> parserTokens) {
    }

    private static int[] utf16Offsets(String input) {
        final int[] offsets = new int[input.codePointCount(0, input.length()) + 1];
        int codePoint = 0;
        for (int offset = 0; offset < input.length(); offset += Character.charCount(input.codePointAt(offset))) {
            offsets[codePoint++] = offset;
        }
        offsets[codePoint] = input.length();
        return offsets;
    }

    static int wordType(String text) {
        return Vocabulary.wordType(text);
    }

    static int symbolType(String text) {
        return Vocabulary.symbolType(text);
    }

    /** Maps generated literal names without maintaining a second keyword list. */
    private static final class Vocabulary {
        private static final Map<String, Integer> WORD_TYPES;
        private static final Map<String, Integer> SYMBOL_TYPES;
        private static final Kind[] KINDS;

        static {
            final var vocabulary = io.debezium.ddl.parser.dameng.generated.DamengSqlLexer.VOCABULARY;
            final Map<String, Integer> wordTypes = new HashMap<>();
            final Map<String, Integer> symbolTypes = new HashMap<>();
            KINDS = new Kind[vocabulary.getMaxTokenType() + 1];
            for (int tokenType = 1; tokenType < KINDS.length; tokenType++) {
                final var literal = vocabulary.getLiteralName(tokenType);
                if (literal == null) {
                    continue;
                }
                final var text = literal.substring(1, literal.length() - 1);
                if (isKeyword(text)) {
                    wordTypes.put(text, tokenType);
                    KINDS[tokenType] = Kind.WORD;
                }
                else if (text.length() == 1 && text.charAt(0) != '\'' && text.charAt(0) != '"') {
                    symbolTypes.put(text, tokenType);
                    KINDS[tokenType] = Kind.SYMBOL;
                }
            }
            WORD_TYPES = Map.copyOf(wordTypes);
            SYMBOL_TYPES = Map.copyOf(symbolTypes);
            KINDS[io.debezium.ddl.parser.dameng.generated.DamengSqlLexer.WORD] = Kind.WORD;
            KINDS[io.debezium.ddl.parser.dameng.generated.DamengSqlLexer.IDENTIFIER] = Kind.IDENTIFIER;
            KINDS[io.debezium.ddl.parser.dameng.generated.DamengSqlLexer.STRING] = Kind.STRING;
            KINDS[io.debezium.ddl.parser.dameng.generated.DamengSqlLexer.HEX_BINARY_LITERAL] = Kind.BINARY;
            KINDS[io.debezium.ddl.parser.dameng.generated.DamengSqlLexer.NUMBER] = Kind.NUMBER;
            KINDS[io.debezium.ddl.parser.dameng.generated.DamengSqlLexer.SYMBOL] = Kind.SYMBOL;
        }

        private static int wordType(String text) {
            return WORD_TYPES.getOrDefault(text.toUpperCase(Locale.ROOT), io.debezium.ddl.parser.dameng.generated.DamengSqlLexer.WORD);
        }

        private static int symbolType(String text) {
            return SYMBOL_TYPES.getOrDefault(text, io.debezium.ddl.parser.dameng.generated.DamengSqlLexer.SYMBOL);
        }

        private static Kind kind(int tokenType) {
            if (tokenType <= 0 || tokenType >= KINDS.length || KINDS[tokenType] == null) {
                throw new IllegalArgumentException("Unexpected Dameng lexer token type " + tokenType);
            }
            return KINDS[tokenType];
        }

        private static boolean isKeyword(String text) {
            if (text.isEmpty() || text.charAt(0) < 'A' || text.charAt(0) > 'Z') {
                return false;
            }
            for (int index = 1; index < text.length(); index++) {
                final char character = text.charAt(index);
                if ((character < 'A' || character > 'Z') && (character < '0' || character > '9') && character != '_') {
                    return false;
                }
            }
            return true;
        }
    }
}
