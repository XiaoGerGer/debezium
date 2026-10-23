/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng.ddl;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** Native DM SQL tokens with original source locations; comments never join adjacent words. */
public final class DamengSqlLexer {
    private final boolean nestedComments;

    public DamengSqlLexer() {
        this(false);
    }

    public DamengSqlLexer(boolean nestedComments) {
        this.nestedComments = nestedComments;
    }

    public enum Kind {
        WORD,
        IDENTIFIER,
        STRING,
        NUMBER,
        SYMBOL
    }

    public record Token(Kind kind, String text, int start, int end, String source) {
        public boolean keyword(String value) {
            return (kind == Kind.WORD || kind == Kind.SYMBOL) && text.equalsIgnoreCase(value);
        }

        public IllegalArgumentException error(String message) {
            return DamengSqlLexer.error(source, start, message);
        }

        public String stringValue() {
            if (kind != Kind.STRING) {
                throw error("Expected SQL string literal");
            }
            if (text.charAt(0) == 'q' || text.charAt(0) == 'Q') {
                return text.substring(3, text.length() - 2);
            }
            final int prefix = text.charAt(0) == '\'' ? 1 : 2;
            return text.substring(prefix, text.length() - 1).replace("''", "'");
        }
    }

    public List<Token> tokenize(String input) {
        Objects.requireNonNull(input, "SQL");
        final List<Token> result = new ArrayList<>();
        int offset = 0;
        while (offset < input.length()) {
            final char c = input.charAt(offset);
            if (Character.isWhitespace(c)) {
                offset++;
                continue;
            }
            if (input.startsWith("--", offset) || input.startsWith("//", offset)) {
                offset += 2;
                while (offset < input.length() && input.charAt(offset) != '\n' && input.charAt(offset) != '\r') {
                    offset++;
                }
                continue;
            }
            if (input.startsWith("/*", offset)) {
                if (input.startsWith("/*!", offset)) {
                    throw error(input, offset, "Executable SQL comments are not supported by the native DDL profile");
                }
                offset = commentEnd(input, offset);
                continue;
            }
            final int start = offset;
            final Kind kind;
            if ((c == 'q' || c == 'Q') && offset + 1 < input.length() && input.charAt(offset + 1) == '\'') {
                kind = Kind.STRING;
                if (offset + 2 >= input.length()) {
                    throw error(input, start, "Unclosed SQL alternative quoted value");
                }
                final char opening = input.charAt(offset + 2);
                if (Character.isWhitespace(opening) || opening == '\'') {
                    throw error(input, start, "Invalid SQL alternative quote delimiter");
                }
                final char closing = switch (opening) {
                    case '[' -> ']';
                    case '(' -> ')';
                    case '{' -> '}';
                    case '<' -> '>';
                    default -> opening;
                };
                final int close = input.indexOf("" + closing + '\'', offset + 3);
                if (close < 0) {
                    throw error(input, start, "Unclosed SQL alternative quoted value");
                }
                offset = close + 2;
            }
            else if (c == '\'' || c == '"' || ((c == 'n' || c == 'N') && offset + 1 < input.length() && input.charAt(offset + 1) == '\'')) {
                kind = c == '"' ? Kind.IDENTIFIER : Kind.STRING;
                final char quote = c == '"' ? '"' : '\'';
                offset += c == 'n' || c == 'N' ? 2 : 1;
                boolean closed = false;
                while (offset < input.length()) {
                    if (input.charAt(offset++) == quote) {
                        if (offset < input.length() && input.charAt(offset) == quote) {
                            offset++;
                        }
                        else {
                            closed = true;
                            break;
                        }
                    }
                }
                if (!closed) {
                    throw error(input, start, "Unclosed SQL quoted value");
                }
            }
            else if (Character.isDigit(c)) {
                kind = Kind.NUMBER;
                offset++;
                while (offset < input.length() && Character.isDigit(input.charAt(offset))) {
                    offset++;
                }
                if (offset < input.length() && input.charAt(offset) == '.') {
                    offset++;
                    while (offset < input.length() && Character.isDigit(input.charAt(offset))) {
                        offset++;
                    }
                }
                if (offset < input.length() && (input.charAt(offset) == 'e' || input.charAt(offset) == 'E')) {
                    offset++;
                    if (offset < input.length() && (input.charAt(offset) == '+' || input.charAt(offset) == '-')) {
                        offset++;
                    }
                    final int exponent = offset;
                    while (offset < input.length() && Character.isDigit(input.charAt(offset))) {
                        offset++;
                    }
                    if (offset == exponent) {
                        throw error(input, start, "Missing SQL numeric exponent");
                    }
                }
            }
            else if (Character.isLetter(c) || c == '_' || c == '#' || c == '$') {
                kind = Kind.WORD;
                offset++;
                while (offset < input.length()) {
                    final char next = input.charAt(offset);
                    if (!Character.isLetterOrDigit(next) && next != '_' && next != '#' && next != '$') {
                        break;
                    }
                    offset++;
                }
            }
            else {
                kind = Kind.SYMBOL;
                offset++;
            }
            result.add(new Token(kind, input.substring(start, offset), start, offset, input));
        }
        return List.copyOf(result);
    }

    private int commentEnd(String input, int start) {
        int depth = 1;
        int offset = start + 2;
        while (offset < input.length()) {
            if (input.startsWith("*/", offset)) {
                offset += 2;
                if (--depth == 0) {
                    return offset;
                }
            }
            else if (nestedComments && input.startsWith("/*", offset)) {
                depth++;
                offset += 2;
            }
            else {
                offset++;
            }
        }
        throw error(input, start, "Unclosed SQL comment");
    }

    public static List<List<Token>> split(List<Token> tokens, String delimiter) {
        final List<List<Token>> result = new ArrayList<>();
        int depth = 0;
        int start = 0;
        for (int index = 0; index < tokens.size(); index++) {
            final var token = tokens.get(index);
            if (token.keyword("(")) {
                depth++;
            }
            else if (token.keyword(")")) {
                if (--depth < 0) {
                    throw token.error("Unexpected closing parenthesis in DDL");
                }
            }
            else if (depth == 0 && token.keyword(delimiter)) {
                result.add(tokens.subList(start, index));
                start = index + 1;
            }
        }
        if (depth != 0) {
            throw tokens.get(tokens.size() - 1).error("Unbalanced parentheses in DDL");
        }
        result.add(tokens.subList(start, tokens.size()));
        return result;
    }

    public static IllegalArgumentException error(String input, int offset, String message) {
        int line = 1;
        int column = 1;
        for (int index = 0; index < offset; index++) {
            final char c = input.charAt(index);
            if (c == '\r' || (c == '\n' && (index == 0 || input.charAt(index - 1) != '\r'))) {
                line++;
                column = 1;
            }
            else if (c != '\n') {
                column++;
            }
        }
        return new IllegalArgumentException(message + " at character " + offset + " (line " + line + ", column " + column + ")");
    }
}
