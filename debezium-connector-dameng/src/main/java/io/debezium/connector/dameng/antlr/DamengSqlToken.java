/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng.antlr;

/** SQL token with its original spelling and UTF-16 source locations. */
public record DamengSqlToken(Kind kind, String text, int start, int end, String source) {
    public enum Kind {
        WORD,
        IDENTIFIER,
        STRING,
        BINARY,
        NUMBER,
        SYMBOL
    }

    public boolean keyword(String value) {
        return (kind == Kind.WORD || kind == Kind.SYMBOL) && text.equalsIgnoreCase(value);
    }

    public IllegalArgumentException error(String message) {
        return error(source, start, message);
    }

    public String stringValue() {
        return stringValue(false);
    }

    public String stringValue(boolean backslashEscape) {
        return decodedString(backslashEscape, false);
    }

    /** Canonical literal matching DM's compiled default, for history and schema conversion. */
    public String defaultValueExpression() {
        return "'" + decodedString(true, true).replace("'", "''") + "'";
    }

    private String decodedString(boolean backslashEscape, boolean defaultValue) {
        if (kind != Kind.STRING) {
            throw error("Expected SQL string literal");
        }
        if (text.charAt(0) == 'q' || text.charAt(0) == 'Q') {
            final var value = text.substring(3, text.length() - 2);
            // DM doubles retained backslashes when compiling a Q-quoted default.
            return backslashEscape && defaultValue ? value.replace("\\", "\\\\") : value;
        }
        final int prefix = text.charAt(0) == '\'' ? 1 : 2;
        final var value = text.substring(prefix, text.length() - 1).replace("''", "'");
        if (!backslashEscape) {
            return value;
        }
        final var decoded = new StringBuilder();
        for (int index = 0; index < value.length(); index++) {
            final char character = value.charAt(index);
            if (character != '\\' || index + 1 == value.length()) {
                decoded.append(character);
                continue;
            }
            final char escaped = value.charAt(++index);
            // Unlike an INSERT literal, DM retains both slashes in a compiled default.
            if (escaped == '\\' && defaultValue) {
                decoded.append('\\');
            }
            decoded.append(switch (escaped) {
                case 'b' -> '\b';
                case 'f' -> '\f';
                case 'n' -> '\n';
                case 'r' -> '\r';
                case 't' -> '\t';
                default -> escaped;
            });
        }
        return decoded.toString();
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
