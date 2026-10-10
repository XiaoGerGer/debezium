/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng.logminer.parser;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;

import io.debezium.data.Envelope.Operation;
import io.debezium.relational.Table;

/**
 * Parses the single-row INSERT, UPDATE and DELETE statements emitted by DM LogMiner.
 * This deliberately does not evaluate SQL or accept arbitrary application predicates.
 */
public class LogMinerDmlParser {
    private static final Pattern NUMBER = Pattern.compile("[+-]?(?:[0-9]+(?:\\.[0-9]*)?|\\.[0-9]+)(?:[eE][+-]?[0-9]+)?");
    private static final Set<String> FUNCTIONS = Set.of("TO_DATE", "TO_TIMESTAMP", "TO_TIMESTAMP_TZ", "TO_NUMBER", "HEXTORAW", "UNISTR",
            "EMPTY_BLOB", "EMPTY_CLOB");
    private static final Set<String> INTERVAL_FIELDS = Set.of("YEAR", "MONTH", "DAY", "HOUR", "MINUTE", "SECOND");

    /**
     * A SQL expression whose value must be decoded by the column value converter.
     * Keeping it separate from String prevents confusing a quoted function name with a function call.
     */
    public record SqlExpression(String sql) {
    }

    /**
     * Values follow the table's column order. Presence bits distinguish missing values from SQL NULL.
     */
    public record DmlEntry(Operation operation, Object[] oldValues, Object[] newValues, BitSet oldPresent, BitSet newPresent) {
    }

    public DmlEntry parse(String sql, Table table) {
        if (sql == null || sql.isBlank() || table == null) {
            throw new DmlParserException("A redo statement and table metadata are required");
        }
        return new StatementParser(sql, table).parse();
    }

    private enum Kind {
        WORD,
        IDENTIFIER,
        STRING,
        NUMBER,
        BINARY,
        SYMBOL,
        END
    }

    private record Token(Kind kind, String value, int start, int end) {
    }

    private static class StatementParser {
        private final String sql;
        private final Table table;
        private final List<Token> tokens = new ArrayList<>();
        private int position;

        StatementParser(String sql, Table table) {
            this.sql = sql;
            this.table = table;
            tokenize();
        }

        DmlEntry parse() {
            final DmlEntry entry;
            if (takeWord("INSERT")) {
                expectWord("INTO");
                readTable();
                entry = insert();
            }
            else if (takeWord("UPDATE")) {
                readTable();
                entry = update();
            }
            else if (takeWord("DELETE")) {
                expectWord("FROM");
                readTable();
                entry = delete();
            }
            else {
                throw error("Expected INSERT, UPDATE or DELETE");
            }
            takeSymbol(";");
            if (peek().kind() != Kind.END) {
                throw error("Unexpected trailing SQL");
            }
            return entry;
        }

        private DmlEntry insert() {
            expectSymbol("(");
            final List<Integer> columns = new ArrayList<>();
            do {
                columns.add(columnIndex(readIdentifier()));
            } while (takeSymbol(","));
            expectSymbol(")");
            expectWord("VALUES");
            expectSymbol("(");
            final Object[] values = new Object[table.columns().size()];
            final BitSet present = new BitSet(values.length);
            for (int i = 0; i < columns.size(); i++) {
                if (i != 0) {
                    expectSymbol(",");
                }
                put(values, present, columns.get(i), readValue());
            }
            expectSymbol(")");
            return new DmlEntry(Operation.CREATE, new Object[0], values, new BitSet(), present);
        }

        private DmlEntry update() {
            expectWord("SET");
            final Object[] after = new Object[table.columns().size()];
            final BitSet afterPresent = new BitSet(after.length);
            do {
                final int column = columnIndex(readIdentifier());
                expectSymbol("=");
                put(after, afterPresent, column, readValue());
            } while (takeSymbol(","));
            expectWord("WHERE");
            final Object[] before = new Object[after.length];
            final BitSet beforePresent = new BitSet(before.length);
            readConjunction(before, beforePresent);
            for (int i = 0; i < after.length; i++) {
                if (!afterPresent.get(i) && beforePresent.get(i)) {
                    after[i] = before[i];
                    afterPresent.set(i);
                }
            }
            return new DmlEntry(Operation.UPDATE, before, after, beforePresent, afterPresent);
        }

        private DmlEntry delete() {
            expectWord("WHERE");
            final Object[] before = new Object[table.columns().size()];
            final BitSet present = new BitSet(before.length);
            readConjunction(before, present);
            return new DmlEntry(Operation.DELETE, before, new Object[0], present, new BitSet());
        }

        private void readConjunction(Object[] values, BitSet present) {
            do {
                if (takeSymbol("(")) {
                    readConjunction(values, present);
                    expectSymbol(")");
                }
                else {
                    final Token identifier = readIdentifier();
                    final int column = columnIndex(identifier);
                    final Object value;
                    if (takeWord("IS")) {
                        expectWord("NULL");
                        value = null;
                    }
                    else {
                        expectSymbol("=");
                        value = readValue();
                    }
                    if (column >= 0) {
                        put(values, present, column, value);
                    }
                }
            } while (takeWord("AND"));
            if (takeWord("OR")) {
                throw error("OR predicates do not identify a complete row image");
            }
        }

        private void put(Object[] values, BitSet present, int index, Object value) {
            if (index < 0) {
                throw error("ROWID is only supported as a row locator in WHERE");
            }
            if (present.get(index)) {
                throw error("Duplicate column in redo statement");
            }
            values[index] = value;
            present.set(index);
        }

        private Object readValue() {
            final Token token = peek();
            if (token.kind() == Kind.STRING || token.kind() == Kind.NUMBER) {
                position++;
                return token.value();
            }
            if (token.kind() == Kind.BINARY) {
                if (token.value().length() % 2 != 0) {
                    throw error("Invalid hexadecimal binary literal");
                }
                position++;
                return HexFormat.of().parseHex(token.value());
            }
            if (takeWord("NULL")) {
                return null;
            }
            if (takeWord("OUT_CLOB") || takeWord("OUT_BLOB")) {
                return new SqlExpression(token.value());
            }
            if (takeWord("N")) {
                if (peek().kind() != Kind.STRING) {
                    throw error("Expected a national string literal");
                }
                return tokens.get(position++).value();
            }
            if (token.kind() == Kind.WORD && FUNCTIONS.contains(token.value().toUpperCase(Locale.ROOT))) {
                position++;
                expectSymbol("(");
                if (!takeSymbol(")")) {
                    do {
                        readValue();
                    } while (takeSymbol(","));
                    expectSymbol(")");
                }
                return new SqlExpression(sql.substring(token.start(), tokens.get(position - 1).end()));
            }
            if (takeWord("DATE") || takeWord("TIME") || takeWord("TIMESTAMP")) {
                if (peek().kind() != Kind.STRING) {
                    throw error("Expected a temporal literal");
                }
                position++;
                return new SqlExpression(sql.substring(token.start(), tokens.get(position - 1).end()));
            }
            if (takeWord("INTERVAL")) {
                if (peek().kind() != Kind.STRING) {
                    throw error("Expected an interval literal");
                }
                position++;
                readIntervalField();
                if (takeWord("TO")) {
                    readIntervalField();
                }
                // Keep the expression intact so excluded interval columns can be
                // parsed without claiming a supported interval value mapping.
                return new SqlExpression(sql.substring(token.start(), tokens.get(position - 1).end()));
            }
            throw error("Unsupported redo value");
        }

        private void readIntervalField() {
            final Token field = peek();
            if (field.kind() != Kind.WORD || !INTERVAL_FIELDS.contains(field.value().toUpperCase(Locale.ROOT))) {
                throw error("Expected an interval field");
            }
            position++;
            if (takeSymbol("(")) {
                do {
                    if (peek().kind() != Kind.NUMBER || !peek().value().matches("[0-9]+")) {
                        throw error("Expected an interval precision");
                    }
                    position++;
                } while (takeSymbol(","));
                expectSymbol(")");
            }
        }

        private void readTable() {
            final List<Token> name = new ArrayList<>();
            name.add(readIdentifier());
            while (takeSymbol(".")) {
                name.add(readIdentifier());
            }
            if (name.size() > 3 || !matches(name.get(name.size() - 1), table.id().table())) {
                throw error("Redo table does not match table metadata");
            }
            if (name.size() >= 2 && table.id().schema() != null && !matches(name.get(name.size() - 2), table.id().schema())) {
                throw error("Redo schema does not match table metadata");
            }
            if (name.size() == 3 && table.id().catalog() != null && !matches(name.get(0), table.id().catalog())) {
                throw error("Redo catalog does not match table metadata");
            }
        }

        private int columnIndex(Token name) {
            for (int i = 0; i < table.columns().size(); i++) {
                if (matches(name, table.columns().get(i).name())) {
                    return i;
                }
            }
            if (name.value().equalsIgnoreCase("ROWID")) {
                return -1;
            }
            throw error("Unknown redo column " + name.value());
        }

        private boolean matches(Token token, String identifier) {
            return Objects.equals(token.kind() == Kind.IDENTIFIER ? token.value() : token.value().toUpperCase(Locale.ROOT), identifier);
        }

        private Token readIdentifier() {
            final Token token = peek();
            if (token.kind() != Kind.WORD && token.kind() != Kind.IDENTIFIER) {
                throw error("Expected an identifier");
            }
            position++;
            return token;
        }

        private Token peek() {
            return tokens.get(position);
        }

        private boolean takeWord(String word) {
            if (peek().kind() == Kind.WORD && word.equalsIgnoreCase(peek().value())) {
                position++;
                return true;
            }
            return false;
        }

        private boolean takeSymbol(String symbol) {
            if (peek().kind() == Kind.SYMBOL && symbol.equals(peek().value())) {
                position++;
                return true;
            }
            return false;
        }

        private void expectWord(String word) {
            if (!takeWord(word)) {
                throw error("Expected " + word);
            }
        }

        private void expectSymbol(String symbol) {
            if (!takeSymbol(symbol)) {
                throw error("Expected " + symbol);
            }
        }

        private DmlParserException error(String message) {
            final int offset = tokens.isEmpty() ? 0 : tokens.get(Math.min(position, tokens.size() - 1)).start();
            return new DmlParserException(message + " at position " + offset + " for " + table.id());
        }

        private void tokenize() {
            int index = 0;
            while (index < sql.length()) {
                final char character = sql.charAt(index);
                if (Character.isWhitespace(character)) {
                    index++;
                    continue;
                }
                if (sql.startsWith("/*", index)) {
                    final int end = sql.indexOf("*/", index + 2);
                    if (end < 0) {
                        throw error("Unterminated SQL comment");
                    }
                    index = end + 2;
                    continue;
                }
                if (sql.startsWith("--", index)) {
                    final int end = sql.indexOf('\n', index + 2);
                    index = end < 0 ? sql.length() : end + 1;
                    continue;
                }
                final int start = index;
                if (character == '\'' || character == '"') {
                    final StringBuilder value = new StringBuilder();
                    index++;
                    boolean closed = false;
                    while (index < sql.length()) {
                        final char next = sql.charAt(index++);
                        if (next == character) {
                            if (index < sql.length() && sql.charAt(index) == character) {
                                value.append(character);
                                index++;
                            }
                            else {
                                closed = true;
                                break;
                            }
                        }
                        else {
                            value.append(next);
                        }
                    }
                    if (!closed) {
                        throw error("Unterminated SQL quotation");
                    }
                    tokens.add(new Token(character == '\'' ? Kind.STRING : Kind.IDENTIFIER, value.toString(), start, index));
                }
                else if (character == '0' && index + 1 < sql.length() && (sql.charAt(index + 1) == 'x' || sql.charAt(index + 1) == 'X')) {
                    index += 2;
                    final int digits = index;
                    while (index < sql.length() && HexFormat.isHexDigit(sql.charAt(index))) {
                        index++;
                    }
                    tokens.add(new Token(Kind.BINARY, sql.substring(digits, index), start, index));
                }
                else if (Character.isDigit(character) || character == '+' || character == '-'
                        || character == '.' && index + 1 < sql.length() && Character.isDigit(sql.charAt(index + 1))) {
                    final var matcher = NUMBER.matcher(sql).region(index, sql.length());
                    if (!matcher.lookingAt()) {
                        throw error("Invalid numeric literal");
                    }
                    index = matcher.end();
                    tokens.add(new Token(Kind.NUMBER, sql.substring(start, index), start, index));
                }
                else if (Character.isLetter(character) || character == '_' || character == '$' || character == '#') {
                    index++;
                    while (index < sql.length()) {
                        final char next = sql.charAt(index);
                        if (!Character.isLetterOrDigit(next) && next != '_' && next != '$' && next != '#') {
                            break;
                        }
                        index++;
                    }
                    tokens.add(new Token(Kind.WORD, sql.substring(start, index), start, index));
                }
                else {
                    index++;
                    tokens.add(new Token(Kind.SYMBOL, Character.toString(character), start, index));
                }
            }
            tokens.add(new Token(Kind.END, "", sql.length(), sql.length()));
        }
    }
}
