/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;

import io.debezium.relational.Attribute;
import io.debezium.relational.Column;
import io.debezium.relational.ColumnEditor;
import io.debezium.relational.Table;
import io.debezium.relational.TableEditor;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;

/**
 * Parses the supported DM table DDL against historical table definitions. Unsupported
 * syntax fails before changing the supplied model; current database metadata is never
 * substituted for the definition that existed at the DDL's log position.
 */
public final class DamengDdlParser {
    static final String CONSTRAINT_PREFIX = "dameng.constraint.";
    private static final Set<String> COLUMN_CLAUSES = Set.of("DEFAULT", "NOT", "NULL", "PRIMARY", "UNIQUE", "CHECK", "REFERENCES", "CONSTRAINT", "IDENTITY");
    private final Charset databaseCharset;
    private final boolean characterSemanticsEnabled;
    private final boolean defaultCharacterLength;
    private final boolean nationalCharacterLength;

    public DamengDdlParser() {
        this(StandardCharsets.UTF_8, true, false, false);
    }

    public DamengDdlParser(Charset databaseCharset, boolean characterSemanticsEnabled, boolean defaultCharacterLength, boolean nationalCharacterLength) {
        this.databaseCharset = databaseCharset;
        this.characterSemanticsEnabled = characterSemanticsEnabled;
        this.defaultCharacterLength = defaultCharacterLength;
        this.nationalCharacterLength = nationalCharacterLength;
    }

    public enum Type {
        CREATE,
        ALTER,
        DROP,
        TRUNCATE,
        RENAME
    }

    public record SchemaChange(Type type, Table table, TableId tableId, TableId previousTableId) {
    }

    /** Resolves the DDL target before table filtering, even when LogMiner omits SEG_OWNER. */
    public TableId resolveTableId(String ddl, String defaultSchema) {
        if (isNonTableDdl(ddl)) {
            return null;
        }
        final var tokens = new Tokens(tokenize(ddl));
        if (tokens.take("CREATE") || tokens.take("ALTER") || tokens.take("DROP") || tokens.take("TRUNCATE")) {
            tokens.expect("TABLE");
        }
        else if (tokens.take("RENAME")) {
            tokens.take("TABLE");
        }
        else {
            throw tokens.error("Unsupported table DDL");
        }
        return tokens.tableId(defaultSchema);
    }

    /** Returns the destination of a table rename without requiring its historical schema. */
    public TableId resolveRenamedTableId(String ddl, String defaultSchema) {
        final var tokens = new Tokens(tokenize(ddl));
        if (tokens.take("ALTER")) {
            tokens.expect("TABLE");
            final var previous = tokens.tableId(defaultSchema);
            if (!tokens.take("RENAME") || !tokens.take("TO")) {
                return null;
            }
            final var renamed = tokens.tableId(previous.schema());
            tokens.end();
            return renamed;
        }
        if (tokens.take("RENAME")) {
            tokens.take("TABLE");
            final var previous = tokens.tableId(defaultSchema);
            tokens.expect("TO");
            final var renamed = tokens.tableId(previous.schema());
            tokens.end();
            return renamed;
        }
        return null;
    }

    /**
     * Identifies object definitions which cannot change a captured table's columns or
     * primary key. Unknown statements are deliberately not classified as harmless.
     */
    public boolean isNonTableDdl(String ddl) {
        final var tokens = new Tokens(tokenize(ddl));
        if (tokens.take("CREATE")) {
            if (tokens.take("OR")) {
                tokens.expect("REPLACE");
            }
            tokens.take("UNIQUE");
            return tokens.peek("INDEX") || tokens.peek("SEQUENCE") || tokens.peek("VIEW") || tokens.peek("SYNONYM")
                    || tokens.peek("PROCEDURE") || tokens.peek("FUNCTION") || tokens.peek("TRIGGER");
        }
        if (tokens.take("DROP") || tokens.take("ALTER")) {
            return tokens.peek("INDEX") || tokens.peek("SEQUENCE") || tokens.peek("VIEW") || tokens.peek("SYNONYM")
                    || tokens.peek("PROCEDURE") || tokens.peek("FUNCTION") || tokens.peek("TRIGGER");
        }
        return false;
    }

    public List<SchemaChange> parse(String ddl, Tables tables, String defaultSchema) {
        Objects.requireNonNull(ddl, "ddl");
        Objects.requireNonNull(tables, "tables");
        final var working = tables.clone();
        List<SchemaChange> changes = new ArrayList<>();
        for (List<Token> statement : split(tokenize(ddl), ";")) {
            if (!statement.isEmpty()) {
                changes.add(parseStatement(new Tokens(statement), working, defaultSchema));
            }
        }
        // A malformed later statement must not leave a partially updated schema behind.
        for (SchemaChange change : changes) {
            if (change.previousTableId() != null) {
                tables.removeTable(change.previousTableId());
            }
            if (change.type() == Type.DROP) {
                tables.removeTable(change.tableId());
            }
            else {
                tables.overwriteTable(change.table());
            }
        }
        return changes;
    }

    private SchemaChange parseStatement(Tokens tokens, Tables tables, String defaultSchema) {
        if (tokens.take("CREATE")) {
            tokens.expect("TABLE");
            final var id = tokens.tableId(defaultSchema);
            if (tables.forTable(id) != null) {
                throw tokens.error("CREATE TABLE refers to an existing table " + id);
            }
            final var editor = Table.editor().tableId(id);
            final var definitions = tokens.group();
            tokens.end();
            addDefinitions(editor, definitions);
            final var table = editor.create();
            tables.overwriteTable(table);
            return new SchemaChange(Type.CREATE, table, id, null);
        }
        if (tokens.take("ALTER")) {
            tokens.expect("TABLE");
            final var id = tokens.tableId(defaultSchema);
            return alter(tokens, tables, id);
        }
        if (tokens.take("RENAME")) {
            tokens.take("TABLE");
            final var oldId = tokens.tableId(defaultSchema);
            tokens.expect("TO");
            final var newId = tokens.tableId(oldId.schema());
            tokens.end();
            return rename(tables, oldId, newId);
        }
        if (tokens.take("DROP")) {
            tokens.expect("TABLE");
            final var id = tokens.tableId(defaultSchema);
            final var table = requireTable(tables, id);
            if (tokens.take("CASCADE")) {
                tokens.take("CONSTRAINTS");
            }
            tokens.take("PURGE");
            tokens.end();
            tables.removeTable(id);
            return new SchemaChange(Type.DROP, table, id, null);
        }
        if (tokens.take("TRUNCATE")) {
            tokens.expect("TABLE");
            final var id = tokens.tableId(defaultSchema);
            if (tokens.take("REUSE") || tokens.take("DROP")) {
                tokens.expect("STORAGE");
            }
            tokens.end();
            return new SchemaChange(Type.TRUNCATE, requireTable(tables, id), id, null);
        }
        throw tokens.error("Unsupported Dameng DDL");
    }

    private SchemaChange alter(Tokens tokens, Tables tables, TableId id) {
        final var historicalTable = requireTable(tables, id);
        final var editor = historicalTable.edit().addAttributes(historicalTable.attributes());
        if (tokens.take("RENAME")) {
            if (tokens.take("COLUMN")) {
                final var from = tokens.identifier();
                tokens.expect("TO");
                final var to = tokens.identifier();
                requireColumn(editor, from);
                if (editor.columnWithName(to) != null) {
                    throw tokens.error("Column already exists: " + to);
                }
                editor.renameColumn(from, to);
            }
            else {
                tokens.expect("TO");
                final var newId = tokens.tableId(id.schema());
                tokens.end();
                return rename(tables, id, newId);
            }
        }
        else if (tokens.take("ADD")) {
            tokens.take("COLUMN");
            addDefinitions(editor, tokens.peek("(") ? tokens.group() : tokens.remaining());
        }
        else if (tokens.take("MODIFY")) {
            tokens.take("COLUMN");
            final var definitions = tokens.peek("(") ? tokens.group() : tokens.remaining();
            for (List<Token> definition : split(definitions, ",")) {
                column(editor, new Tokens(definition), true);
            }
        }
        else if (tokens.take("DROP")) {
            if (tokens.take("PRIMARY")) {
                tokens.expect("KEY");
                editor.setPrimaryKeyNames(List.of());
            }
            else if (tokens.take("CONSTRAINT")) {
                final var constraintName = tokens.identifier();
                final var attribute = editor.attributeWithName(CONSTRAINT_PREFIX + constraintName);
                if (attribute == null) {
                    throw tokens.error("Cannot resolve historical constraint " + constraintName);
                }
                if ("PRIMARY".equals(attribute.value())) {
                    editor.setPrimaryKeyNames(List.of());
                }
                editor.removeAttribute(CONSTRAINT_PREFIX + constraintName);
            }
            else {
                tokens.take("COLUMN");
                List<String> columns = tokens.peek("(") ? identifiers(tokens.group()) : List.of(tokens.identifier());
                for (String name : columns) {
                    requireColumn(editor, name);
                    editor.removeColumn(name);
                }
            }
            if (tokens.take("CASCADE")) {
                tokens.expect("CONSTRAINTS");
            }
        }
        else if (tokens.take("ALTER")) {
            tokens.take("COLUMN");
            final var name = tokens.identifier();
            final var column = requireColumn(editor, name).edit();
            if (tokens.take("SET")) {
                if (tokens.take("DEFAULT")) {
                    column.defaultValueExpression(expression(tokens.remaining()));
                }
                else {
                    tokens.expect("NOT");
                    tokens.expect("NULL");
                    column.optional(false);
                }
            }
            else if (tokens.take("DROP")) {
                if (tokens.take("DEFAULT")) {
                    column.unsetDefaultValueExpression();
                }
                else {
                    tokens.expect("NOT");
                    tokens.expect("NULL");
                    column.optional(true);
                }
            }
            else {
                throw tokens.error("Unsupported ALTER COLUMN operation");
            }
            editor.updateColumn(column.create());
        }
        else {
            throw tokens.error("Unsupported ALTER TABLE operation");
        }
        tokens.end();
        final var table = editor.create();
        tables.overwriteTable(table);
        return new SchemaChange(Type.ALTER, table, id, null);
    }

    private static SchemaChange rename(Tables tables, TableId from, TableId to) {
        final var original = requireTable(tables, from);
        if (tables.forTable(to) != null) {
            throw new IllegalArgumentException("Cannot rename table to existing table " + to);
        }
        final var renamed = original.edit().tableId(to).addAttributes(original.attributes()).create();
        tables.removeTable(from);
        tables.overwriteTable(renamed);
        return new SchemaChange(Type.RENAME, renamed, to, from);
    }

    private void addDefinitions(TableEditor editor, List<Token> definitions) {
        List<List<Token>> constraints = new ArrayList<>();
        for (List<Token> definition : split(definitions, ",")) {
            final var tokens = new Tokens(definition);
            if (tokens.peek("CONSTRAINT") || tokens.peek("PRIMARY") || tokens.peek("UNIQUE") || tokens.peek("FOREIGN") || tokens.peek("CHECK")) {
                constraints.add(definition);
            }
            else {
                column(editor, tokens, false);
            }
        }
        // A table constraint may precede the columns that it references.
        for (List<Token> constraint : constraints) {
            tableConstraint(editor, new Tokens(constraint));
        }
    }

    private static void tableConstraint(TableEditor editor, Tokens tokens) {
        final var name = tokens.take("CONSTRAINT") ? tokens.identifier() : null;
        final String kind;
        if (tokens.take("PRIMARY")) {
            tokens.expect("KEY");
            final var names = identifiers(tokens.group());
            setPrimaryKey(editor, names);
            kind = "PRIMARY";
        }
        else if (tokens.take("UNIQUE")) {
            identifiers(tokens.group()).forEach(column -> requireColumn(editor, column));
            kind = "UNIQUE";
        }
        else if (tokens.take("FOREIGN")) {
            tokens.expect("KEY");
            identifiers(tokens.group()).forEach(column -> requireColumn(editor, column));
            references(tokens);
            kind = "FOREIGN";
        }
        else if (tokens.take("CHECK")) {
            expression(tokens.group());
            kind = "CHECK";
        }
        else {
            throw tokens.error("Unsupported table constraint");
        }
        tokens.take("ENABLE");
        tokens.end();
        recordConstraint(editor, name, kind);
    }

    private void column(TableEditor table, Tokens tokens, boolean modifying) {
        final var name = tokens.identifier();
        final var existing = table.columnWithName(name);
        if (modifying && existing == null) {
            throw tokens.error("Cannot modify unknown column " + name);
        }
        if (!modifying && existing != null) {
            throw tokens.error("Column already exists: " + name);
        }
        final var column = existing == null ? Column.editor().name(name).optional(true) : existing.edit();
        if (!tokens.finished() && !COLUMN_CLAUSES.contains(tokens.peekKeyword())) {
            dataType(column, tokens);
        }
        else if (!modifying) {
            throw tokens.error("Column type is required");
        }
        boolean primaryKey = false;
        while (!tokens.finished()) {
            final var constraintName = tokens.take("CONSTRAINT") ? tokens.identifier() : null;
            if (tokens.take("NOT")) {
                tokens.expect("NULL");
                column.optional(false);
            }
            else if (tokens.take("NULL")) {
                column.optional(true);
            }
            else if (tokens.take("DEFAULT")) {
                column.defaultValueExpression(expression(tokens.defaultExpression()));
            }
            else if (tokens.take("PRIMARY")) {
                tokens.expect("KEY");
                primaryKey = true;
                column.optional(false);
                recordConstraint(table, constraintName, "PRIMARY");
            }
            else if (tokens.take("UNIQUE")) {
                recordConstraint(table, constraintName, "UNIQUE");
            }
            else if (tokens.peek("REFERENCES")) {
                references(tokens);
                recordConstraint(table, constraintName, "FOREIGN");
            }
            else if (tokens.take("CHECK")) {
                expression(tokens.group());
                recordConstraint(table, constraintName, "CHECK");
            }
            else if (tokens.take("IDENTITY")) {
                if (tokens.peek("(")) {
                    final var identity = new Tokens(tokens.group());
                    identity.integer();
                    identity.expect(",");
                    identity.integer();
                    identity.end();
                }
                column.autoIncremented(true);
            }
            else {
                throw tokens.error("Unsupported column clause");
            }
        }
        table.addColumn(column.create());
        if (primaryKey) {
            setPrimaryKey(table, List.of(name));
        }
    }

    private static void references(Tokens tokens) {
        tokens.expect("REFERENCES");
        tokens.tableId(null);
        if (tokens.peek("(")) {
            identifiers(tokens.group());
        }
        if (tokens.take("ON")) {
            tokens.expect("DELETE");
            if (!tokens.take("CASCADE")) {
                tokens.expect("SET");
                tokens.expect("NULL");
            }
        }
    }

    private static void recordConstraint(TableEditor editor, String name, String kind) {
        if (name != null) {
            editor.addAttribute(Attribute.editor().name(CONSTRAINT_PREFIX + name).value(kind).create());
        }
    }

    private static void setPrimaryKey(TableEditor editor, List<String> names) {
        if (!editor.primaryKeyColumnNames().isEmpty()) {
            throw new IllegalArgumentException("Multiple primary keys for " + editor.tableId());
        }
        for (String name : names) {
            editor.updateColumn(requireColumn(editor, name).edit().optional(false).create());
        }
        editor.setPrimaryKeyNames(names);
    }

    private void dataType(ColumnEditor column, Tokens tokens) {
        final int start = tokens.index;
        String name = tokens.identifier().toUpperCase(Locale.ROOT);
        if (name.equals("DOUBLE")) {
            tokens.take("PRECISION");
        }
        if (name.equals("CHARACTER")) {
            name = tokens.take("VARYING") ? "VARCHAR" : "CHAR";
        }
        int length = -1;
        Integer scale = null;
        boolean characterLength = defaultCharacterLength || (name.equals("NCHAR") && nationalCharacterLength);
        if (tokens.take("(")) {
            length = tokens.integer();
            if (tokens.take(",")) {
                scale = tokens.integer();
            }
            if (tokens.take("CHAR")) {
                characterLength = true;
            }
            else if (tokens.take("BYTE")) {
                characterLength = false;
            }
            tokens.expect(")");
        }
        boolean timeZone = false;
        if (name.equals("TIMESTAMP") || name.equals("DATETIME") || name.equals("TIME") || name.equals("DATETIME2")) {
            if (tokens.take("WITH")) {
                // LOCAL TIME ZONE depends on the database session and is not treated as
                // a standard offset timestamp without an explicit conversion contract.
                tokens.expect("TIME");
                tokens.expect("ZONE");
                timeZone = true;
            }
            if (scale != null) {
                throw tokens.error("Temporal types accept one precision parameter");
            }
            scale = length < 0 ? (name.equals("DATETIME2") ? 9 : 6) : length;
            length = (name.equals("TIME") ? 15 : 29) + (scale == 0 ? 0 : scale + 1) + (timeZone ? 7 : 0);
        }
        final int jdbcType = switch (name) {
            case "INT", "INTEGER", "PLS_INTEGER" -> Types.INTEGER;
            case "BIGINT" -> Types.BIGINT;
            case "SMALLINT" -> Types.SMALLINT;
            case "TINYINT", "BYTE" -> Types.TINYINT;
            case "NUMERIC", "NUMBER" -> Types.DECIMAL;
            case "DEC", "DECIMAL" -> Types.DECIMAL;
            case "FLOAT" -> length > 0 && length <= 24 ? Types.REAL : Types.FLOAT;
            case "DOUBLE" -> Types.DOUBLE;
            case "REAL" -> Types.REAL;
            case "BIT" -> Types.BIT;
            case "BOOLEAN" -> Types.BOOLEAN;
            case "CHAR" -> Types.CHAR;
            case "NCHAR" -> Types.CHAR;
            case "VARCHAR", "VARCHAR2" -> Types.VARCHAR;
            case "NVARCHAR", "NVARCHAR2" -> Types.VARCHAR;
            case "CLOB", "TEXT", "LONGVARCHAR" -> Types.CLOB;
            case "BLOB", "IMAGE", "LONGVARBINARY" -> Types.BLOB;
            case "BINARY" -> Types.BINARY;
            case "RAW", "VARBINARY" -> Types.VARBINARY;
            case "DATE" -> Types.DATE;
            case "TIME" -> timeZone ? Types.TIME_WITH_TIMEZONE : Types.TIME;
            case "TIMESTAMP", "DATETIME", "DATETIME2" -> timeZone ? Types.TIMESTAMP_WITH_TIMEZONE : Types.TIMESTAMP;
            default -> throw tokens.error("Unsupported DM column type " + name);
        };
        if (length < 0) {
            length = switch (jdbcType) {
                case Types.INTEGER -> 10;
                case Types.SMALLINT -> 5;
                case Types.TINYINT -> 3;
                case Types.BIGINT -> 19;
                case Types.CHAR, Types.NCHAR, Types.BINARY, Types.BIT, Types.BOOLEAN -> 1;
                case Types.VARCHAR, Types.NVARCHAR, Types.VARBINARY -> 8188;
                case Types.NUMERIC, Types.DECIMAL -> 0;
                case Types.REAL -> 24;
                case Types.FLOAT, Types.DOUBLE -> 53;
                case Types.DATE -> 13;
                case Types.CLOB, Types.BLOB -> Integer.MAX_VALUE;
                default -> -1;
            };
        }
        if (scale == null && Set.of(Types.DECIMAL, Types.NUMERIC, Types.INTEGER, Types.BIGINT, Types.SMALLINT,
                Types.TINYINT, Types.DATE, Types.BIT, Types.BOOLEAN).contains(jdbcType)) {
            scale = 0;
        }
        if (name.equals("RAW")) {
            name = "VARBINARY";
        }
        else if (name.equals("FLOAT") && jdbcType == Types.REAL) {
            name = "REAL";
            length = 24;
        }
        else if (jdbcType == Types.FLOAT || jdbcType == Types.DOUBLE) {
            length = 53;
        }
        if (timeZone && !name.equals("TIME")) {
            name = "DATETIME";
        }
        column.jdbcType(jdbcType).type(name + (timeZone ? " WITH TIME ZONE" : ""), expression(tokens.values.subList(start, tokens.index)))
                .length(length).scale(scale);
        if (jdbcType == Types.CHAR || jdbcType == Types.NCHAR) {
            // Persist resolved semantics with the historical column; querying its
            // current metadata later would reinterpret redo from before a type change.
            column.charsetName(databaseCharset.name()).type(name,
                    name + "(" + length + (characterSemanticsEnabled && characterLength ? " CHAR)" : " BYTE)"));
        }
    }

    private static Table requireTable(Tables tables, TableId id) {
        final var table = tables.forTable(id);
        if (table == null) {
            throw new IllegalArgumentException("DDL refers to unknown table " + id);
        }
        return table;
    }

    private static Column requireColumn(TableEditor table, String name) {
        final var column = table.columnWithName(name);
        if (column == null) {
            throw new IllegalArgumentException("DDL refers to unknown column " + table.tableId() + "." + name);
        }
        return column;
    }

    private static List<String> identifiers(List<Token> values) {
        List<String> result = new ArrayList<>();
        for (List<Token> item : split(values, ",")) {
            final var tokens = new Tokens(item);
            result.add(tokens.identifier());
            tokens.end();
        }
        return result;
    }

    private static String expression(List<Token> values) {
        if (values.isEmpty()) {
            throw new IllegalArgumentException("Empty SQL expression");
        }
        return String.join(" ", values.stream().map(Token::text).toList());
    }

    private record Token(String text) {
        boolean keyword(String keyword) {
            return text.equalsIgnoreCase(keyword);
        }
    }

    private static final class Tokens {
        private final List<Token> values;
        private int index;

        private Tokens(List<Token> values) {
            this.values = values;
        }

        private boolean finished() {
            return index == values.size();
        }

        private boolean peek(String keyword) {
            return !finished() && values.get(index).keyword(keyword);
        }

        private String peekKeyword() {
            return finished() ? "" : values.get(index).text().toUpperCase(Locale.ROOT);
        }

        private boolean take(String keyword) {
            if (peek(keyword)) {
                index++;
                return true;
            }
            return false;
        }

        private void expect(String keyword) {
            if (!take(keyword)) {
                throw error("Expected " + keyword);
            }
        }

        private void end() {
            if (!finished()) {
                throw error("Unsupported trailing DDL syntax");
            }
        }

        private IllegalArgumentException error(String message) {
            return new IllegalArgumentException(message + " near " + values.subList(index, values.size()));
        }

        private String identifier() {
            if (finished()) {
                throw error("Expected identifier");
            }
            final var text = values.get(index++).text();
            if (text.startsWith("\"")) {
                return text.substring(1, text.length() - 1).replace("\"\"", "\"");
            }
            if (!Character.isLetter(text.charAt(0)) && text.charAt(0) != '_' && text.charAt(0) != '#') {
                throw error("Invalid unquoted identifier " + text);
            }
            return text.toUpperCase(Locale.ROOT);
        }

        private TableId tableId(String defaultSchema) {
            final var first = identifier();
            return take(".") ? new TableId(null, first, identifier()) : new TableId(null, defaultSchema, first);
        }

        private int integer() {
            final boolean negative = take("-");
            take("+");
            if (finished()) {
                throw error("Expected integer");
            }
            try {
                return Integer.parseInt((negative ? "-" : "") + values.get(index++).text());
            }
            catch (NumberFormatException e) {
                throw error("Expected integer");
            }
        }

        private List<Token> remaining() {
            final var result = values.subList(index, values.size());
            index = values.size();
            return result;
        }

        private List<Token> group() {
            expect("(");
            final int start = index;
            int depth = 1;
            while (!finished()) {
                final var token = values.get(index++);
                if (token.keyword("(")) {
                    depth++;
                }
                else if (token.keyword(")") && --depth == 0) {
                    return values.subList(start, index - 1);
                }
            }
            throw error("Unclosed parentheses");
        }

        private List<Token> defaultExpression() {
            final int start = index;
            int depth = 0;
            while (!finished()) {
                final var token = values.get(index);
                if (depth == 0 && index > start && COLUMN_CLAUSES.contains(peekKeyword())) {
                    break;
                }
                if (token.keyword("(")) {
                    depth++;
                }
                else if (token.keyword(")")) {
                    depth--;
                }
                index++;
            }
            return values.subList(start, index);
        }
    }

    private static List<List<Token>> split(List<Token> tokens, String delimiter) {
        List<List<Token>> result = new ArrayList<>();
        int depth = 0;
        int start = 0;
        for (int index = 0; index < tokens.size(); index++) {
            final var token = tokens.get(index);
            if (token.keyword("(")) {
                depth++;
            }
            else if (token.keyword(")")) {
                depth--;
            }
            else if (depth == 0 && token.keyword(delimiter)) {
                result.add(tokens.subList(start, index));
                start = index + 1;
            }
            if (depth < 0) {
                throw new IllegalArgumentException("Unexpected closing parenthesis in DDL");
            }
        }
        if (depth != 0) {
            throw new IllegalArgumentException("Unbalanced parentheses in DDL");
        }
        result.add(tokens.subList(start, tokens.size()));
        return result;
    }

    private static List<Token> tokenize(String input) {
        List<Token> result = new ArrayList<>();
        int offset = 0;
        while (offset < input.length()) {
            final char c = input.charAt(offset);
            if (Character.isWhitespace(c)) {
                offset++;
                continue;
            }
            if (input.startsWith("--", offset)) {
                final int newline = input.indexOf('\n', offset + 2);
                offset = newline < 0 ? input.length() : newline + 1;
                continue;
            }
            if (input.startsWith("/*", offset)) {
                final int close = input.indexOf("*/", offset + 2);
                if (close < 0) {
                    throw new IllegalArgumentException("Unclosed SQL comment");
                }
                offset = close + 2;
                continue;
            }
            final int start = offset++;
            if (c == '\'' || c == '"') {
                boolean closed = false;
                while (offset < input.length()) {
                    if (input.charAt(offset++) == c) {
                        if (offset < input.length() && input.charAt(offset) == c) {
                            offset++;
                        }
                        else {
                            closed = true;
                            break;
                        }
                    }
                }
                if (!closed) {
                    throw new IllegalArgumentException("Unclosed SQL quoted value");
                }
            }
            else if (Character.isDigit(c)) {
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
                    while (offset < input.length() && Character.isDigit(input.charAt(offset))) {
                        offset++;
                    }
                }
            }
            else if (Character.isLetter(c) || c == '_' || c == '#' || c == '$') {
                while (offset < input.length()) {
                    final char next = input.charAt(offset);
                    if (!Character.isLetterOrDigit(next) && next != '_' && next != '#' && next != '$') {
                        break;
                    }
                    offset++;
                }
            }
            result.add(new Token(input.substring(start, offset)));
        }
        return result;
    }
}
