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

import io.debezium.connector.dameng.compatibility.DamengIdentifierRules;
import io.debezium.connector.dameng.ddl.DamengDdlStatementAnalyzer;
import io.debezium.connector.dameng.ddl.DamengDdlStatementAnalyzer.Analysis;
import io.debezium.connector.dameng.ddl.DamengDdlStatementAnalyzer.Impact;
import io.debezium.connector.dameng.ddl.DamengSqlLexer;
import io.debezium.connector.dameng.ddl.DamengSqlLexer.Kind;
import io.debezium.connector.dameng.ddl.DamengSqlLexer.Token;
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
    private static final Set<String> COLUMN_CLAUSES = Set.of("DEFAULT", "NOT", "NULL", "PRIMARY", "UNIQUE", "CHECK", "REFERENCES", "CONSTRAINT", "IDENTITY",
            "COMMENT", "GENERATED", "AS", "VIRTUAL", "VISIBLE", "STORAGE");
    private final Charset databaseCharset;
    private final boolean characterSemanticsEnabled;
    private final boolean defaultCharacterLength;
    private final boolean nationalCharacterLength;
    private final DamengSqlLexer lexer;
    private final DamengDdlStatementAnalyzer analyzer;
    private final DamengIdentifierRules identifierRules;

    public DamengDdlParser() {
        this(StandardCharsets.UTF_8, true, false, false);
    }

    public DamengDdlParser(Charset databaseCharset, boolean characterSemanticsEnabled, boolean defaultCharacterLength, boolean nationalCharacterLength) {
        this(databaseCharset, characterSemanticsEnabled, defaultCharacterLength, nationalCharacterLength, false);
    }

    public DamengDdlParser(Charset databaseCharset, boolean characterSemanticsEnabled, boolean defaultCharacterLength, boolean nationalCharacterLength,
                           boolean nestedComments) {
        this(databaseCharset, characterSemanticsEnabled, defaultCharacterLength, nationalCharacterLength, nestedComments, new DamengIdentifierRules(false));
    }

    public DamengDdlParser(Charset databaseCharset, boolean characterSemanticsEnabled, boolean defaultCharacterLength, boolean nationalCharacterLength,
                           boolean nestedComments, DamengIdentifierRules identifierRules) {
        this.databaseCharset = databaseCharset;
        this.characterSemanticsEnabled = characterSemanticsEnabled;
        this.defaultCharacterLength = defaultCharacterLength;
        this.nationalCharacterLength = nationalCharacterLength;
        this.lexer = new DamengSqlLexer(nestedComments);
        this.identifierRules = identifierRules;
        this.analyzer = new DamengDdlStatementAnalyzer(lexer, identifierRules);
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

    /** Analyze one original LogMiner record once, before filtering or parsing. */
    public Analysis analyze(String ddl, String defaultSchema) {
        return analyzer.analyze(ddl, defaultSchema).requireSupported();
    }

    /** Resolves the DDL target before table filtering, even when LogMiner omits SEG_OWNER. */
    public TableId resolveTableId(String ddl, String defaultSchema) {
        return analyze(ddl, defaultSchema).tableId();
    }

    /** Returns the destination of a table rename without requiring its historical schema. */
    public TableId resolveRenamedTableId(String ddl, String defaultSchema) {
        return analyze(ddl, defaultSchema).renamedTableId();
    }

    public boolean isNonTableDdl(String ddl) {
        return analyzer.analyze(ddl, null).impact() == Impact.NO_TABLE_CHANGE;
    }

    /** Computes a single historical change without modifying the live model. */
    public List<SchemaChange> plan(Analysis analysis, Tables tables) {
        analysis.requireSupported();
        if (analysis.impact() == Impact.NO_TABLE_CHANGE) {
            return List.of();
        }
        return List.of(parseStatement(new Tokens(analysis.tokens()), tables.clone(), analysis.tableId().schema()));
    }

    public List<SchemaChange> parse(String ddl, Tables tables, String defaultSchema) {
        Objects.requireNonNull(ddl, "ddl");
        Objects.requireNonNull(tables, "tables");
        final var values = lexer.tokenize(ddl);
        if (analyzer.analyzeTokens(ddl, values, defaultSchema).impact() == Impact.NO_TABLE_CHANGE) {
            return List.of();
        }
        final var working = tables.clone();
        List<SchemaChange> changes = new ArrayList<>();
        for (List<Token> statement : split(values, ";")) {
            if (!statement.isEmpty()) {
                final var analysis = analyzer.analyzeTokens(ddl, statement, defaultSchema).requireSupported();
                if (analysis.impact() == Impact.TABLE) {
                    changes.add(parseStatement(new Tokens(statement), working, defaultSchema));
                }
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
            if (tokens.peek("GLOBAL") || tokens.peek("TEMPORARY") || tokens.peek("EXTERNAL") || tokens.peek("HUGE")) {
                throw tokens.error("Captured temporary, external and HUGE tables do not have verified complete LogMiner row images");
            }
            tokens.expect("TABLE");
            final var id = tokens.tableId(defaultSchema);
            if (id.table().startsWith("#")) {
                throw tokens.error("Captured temporary tables do not have complete LogMiner row images");
            }
            if (tables.forTable(id) != null) {
                throw tokens.error("CREATE TABLE refers to an existing table " + id);
            }
            final var editor = Table.editor().tableId(id);
            if (tokens.take("LIKE")) {
                copyColumns(editor, requireTable(tables, tokens.tableId(defaultSchema)));
            }
            else if (tokens.take("AS")) {
                selectColumns(editor, tokens, tables, defaultSchema);
            }
            else {
                addDefinitions(editor, tokens.group());
                tableOptions(tokens, editor);
            }
            tokens.end();
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

    private void copyColumns(TableEditor target, Table source) {
        for (var column : source.columns()) {
            if (target.columnWithName(column.name()) != null) {
                throw new IllegalArgumentException("Duplicate CTAS output column " + column.name());
            }
            target.addColumn(copiedColumn(column).create());
        }
    }

    private ColumnEditor copiedColumn(Column source) {
        // Native CTAS/LIKE copies values and nullability, not keys, defaults,
        // identity generators, constraints, comments or the source object id.
        return source.edit().unsetDefaultValueExpression().autoIncremented(false).generated(false).comment(null);
    }

    private void selectColumns(TableEditor target, Tokens tokens, Tables tables, String defaultSchema) {
        tokens.expect("SELECT");
        final int start = tokens.index;
        int depth = 0;
        while (!tokens.finished()) {
            if (depth == 0 && tokens.peek("FROM")) {
                break;
            }
            if (tokens.take("(")) {
                depth++;
            }
            else if (tokens.take(")")) {
                depth--;
            }
            else {
                tokens.index++;
            }
        }
        final var projections = split(tokens.values.subList(start, tokens.index), ",");
        tokens.expect("FROM");
        final var source = requireTable(tables, tokens.tableId(defaultSchema));
        String alias = source.id().table();
        if (tokens.take("AS")) {
            alias = tokens.identifier();
        }
        else if (!tokens.finished() && !tokens.peek("WHERE")) {
            alias = tokens.identifier();
        }
        // Predicates choose rows but do not change projection types. Expressions
        // with joins, set operators or aggregates require a separate type model.
        if (tokens.take("WHERE")) {
            final var predicate = tokens.remaining();
            expression(predicate);
            for (var token : predicate) {
                if (Set.of("SELECT", "UNION", "INTERSECT", "MINUS", "EXCEPT", "GROUP", "HAVING", "INTO").stream().anyMatch(token::keyword)) {
                    throw token.error("Unsupported CTAS query scope; only a single historical source table is supported");
                }
            }
        }
        tokens.end();
        for (var projection : projections) {
            final var item = new Tokens(projection);
            if (item.take("*")) {
                item.end();
                copyColumns(target, source);
                continue;
            }
            final ColumnEditor column;
            if (item.take("CAST")) {
                final var cast = new Tokens(item.group());
                final var original = projectedColumn(cast, source, alias);
                cast.expect("AS");
                column = copiedColumn(original).optional(true);
                dataType(column, cast);
                cast.end();
                item.expect("AS");
                column.name(item.identifier());
            }
            else {
                column = copiedColumn(projectedColumn(item, source, alias));
                if (item.take("AS")) {
                    column.name(item.identifier());
                }
                else if (!item.finished()) {
                    column.name(item.identifier());
                }
            }
            item.end();
            if (target.columnWithName(column.name()) != null) {
                throw item.error("Duplicate CTAS output column " + column.name());
            }
            target.addColumn(column.create());
        }
    }

    private Column projectedColumn(Tokens tokens, Table source, String alias) {
        String name = tokens.identifier();
        if (tokens.take(".")) {
            if (!name.equals(alias)) {
                throw tokens.error("Unknown CTAS source qualifier " + name);
            }
            name = tokens.identifier();
        }
        final var column = source.columnWithName(name);
        if (column == null) {
            throw tokens.error("Unknown historical CTAS source column " + name);
        }
        return column;
    }

    private void tableOptions(Tokens tokens, TableEditor table) {
        while (!tokens.finished()) {
            if (tokens.take("TABLESPACE")) {
                tokens.identifier();
            }
            else if (tokens.take("STORAGE")) {
                storageOptions(new Tokens(tokens.group()));
            }
            else if (tokens.take("PARTITION")) {
                partitionOptions(tokens, table);
            }
            else if (tokens.take("DISKSPACE")) {
                if (!tokens.take("UNLIMITED")) {
                    tokens.expect("LIMIT");
                    tokens.integer();
                }
            }
            else if (tokens.take("NOPARALLEL")) {
                // Physical execution policy does not alter the captured columns.
            }
            else if (tokens.take("PARALLEL")) {
                if (!tokens.finished() && tokens.values.get(tokens.index).kind() == Kind.NUMBER) {
                    tokens.integer();
                }
            }
            else if (tokens.take("SEGMENT")) {
                tokens.expect("CREATION");
                if (!tokens.take("IMMEDIATE")) {
                    tokens.expect("DEFERRED");
                }
            }
            else if (tokens.take("COMPRESS")) {
                tokens.take("EXCEPT");
                if (tokens.peek("(")) {
                    identifiers(tokens.group()).forEach(name -> requireColumn(table, name));
                }
            }
            else {
                throw tokens.error("Unsupported table storage or partition option");
            }
        }
    }

    private void storageOptions(Tokens tokens) {
        do {
            if (tokens.take("ON")) {
                tokens.identifier();
            }
            else if (tokens.take("INITIAL") || tokens.take("NEXT") || tokens.take("MINEXTENTS") || tokens.take("FILLFACTOR")) {
                tokens.integer();
            }
            else if (tokens.take("BRANCH")) {
                if (tokens.take("(")) {
                    tokens.integer();
                    tokens.expect(",");
                    tokens.integer();
                    tokens.expect(")");
                }
                else {
                    tokens.integer();
                }
            }
            else if (tokens.take("NOBRANCH") || tokens.take("CLUSTERBTR")) {
                // Heap/index organization preserves the logical column model.
            }
            else if (tokens.take("WITH") || tokens.take("WITHOUT")) {
                tokens.expect("COUNTER");
            }
            else {
                tokens.take("DISABLE");
                tokens.expect("USING");
                tokens.expect("LONG");
                tokens.expect("ROW");
            }
        } while (tokens.take(","));
        tokens.end();
    }

    private void partitionOptions(Tokens tokens, TableEditor table) {
        tokens.expect("BY");
        final boolean hash = tokens.take("HASH");
        final boolean range = !hash && tokens.take("RANGE");
        if (!hash && !range) {
            tokens.expect("LIST");
        }
        identifiers(tokens.group()).forEach(name -> requireColumn(table, name));
        if (hash && tokens.take("PARTITIONS")) {
            if (tokens.integer() <= 0) {
                throw tokens.error("Partition count must be positive");
            }
            if (tokens.take("STORE")) {
                tokens.expect("IN");
                identifiers(tokens.group());
            }
            return;
        }
        for (var definition : split(tokens.group(), ",")) {
            final var partition = new Tokens(definition);
            partition.expect("PARTITION");
            partition.identifier();
            if (!hash) {
                partition.expect("VALUES");
                if (range) {
                    if (partition.take("EQU")) {
                        partition.expect("OR");
                    }
                    partition.expect("LESS");
                    partition.expect("THAN");
                }
                expression(partition.group());
            }
            while (!partition.finished()) {
                if (partition.take("TABLESPACE")) {
                    partition.identifier();
                }
                else {
                    partition.expect("STORAGE");
                    storageOptions(new Tokens(partition.group()));
                }
            }
        }
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
                    column.defaultValueExpression(defaultExpression(tokens.remaining()));
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

    private void tableConstraint(TableEditor editor, Tokens tokens) {
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
                if (tokens.take("ON")) {
                    tokens.expect("NULL");
                    column.optional(false);
                }
                column.defaultValueExpression(defaultExpression(tokens.defaultExpression()));
            }
            else if (tokens.take("COMMENT")) {
                column.comment(tokens.string());
            }
            else if (tokens.peek("GENERATED") || tokens.peek("AS") || tokens.peek("VIRTUAL")) {
                throw tokens.error("Virtual/generated columns contain NULL rather than computed values in LogMiner redo; capture is unsupported");
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

    private void references(Tokens tokens) {
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
        if (name.equals("INTERVAL")) {
            tokens.identifier();
            if (tokens.peek("(")) {
                tokens.group();
            }
            if (tokens.take("TO")) {
                tokens.identifier();
                if (tokens.peek("(")) {
                    tokens.group();
                }
            }
            final var declaration = expression(tokens.values.subList(start, tokens.index));
            final var interval = DamengInterval.type(declaration);
            column.jdbcType(Types.OTHER).type(interval.typeName(), declaration).length(interval.leadingPrecision()).scale(interval.fractionalPrecision());
            return;
        }
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
                // TODO Add LOCAL TIME ZONE DDL metadata when the source time-zone contract in
                // DamengValueConverters.schemaBuilder is implemented and verified. Preserve its
                // distinct type semantics; snapshot and historical redo must agree on the instant.
                // LOCAL TIME ZONE depends on the database session and is not treated as
                // a standard offset timestamp without an explicit conversion contract.
                tokens.expect("TIME");
                tokens.expect("ZONE");
                timeZone = true;
            }
            if (scale != null) {
                throw tokens.error("Temporal types accept one precision parameter");
            }
            scale = length < 0 ? (name.equals("TIME") ? 0 : name.equals("DATETIME2") ? 9 : 6) : length;
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

    private List<String> identifiers(List<Token> values) {
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
        final var first = values.get(0);
        return first.source().substring(first.start(), values.get(values.size() - 1).end());
    }

    private static String defaultExpression(List<Token> values) {
        // History is read without the original database session. Remove only the
        // comments already identified by this configured lexer, so later default
        // conversion cannot reinterpret a nested comment as SQL or hide an operator.
        // Preserve token spelling and adjacency (e.g. .5), and all string contents.
        if (values.isEmpty()) {
            throw new IllegalArgumentException("Empty SQL expression");
        }
        final var result = new StringBuilder();
        int end = values.get(0).start();
        for (var token : values) {
            final var gap = token.source().substring(end, token.start());
            result.append(gap.isBlank() ? gap : " ").append(token.text());
            end = token.end();
        }
        return result.toString();
    }

    private final class Tokens {
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
            return finished() || values.get(index).kind() != Kind.WORD ? "" : values.get(index).text().toUpperCase(Locale.ROOT);
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
            if (!finished()) {
                return values.get(index).error(message);
            }
            final var last = values.isEmpty() ? null : values.get(values.size() - 1);
            return DamengSqlLexer.error(last == null ? "" : last.source(), last == null ? 0 : last.end(), message);
        }

        private String identifier() {
            if (finished()) {
                throw error("Expected identifier");
            }
            return identifierRules.identifier(values.get(index++));
        }

        private TableId tableId(String defaultSchema) {
            final var first = identifier();
            return take(".") ? new TableId(null, first, identifier()) : new TableId(null, identifierRules.canonicalName(defaultSchema), first);
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

        private String string() {
            if (finished()) {
                throw error("Expected SQL string literal");
            }
            return values.get(index++).stringValue();
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
        return DamengSqlLexer.split(tokens, delimiter);
    }

}
