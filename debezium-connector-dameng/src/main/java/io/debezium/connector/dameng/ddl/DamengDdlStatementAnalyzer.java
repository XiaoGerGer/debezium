/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng.ddl;

import java.util.List;
import java.util.Locale;
import java.util.Set;

import io.debezium.connector.dameng.compatibility.DamengIdentifierRules;
import io.debezium.connector.dameng.ddl.DamengSqlLexer.Kind;
import io.debezium.connector.dameng.ddl.DamengSqlLexer.Token;
import io.debezium.relational.TableId;

/** Determines the scope of one archived statement before capture filtering or model updates. */
public final class DamengDdlStatementAnalyzer {
    private static final Set<String> NON_TABLE_OBJECTS = Set.of("INDEX", "SEQUENCE", "VIEW", "SYNONYM", "PROCEDURE", "FUNCTION", "TRIGGER");
    private final DamengSqlLexer lexer;
    private final DamengIdentifierRules identifiers;

    public DamengDdlStatementAnalyzer() {
        this(new DamengSqlLexer());
    }

    public DamengDdlStatementAnalyzer(DamengSqlLexer lexer) {
        this(lexer, new DamengIdentifierRules(false));
    }

    public DamengDdlStatementAnalyzer(DamengSqlLexer lexer, DamengIdentifierRules identifiers) {
        this.lexer = lexer;
        this.identifiers = identifiers;
    }

    public enum Impact {
        TABLE,
        NO_TABLE_CHANGE,
        UNSUPPORTED
    }

    public record Analysis(String sql, List<Token> tokens, Impact impact, TableId tableId, TableId renamedTableId,
            IllegalArgumentException failure) {
        public Analysis requireSupported() {
            if (failure != null) {
                throw failure;
            }
            return this;
        }
    }

    public Analysis analyze(String sql, String defaultSchema) {
        return analyzeTokens(sql, lexer.tokenize(sql), defaultSchema);
    }

    public Analysis analyzeTokens(String sql, List<Token> tokens, String defaultSchema) {
        try {
            if (tokens.isEmpty()) {
                throw DamengSqlLexer.error(sql, sql.length(), "Unsupported table DDL: empty statement");
            }
            // Stored program definitions contain real semicolons. Only a balanced body
            // ending at the end of this record is harmless; subsequent SQL is rejected.
            final var program = new Cursor(tokens);
            if (program.take("CREATE")) {
                replacement(program);
                if (program.take("PROCEDURE") || program.take("FUNCTION") || program.take("TRIGGER")) {
                    program.identifier();
                    validateProgram(program);
                    return new Analysis(sql, tokens, Impact.NO_TABLE_CHANGE, null, null, null);
                }
            }
            final var statements = DamengSqlLexer.split(tokens, ";").stream().filter(value -> !value.isEmpty()).toList();
            TableId target = null;
            TableId renamed = null;
            List<Token> structural = null;
            for (var statement : statements) {
                final var cursor = new Cursor(statement);
                if (cursor.peek("GRANT") || cursor.peek("REVOKE")) {
                    validatePrivileges(cursor);
                }
                else if (cursor.take("COMMENT")) {
                    validateComment(cursor);
                }
                else {
                    final boolean create = cursor.take("CREATE");
                    if (create) {
                        replacement(cursor);
                        cursor.take("UNIQUE");
                        cursor.take("GLOBAL");
                        if (!cursor.take("TEMPORARY") && !cursor.take("HUGE")) {
                            cursor.take("EXTERNAL");
                        }
                    }
                    final boolean rename = !create && cursor.take("RENAME");
                    if (!create && !rename && !cursor.take("ALTER") && !cursor.take("DROP") && !cursor.take("TRUNCATE")) {
                        throw cursor.error("Unsupported table DDL: unknown statement family");
                    }
                    if (rename || cursor.take("TABLE")) {
                        if (rename) {
                            cursor.take("TABLE");
                        }
                        target = cursor.tableId(defaultSchema);
                        rejectAdditionalStatements(statement, cursor.index, true);
                        if (rename || (cursor.take("RENAME") && cursor.take("TO"))) {
                            if (rename) {
                                cursor.expect("TO");
                            }
                            renamed = cursor.tableId(target.schema());
                            cursor.end();
                        }
                        structural = statement;
                    }
                    else {
                        final var object = cursor.next();
                        if (object.kind() != Kind.WORD || !NON_TABLE_OBJECTS.contains(object.text().toUpperCase(Locale.ROOT))) {
                            throw object.error("Unsupported table DDL: object or compound scope " + object.text());
                        }
                        cursor.tableId(defaultSchema);
                        rejectAdditionalStatements(statement, cursor.index, false);
                    }
                }
            }
            if (structural != null && statements.size() != 1) {
                throw structural.get(0).error("Unsupported table DDL: one LogMiner record must contain exactly one table statement");
            }
            if (statements.isEmpty()) {
                throw tokens.get(0).error("Unsupported table DDL: empty statement");
            }
            return new Analysis(sql, structural == null ? tokens : structural,
                    structural == null ? Impact.NO_TABLE_CHANGE : Impact.TABLE, target, renamed, null);
        }
        catch (IllegalArgumentException failure) {
            return new Analysis(sql, tokens, Impact.UNSUPPORTED, null, null, failure);
        }
    }

    private static void replacement(Cursor cursor) {
        if (cursor.take("OR")) {
            cursor.expect("REPLACE");
        }
    }

    private static void rejectAdditionalStatements(List<Token> tokens, int start, boolean tableClause) {
        for (int index = start; index < tokens.size(); index++) {
            final var token = tokens.get(index);
            if (token.keyword("CREATE") || (token.keyword("ALTER") && !(tableClause && index == start && index + 1 < tokens.size()
                    && !tokens.get(index + 1).keyword("TABLE"))) || token.keyword("GRANT") || token.keyword("REVOKE")
                    || token.keyword("TRUNCATE") || (token.keyword("COMMENT") && !(tableClause && index + 1 < tokens.size()
                            && tokens.get(index + 1).kind() == Kind.STRING))
                    || ((token.keyword("DROP") || token.keyword("RENAME")) && index + 1 < tokens.size()
                            && (tokens.get(index + 1).keyword("TABLE") || tokens.get(index + 1).keyword("SCHEMA") || tokens.get(index + 1).keyword("USER")))) {
                throw token.error("Unsupported table DDL: additional statement inside one SQL statement");
            }
        }
    }

    private static void validatePrivileges(Cursor cursor) {
        final boolean grant = cursor.take("GRANT");
        if (!grant) {
            cursor.expect("REVOKE");
        }
        final int start = cursor.index;
        while (!cursor.finished() && !cursor.peek(grant ? "TO" : "FROM")) {
            cursor.next();
        }
        if (cursor.index == start) {
            throw cursor.error("Expected privilege or role");
        }
        cursor.expect(grant ? "TO" : "FROM");
        do {
            cursor.identifier();
        } while (cursor.take(","));
        if (grant && cursor.take("WITH")) {
            if (!cursor.take("ADMIN")) {
                cursor.expect("GRANT");
            }
            cursor.expect("OPTION");
        }
        cursor.end();
    }

    private static void validateComment(Cursor cursor) {
        cursor.expect("ON");
        final boolean column = cursor.take("COLUMN");
        if (!column) {
            cursor.expect("TABLE");
        }
        cursor.identifier();
        int parts = 1;
        while (cursor.take(".")) {
            cursor.identifier();
            parts++;
        }
        if (parts > (column ? 3 : 2) || (column && parts < 2)) {
            throw cursor.error("Invalid COMMENT ON target");
        }
        cursor.expect("IS");
        if (!cursor.take("NULL") && cursor.next().kind() != Kind.STRING) {
            throw cursor.error("Expected COMMENT ON string or NULL");
        }
        cursor.end();
    }

    private static void validateProgram(Cursor cursor) {
        while (!cursor.finished() && !cursor.peek("BEGIN")) {
            if (cursor.peek(";")) {
                throw cursor.error("Unsupported stored program declaration before BEGIN");
            }
            cursor.next();
        }
        cursor.expect("BEGIN");
        int depth = 1;
        while (!cursor.finished()) {
            if (cursor.take("BEGIN") || cursor.take("CASE")) {
                depth++;
            }
            else if (cursor.take("END")) {
                if (cursor.take("IF") || cursor.take("LOOP")) {
                    continue;
                }
                cursor.take("CASE");
                if (--depth == 0) {
                    if (!cursor.finished() && !cursor.peek(";")) {
                        cursor.identifier();
                    }
                    cursor.take(";");
                    cursor.end();
                    return;
                }
            }
            else {
                cursor.next();
            }
        }
        throw cursor.error("Unclosed stored program body");
    }

    private final class Cursor {
        private final List<Token> tokens;
        private int index;

        private Cursor(List<Token> tokens) {
            this.tokens = tokens;
        }

        private boolean finished() {
            return index == tokens.size();
        }

        private boolean peek(String value) {
            return !finished() && tokens.get(index).keyword(value);
        }

        private boolean take(String value) {
            if (!peek(value)) {
                return false;
            }
            index++;
            return true;
        }

        private void expect(String value) {
            if (!take(value)) {
                throw error("Expected " + value);
            }
        }

        private Token next() {
            if (finished()) {
                throw error("Unexpected end of DDL");
            }
            return tokens.get(index++);
        }

        private String identifier() {
            return identifiers.identifier(next());
        }

        private TableId tableId(String schema) {
            final var first = identifier();
            return take(".") ? new TableId(null, first, identifier()) : new TableId(null, identifiers.canonicalName(schema), first);
        }

        private void end() {
            if (!finished()) {
                throw error("Unsupported trailing DDL syntax");
            }
        }

        private IllegalArgumentException error(String message) {
            if (!finished()) {
                return tokens.get(index).error(message);
            }
            final var last = tokens.get(tokens.size() - 1);
            return DamengSqlLexer.error(last.source(), last.end(), message);
        }
    }
}
