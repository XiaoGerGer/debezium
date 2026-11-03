/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng.antlr;

import java.util.ArrayList;
import java.util.List;

import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CommonToken;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ListTokenSource;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;

import io.debezium.connector.dameng.compatibility.DamengIdentifierRules;
import io.debezium.ddl.parser.dameng.generated.DamengSqlParser;

/** Parses DDL once and retains original SQL and UTF-16 locations on its parse-tree tokens. */
public final class DamengParseTree {
    private DamengParseTree() {
    }

    public record Parsed<T extends ParserRuleContext>(String sql, List<DamengSqlToken> tokens, T tree) {
    }

    public static Parsed<DamengSqlParser.DdlScopeContext> parseScope(String sql, boolean nestedComments) {
        return parseScope(sql, DamengAntlrSqlLexer.lex(sql, nestedComments));
    }

    public static Parsed<DamengSqlParser.DdlScriptContext> parseScript(String sql, boolean nestedComments) {
        final var lexed = DamengAntlrSqlLexer.lex(sql, nestedComments);
        return new Parsed<>(sql, lexed.tokens(), parser(sql, lexed).ddlScript());
    }

    static Parsed<DamengSqlParser.DdlScopeContext> parseScope(String sql, DamengAntlrSqlLexer.Lexed lexed) {
        return new Parsed<>(sql, lexed.tokens(), parser(sql, lexed).ddlScope());
    }

    static Parsed<DamengSqlParser.DdlScopeContext> parseTokens(String sql, List<DamengSqlToken> tokens) {
        final List<CommonToken> adapted = new ArrayList<>(tokens.size() + 1);
        for (var token : tokens) {
            final int type = switch (token.kind()) {
                case WORD -> DamengAntlrSqlLexer.wordType(token.text());
                case SYMBOL -> DamengAntlrSqlLexer.symbolType(token.text());
                case IDENTIFIER -> io.debezium.ddl.parser.dameng.generated.DamengSqlLexer.IDENTIFIER;
                case STRING -> io.debezium.ddl.parser.dameng.generated.DamengSqlLexer.STRING;
                case BINARY -> io.debezium.ddl.parser.dameng.generated.DamengSqlLexer.HEX_BINARY_LITERAL;
                case NUMBER -> io.debezium.ddl.parser.dameng.generated.DamengSqlLexer.NUMBER;
            };
            final var adaptedToken = new CommonToken(type, token.text());
            adaptedToken.setStartIndex(token.start());
            adaptedToken.setStopIndex(token.end() - 1);
            adapted.add(adaptedToken);
        }
        final var eof = new CommonToken(org.antlr.v4.runtime.Token.EOF, "<EOF>");
        final int end = tokens.isEmpty() ? sql.length() : tokens.get(tokens.size() - 1).end();
        eof.setStartIndex(end);
        eof.setStopIndex(end - 1);
        adapted.add(eof);
        return parseScope(sql, new DamengAntlrSqlLexer.Lexed(tokens, adapted));
    }

    public static String source(ParserRuleContext context) {
        final var input = input(context);
        return input.sql().substring(context.getStart().getStartIndex(), context.getStop().getStopIndex() + 1);
    }

    public static IllegalArgumentException error(ParserRuleContext context, String message) {
        return DamengSqlToken.error(input(context).sql(), context.getStart().getStartIndex(), message);
    }

    public static List<DamengSqlToken> tokens(ParserRuleContext context) {
        final var input = input(context);
        return input.tokens().subList(lowerBound(input.tokens(), context.getStart().getStartIndex()),
                lowerBound(input.tokens(), context.getStop().getStopIndex() + 1));
    }

    public static String identifier(ParserRuleContext context, DamengIdentifierRules identifiers) {
        return identifiers.identifier(tokens(context).get(0));
    }

    public static String defaultExpression(ParserRuleContext context) {
        final var values = tokens(context);
        if (values.isEmpty()) {
            return "";
        }
        final var sql = input(context).sql();
        final var result = new StringBuilder();
        int previousEnd = values.get(0).start();
        for (var token : values) {
            final var gap = sql.substring(previousEnd, token.start());
            result.append(gap.isBlank() ? gap : " ");
            result.append(token.text());
            previousEnd = token.end();
        }
        return result.toString();
    }

    private static DamengSqlParser parser(String sql, DamengAntlrSqlLexer.Lexed lexed) {
        final var input = new Input(sql, lexed.tokens());
        final List<org.antlr.v4.runtime.Token> tokens = new ArrayList<>(lexed.parserTokens().size());
        for (var token : lexed.parserTokens()) {
            tokens.add(new SourceToken(token, input));
        }
        final var parser = new DamengSqlParser(new CommonTokenStream(new ListTokenSource(tokens)));
        parser.removeErrorListeners();
        parser.addErrorListener(new BaseErrorListener() {
            @Override
            public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol, int line, int charPositionInLine, String message,
                                    RecognitionException exception) {
                final var token = (org.antlr.v4.runtime.Token) offendingSymbol;
                final var rules = parser.getRuleInvocationStack();
                final var operation = rules.contains("alterColumnClause") ? "Unsupported ALTER COLUMN: "
                        : rules.contains("alterTable") ? "Unsupported ALTER TABLE: " : "";
                throw DamengSqlToken.error(sql, token.getStartIndex(), "Unsupported table DDL or additional statement: " + operation + message);
            }
        });
        return parser;
    }

    private static Input input(ParserRuleContext context) {
        if (context.getStart() instanceof SourceToken token) {
            return token.input;
        }
        throw new IllegalArgumentException("DDL context was not created by the Dameng ANTLR parser");
    }

    private static int lowerBound(List<DamengSqlToken> tokens, int offset) {
        int low = 0;
        int high = tokens.size();
        while (low < high) {
            final int middle = (low + high) >>> 1;
            if (tokens.get(middle).start() < offset) {
                low = middle + 1;
            }
            else {
                high = middle;
            }
        }
        return low;
    }

    private record Input(String sql, List<DamengSqlToken> tokens) {
    }

    private static final class SourceToken extends CommonToken {
        private final Input input;

        private SourceToken(CommonToken token, Input input) {
            super(token);
            this.input = input;
        }
    }
}
