/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.ddl.parser.dameng;

import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.Lexer;

/** Stateful comment and quoted-value handling shared by the generated Dameng lexer. */
public abstract class DamengSqlLexerBase extends Lexer {
    private boolean nestedComments;
    private int commentDepth;
    private int quoteDelimiter;

    public DamengSqlLexerBase(CharStream input) {
        super(input);
    }

    public void setNestedComments(boolean nestedComments) {
        this.nestedComments = nestedComments;
    }

    protected boolean isNestedComments() {
        return nestedComments;
    }

    protected void startComment() {
        if (_input.LA(1) == '!') {
            fail("Executable SQL comments are not supported by the native DDL profile");
        }
        commentDepth = 1;
    }

    protected void openNestedComment() {
        commentDepth++;
    }

    protected void closeComment() {
        if (--commentDepth == 0) {
            popMode();
            skip();
        }
        else {
            more();
        }
    }

    protected void startAlternativeQuote() {
        final int opening = _input.LA(1);
        if (opening == EOF) {
            fail("Unclosed SQL alternative quoted value");
        }
        if (Character.isWhitespace(opening) || opening == '\'' || opening > Character.MAX_VALUE) {
            fail("Invalid SQL alternative quote delimiter");
        }
    }

    protected void readAlternativeQuoteDelimiter() {
        quoteDelimiter = switch (_input.LA(-1)) {
            case '[' -> ']';
            case '(' -> ')';
            case '{' -> '}';
            case '<' -> '>';
            default -> _input.LA(-1);
        };
    }

    protected boolean isAlternativeQuoteEnd() {
        return _input.LA(1) == quoteDelimiter && _input.LA(2) == '\'';
    }

    protected boolean isBmpLetter() {
        return _input.LA(1) <= Character.MAX_VALUE && Character.isLetter(_input.LA(1));
    }

    protected boolean isBmpDigit() {
        return _input.LA(1) <= Character.MAX_VALUE && Character.isDigit(_input.LA(1));
    }

    protected boolean isWhitespace() {
        return Character.isWhitespace(_input.LA(1));
    }

    protected void fail(String message) {
        getErrorListenerDispatch().syntaxError(this, null, _tokenStartLine, _tokenStartCharPositionInLine, message, null);
        throw new IllegalArgumentException(message);
    }
}
