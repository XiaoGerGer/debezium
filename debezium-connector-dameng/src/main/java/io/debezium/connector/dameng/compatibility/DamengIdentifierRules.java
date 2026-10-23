/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng.compatibility;

import java.util.Locale;

import io.debezium.connector.dameng.ddl.DamengSqlLexer.Kind;
import io.debezium.connector.dameng.ddl.DamengSqlLexer.Token;

/** Canonical identifier spelling shared by scope analysis and historical parsing. */
public record DamengIdentifierRules(boolean uppercaseQuoted) {
    public String canonicalName(String name) {
        return name == null || !uppercaseQuoted ? name : name.toUpperCase(Locale.ROOT);
    }

    public String identifier(Token token) {
        if (token.kind() == Kind.IDENTIFIER) {
            final var name = token.text().substring(1, token.text().length() - 1).replace("\"\"", "\"");
            return canonicalName(name);
        }
        if (token.kind() != Kind.WORD) {
            throw token.error("Expected identifier");
        }
        return token.text().toUpperCase(Locale.ROOT);
    }
}
