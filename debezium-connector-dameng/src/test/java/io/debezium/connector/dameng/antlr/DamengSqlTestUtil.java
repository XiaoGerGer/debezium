/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng.antlr;

import java.util.ArrayList;
import java.util.List;

/** Token slicing for SQL corpus fixtures; production DDL boundaries are parsed by ANTLR. */
public final class DamengSqlTestUtil {
    private DamengSqlTestUtil() {
    }

    public static List<List<DamengSqlToken>> split(List<DamengSqlToken> tokens, String delimiter) {
        final List<List<DamengSqlToken>> result = new ArrayList<>();
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
}
