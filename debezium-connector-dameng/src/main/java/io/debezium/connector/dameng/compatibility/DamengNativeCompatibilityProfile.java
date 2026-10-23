/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng.compatibility;

import java.nio.charset.Charset;
import java.util.Set;

import io.debezium.connector.dameng.DamengDdlParser;

/** Existing native-mode rules, kept together rather than branching on mode in the pipeline. */
final class DamengNativeCompatibilityProfile implements DamengCompatibilityProfile {
    @Override
    public String id() {
        return "native-v1";
    }

    @Override
    public Set<DamengCompatibilityMode> modes() {
        return Set.of(new DamengCompatibilityMode("0"));
    }

    @Override
    public Set<String> parameterNames() {
        return Set.of("LENGTH_IN_CHAR", "NLS_LENGTH_SEMANTICS", "NVARCHAR_LENGTH_IN_CHAR", "BACKSLASH_ESCAPE", "NESTED_C_STYLE_COMMENT", "IDENTIFIER_MODE");
    }

    @Override
    public void validate(DamengCompatibilityEnvironment environment) {
        if (!"1".equals(environment.parameter("CASE_SENSITIVE")) && !"0".equals(environment.parameter("CASE_SENSITIVE"))) {
            throw new IllegalStateException("Dameng native capture requires CASE_SENSITIVE=0 or 1 identifier semantics; actual value was "
                    + environment.parameter("CASE_SENSITIVE"));
        }
        // The tested 2022 DM build predates IDENTIFIER_MODE. Absence is retained
        // in the offset manifest, distinct from a later server's explicit zero.
        final var identifierMode = environment.parameter("IDENTIFIER_MODE");
        if (identifierMode != null && !"0".equals(identifierMode) && !"1".equals(identifierMode)) {
            throw new IllegalStateException("Dameng native capture requires IDENTIFIER_MODE=0 or 1 when available; actual value was " + identifierMode);
        }
        if (!"0".equals(environment.parameter("LENGTH_IN_CHAR"))) {
            // Redo DDL does not identify the original session's default length semantics.
            throw new IllegalStateException("Dameng compatibility profile " + id()
                    + " requires LENGTH_IN_CHAR=0 for verified native byte length semantics; actual value was "
                    + environment.parameter("LENGTH_IN_CHAR"));
        }
        // A current parameter value cannot reconstruct historical lexical settings.
        // Keep these settings unchanged throughout the retained archive interval.
        for (var name : Set.of("BACKSLASH_ESCAPE", "NESTED_C_STYLE_COMMENT")) {
            if (!"0".equals(environment.parameter(name)) && !"1".equals(environment.parameter(name))) {
                throw new IllegalStateException("Dameng compatibility profile " + id() + " requires " + name
                        + "=0 or 1 for verified DDL lexical semantics; actual value was " + environment.parameter(name));
            }
        }
    }

    @Override
    public DamengDdlParser createDdlParser(DamengCompatibilityEnvironment environment, Charset databaseCharset) {
        validate(environment);
        // On the tested native DM build, BACKSLASH_ESCAPE=1 leaves ordinary,
        // national and Q-quoted literals unchanged (including JDBC defaults and
        // LogMiner output). Do not apply MySQL/PostgreSQL escape rules here.
        return new DamengDdlParser(databaseCharset, false,
                "CHAR".equalsIgnoreCase(environment.parameter("NLS_LENGTH_SEMANTICS")),
                "1".equals(environment.parameter("NVARCHAR_LENGTH_IN_CHAR")),
                "1".equals(environment.parameter("NESTED_C_STYLE_COMMENT")),
                // Case-insensitive databases preserve spelling inconsistently (keywords can
                // be uppercased). Use the same canonical names as JDBC metadata ingestion.
                new DamengIdentifierRules("0".equals(environment.parameter("CASE_SENSITIVE"))
                        || "1".equals(environment.parameter("IDENTIFIER_MODE"))));
    }
}
