/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng.compatibility;

import java.nio.charset.Charset;
import java.util.Set;

import io.debezium.connector.dameng.DamengDdlParser;

/**
 * A registered implementation of database semantics. Sharing an implementation
 * across mode identifiers requires evidence that their relevant rules agree.
 */
public interface DamengCompatibilityProfile {
    String id();

    Set<DamengCompatibilityMode> modes();

    Set<String> parameterNames();

    void validate(DamengCompatibilityEnvironment environment);

    DamengDdlParser createDdlParser(DamengCompatibilityEnvironment environment, Charset databaseCharset);
}
