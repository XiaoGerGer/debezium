/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng.logminer.parser;

import io.debezium.DebeziumException;

/**
 * Indicates that a redo statement cannot be represented as a single row change.
 */
public class DmlParserException extends DebeziumException {
    public DmlParserException(String message) {
        super(message);
    }
}
