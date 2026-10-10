/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.sql.Statement;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import io.debezium.jdbc.JdbcConfiguration;

class DamengQueryTimeoutTest {
    @ParameterizedTest
    @CsvSource({ "0,0", "1,1", "999,1", "1000,1", "1001,2", "2147483647,2147484" })
    void shouldRoundPositiveTimeoutUpToJdbcSeconds(int milliseconds, int seconds) throws Exception {
        try (var connection = new DamengConnection(JdbcConfiguration.create().withQueryTimeoutMs(milliseconds).build())) {
            final var statement = mock(Statement.class);
            connection.initializeStatement(statement);
            verify(statement).setQueryTimeout(seconds);
        }
    }
}
