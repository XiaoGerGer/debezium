/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class DamengDatabaseVersionTest {
    @ParameterizedTest
    @CsvSource({
            "DM DBMS, 8.1.2.128, 8, 1, true",
            "DM DBMS, 8.1.2.129, 8, 1, false",
            "DM DBMS, 8.1.3.140, 8, 1, false",
            "DM DBMS, 8.4.0.0, 8, 4, false",
            "DM DBMS, 8.1, 8, 1, false",
            "DM DBMS, unknown, 8, 1, false",
            "DM DBMS, , 8, 1, false",
            "DM DBMS, '', 8, 1, false",
            "DM DBMS, 8.1.2.128, 9, 1, false",
            "DM DBMS, 8.1.2.128, 8, 2, false",
            "Other DBMS, 8.1.2.128, 8, 1, false",
            ", 8.1.2.128, 8, 1, false"
    })
    void shouldRecognizeOnlyExactlyTestedJdbcVersions(String productName, String productVersion, int major, int minor, boolean tested) {
        assertThat(new DamengDatabaseVersion(productName, productVersion, major, minor).isTested()).isEqualTo(tested);
    }
}
