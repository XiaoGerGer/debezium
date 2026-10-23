/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.Set;

/** Database version reported by JDBC, independently of the JDBC driver's version. */
public record DamengDatabaseVersion(String productName, String productVersion, int major, int minor) {

    // DM8 build 08134283904-20220804-166351-20005 reports this version through JDBC.
    static final Set<DamengDatabaseVersion> TESTED_VERSIONS = Set.of(new DamengDatabaseVersion("DM DBMS", "8.1.2.128", 8, 1));

    public static DamengDatabaseVersion read(DatabaseMetaData metadata) throws SQLException {
        return new DamengDatabaseVersion(metadata.getDatabaseProductName(), metadata.getDatabaseProductVersion(),
                metadata.getDatabaseMajorVersion(), metadata.getDatabaseMinorVersion());
    }

    public boolean isTested() {
        return TESTED_VERSIONS.contains(this);
    }
}
