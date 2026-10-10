/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng.logminer;

import java.util.List;
import java.util.Objects;

import io.debezium.relational.TableId;

/** SQL used by the DM DBMS_LOGMNR package. Values are quoted before interpolation. */
public final class LogMinerSql {
    public static final int ADD_FILE = 3;
    public static final int COMMITTED_DATA_ONLY = 2;
    public static final int DICT_FROM_ONLINE_CATALOG = 16;
    public static final int NO_SQL_DELIMITER = 64;
    public static final int NO_ROWID_IN_STMT = 2048;
    // Older DM 8 builds remove the last DDL character unconditionally when
    // NO_SQL_DELIMITER is set, even when the original DDL has no delimiter.
    // Online dictionary mode applies the current column layout to historical redo,
    // corrupting rows after a rename, add, or drop. Resolve OBJ#/COL n using schema history.
    public static final int DEFAULT_OPTIONS = NO_ROWID_IN_STMT;

    private LogMinerSql() {
    }

    public static String addLogFile(String filename) {
        return "BEGIN DBMS_LOGMNR.ADD_LOGFILE(LOGFILENAME => '" + quoteLiteral(filename) + "', OPTIONS => DBMS_LOGMNR.ADDFILE); END;";
    }

    public static String removeLogFile(String filename) {
        return "BEGIN DBMS_LOGMNR.REMOVE_LOGFILE(LOGFILENAME => '" + quoteLiteral(filename) + "'); END;";
    }

    public static String start(long startLsn, long endLsn, boolean committedOnly) {
        int options = DEFAULT_OPTIONS | (committedOnly ? COMMITTED_DATA_ONLY : 0);
        return "BEGIN DBMS_LOGMNR.START_LOGMNR(STARTSCN => " + startLsn + ", ENDSCN => " + endLsn + ", OPTIONS => " + options + "); END;";
    }

    public static String end() {
        return "BEGIN DBMS_LOGMNR.END_LOGMNR(); END;";
    }

    public static String contentsQuery(List<TableId> tables, boolean committedOnly) {
        // Large transactions can be replayed across many windows. Fetch only the
        // consumed fields, avoiding repeated JDBC transfer of unused undo SQL and values.
        StringBuilder sql = new StringBuilder(
                "SELECT SCN, START_SCN, COMMIT_SCN, TIMESTAMP, XID, OPERATION_CODE, ROLL_BACK, SEG_OWNER, TABLE_NAME, USERNAME, SQL_REDO, RS_ID, SSN, CSF, DATA_OBJ# FROM V$LOGMNR_CONTENTS WHERE SCN >= ? AND SCN <= ?");
        // Keep every operation and every table in the window. A transaction can start in an
        // earlier window and commit in this one; filtering DML here would make pinning unsafe.
        sql.append(" ORDER BY COMMIT_SCN, SCN, RBASQN, RBABLK, RBABYTE, SSN");
        return sql.toString();
    }

    public static String quoteLiteral(String value) {
        Objects.requireNonNull(value, "value");
        return value.replace("'", "''");
    }
}
