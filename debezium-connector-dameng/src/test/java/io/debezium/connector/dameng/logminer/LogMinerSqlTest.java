/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng.logminer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

import org.junit.jupiter.api.Test;

class LogMinerSqlTest {
    @Test
    void usesDamengOptionsAndInclusiveWindow() {
        String sql = LogMinerSql.start(10, 20, true);
        assertThat(sql).contains("STARTSCN => 10", "ENDSCN => 20", "OPTIONS => 2050");
        assertThat(LogMinerSql.DEFAULT_OPTIONS & LogMinerSql.NO_SQL_DELIMITER).isZero();
        assertThat(LogMinerSql.DEFAULT_OPTIONS & LogMinerSql.DICT_FROM_ONLINE_CATALOG).isZero();
    }

    @Test
    void readsEveryOperationForTransactionPinning() {
        String sql = LogMinerSql.contentsQuery(null, false);
        assertThat(sql).contains("SCN >= ? AND SCN <= ?");
        assertThat(sql).doesNotContain("OPERATION_CODE IN");
        assertThat(sql).contains("ORDER BY COMMIT_SCN, SCN, RBASQN, RBABLK, RBABYTE, SSN");
        assertThat(sql).contains("DATA_OBJ#");
    }

    @Test
    void quotesArchivePath() {
        assertThat(LogMinerSql.addLogFile("/tmp/a'b.log")).contains("/tmp/a''b.log");
        assertThat(LogMinerSql.removeLogFile("/tmp/a'b.log")).contains("/tmp/a''b.log");
        assertThat(LogMinerSql.end()).isEqualTo("BEGIN DBMS_LOGMNR.END_LOGMNR(); END;");
        assertThatNullPointerException().isThrownBy(() -> LogMinerSql.quoteLiteral(null));
    }

    @Test
    void readsUncommittedChangesSoOpenTransactionsPinTheReplayPosition() {
        assertThat(LogMinerSql.start(10, 20, false)).contains("OPTIONS => 2048");
        assertThat(LogMinerSql.contentsQuery(null, false)).doesNotContain("COMMIT_SCN > 0", "ROLL_BACK = 0");
    }
}
