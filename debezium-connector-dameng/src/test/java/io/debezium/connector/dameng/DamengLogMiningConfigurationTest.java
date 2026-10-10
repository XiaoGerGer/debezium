/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

class DamengLogMiningConfigurationTest {
    private DamengConnection connection;

    @BeforeEach
    void before() throws Exception {
        connection = mock(DamengConnection.class);
        doCallRealMethod().when(connection).validateLogMiningConfiguration();
        doCallRealMethod().when(connection).validateCompatibilityMode();
        when(connection.isArchiveModeEnabled()).thenReturn(true);
        when(connection.getIniParameter("RLOG_APPEND_LOGIC")).thenReturn("2");
        when(connection.getIniParameter("RLOG_IGNORE_TABLE_SET")).thenReturn("1");
        when(connection.getIniParameter("RLOG_LLOG_COMPRESS")).thenReturn("0");
        when(connection.getIniParameter("COMPATIBLE_MODE")).thenReturn("0");
        when(connection.getIniParameter("LENGTH_IN_CHAR")).thenReturn("0");
    }

    @ParameterizedTest
    @NullSource
    @ValueSource(strings = "0")
    void shouldAcceptAnOlderVersionWithoutLogicalLogCompressionParameter(String compression) throws Exception {
        // The DM8 build 20220804 image does not expose RLOG_LLOG_COMPRESS.
        when(connection.getIniParameter("RLOG_LLOG_COMPRESS")).thenReturn(compression);

        assertThatCode(connection::validateLogMiningConfiguration).doesNotThrowAnyException();
    }

    @Test
    void shouldRejectDisabledArchiveMode() throws Exception {
        when(connection.isArchiveModeEnabled()).thenReturn(false);

        assertThatThrownBy(connection::validateLogMiningConfiguration)
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("ARCH_MODE");
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = { "0", "1", "3" })
    void shouldRejectIncompleteLogicalBeforeImages(String appendLogic) throws Exception {
        when(connection.getIniParameter("RLOG_APPEND_LOGIC")).thenReturn(appendLogic);

        assertThatThrownBy(connection::validateLogMiningConfiguration)
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("RLOG_APPEND_LOGIC");
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = { "0", "2" })
    void shouldRejectLogicalLoggingThatCanBeDisabledPerTable(String ignoreTableSet) throws Exception {
        when(connection.getIniParameter("RLOG_IGNORE_TABLE_SET")).thenReturn(ignoreTableSet);

        assertThatThrownBy(connection::validateLogMiningConfiguration)
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("RLOG_IGNORE_TABLE_SET");
    }

    @ParameterizedTest
    @ValueSource(strings = { "", "1", "2" })
    void shouldRejectEnabledOrUnknownLogicalLogCompression(String compression) throws Exception {
        when(connection.getIniParameter("RLOG_LLOG_COMPRESS")).thenReturn(compression);

        assertThatThrownBy(connection::validateLogMiningConfiguration)
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("RLOG_LLOG_COMPRESS");
    }

    @ParameterizedTest
    @ValueSource(strings = { "", "1", "2", "3", "4", "5", "6", "7" })
    void shouldRejectNonNativeTypeSemantics(String compatibilityMode) throws Exception {
        when(connection.getIniParameter("COMPATIBLE_MODE")).thenReturn(compatibilityMode);

        assertThatThrownBy(connection::validateLogMiningConfiguration)
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("COMPATIBLE_MODE");
    }
}
