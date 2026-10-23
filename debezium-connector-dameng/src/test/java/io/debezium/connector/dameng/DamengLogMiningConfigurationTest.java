/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.LoggerFactory;

import io.debezium.junit.logging.LogInterceptor;

import ch.qos.logback.classic.Logger;

class DamengLogMiningConfigurationTest {
    private final LogInterceptor logs = LogInterceptor.forClass(DamengConnection.class);
    private DamengConnection connection;

    @AfterEach
    void stopCapturingLogs() {
        logs.stop();
        ((Logger) LoggerFactory.getLogger(DamengConnection.class)).detachAppender(logs);
    }

    @BeforeEach
    void before() throws Exception {
        connection = mock(DamengConnection.class);
        doCallRealMethod().when(connection).validateLogMiningConfiguration();
        doCallRealMethod().when(connection).validateCompatibilityMode();
        doCallRealMethod().when(connection).getCompatibilityEnvironment();
        when(connection.isArchiveModeEnabled()).thenReturn(true);
        when(connection.getIniParameter("RLOG_APPEND_LOGIC")).thenReturn("2");
        when(connection.getIniParameter("RLOG_IGNORE_TABLE_SET")).thenReturn("1");
        when(connection.getIniParameter("RLOG_LLOG_COMPRESS")).thenReturn("0");
        when(connection.getIniParameter("COMPATIBLE_MODE")).thenReturn("0");
        when(connection.getIniParameter("LENGTH_IN_CHAR")).thenReturn("0");
        when(connection.getIniParameter("BACKSLASH_ESCAPE")).thenReturn("0");
        when(connection.getIniParameter("NESTED_C_STYLE_COMMENT")).thenReturn("0");
        when(connection.getCaseSensitiveFlag()).thenReturn("1");
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
    void shouldRejectUnverifiedOrUnknownLexicalParameters() throws Exception {
        for (var name : java.util.List.of("BACKSLASH_ESCAPE", "NESTED_C_STYLE_COMMENT")) {
            for (var value : java.util.Arrays.asList("2", "-1", "true", "", null)) {
                when(connection.getIniParameter(name)).thenReturn(value);
                assertThatThrownBy(connection::validateCompatibilityMode).hasMessageContaining(name + "=0", "DDL lexical semantics");
            }
            when(connection.getIniParameter(name)).thenReturn("0");
        }
    }

    @Test
    void shouldAcceptBothVerifiedValuesOfEachLexicalParameter() throws Exception {
        for (var backslash : java.util.List.of("0", "1")) {
            for (var nested : java.util.List.of("0", "1")) {
                when(connection.getIniParameter("BACKSLASH_ESCAPE")).thenReturn(backslash);
                when(connection.getIniParameter("NESTED_C_STYLE_COMMENT")).thenReturn(nested);
                assertThatCode(connection::validateLogMiningConfiguration).doesNotThrowAnyException();
            }
        }
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

    @Test
    void shouldWarnButAllowManuallyConfiguredTableLogging() throws Exception {
        when(connection.getIniParameter("RLOG_IGNORE_TABLE_SET")).thenReturn("0");

        assertThatCode(connection::validateLogMiningConfiguration).doesNotThrowAnyException();
        assertThat(logs.containsWarnMessage("RLOG_IGNORE_TABLE_SET=0")).isTrue();
        assertThat(logs.containsWarnMessage("any configured signal table, and any tables added later")).isTrue();
        assertThat(logs.containsWarnMessage("silent CDC data loss")).isTrue();
    }

    @Test
    void shouldNotWarnWhenAllTablesHaveLogicalLogging() throws Exception {
        assertThatCode(connection::validateLogMiningConfiguration).doesNotThrowAnyException();
        assertThat(logs.containsWarnMessage("RLOG_IGNORE_TABLE_SET=0")).isFalse();
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = "2")
    void shouldRejectUnknownTableLoggingMode(String ignoreTableSet) throws Exception {
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
    @ValueSource(strings = { "1", "2", "3", "4", "5", "6", "7", "42" })
    void shouldRejectNonNativeTypeSemantics(String compatibilityMode) throws Exception {
        when(connection.getIniParameter("COMPATIBLE_MODE")).thenReturn(compatibilityMode);

        assertThatThrownBy(connection::validateLogMiningConfiguration)
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("COMPATIBLE_MODE");
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = { "-1", "oracle", "0.0" })
    void shouldRejectUnreadableOrMalformedModeInsteadOfAssumingNativeSemantics(String compatibilityMode) throws Exception {
        when(connection.getIniParameter("COMPATIBLE_MODE")).thenReturn(compatibilityMode);
        assertThatThrownBy(connection::validateLogMiningConfiguration)
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("COMPATIBLE_MODE");
    }
}
