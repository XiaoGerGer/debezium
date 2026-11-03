/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng.logminer.events;

/**
 * Operation codes documented for {@code V$LOGMNR_CONTENTS} in the DM8 system dynamic views manual.
 * An event type being recognized does not imply that the connector supports processing it.
 *
 * @see <a href="https://eco.dameng.com/document/dm/zh-cn/pm/dm8-admin-manual-appendix2.html">DM8 system dynamic views</a>
 */
public enum EventType {
    INSERT(1),
    DELETE(2),
    UPDATE(3),
    BATCH_UPDATE(4),
    DDL(5),
    START(6),
    COMMIT(7),
    LOB_NEW(13),
    LOB_DATA(14),
    PRE_COMMIT(15),
    ROLLBACK(36),
    /** Named {@code SEQ MODIFY} in the manual. */
    SEQUENCE_MODIFY(37),
    XA_COMMIT(38),
    UNSUPPORTED(255);

    private static final EventType[] TYPES = new EventType[256];

    static {
        for (EventType type : values()) {
            TYPES[type.getValue()] = type;
        }
    }

    private final int value;

    EventType(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }

    public boolean isDataChange() {
        return this == INSERT || this == DELETE || this == UPDATE || this == BATCH_UPDATE;
    }

    /** Unknown operation codes map to {@link #UNSUPPORTED}; the row retains the original code. */
    public static EventType from(int value) {
        return value >= 0 && value < TYPES.length && TYPES[value] != null ? TYPES[value] : UNSUPPORTED;
    }
}
