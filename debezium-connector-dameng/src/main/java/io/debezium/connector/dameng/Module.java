/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng;

import java.util.Properties;

import io.debezium.util.IoUtil;

public final class Module {
    private static final Properties INFO = IoUtil.loadProperties(Module.class, "io/debezium/connector/dameng/build.version");

    private Module() {
    }

    public static String version() {
        return INFO.getProperty("version", "${project.version}");
    }

    public static String name() {
        return "dameng";
    }

    public static String contextName() {
        return "Dameng";
    }
}
