/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng.datagen;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

/** Tracks live rows, so UPDATE/DELETE never intentionally target already deleted keys. */
final class KeyPool {
    private final List<Long> keys = new ArrayList<>();
    private final Map<Long, Integer> positions = new HashMap<>();

    void add(long key) {
        if (positions.putIfAbsent(key, keys.size()) != null) {
            throw new IllegalStateException("重复主键序号: " + key);
        }
        keys.add(key);
    }

    void remove(long key) {
        Integer position = positions.remove(key);
        if (position == null) {
            throw new IllegalStateException("找不到待删除主键: " + key);
        }
        long last = keys.remove(keys.size() - 1);
        if (position < keys.size()) {
            keys.set(position, last);
            positions.put(last, position);
        }
    }

    long random(Random random) {
        return keys.get(random.nextInt(keys.size()));
    }

    int size() {
        return keys.size();
    }
}
