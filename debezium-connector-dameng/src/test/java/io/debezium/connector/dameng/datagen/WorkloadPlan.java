/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng.datagen;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

final class WorkloadPlan {
    enum Operation {
        INSERT,
        UPDATE,
        DELETE
    }

    static List<Integer> cycle(Config config, Random random) {
        List<Integer> sizes = new ArrayList<>(Collections.nCopies(9, config.smallSize));
        sizes.add(config.largeSize);
        Collections.shuffle(sizes, random);
        return sizes;
    }

    static List<Operation> transaction(int size, Random random) {
        if (size <= 0 || size % 100 != 0) {
            throw new IllegalArgumentException("事务操作数必须为正的 100 的倍数");
        }
        List<Operation> operations = new ArrayList<>(size);
        operations.addAll(Collections.nCopies(inserts(size), Operation.INSERT));
        operations.addAll(Collections.nCopies(updates(size), Operation.UPDATE));
        operations.addAll(Collections.nCopies(deletes(size), Operation.DELETE));
        Collections.shuffle(operations, random);
        return operations;
    }

    static int inserts(int size) {
        return size / 100 * 7;
    }

    static int updates(int size) {
        return size / 100 * 90;
    }

    static int deletes(int size) {
        return size / 100 * 3;
    }

    private WorkloadPlan() {
    }
}
