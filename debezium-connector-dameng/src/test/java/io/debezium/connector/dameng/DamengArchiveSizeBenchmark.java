/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.debezium.connector.dameng;

/** Explicit manual entry point, without JUnit annotations or automatic test discovery. */
public final class DamengArchiveSizeBenchmark {
    private DamengArchiveSizeBenchmark() {
    }

    public static void main(String[] args) throws Exception {
        if (!Boolean.getBoolean("dameng.archive.benchmark.enabled")) {
            throw new IllegalArgumentException("Set dameng.archive.benchmark.enabled=true to explicitly run this performance experiment");
        }
        new DamengDatagenPerformanceIT().runArchiveSizeBenchmark();
    }
}
