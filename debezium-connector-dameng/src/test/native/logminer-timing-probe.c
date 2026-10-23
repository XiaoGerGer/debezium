/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
/* Diagnostic LD_PRELOAD interposer. Forwards every call unchanged to the vendor library.
 * No vendor struct layout is assumed. String timings sample 1/64 calls by default.
 * Not part of the Connector build or runtime. Requires Linux, libdl and a JDK's JNI headers.
 */
#define _GNU_SOURCE
#include <dlfcn.h>
#include <jni.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>

static __thread struct {
    uint64_t batches, rows, core_wall, core_cpu, strings, nonnull, samples, string_wall;
    uint64_t seed;
    int in_core;
    uint64_t queue_sleeps, queue_sleep_wall, trx_sleeps, trx_sleep_wall;
    uintptr_t addresses[32];
    uint64_t site_samples[32], site_wall[32];
} stats;

static uint64_t nanos(clockid_t clock_id) {
    struct timespec ts;
    clock_gettime(clock_id, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + ts.tv_nsec;
}

static void *symbol(const char *name) {
    void *lib = dlopen("libdmlogmnr_client.so", RTLD_NOW | RTLD_NOLOAD);
    void *fn = lib ? dlsym(lib, name) : NULL;
    if (!fn) {
        fprintf(stderr, "DM_NATIVE_PROBE missing original symbol %s: %s\n", name, dlerror());
        abort();
    }
    dlclose(lib);
    return fn;
}

int logmnr_client_get_data(void *conn, int requested, void ***records, int *actual) {
    static __thread int (*original)(void *, int, void ***, int *);
    if (!original) original = symbol("logmnr_client_get_data");
    uint64_t wall = nanos(CLOCK_MONOTONIC), cpu = nanos(CLOCK_THREAD_CPUTIME_ID);
    stats.in_core = 1;
    int result = original(conn, requested, records, actual);
    stats.in_core = 0;
    stats.core_cpu += nanos(CLOCK_THREAD_CPUTIME_ID) - cpu;
    stats.core_wall += nanos(CLOCK_MONOTONIC) - wall;
    stats.batches++;
    if (actual && *actual > 0) stats.rows += *actual;
    return result;
}

void os_thread_sleep_low(unsigned millis) {
    /* Resolve separately on each calling thread: no unsynchronized shared cache. */
    static __thread void (*original)(unsigned);
    if (!original) original = symbol("os_thread_sleep_low");
    int measure = stats.in_core;
    uint64_t begin = measure ? nanos(CLOCK_MONOTONIC) : 0;
    original(millis);
    if (measure) {
        uint64_t elapsed = nanos(CLOCK_MONOTONIC) - begin;
        if (millis == 1) {
            stats.queue_sleeps++;
            stats.queue_sleep_wall += elapsed;
        } else if (millis == 100) {
            stats.trx_sleeps++;
            stats.trx_sleep_wall += elapsed;
        }
    }
}

jstring WindowsTojstring(JNIEnv *env, const char *bytes) {
    static __thread jstring (*original)(JNIEnv *, const char *);
    static __thread int sample_mask = -1;
    if (!original) {
        original = symbol("WindowsTojstring");
        const char *setting = getenv("DM_PROBE_SAMPLE_MASK");
        sample_mask = setting ? atoi(setting) : 63;
    }
    stats.strings++;
    if (bytes) stats.nonnull++;
    if (!stats.seed) stats.seed = 0x9e3779b97f4a7c15ULL;
    stats.seed ^= stats.seed << 13;
    stats.seed ^= stats.seed >> 7;
    stats.seed ^= stats.seed << 17;
    int sampled = (stats.seed & (unsigned)sample_mask) == 0;
    int site = -1;
    if (sampled) {
        uintptr_t address = (uintptr_t)__builtin_return_address(0);
        for (int i = 0; i < 32; i++) {
            if (stats.addresses[i] == address || !stats.addresses[i]) {
                stats.addresses[i] = address;
                site = i;
                break;
            }
        }
    }
    uint64_t begin = sampled ? nanos(CLOCK_MONOTONIC) : 0;
    jstring result = original(env, bytes);
    if (sampled) {
        uint64_t elapsed = nanos(CLOCK_MONOTONIC) - begin;
        stats.samples++;
        stats.string_wall += elapsed;
        if (site >= 0) {
            stats.site_samples[site]++;
            stats.site_wall[site] += elapsed;
        }
    }
    return result;
}

int logmnr_client_end(void *conn, unsigned options) {
    static __thread int (*original)(void *, unsigned);
    if (!original) original = symbol("logmnr_client_end");
    uint64_t wall = nanos(CLOCK_MONOTONIC), cpu = nanos(CLOCK_THREAD_CPUTIME_ID);
    int result = original(conn, options);
    uint64_t end_cpu = nanos(CLOCK_THREAD_CPUTIME_ID) - cpu;
    uint64_t end_wall = nanos(CLOCK_MONOTONIC) - wall;
    if (stats.batches) {
        fprintf(stderr, "DM_NATIVE_PROBE batches=%llu rows=%llu core_wall_ms=%.3f core_cpu_ms=%.3f strings=%llu nonnull=%llu samples=%llu string_sample_ms=%.3f string_estimated_ms=%.3f end_wall_ms=%.3f end_cpu_ms=%.3f queue_sleeps=%llu queue_sleep_ms=%.3f trx_sleeps=%llu trx_sleep_ms=%.3f\n",
                (unsigned long long)stats.batches, (unsigned long long)stats.rows,
                stats.core_wall / 1e6, stats.core_cpu / 1e6,
                (unsigned long long)stats.strings, (unsigned long long)stats.nonnull,
                (unsigned long long)stats.samples, stats.string_wall / 1e6,
                stats.samples ? stats.string_wall / 1e6 * stats.strings / stats.samples : 0,
                end_wall / 1e6, end_cpu / 1e6,
                (unsigned long long)stats.queue_sleeps, stats.queue_sleep_wall / 1e6,
                (unsigned long long)stats.trx_sleeps, stats.trx_sleep_wall / 1e6);
        for (int i = 0; i < 32 && stats.addresses[i]; i++) {
            Dl_info info;
            dladdr((void *)stats.addresses[i], &info);
            fprintf(stderr, "DM_NATIVE_STRING_SITE offset=0x%lx samples=%llu sample_ms=%.3f\n",
                    (unsigned long)(stats.addresses[i] - (uintptr_t)info.dli_fbase),
                    (unsigned long long)stats.site_samples[i], stats.site_wall[i] / 1e6);
        }
        fflush(stderr);
        stats = (__typeof__(stats)){0};
    }
    return result;
}
