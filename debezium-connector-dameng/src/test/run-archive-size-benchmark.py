#!/usr/bin/env python3
"""Explicit archive-size performance experiment. Not a regression-suite entry point."""

import argparse
import datetime
import json
import os
from pathlib import Path
import random
import shutil
import signal
import subprocess
import time
import uuid


MODULE = Path(__file__).resolve().parents[2]
ROOT = MODULE.parent
LABEL = "dameng.archive.benchmark.run"


def command(arguments, **kwargs):
    return subprocess.run(arguments, check=True, text=True, **kwargs)


def cleanup(run_id):
    selected = command(["docker", "ps", "-aq", "--filter", f"label={LABEL}={run_id}"], capture_output=True).stdout.split()
    if selected:
        command(["docker", "rm", "-fv", *selected], capture_output=True)
    remaining = command(["docker", "ps", "-aq", "--filter", f"label={LABEL}={run_id}"], capture_output=True).stdout.strip()
    if remaining:
        raise RuntimeError(f"Benchmark containers remain: {remaining}")
    return selected


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sizes", default="64,128,256,512,1024,2048")
    parser.add_argument("--operations", type=int, default=1_000_000)
    parser.add_argument("--rates", default="0", help="Committed DML/s; 0 generates without pacing")
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--timeout-seconds", type=int, default=7200)
    parser.add_argument("--min-free-gib", type=int, default=10)
    parser.add_argument("--max-growth-gib", type=int, default=45)
    parser.add_argument("--java-home", type=Path, default=Path("/opt/java/jdk-25.0.4+7"))
    parser.add_argument("--skip-build", action="store_true")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    sizes = [int(value) for value in args.sizes.split(",")]
    rates = [int(value) for value in args.rates.split(",")]
    if any(size not in (64, 128, 256, 512, 1024, 2048) for size in sizes):
        parser.error("Sizes must be selected from 64,128,256,512,1024,2048 MiB")
    if args.operations <= 0 or args.operations > 3_000_000 or args.operations % 100:
        parser.error("Operations must be a positive multiple of 100, at most 3000000")
    if args.repeats < 1 or any(rate < 0 for rate in rates):
        parser.error("Repeats must be positive and rates nonnegative")
    stamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    output = (args.output or MODULE / "target" / "archive-size-benchmark" / stamp).resolve()
    output.mkdir(parents=True, exist_ok=False)
    environment = dict(os.environ, JAVA_HOME=str(args.java_home))
    with (output / "build.log").open("w") as log:
        if not args.skip_build:
            command([str(ROOT / "mvnw"), "-B", "-ntp", "-pl", MODULE.name, "-am", "-Passembly",
                     "process-sources", "install", "-Dquick"], cwd=ROOT, env=environment, stdout=log, stderr=subprocess.STDOUT)
        command([str(ROOT / "mvnw"), "-B", "-ntp", "-pl", MODULE.name, "dependency:build-classpath",
                 "-Dmdep.includeScope=test", f"-Dmdep.outputFile={output / 'dependencies.classpath'}"],
                cwd=ROOT, env=environment, stdout=log, stderr=subprocess.STDOUT)
    classpath = os.pathsep.join([str(MODULE / "target" / "test-classes"), str(MODULE / "target" / "classes"),
                                (output / "dependencies.classpath").read_text().strip()])
    manifest = {"arguments": vars(args) | {"java_home": str(args.java_home), "output": str(output)},
                "gitHead": command(["git", "rev-parse", "HEAD"], cwd=ROOT, capture_output=True).stdout.strip(),
                "runs": []}
    for repeat in range(1, args.repeats + 1):
        order = list(sizes)
        random.Random(20261103 + repeat).shuffle(order)
        for rate in rates:
            for size in order:
                if shutil.disk_usage(output).free < args.min_free_gib * 1024 ** 3:
                    raise RuntimeError("Insufficient free space before starting the next run")
                run_id = uuid.uuid4().hex
                directory = output / f"r{repeat}-{size}mib-rate{rate}"
                directory.mkdir()
                arguments = [str(args.java_home / "bin" / "java"), "-Xmx1536m",
                             "-Ddameng.archive.benchmark.enabled=true", f"-Ddameng.archive.benchmark.run={run_id}",
                             f"-Ddameng.archive.benchmark.operations={args.operations}",
                             f"-Ddameng.archive.benchmark.target.rate={rate}",
                             f"-Ddameng.test.archive.file.size.mib={size}", "-Ddameng.test.archive.space.limit.mib=32768",
                             "-Ddameng.perf.cycles=100000", "-Ddameng.perf.interval.ms=0",
                             f"-Ddameng.perf.timeout.seconds={args.timeout_seconds}",
                             f"-Ddameng.perf.min.free.gib={args.min_free_gib}",
                             f"-Ddameng.perf.docker.min.free.gib={args.min_free_gib}",
                             f"-Ddameng.perf.max.growth.gib={args.max_growth_gib}",
                             "-Ddameng.perf.window.strategy=archive_bytes", "-Ddameng.perf.window.max.bytes=67108864",
                             "-Ddameng.perf.worker.heap.opts=-Xms512m -Xmx2g", "-Ddameng.perf.worker.memory.mib=3072",
                             "-Ddameng.perf.producer.batch.size=65536", f"-Ddameng.perf.directory={directory}",
                             "-cp", classpath, "io.debezium.connector.dameng.DamengArchiveSizeBenchmark"]
                (directory / "command.json").write_text(json.dumps(arguments, indent=2))
                started = time.time()
                print(f"START size={size}MiB rate={rate} repeat={repeat} operations={args.operations} output={directory}", flush=True)
                process = None
                entry = {"sizeMiB": size, "rate": rate, "repeat": repeat, "directory": str(directory), "runId": run_id}
                try:
                    with (directory / "console.log").open("w") as log:
                        process = subprocess.Popen(arguments, cwd=MODULE, env=environment, stdout=log,
                                                   stderr=subprocess.STDOUT, start_new_session=True)
                        entry["exitCode"] = process.wait(timeout=args.timeout_seconds + 900)
                finally:
                    if process is not None and process.poll() is None:
                        os.killpg(process.pid, signal.SIGTERM)
                        try:
                            process.wait(timeout=20)
                        except subprocess.TimeoutExpired:
                            os.killpg(process.pid, signal.SIGKILL)
                            process.wait()
                    entry["cleanupRemovedContainers"] = cleanup(run_id)
                    entry["elapsedSeconds"] = time.time() - started
                    entry["freeBytesAfterCleanup"] = shutil.disk_usage(output).free
                    manifest["runs"].append(entry)
                    (output / "manifest.json").write_text(json.dumps(manifest, indent=2, default=str))
                print(f"END size={size}MiB exit={entry['exitCode']} seconds={entry['elapsedSeconds']:.1f} freeGiB={entry['freeBytesAfterCleanup'] / 1024 ** 3:.1f}", flush=True)
                if entry["exitCode"]:
                    raise RuntimeError(f"Run failed; inspect {directory / 'console.log'}")
    print(f"COMPLETE {output}", flush=True)


if __name__ == "__main__":
    main()
