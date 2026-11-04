#!/usr/bin/env python3
"""Summarize completed manual archive-size experiments from preserved evidence."""

import argparse
import csv
import json
from pathlib import Path
import re
import statistics


def summarize(directory):
    command = json.loads((directory / "command.json").read_text())
    properties = dict(argument[2:].split("=", 1) for argument in command if argument.startswith("-D") and "=" in argument)
    operations = int(properties["dameng.archive.benchmark.operations"])
    if not (directory / "evidence.txt").exists():
        return {"directory": str(directory), "status": "incomplete"}
    evidence = (directory / "evidence.txt").read_text()
    if "ALL_EVENT_COUNTS_BEFORE_IMAGES_AND_FINAL_STATE_PASSED=true" not in evidence:
        return {"directory": str(directory), "status": "incomplete"}
    elapsed_ms = int(re.search(r"elapsedMs=(\d+) received=", evidence)[1])
    latency = json.loads((directory / "latency.json").read_text())
    with (directory / "jdbc-sessions.csv").open() as stream:
        sessions = [{key: int(value) for key, value in row.items()} for row in csv.DictReader(stream)]
    with (directory / "samples.csv").open() as stream:
        samples = list(csv.DictReader(stream))
    with (directory / "commits.csv").open() as stream:
        commits = [{key: int(value) for key, value in row.items()} for row in csv.DictReader(stream)]
    with (directory / "archives.csv").open() as stream:
        archives = list(csv.DictReader(stream))
    latest_archives = {row["path"]: row for row in archives}
    totals = {key: sum(session[key] for session in sessions) for key in
              ("rows", "add_ns", "start_ns", "query_ns", "next_ns", "read_decode_buffer_ns", "end_ns")}
    result = {"directory": str(directory), "status": "passed", "archiveMiB": int(properties["dameng.test.archive.file.size.mib"]),
              "targetRate": int(properties["dameng.archive.benchmark.target.rate"]), "operations": operations,
              "elapsedSeconds": elapsed_ms / 1000, "deliveredDmlPerSecond": operations * 1000 / elapsed_ms,
              "p50Ms": latency["overall"]["p50Ms"], "p95Ms": latency["overall"]["p95Ms"], "p99Ms": latency["overall"]["p99Ms"],
              "smallP99Ms": latency["small"].get("p99Ms"), "largeP99Ms": latency["large"].get("p99Ms"),
              "sessions": len(sessions), "startTotalSeconds": totals["start_ns"] / 1e9,
              "startMeanMs": statistics.mean(row["start_ns"] for row in sessions) / 1e6,
              "queryTotalSeconds": totals["query_ns"] / 1e9, "nextTotalSeconds": totals["next_ns"] / 1e9,
              "readDecodeBufferTotalSeconds": totals["read_decode_buffer_ns"] / 1e9,
              "addTotalSeconds": totals["add_ns"] / 1e9, "endTotalSeconds": totals["end_ns"] / 1e9,
              "rawRows": totals["rows"], "reconstructionSessions": int(samples[-1]["ReconstructionSessions"]),
              "forwardMiningSeconds": int(samples[-1]["ForwardMiningDurationMillis"]) / 1000,
              "reconstructionSeconds": int(samples[-1]["ReconstructionDurationMillis"]) / 1000,
              "dispatchSeconds": int(samples[-1]["DispatchDurationMillis"]) / 1000,
              "peakTransactionDiskMiB": max(int(row["TransactionBufferDiskBytes"]) for row in samples) / 1024 ** 2,
              "maximumObservedConsumerLagRecords": latency["maximumObservedConsumerLagRecords"],
              "observedArchiveFiles": len(latest_archives),
              "archiveWrittenGiB": sum(int(row["free"]) for row in latest_archives.values()) / 1024 ** 3,
              "maximumObservedFileMiB": max(int(row["len"]) for row in archives) / 1024 ** 2,
              "commitTransactions": len(commits), "committedOperations": sum(row["operations"] for row in commits)}
    result["miningSecondsPerTenThousandDml"] = (result["forwardMiningSeconds"] + result["reconstructionSeconds"]) * 10000 / operations
    for column in ("start_ns", "query_ns", "first_row_ns", "read_decode_buffer_ns"):
        values = sorted(row[column] for row in sessions)
        result[column.replace("_ns", "P95Ms")] = values[int((len(values) - 1) * 0.95)] / 1e6
    (directory / "result.json").write_text(json.dumps(result, indent=2))
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    args = parser.parse_args()
    results = [summarize(directory) for directory in sorted(args.directory.glob("r*-*mib-rate*"))]
    (args.directory / "results.json").write_text(json.dumps(results, indent=2))
    completed = sorted((row for row in results if row["status"] == "passed"), key=lambda row: (row["targetRate"], row["archiveMiB"]))
    if completed:
        with (args.directory / "summary.csv").open("w") as stream:
            writer = csv.DictWriter(stream, fieldnames=list(completed[0]))
            writer.writeheader()
            writer.writerows(completed)
    for row in completed:
        print(f"{row['archiveMiB']:4} MiB  {row['deliveredDmlPerSecond']:8.1f} DML/s  P95={row['p95Ms']:7} ms  P99={row['p99Ms']:7} ms  START={row['startTotalSeconds']:.3f} s  sessions={row['sessions']}")


if __name__ == "__main__":
    main()
