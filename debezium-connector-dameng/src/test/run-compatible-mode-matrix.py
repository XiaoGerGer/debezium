#!/usr/bin/env python3
"""Run identical DM datatype cases in separate Maven JVMs and retain every mode's evidence."""

import argparse
import datetime
import json
import os
import re
from pathlib import Path
import subprocess
import sys
import uuid
import xml.etree.ElementTree as ET


def report(directory):
    counts = dict(tests=0, failures=0, errors=0, skipped=0)
    problems = []
    catalog_path = directory.parent / "case-catalog.txt"
    catalog = [line.split("\t")[1] for line in catalog_path.read_text().splitlines()] if catalog_path.exists() else []
    for path in sorted(directory.glob("TEST-*.xml")):
        suite = ET.parse(path).getroot()
        for key in counts:
            counts[key] += int(suite.get(key, "0"))
        for case in suite.findall("testcase"):
            for kind in ("failure", "error", "skipped"):
                for problem in case.findall(kind):
                    message = problem.get("message", "") + "\n" + (problem.text or "")
                    category = "assertion" if kind == "failure" else kind
                    if "No implemented Dameng compatibility profile" in message or "currently requires COMPATIBLE_MODE=0" in message:
                        category = "connector-mode-guard"
                    elif "Fixture cannot set COMPATIBLE_MODE=" in message:
                        category = "database-mode-configuration"
                    elif kind == "error" and "Database rejected input" in message:
                        category = "database-input-capability"
                    elif kind == "error" and any(text in message for text in ("Precision is out of range", "Invalid base class name")):
                        category = "database-declaration-capability"
                    case_name = None
                    if case.get("name", "").startswith(("shouldCompareJdbcAndArchivedCrudAgainstStoredBoundaries", "shouldPreserveTypeBoundariesAndNullCrudImages")):
                        index = re.search(r"\[(\d+)\]$", case.get("name", ""))
                        if index and int(index[1]) <= len(catalog):
                            case_name = catalog[int(index[1]) - 1]
                    problems.append(dict(test=case.get("name"), case=case_name, kind=kind, category=category,
                                         message=problem.get("message", "")))
    return dict(counts=counts, problems=problems)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--modes", default="0", help="Comma-separated mode identifiers; default: current native-mode regression")
    parser.add_argument("--layer", choices=("both", "probe", "engine"), default="both")
    parser.add_argument("--case", default=".*", help="Java regular expression matching shared case names")
    parser.add_argument("--image", help="Fixture-owned DM image; must use the documented fixture layout")
    parser.add_argument("--skip-build", action="store_true", help="Use already installed reactor dependencies")
    parser.add_argument("--evidence", type=Path, help="A new output directory; existing paths are refused")
    args = parser.parse_args()
    try:
        modes = [int(value) for value in args.modes.split(",")]
    except ValueError:
        parser.error("modes must be comma-separated nonnegative integers")
    if not modes or len(set(modes)) != len(modes) or any(value < 0 for value in modes):
        parser.error("modes must be distinct nonnegative integers")
    root = Path(__file__).resolve().parents[3]
    module = root / "debezium-connector-dameng"
    stamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    evidence = (args.evidence or module / "target" / "compatible-mode-runs" / f"{stamp}-{uuid.uuid4().hex[:8]}").resolve()
    evidence.mkdir(parents=True, exist_ok=False)
    environment = os.environ.copy()
    environment.setdefault("TESTCONTAINERS_RYUK_CONTAINER_IMAGE", "testcontainers/ryuk:0.13.0")
    base = [str(root / "mvnw"), "-B", "-ntp", "-pl", "debezium-connector-dameng"]
    if not args.skip_build:
        with (evidence / "build.log").open("w") as log:
            result = subprocess.run(base + ["-am", "install", "-DskipTests", "-DskipITs"], cwd=root,
                                    env=environment, stdout=log, stderr=subprocess.STDOUT)
        if result.returncode:
            print(f"Dependency build failed: {evidence / 'build.log'}", flush=True)
            return result.returncode
    tests = []
    if args.layer in ("both", "probe"):
        tests.append("DamengDatatypeProbeIT")
    if args.layer in ("both", "engine"):
        tests.append("DamengDatatypesIT")
    summary = dict(layer=args.layer, selection=args.case, modes=[], evidence=str(evidence))
    failed = False
    for mode in modes:
        directory = evidence / f"mode-{mode}"
        directory.mkdir()
        xml_directory = directory / "failsafe-reports"
        command = base + ["verify", "-Pdameng-tests,dameng-coverage", "-Dtest=__MatrixUnitTestsNotSelected",
                          "-Dsurefire.failIfNoSpecifiedTests=false", "-Djacoco.skipCheck=true",
                          f"-Dit.test={','.join(tests)}", f"-Ddameng.compatible.mode={mode}",
                          f"-Ddameng.datatype.case={args.case}",
                          f"-Ddameng.failsafe.reports.directory={xml_directory}",
                          f"-Ddameng.evidence.directory={directory}"]
        if args.image:
            command.append(f"-Ddameng.image={args.image}")
        print(f"Running mode {mode}, layer {args.layer}; log: {directory / 'maven.log'}", flush=True)
        (directory / "command.json").write_text(json.dumps(command, indent=2) + "\n")
        with (directory / "maven.log").open("w") as log:
            result = subprocess.run(command, cwd=root, env=environment, stdout=log, stderr=subprocess.STDOUT)
        outcome = report(xml_directory)
        outcome.update(mode=mode, exit_code=result.returncode)
        if not outcome["counts"]["tests"]:
            outcome["status"] = "no-tests-executed"
        else:
            outcome["status"] = "passed" if result.returncode == 0 else "failed"
        # A skipped test or absent XML never establishes datatype consistency.
        failed |= bool(result.returncode or not outcome["counts"]["tests"] or outcome["counts"]["skipped"])
        summary["modes"].append(outcome)
        (evidence / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n")
        print(f"Mode {mode}: {outcome['status']} {outcome['counts']}", flush=True)
    print(f"Evidence: {evidence}", flush=True)
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
