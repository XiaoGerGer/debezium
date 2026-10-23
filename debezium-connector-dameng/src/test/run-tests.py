#!/usr/bin/env python3
# Copyright Debezium Authors.
# Licensed under the Apache Software License version 2.0.
"""Reproduce Dameng functional regressions using only this checkout and owned containers."""

import argparse
import datetime
import json
import os
from pathlib import Path
import platform
import re
import shutil
import signal
import subprocess
import sys
import time
import uuid
import xml.etree.ElementTree as ET

ROOT = Path(__file__).resolve().parents[3]
MODULE = ROOT / 'debezium-connector-dameng'
SOURCE = MODULE / 'src/test/java'
DM_IMAGE = 'dameng:8.1.3.162-20240621-rev232765'
SUITES = ('unit', 'database', 'connect', 'performance', 'probes')
SPECIAL = {'DamengKafkaConnectIT', 'DamengDatagenPerformanceIT', 'DamengLogMinerBenchmarkIT'}
GIB = 1024 ** 3


def selection(suite):
    if suite == 'unit':
        return sorted(p.stem for p in SOURCE.rglob('*Test.java'))
    if suite == 'connect':
        return ['DamengKafkaConnectIT']
    if suite == 'performance':
        return ['DamengDatagenPerformanceIT']
    if suite == 'probes':
        return sorted(p.stem for p in SOURCE.rglob('*ProbeIT.java'))
    return sorted(p.stem for p in SOURCE.rglob('*IT.java') if p.stem not in SPECIAL and not p.stem.endswith('ProbeIT'))


def report(directory, expected):
    counts = dict(tests=0, failures=0, errors=0, skipped=0)
    executed = set()
    problems = []
    catalog_file = directory.parent / 'database-evidence/case-catalog.txt'
    catalog = [line.split('\t')[1] for line in catalog_file.read_text().splitlines()] if catalog_file.exists() else []
    for path in sorted(directory.glob('TEST-*.xml')):
        suite = ET.parse(path).getroot()
        for key in counts:
            counts[key] += int(suite.get(key, '0'))
        for case in suite.findall('testcase'):
            executed.add(case.get('classname', '').rsplit('.', 1)[-1].split('$')[0])
            for kind in ('failure', 'error', 'skipped'):
                for failure in case.findall(kind):
                    item = dict(test_class=case.get('classname'), test=case.get('name'), kind=kind,
                                message=failure.get('message', ''), detail=failure.text or '')
                    if case.get('name', '').startswith(('shouldPreserveTypeBoundariesAndNullCrudImages',
                                                       'shouldCompareJdbcAndArchivedCrudAgainstStoredBoundaries')):
                        index = re.search(r'\[(\d+)\]$', case.get('name', ''))
                        if index and 0 < int(index[1]) <= len(catalog):
                            item['datatype_case'] = catalog[int(index[1]) - 1]
                    problems.append(item)
    missing = sorted(set(expected) - executed)
    passed = counts['tests'] > 0 and not missing and not any(counts[key] for key in ('failures', 'errors', 'skipped'))
    return dict(counts=counts, missing_classes=missing, problems=problems, passed=passed)


def capture(command, env):
    return subprocess.check_output(command, cwd=ROOT, env=env, text=True, stderr=subprocess.STDOUT).strip()


def execute(command, directory, env):
    directory.mkdir(parents=True, exist_ok=True)
    (directory / 'command.json').write_text(json.dumps(command, indent=2) + '\n')
    print(f"Running {directory.name}; log: {directory / 'maven.log'}", flush=True)
    start = time.monotonic()
    with (directory / 'maven.log').open('w') as log:
        process = subprocess.Popen(command, cwd=ROOT, env=env, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
        try:
            while True:
                try:
                    return process.wait(timeout=30)
                except subprocess.TimeoutExpired:
                    print(f'{directory.name}: running ({int(time.monotonic() - start)}s)', flush=True)
        except BaseException:
            os.killpg(process.pid, signal.SIGTERM)
            try:
                process.wait(timeout=30)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait()
            raise


def preflight(args, suites, env):
    java = Path(env['JAVA_HOME']) / 'bin/java' if env.get('JAVA_HOME') else Path(shutil.which('java') or 'java')
    java_version = capture([str(java), '-version'], env)
    match = re.search(r'version "(\d+)', java_version)
    if not match or int(match[1]) < 21:
        raise RuntimeError('Set JAVA_HOME to JDK 21 or newer (the root POM build requirement).')
    for name in ('MAVEN_ARGS', 'MAVEN_OPTS', 'JAVA_TOOL_OPTIONS', 'JDK_JAVA_OPTIONS', '_JAVA_OPTIONS'):
        if env.get(name) and any(token in env[name] for token in ('dameng.', 'skipTests', 'skipITs', 'maven.test.skip', '-Dtest=', '-Dit.test=', 'jacoco.')):
            raise RuntimeError(f'{name} contains test overrides; remove them for a reproducible run.')
    if shutil.disk_usage(MODULE).free < 2 * GIB:
        raise RuntimeError('At least 2 GiB free space is needed in the checkout for build outputs.')
    info = dict(java=java_version, platform=platform.platform(), source_root=str(ROOT))
    try:
        info['git_head'] = capture(['git', 'rev-parse', 'HEAD'], env)
        info['git_status'] = capture(['git', 'status', '--short'], env)
    except (OSError, subprocess.CalledProcessError):
        info['git_head'] = None
        info['git_status'] = 'Source archive without Git metadata'
    if suites == ['unit']:
        return info
    endpoint = env.get('DOCKER_HOST') or capture(['docker', 'context', 'inspect', '--format', '{{.Endpoints.docker.Host}}'], env)
    if platform.system() != 'Linux' or not endpoint.startswith('unix://'):
        raise RuntimeError('The complete suite requires native Linux and local Docker; performance uses host networking.')
    docker = json.loads(capture(['docker', 'info', '--format', '{{json .}}'], env))
    if docker['OSType'] != 'linux' or docker['Architecture'] not in ('x86_64', 'amd64'):
        raise RuntimeError('The tested DM image requires Linux x86_64; ARM emulation is not a validated baseline.')
    if docker['MemTotal'] < 8 * GIB:
        raise RuntimeError('At least 8 GiB Docker memory is required; 12 GiB or more is recommended for the HA tests.')
    info['docker'] = {key: docker[key] for key in ('ServerVersion', 'Architecture', 'MemTotal', 'NCPU', 'DockerRootDir')}
    if env.get('TESTCONTAINERS_RYUK_DISABLED', '').lower() == 'true':
        raise RuntimeError('Enable Ryuk so failed tests also clean up their owned containers.')
    images = [args.dm_image, args.ryuk_image]
    if 'connect' in suites:
        images.append(args.java_image)
    if 'performance' in suites:
        images.append(args.kafka_image)
    info['images'] = []
    for image in dict.fromkeys(images):
        found = subprocess.run(['docker', 'image', 'inspect', image], env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        if found.returncode:
            if args.offline:
                raise RuntimeError(f'Missing offline image: {image}. Load it or specify the loaded image name.')
            subprocess.run(['docker', 'pull', image], env=env, check=True)
        metadata = json.loads(capture(['docker', 'image', 'inspect', image], env))[0]
        info['images'].append({key: metadata.get(key) for key in ('Id', 'RepoTags', 'RepoDigests', 'Architecture')})
    # Ask a disposable container, since DockerRootDir can differ from the checkout filesystem
    # and is often unreadable by non-root users. Never mount or touch an existing data volume.
    df = capture(['docker', 'run', '--rm', '--network', 'none', '--entrypoint', 'df', args.dm_image, '-Pk', '/'], env)
    free = int(df.splitlines()[-1].split()[3]) * 1024
    info['docker_free_bytes'] = free
    if free < args.min_free_gib * GIB:
        raise RuntimeError(f'Docker storage has {free / GIB:.1f} GiB free; need {args.min_free_gib} GiB.')
    return info


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--suite', choices=('all',) + SUITES, default='all')
    parser.add_argument('--list', action='store_true', help='List selected classes without running or accessing Docker')
    parser.add_argument('--skip-build', action='store_true', help='Development only: reuse installed reactor dependencies')
    parser.add_argument('--maven-repo', type=Path, help='Use a separate Maven local repository')
    parser.add_argument('--offline', action='store_true', help='Require cached Maven dependencies and loaded images')
    parser.add_argument('--dm-image', default=DM_IMAGE)
    parser.add_argument('--java-image', default='eclipse-temurin:21-jre')
    parser.add_argument('--kafka-image', default='apache/kafka:3.9.1')
    parser.add_argument('--ryuk-image', default=os.getenv('TESTCONTAINERS_RYUK_CONTAINER_IMAGE', 'testcontainers/ryuk:0.13.0'))
    parser.add_argument('--min-free-gib', type=int, default=15, help='Required free Docker storage; checked during performance too')
    parser.add_argument('--evidence', type=Path, help='New, nonexistent results directory')
    args = parser.parse_args()
    if args.min_free_gib < 1:
        parser.error('--min-free-gib must be positive')
    suites = list(SUITES[:-1]) if args.suite == 'all' else [args.suite]
    selected = {suite: selection(suite) for suite in suites}
    if args.list:
        print(json.dumps(selected, indent=2))
        return 0
    directory = (args.evidence or MODULE / 'target' / ('regression-' + datetime.datetime.now().strftime('%Y%m%d-%H%M%S') + '-' + uuid.uuid4().hex[:6])).resolve()
    directory.mkdir(parents=True, exist_ok=False)
    env = dict(os.environ, TESTCONTAINERS_RYUK_CONTAINER_IMAGE=args.ryuk_image)
    summary = dict(suites=selected, complete_functional_suite=args.suite == 'all', passed=False, phases=[],
                   source_dependencies_rebuilt=not args.skip_build,
                   separate_diagnostics=selection('probes') + ['DamengLogMinerBenchmarkIT (requires vendor JNI libraries)'])
    base = [str(ROOT / 'mvnw'), '-B', '-ntp', '-pl', 'debezium-connector-dameng']
    if args.offline:
        base.append('-o')
    if args.maven_repo:
        base.append(f'-Dmaven.repo.local={args.maven_repo.resolve()}')
    coverage = directory / 'jacoco.exec'
    common = [f'-Ddameng.image={args.dm_image}', f'-Ddameng.connect.java.image={args.java_image}',
              f'-Ddameng.perf.worker.image={args.kafka_image}', f'-Ddameng.perf.broker.image={args.kafka_image}',
              f'-Ddameng.perf.docker.min.free.gib={args.min_free_gib}', '-Ddameng.perf.min.free.gib=2',
              f'-Ddameng.coverage.data.file={coverage}', '-Djacoco.skipCheck=true']
    try:
        summary['environment'] = preflight(args, suites, env)
        (directory / 'environment.json').write_text(json.dumps(summary['environment'], indent=2) + '\n')
        if not args.skip_build:
            # Keep formatting and installation in one reactor invocation: a fresh machine
            # has neither this version's formatter config artifact nor connector dependencies.
            # Each upstream module is formatted/built/installed before the next module.
            build = base + ['-am', 'process-sources', 'install', '-DskipTests', '-DskipITs']
            # Assembly descriptors are reactor dependencies only while that profile is active.
            if 'connect' in suites:
                build.append('-Passembly,dameng-connect-tests')
            elif 'performance' in suites:
                build.append('-Passembly')
            if execute(build, directory / 'build', env):
                raise RuntimeError('Reactor formatting/dependency build failed; see build/maven.log')
        elif execute(base + ['process-sources'], directory / 'format', env):
            raise RuntimeError('Formatting failed; see format/maven.log')
        for suite in suites:
            phase = directory / suite
            reports = phase / 'reports'
            if suite == 'unit':
                command = base + ['test', '-Pdameng-coverage', '-DskipITs', f'-Ddameng.surefire.reports.directory={reports}']
            else:
                profiles = 'dameng-connect-tests,assembly' if suite == 'connect' else 'dameng-tests,dameng-coverage'
                if suite == 'performance':
                    profiles += ',assembly'
                command = base + ['verify', f'-P{profiles}', '-Dtest=__NoUnitTestsInThisPhase',
                                  '-Dsurefire.failIfNoSpecifiedTests=false', '-Dfailsafe.failIfNoSpecifiedTests=true',
                                  f'-Dit.test={",".join(selected[suite])}', f'-Ddameng.failsafe.reports.directory={reports}',
                                  f'-Ddameng.evidence.directory={phase / "database-evidence"}',
                                  f'-Ddameng.perf.directory={phase / "performance"}', '-Ddameng.perf.enabled=true']
            exit_code = execute(command + common, phase, env)
            try:
                outcome = report(reports, selected[suite])
            except (OSError, ValueError, ET.ParseError) as error:
                outcome = dict(counts=dict(tests=0, failures=0, errors=0, skipped=0), passed=False,
                               missing_classes=selected[suite], report_error=str(error))
            outcome.update(suite=suite, exit_code=exit_code)
            summary['phases'].append(outcome)
            (directory / 'summary.json').write_text(json.dumps(summary, indent=2) + '\n')
            if exit_code or not outcome['passed']:
                print(f'FAILED phase: {suite}; continuing independent phases; see {phase}', flush=True)
        if args.suite == 'all':
            command = base + ['verify', '-Pdameng-coverage', '-DskipTests', '-DskipITs',
                              f'-Ddameng.coverage.data.file={coverage}']
            if execute(command, directory / 'coverage', env):
                raise RuntimeError('Fresh-run coverage check failed; see coverage/maven.log')
            xml = MODULE / 'target/site/jacoco/jacoco.xml'
            shutil.copy2(xml, directory / 'coverage/jacoco.xml')
            for counter in ET.parse(xml).getroot().findall('counter'):
                if counter.get('type') == 'LINE':
                    covered, missed = int(counter.get('covered')), int(counter.get('missed'))
                    summary['line_coverage'] = dict(covered=covered, missed=missed, ratio=covered / (covered + missed))
            if summary.get('line_coverage', {}).get('ratio', 0) < .95:
                raise RuntimeError('Missing coverage report or line coverage below 95%')
        summary['passed'] = all(phase['passed'] and phase['exit_code'] == 0 for phase in summary['phases'])
        if not summary['passed']:
            summary['error'] = 'One or more test phases failed, skipped tests, or lacked complete reports'
    except (Exception, KeyboardInterrupt) as error:
        summary['error'] = str(error) or type(error).__name__
        print(f'FAILED: {summary["error"]}', file=sys.stderr, flush=True)
    finally:
        summary['completed_suites'] = [phase['suite'] for phase in summary['phases']]
        summary['all_requested_suites_completed'] = summary['completed_suites'] == suites
        (directory / 'summary.json').write_text(json.dumps(summary, indent=2) + '\n')
        print(f'Results: {directory / "summary.json"}', flush=True)
    return 0 if summary['passed'] else 1


if __name__ == '__main__':
    sys.exit(main())
