# Copyright Debezium Authors.
# Licensed under the Apache Software License version 2.0.
"""Test result validation without Docker or Maven."""
import importlib.util
from pathlib import Path
import tempfile
import unittest
import xml.etree.ElementTree as ET

spec = importlib.util.spec_from_file_location('runner', Path(__file__).with_name('run-tests.py'))
runner = importlib.util.module_from_spec(spec)
spec.loader.exec_module(runner)


class ReportsTest(unittest.TestCase):
    def outcome(self, xml, expected=('ExampleTest',)):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)
            if xml:
                (path / 'TEST-example.xml').write_text(xml)
            return runner.report(path, expected)

    def test_success(self):
        result = self.outcome('<testsuite tests="2"><testcase classname="a.ExampleTest"/><testcase classname="a.ExampleTest$Nested"/></testsuite>')
        self.assertTrue(result['passed'])
        self.assertEqual(2, result['counts']['tests'])

    def test_no_report_cannot_pass(self):
        self.assertFalse(self.outcome('')['passed'])

    def test_missing_selected_class_cannot_pass(self):
        self.assertFalse(self.outcome('<testsuite tests="1"><testcase classname="a.DifferentTest"/></testsuite>')['passed'])

    def test_skipped_failures_and_errors_cannot_pass(self):
        for kind in ('skipped', 'failures', 'errors'):
            with self.subTest(kind=kind):
                self.assertFalse(self.outcome(f'<testsuite tests="1" {kind}="1"><testcase classname="a.ExampleTest"/></testsuite>')['passed'])

    def test_malformed_xml_cannot_pass(self):
        with self.assertRaises(ET.ParseError):
            self.outcome('<testsuite')

    def test_every_integration_class_is_accounted_for(self):
        classified = set().union(*(runner.selection(suite) for suite in ('database', 'connect', 'performance', 'probes')))
        classified.add('DamengLogMinerBenchmarkIT')
        self.assertEqual({path.stem for path in runner.SOURCE.rglob('*IT.java')}, classified)


class ContinuationTest(unittest.TestCase):
    def test_failed_database_phase_does_not_hide_connect_and_performance_results(self):
        import json
        from unittest.mock import patch

        with tempfile.TemporaryDirectory() as directory:
            module = Path(directory)
            xml = module / 'target/site/jacoco/jacoco.xml'
            xml.parent.mkdir(parents=True)
            xml.write_text('<report><counter type="LINE" covered="96" missed="4"/></report>')
            results = module / 'results'
            executed = []

            def execute(command, phase, env):
                phase.mkdir(parents=True, exist_ok=True)
                executed.append(phase.name)
                return 1 if phase.name == 'database' else 0

            def report(path, expected):
                return dict(counts=dict(tests=1, failures=int(path.parent.name == 'database'), errors=0, skipped=0),
                            passed=path.parent.name != 'database', missing_classes=[])

            with patch.object(runner, 'MODULE', module), patch.object(runner, 'preflight', return_value={}), \
                    patch.object(runner, 'selection', return_value=['ExampleTest']), \
                    patch.object(runner, 'execute', side_effect=execute), patch.object(runner, 'report', side_effect=report), \
                    patch('sys.argv', ['run-tests.py', '--skip-build', '--evidence', str(results)]):
                self.assertEqual(1, runner.main())
            summary = json.loads((results / 'summary.json').read_text())
            self.assertFalse(summary['passed'])
            self.assertTrue(summary['all_requested_suites_completed'])
            self.assertEqual(['unit', 'database', 'connect', 'performance'], summary['completed_suites'])
            self.assertEqual(['format', 'unit', 'database', 'connect', 'performance', 'coverage'], executed)
            self.assertEqual(.96, summary['line_coverage']['ratio'])


if __name__ == '__main__':
    unittest.main()
