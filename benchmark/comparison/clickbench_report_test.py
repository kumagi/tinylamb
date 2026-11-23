#!/usr/bin/env python3
import json
from pathlib import Path
import tempfile
import unittest

from clickbench_report import read_run, report


class ReportTest(unittest.TestCase):
    def make_run(self, root, reference):
        path = root / reference
        path.mkdir()
        manifest = {'reference': reference, 'binary_sha256': 'same', 'threads': 8,
                    'cpus': list(range(8)), 'warmups': 1, 'repeats': 3,
                    'reverse': False, 'reference_session': {'timezone': 'UTC'},
                    'dataset': {'rows': 10_000_000, 'sources': [{'sha256': 'data'}],
                                'query_sha256': {'q1': 'query'}, 'timezone': 'UTC',
                                'upstream_commit': 'revision'}}
        (path / 'run.json').write_text(json.dumps(manifest))
        records = [{'query': q, 'correct': True,
                    'tinylamb': {'samples_ms': [3, 2, 1], 'median_ms': 2},
                    reference: {'samples_ms': [4, 3, 2], 'median_ms': 3}}
                   for q in range(1, 44)]
        self.write_records(path, records)
        return path

    def write_records(self, path, records):
        (path / 'results.jsonl').write_text(''.join(json.dumps(r) + '\n' for r in records))

    def test_rejects_missing_duplicate_incorrect_or_forged_samples(self):
        with tempfile.TemporaryDirectory() as directory:
            path = self.make_run(Path(directory), 'duckdb')
            _, records = read_run(path, 'duckdb')
            for change in ('missing', 'duplicate', 'incorrect', 'median', 'sample'):
                altered = json.loads(json.dumps(records))
                if change == 'missing':
                    altered.pop()
                elif change == 'duplicate':
                    altered[0]['query'] = 2
                elif change == 'incorrect':
                    altered[0]['correct'] = False
                elif change == 'median':
                    altered[0]['tinylamb']['median_ms'] = 0.001
                else:
                    altered[0]['tinylamb']['samples_ms'].pop()
                self.write_records(path, altered)
                with self.subTest(change=change), self.assertRaises(ValueError):
                    read_run(path, 'duckdb')

    def test_rejects_different_binary_or_inputs_before_plotting(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            duck = self.make_run(root, 'duckdb')
            click = self.make_run(root, 'clickhouse')
            original = json.loads((click / 'run.json').read_text())
            for change in ('binary', 'dataset'):
                manifest = json.loads(json.dumps(original))
                if change == 'binary':
                    manifest['binary_sha256'] = 'different'
                else:
                    manifest['dataset']['sources'][0]['sha256'] = 'different'
                (click / 'run.json').write_text(json.dumps(manifest))
                with self.subTest(change=change), self.assertRaises(ValueError):
                    report(duck, click, root / 'report')
                self.assertFalse((root / 'report').exists())

    def test_rejects_runs_with_different_order_or_reference_session(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            duck = self.make_run(root, 'duckdb')
            click = self.make_run(root, 'clickhouse')
            original = json.loads((click / 'run.json').read_text())
            for field, value in (('reverse', True),
                                 ('reference_session', {'timezone': 'Asia/Tokyo'}),
                                 ('reference_session_timezone', 'Asia/Tokyo')):
                manifest = json.loads(json.dumps(original))
                manifest[field] = value
                (click / 'run.json').write_text(json.dumps(manifest))
                with self.subTest(field=field), self.assertRaises(ValueError):
                    report(duck, click, root / 'report')
                self.assertFalse((root / 'report').exists())


if __name__ == '__main__':
    unittest.main()
