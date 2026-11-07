"""Reject incomplete matrices and misleading correctness/provenance flags."""

import copy
import hashlib
import json
from pathlib import Path
import tempfile
import unittest

from duckdb_report import audit


class DuckDBReportTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.directory = Path(self.temporary.name)
        runtime = self.directory / 'runtime-harness'
        runtime.mkdir()
        (runtime / 'run.py').write_text('frozen measurement code\n')
        self.env = dict(finished='2026-11-05',
                        harness_sha256={'run.py': hashlib.sha256((runtime / 'run.py').read_bytes()).hexdigest()},
                        args=dict(include_oracle=False, include_duckdb=True, sizes=[100],
                                  olap_workloads=['count'], htap_reader_baseline=False,
                                  skip_olap=False, tpch_scale=0, skip_oltp=True,
                                  secondary_indexes=False, repeats=2))
        self.records = []
        for engine in ('tinylamb', 'postgresql', 'mysql', 'duckdb'):
            self.records.extend([dict(kind='load', engine=engine, rows=100, valid=True),
                                 dict(kind='olap', suite='micro', engine=engine, rows=100,
                                      workload='count', correct=True, stable=True,
                                      status='ok', samples_ms=[1., 2.])])

    def save(self, env=None, records=None):
        (self.directory / 'environment.json').write_text(json.dumps(self.env if env is None else env))
        (self.directory / 'results.jsonl').write_text('\n'.join(json.dumps(r) for r in (self.records if records is None else records)))

    def test_complete_small_matrix_accepted(self):
        self.save()
        result = audit(self.directory)[-1]
        self.assertEqual(result['correct_olap'], 4)
        self.assertTrue(result['complete'])

    def test_invalid_or_missing_conditions_rejected(self):
        cases = []
        cases.append((self.env, [r for r in self.records if r['engine'] != 'mysql']))
        cases.append((self.env, self.records + [self.records[-1]]))
        selected_oracle = copy.deepcopy(self.env)
        selected_oracle['args']['include_oracle'] = True
        cases.append((selected_oracle, self.records))
        incomplete = copy.deepcopy(self.env)
        incomplete.pop('finished')
        cases.append((incomplete, self.records))
        for key, value in [('correct', False), ('stable', False), ('samples_ms', [1.]), ('status', 'error')]:
            records = copy.deepcopy(self.records)
            records[-1][key] = value
            cases.append((self.env, records))
        records = copy.deepcopy(self.records)
        records[0]['valid'] = False
        cases.append((self.env, records))
        for i, (env, records) in enumerate(cases):
            with self.subTest(case=i):
                self.save(env, records)
                with self.assertRaises(ValueError):
                    audit(self.directory)

    def test_changed_runtime_source_rejected(self):
        self.save()
        (self.directory / 'runtime-harness/run.py').write_text('different code\n')
        with self.assertRaisesRegex(ValueError, 'runtime harness'):
            audit(self.directory)

    def test_recovery_requires_complete_unchanged_artifacts(self):
        self.env.pop('finished')
        self.save()
        (self.directory / 'run-finalization.log').write_text('all measurements saved; capacity metadata failed\n')
        recovery = dict(stage='post-measurement-capacity-metadata', preserved_files_sha256={
            name: hashlib.sha256((self.directory / name).read_bytes()).hexdigest()
            for name in ('environment.json', 'results.jsonl', 'run-finalization.log')})
        (self.directory / 'completion-recovery.json').write_text(json.dumps(recovery))
        self.assertTrue(audit(self.directory)[-1]['complete'])
        self.assertIn('recovered', audit(self.directory)[-1]['finalization'])
        self.records.pop()
        self.save()
        with self.assertRaisesRegex(ValueError, 'recovered source artifact changed'):
            audit(self.directory)


if __name__ == '__main__':
    unittest.main()
