"""Invalid measurements must fail validation, including under python -O."""
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch

import oracle_report
import run_spsc_aggregation as spsc
import tpcc_pg
from tpcc_read_probe import measure_reads
from tpch_pg import validate_fixture_counts


class MeasurementValidationTest(unittest.TestCase):
    def test_oracle_requires_finished_timestamp_before_writing_audit(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / 'environment.json').write_text('{}')
            with patch.object(sys, 'argv', ['report', tmp, '--output', str(root / 'report.md')]):
                with self.assertRaisesRegex(ValueError, 'no finished timestamp'):
                    oracle_report.main()
            self.assertFalse((root / 'audit.json').exists())

    def test_spsc_rejects_unbalanced_verified_modes(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / 'manifest.json').write_text(json.dumps({'cases': [{'name': 'case', 'threads': 2}]}))
            rows = [dict(mode=mode, warmup=i == 0, verified=True, wall_ms=1)
                    for mode, count in zip(spsc.MODES, (5, 7, 6)) for i in range(count)]
            (root / 'case_t2.jsonl').write_text('\n'.join(map(json.dumps, rows)))
            with self.assertRaisesRegex(ValueError, 'expected 5 measured'):
                spsc.summarize(root)
            self.assertFalse((root / 'summary.json').exists())

    def test_tpcc_rejects_invalid_initial_fixture(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / 'fixture.log.meta').touch()
            binary = root / 'client'
            binary.touch()
            stack = Mock(work=root)
            stack.metadata.return_value = {}
            argv = ['tpcc', '--output', str(root / 'out'), '--server', str(binary),
                    '--client', str(binary), '--fixture', str(root / 'fixture')]
            with (patch.object(sys, 'argv', argv), patch.object(tpcc_pg, 'Stack', return_value=stack),
                  patch.object(tpcc_pg, 'Client'), patch.object(tpcc_pg, 'check', return_value={'valid': False})):
                with self.assertRaisesRegex(ValueError, 'invalid tinylamb starting state'):
                    tpcc_pg.main()
            stack.close.assert_called_once()
            self.assertFalse((root / 'out/finished.json').exists())

    def test_probe_requires_identical_rows_on_every_read(self):
        client = Mock()
        client.execute.side_effect = [[(1,)], [(1,)], [(2,)]]
        with self.assertRaisesRegex(ValueError, 'repeat 2'):
            measure_reads(client, 'point', 'SELECT x FROM t', 2)

    def test_tpch_rejects_mismatched_fixture_before_analyze(self):
        client = Mock()
        client.execute.return_value = [(2,)]
        with self.assertRaisesRegex(ValueError, 'expected 3 rows, got 2'):
            validate_fixture_counts(client, 'tinylamb', {'lineitem': 3})
        client.execute.assert_called_once_with('SELECT COUNT(*) FROM lineitem')
