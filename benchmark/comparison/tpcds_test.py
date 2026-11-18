"""TPC-DS protocol, dialect and answer-verification regression tests."""
import sys
import tempfile
from unittest import mock
import contextlib
import io
import unittest

import tpcds


class AnswerTest(unittest.TestCase):
    def test_duplicate_multiplicity_and_nulls(self):
        self.assertIsNone(tpcds.compare([[None, ''], [1, 'x'], [1, 'x']],
                                       [[1, 'x'], [None, ''], [1, 'x']]))
        self.assertIsNotNone(tpcds.compare([[1], [1]], [[1], [2]]))
        self.assertIsNotNone(tpcds.compare([[None]], [['']]))
        self.assertIsNotNone(tpcds.compare([[1, 2]], [[2, 1]]))

    def test_counts_are_exact_and_float_sums_tolerate_roundoff(self):
        self.assertIsNotNone(tpcds.compare([[1000000001]], [[1000000000]]))
        self.assertIsNone(tpcds.compare([[12345.6700000002]], [[12345.67]]))
        self.assertIsNotNone(tpcds.compare([[12345.68]], [[12345.67]]))

    def test_nonfinite_values_are_not_null(self):
        nan = tpcds.normalize(float('nan'))
        self.assertIsNone(tpcds.compare([[nan]], [[nan]]))
        self.assertIsNotNone(tpcds.compare([[nan]], [[None]]))
        self.assertIsNotNone(tpcds.compare([[tpcds.normalize(float('inf'))]],
                                           [[tpcds.normalize(float('-inf'))]]))


class WorkerTest(unittest.TestCase):
    def test_dead_worker_can_be_closed_repeatedly(self):
        with tempfile.TemporaryFile(mode='w+') as log:
            worker = tpcds.Worker([sys.executable, '-c', 'pass'], log, 2)
            self.assertIn('error', worker.query('q1.sql'))
            worker.close()
            worker.close()

    def test_timeout_kills_worker_and_allows_restart(self):
        script = ('import sys,time; print(\'{"ready":true}\',flush=True); '
                  'sys.stdin.readline(); time.sleep(10)')
        with tempfile.TemporaryFile(mode='w+') as log:
            worker = tpcds.Worker([sys.executable, '-c', script], log, .1)
            try:
                self.assertIn('exceeded', worker.query('q1.sql')['error'])
                self.assertIsNone(worker.process)
                self.assertIn('exceeded', worker.query('q1.sql')['error'])
            finally:
                worker.close()


class SequentialGateTest(unittest.TestCase):
    class FakeWorker:
        def __init__(self, responses):
            self.responses = iter(responses)
            self.queries = []

        def query(self, name):
            self.queries.append(name)
            return next(self.responses)

    def run_gate(self, tiny, duck, limit=2):
        workers = {'tinylamb': self.FakeWorker(tiny),
                   'duckdb': self.FakeWorker(duck)}
        records = list(tpcds.query_records(workers, [1, 2, 3], 1, 3, limit))
        return records, workers

    def test_stops_before_next_query_after_slow_median(self):
        def samples(times):
            return [{'ms': ms, 'rows': [[1]]} for ms in times]
        records, workers = self.run_gate(
            samples([1000, 2, 2, 2, 1, 2, 3, 4]),
            samples([1] * 8))
        self.assertEqual([r['query'] for r in records], [1, 2])
        self.assertTrue(records[0]['performance_pass'])  # equality passes; warmup excluded
        self.assertFalse(records[1]['performance_pass'])
        self.assertEqual(records[1]['time_ratio'], 3)
        for worker in workers.values():
            self.assertEqual(worker.queries, ['q1.sql'] * 4 + ['q2.sql'] * 4)

    def test_stops_on_wrong_answer_or_error(self):
        for response, status in [({'ms': 1, 'rows': [[2]]}, 'mismatch'),
                                 ({'error': 'failed'}, 'error')]:
            with self.subTest(status=status):
                records, workers = self.run_gate([response] * 4,
                                                 [{'ms': 1, 'rows': [[1]]}] * 4)
                self.assertEqual([r['status'] for r in records], [status])
                for worker in workers.values():
                    self.assertEqual(set(worker.queries), {'q1.sql'})

    def test_stops_when_warmup_answer_differs(self):
        first = {'ms': 100, 'rows': [[2]]}
        steady = {'ms': 1, 'rows': [[1]]}
        records, workers = self.run_gate([first] + [steady] * 3,
                                         [steady] * 4)
        self.assertEqual([r['status'] for r in records], ['unstable'])
        self.assertEqual(records[0]['tinylamb']['unstable_answers'],
                         [[[2]], [[1]], [[1]], [[1]]])
        for worker in workers.values():
            self.assertEqual(set(worker.queries), {'q1.sql'})


class TpchCliTest(unittest.TestCase):
    def test_tpch_rejects_queries_outside_22_and_missing_dataset(self):
        for args, message in [
                (['--output', '/unused', '--dataset', '/unused', '--queries', '23'], '1..22'),
                (['--output', '/unused'], 'requires an existing --dataset')]:
            with self.subTest(args=args), mock.patch.object(sys, 'argv', ['tpch.py'] + args):
                error = io.StringIO()
                with contextlib.redirect_stderr(error), self.assertRaises(SystemExit) as raised:
                    tpcds.main('tpch')
                self.assertEqual(raised.exception.code, 2)
                self.assertIn(message, error.getvalue())


class DialectTest(unittest.TestCase):
    def test_decimal_conversion_and_window_null_order(self):
        try:
            import sqlglot
        except ImportError:
            self.skipTest('optional requirements-tpcds.txt not installed')
        sql = tpcds.port_query('SELECT CAST(x AS DECIMAL(12,2)), '
                              'SUM(x) OVER (ORDER BY k NULLS LAST '
                              'ROWS UNBOUNDED PRECEDING) FROM t')
        self.assertIn('DOUBLE', sql['duckdb'])
        self.assertIn('FLOAT64', sql['tinylamb'])
        self.assertIn('NULLS LAST', sql['tinylamb'])
        self.assertNotIn('DECIMAL', sql['duckdb'])


if __name__ == '__main__':
    unittest.main()
