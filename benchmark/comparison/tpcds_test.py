"""TPC-DS protocol, dialect and answer-verification regression tests."""
import sys
import tempfile
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
