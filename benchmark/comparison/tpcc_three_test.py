"""Lossless fixture import and binary name ordering for the shared TPC-C run."""
import tempfile
from pathlib import Path
import unittest
from unittest.mock import patch

from tpcc_three import ALLOWED_PIDS, competing_work, fingerprint, schema_sql, schemas, typed_csv


class TpccThreeTest(unittest.TestCase):
    def test_other_benchmarks_detected_without_flagging_own_process(self):
        own = next(iter(ALLOWED_PIDS))
        snapshot = f'''{own} python python benchmark/comparison/tpcc_three.py
99999991 python python benchmark/comparison/run.py
99999992 cc1plus cc1plus source.cpp
99999993 tinylamb_tpcc tinylamb_tpcc_benchmark
99999994 chrome chrome
'''
        with patch('tpcc_three.subprocess.check_output', return_value=snapshot):
            self.assertEqual(competing_work(), dict(other_comparison_driver=1,cc1plus=1,other_tpc_client=1))

    def test_csv_distinguishes_empty_string_null_and_quoted_text(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'fixture.csv'
            path.write_text('1,1.25,"",,"a""b"\n')
            ddl = 'CREATE TABLE t (i INT64, n NUMERIC, a STRING, b STRING, c STRING)'
            self.assertEqual(list(typed_csv(path, ddl)), [(1, 1.25, '', None, 'a"b')])

    def test_nine_schemas_keep_binary_indexed_names_with_bounded_width(self):
        ddls = schemas()
        self.assertEqual(len(ddls), 9)
        mysql = schema_sql(ddls[2], 'mysql')
        self.assertIn('c_first VARCHAR(32) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin', mysql)
        self.assertIn('c_last VARCHAR(32) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin', mysql)
        self.assertIn('c_data VARCHAR(500)', mysql)
        self.assertTrue(mysql.endswith('ENGINE=InnoDB'))
        self.assertNotIn('STRING', mysql)
        self.assertIn('c_first TEXT', schema_sql(ddls[2], 'postgresql'))

    def test_fingerprint_is_order_independent_but_checks_values_and_duplicates(self):
        rows = [(1, None, ''), (2, 1.25, 'abc')]
        self.assertEqual(fingerprint(rows), fingerprint(rows[::-1]))
        self.assertNotEqual(fingerprint(rows), fingerprint(rows + rows[:1]))
        self.assertNotEqual(fingerprint(rows), fingerprint([(1, '', ''), rows[1]]))
        self.assertNotEqual(fingerprint([(1,)]), fingerprint([(1.,)]))

    def test_csv_width_mismatch_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'fixture.csv'
            path.write_text('1,2\n')
            with self.assertRaises(ValueError):
                list(typed_csv(path, 'CREATE TABLE t (i INT64)'))


if __name__ == '__main__':
    unittest.main()
