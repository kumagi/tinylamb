"""Check that report tables expose errors and exclude invalid results."""

import unittest
from unittest.mock import patch

import analyze

from analyze import ENGINES, olap_table, oltp_tables


class AnalyzeTest(unittest.TestCase):
    def test_optional_backends_selected_independently(self):
        for optional in (['duckdb'], ['oracle'], ['oracle', 'duckdb']):
            engines, labels, colors = analyze.selected_engines([{'engine': e} for e in [*ENGINES, *optional]])
            self.assertEqual(engines, (*ENGINES, *optional))
            self.assertEqual(len(engines), len(labels))
            self.assertEqual(len(engines), len(colors))

    def test_four_engine_table_keeps_oracle_and_invalid_results(self):
        engines = (*ENGINES, 'oracle')
        records = [dict(kind='olap', suite='micro', rows=10000, workload='count', engine=e,
                        status='ok', median_ms=i + 1., correct=e != 'oracle')
                   for i, e in enumerate(engines)]
        with patch.object(analyze, 'ENGINES', engines), patch.object(analyze, 'LABELS', (*analyze.LABELS, 'Oracle Free')):
            table, _ = olap_table(records, 'micro', 10000)
        self.assertIn('Oracle Free ms', table)
        self.assertIn('不一致／失敗 (4.000)', table)

    def test_incorrect_query_is_labelled_instead_of_ranked(self):
        records = [dict(kind="olap", suite="tpch", rows=.01, workload="Q06", engine=e,
                        status="ok", median_ms=1., correct=e != "tinylamb") for e in ENGINES]
        table, groups = olap_table(records, "tpch", .01)
        self.assertIn("不一致／失敗 (1.000)", table)
        self.assertFalse(groups["Q06"]["tinylamb"]["correct"])

    def test_transaction_errors_stay_visible_with_successful_tps(self):
        records = []
        for engine in ENGINES:
            for trial, tps in enumerate([100., 300., 200.]):
                records.append(dict(kind="oltp", workload="transfer", clients=4, engine=engine,
                                    trial=trial, tps=tps, valid=True, errors=2, completed=1000,
                                    workers=[{"attempted": 100}], p95_ms=3., p99_ms=4.))
        table, latency, _, _ = oltp_tables(records)
        self.assertIn("200.0 [100.0–300.0]; 2.00%", table)
        self.assertIn("3.000 / 4.000", latency)

    def test_one_invalid_trial_invalidates_the_condition(self):
        records = [dict(kind="oltp", workload="update", clients=1, engine=e,
                        tps=9999., valid=e != "tinylamb", errors=0, completed=1000,
                        workers=[{"attempted": 10000}], p95_ms=1., p99_ms=2.) for e in ENGINES]
        table, _, _, _ = oltp_tables(records)
        self.assertIn("不変条件／実行失敗", table)
        self.assertNotIn("9,999.0 [9,999.0–9,999.0]; 0.00% | 9,999.0 [9,999.0–9,999.0]; 0.00% | 9,999.0", table)

    def test_latency_table_marks_sparse_success_samples(self):
        records = []
        for engine in ENGINES:
            for trial in range(3):
                records.append(dict(kind="oltp", workload="insert_delete", clients=16, engine=engine,
                                    tps=0., valid=True, errors=10, completed=1 if trial == 0 else 0,
                                    workers=[{"attempted": 10}], p95_ms=1. if trial == 0 else None,
                                    p99_ms=1. if trial == 0 else None))
        _, latency, _, _ = oltp_tables(records)
        self.assertIn("成功1件、有値1/3試行", latency)


if __name__ == "__main__":
    unittest.main()
