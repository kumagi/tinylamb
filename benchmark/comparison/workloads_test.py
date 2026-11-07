"""Regression checks for cross-engine fairness and result validation."""

import datetime
import decimal
import random
import unittest

from workloads import (OLTP, accounts, adapt_sql, equivalent, insert_sql,
                       olap_queries, sales, tpch_queries, tpch_schema, transaction)
from run import percentiles


class WorkloadsTest(unittest.TestCase):
    def test_fixtures_cover_keys_and_selectivity(self):
        generated = list(sales(10000, 10000))
        self.assertEqual({r[1] for r in generated}, set(range(10000)))
        self.assertEqual(sum(r[2] == 7 for r in generated), 100)
        self.assertEqual(sum(r[2] < 10 for r in generated), 1000)
        self.assertEqual(sum(r[2] >= 90 for r in generated), 1000)
        self.assertEqual(sum(r[2] for r in accounts(100)), 10000000)

    def test_integer_validation_does_not_round_large_values(self):
        self.assertFalse(equivalent([(2**60,)], [(2**60 + 1,)]))
        self.assertFalse(equivalent([(2**60,)], [(decimal.Decimal(2**60 + 1),)]))
        self.assertFalse(equivalent([(None,)], [(0,)]))
        self.assertFalse(equivalent([(1, 2)], [(1,)]))
        self.assertFalse(equivalent([(1,), (2,)], [(2,), (1,)]))
        self.assertTrue(equivalent([(decimal.Decimal("1.50"),)], [(1.5,)]))
        self.assertFalse(equivalent([(decimal.Decimal("1.50"),)], [(1.6,)]))
        self.assertFalse(equivalent([(float("nan"),)], [(float("nan"),)]))

    def test_date_folding_keeps_templates_equivalent(self):
        for engine in ("tinylamb", "postgresql", "mysql"):
            self.assertEqual(adapt_sql("date_sub(date '1998-12-01', INTERVAL 74 day)", engine), "DATE '1998-09-18'")
            self.assertEqual(adapt_sql("date_add(date '2024-01-31', INTERVAL 1 month)", engine), "DATE '2024-02-29'")
            self.assertEqual(adapt_sql("date_add(date '2024-02-29', INTERVAL 1 year)", engine), "DATE '2025-02-28'")

    def test_schema_and_all_tpch_templates_are_read(self):
        schemas = tpch_schema()
        self.assertEqual([s[0] for s in schemas], ["region", "nation", "supplier", "customer", "part", "partsupp", "orders", "lineitem"])
        self.assertEqual(len(schemas[-1][2]), 16)
        queries = tpch_queries(.01)
        self.assertEqual(len(queries), 22)
        self.assertIn("* 0.01", queries["Q11"])
        self.assertEqual(len(olap_queries(10000)), 18)

    def test_write_workloads_have_bounded_unique_ids_and_balanced_writes(self):
        for name in OLTP:
            sql, delta = transaction(name, random.Random(42), 16, 123, 10000)
            self.assertTrue(sql)
            if name in ("transfer", "read_write", "htap"):
                self.assertEqual(sql[0], "BEGIN")
                self.assertEqual(sql[-1], "COMMIT")
                updates = [s for s in sql if s.startswith("UPDATE")]
                lo, hi = [int(s.split(" = ")[-1]) for s in updates]
                self.assertLess(lo, hi)
                self.assertEqual(delta, 0)
            if name == "rollback":
                self.assertEqual(sql[-1], "ROLLBACK")
                self.assertEqual(delta, 0)
            if name == "insert_delete":
                self.assertIn("16000000123", sql[1])

    def test_insert_quotes_strings_and_nulls(self):
        self.assertEqual(insert_sql("t", [(1, "it's", None)]), "INSERT INTO t VALUES (1,'it''s',NULL)")
        self.assertEqual(insert_sql("t", [(datetime.date(2026, 10, 28),)]), "INSERT INTO t VALUES (DATE '2026-10-28')")
        self.assertEqual(adapt_sql("CREATE TABLE t (v DOUBLE PRECISION)", "tinylamb"), "CREATE TABLE t (v FLOAT64)")

    def test_nearest_rank_latency_percentiles(self):
        result = percentiles(list(range(1, 101)))
        self.assertEqual(result, {"p50_ms": 50, "p95_ms": 95, "p99_ms": 99, "max_ms": 100})
        self.assertIsNone(percentiles([])["p99_ms"])


if __name__ == "__main__":
    unittest.main()
