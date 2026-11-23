#!/usr/bin/env python3
"""Unit tests for the standalone derived-analytics benchmark (no DB servers)."""
import argparse
import importlib.util
import json
import tempfile
import unittest
from pathlib import Path

import analytics_bench as ab


class GenerationTest(unittest.TestCase):
    def test_all_families_shape(self):
        self.assertEqual(set(ab.ALL_FAMILIES), {f.key for f in ab.FAMILIES})
        for family in ab.FAMILIES:
            with self.subTest(family=family.key):
                self.assertTrue(family.tables)
                self.assertTrue(family.queries)
                self.assertEqual(len({t.name for t in family.tables}), len(family.tables))
                self.assertEqual(len({q.name for q in family.queries}), len(family.queries))
                with tempfile.TemporaryDirectory() as directory:
                    counts, digest = ab.generate_family(family, 0.01, 7, Path(directory))
                    self.assertEqual(set(counts), {t.name for t in family.tables})
                    self.assertTrue(all(n > 0 for n in counts.values()))
                    self.assertEqual(len(digest), 64)

    def test_generation_is_deterministic_and_seed_sensitive(self):
        family = ab.FAMILY_BY_KEY["h2o"]
        with tempfile.TemporaryDirectory() as one, tempfile.TemporaryDirectory() as two, \
                tempfile.TemporaryDirectory() as three:
            _, a = ab.generate_family(family, 0.05, 42, Path(one))
            _, b = ab.generate_family(family, 0.05, 42, Path(two))
            _, c = ab.generate_family(family, 0.05, 43, Path(three))
            self.assertEqual(a, b)
            self.assertNotEqual(a, c)

    def test_row_counts_follow_scale(self):
        family = ab.FAMILY_BY_KEY["h2o"]
        with tempfile.TemporaryDirectory() as one, tempfile.TemporaryDirectory() as two:
            small, _ = ab.generate_family(family, 0.05, 1, Path(one))
            large, _ = ab.generate_family(family, 0.10, 1, Path(two))
            for name in small:
                self.assertEqual(large[name], 2 * small[name])

    def test_csv_field_distinguishes_null_from_empty_string(self):
        self.assertEqual(ab.csv_field(None), r"\N")
        self.assertEqual(ab.csv_field(""), '""')
        self.assertEqual(ab.csv_field('a"b'), '"a""b"')
        self.assertEqual(ab.csv_field(12), "12")

    def test_skewed_keys_are_not_uniform(self):
        family = ab.FAMILY_BY_KEY["h2o"]
        with tempfile.TemporaryDirectory() as directory:
            ab.generate_family(family, 0.05, 3, Path(directory))
            rows = (Path(directory) / "x.csv").read_text().splitlines()[1:]
            first = [int(line.split(",")[0]) for line in rows]
            top = max(first.count(v) for v in set(first))
            # Four hundred id1 values: uniform expectation is ~len(rows)/400.
            self.assertGreater(top, len(first) / 400 * 5)

    def test_job_year_kind_correlation(self):
        family = ab.FAMILY_BY_KEY["job"]
        with tempfile.TemporaryDirectory() as directory:
            ab.generate_family(family, 0.02, 5, Path(directory))
            rows = (Path(directory) / "title.csv").read_text().splitlines()[1:]
            old = [r for r in rows if int(r.split(",")[2]) < 1980]
            self.assertTrue(old)
            self.assertTrue(all(r.split(",")[1] == "1" for r in old))


class SqlTest(unittest.TestCase):
    def test_queries_are_single_portable_statements(self):
        forbidden = ("::", "STRFTIME", "DATE_TRUNC", "ILIKE", "REGEXP",
                     "QUALIFY", "ARRAY_AGG", "NULLS LAST")
        for family in ab.FAMILIES:
            known = {t.name for t in family.tables}
            for query in family.queries:
                with self.subTest(query=f"{family.key}:{query.name}"):
                    self.assertNotIn(";", query.sql)
                    self.assertNotIn("LIMIT", query.sql.upper())
                    self.assertFalse(any(w in query.sql.upper() for w in forbidden))
                    self.assertTrue(any(table in query.sql for table in known))

    @unittest.skipUnless(importlib.util.find_spec("duckdb"), "duckdb not installed")
    def test_job_j1_has_a_nonempty_answer(self):
        """The year+kind+genre filters must intersect in the generated data."""
        import duckdb
        family = ab.FAMILY_BY_KEY["job"]
        with tempfile.TemporaryDirectory() as directory:
            ab.generate_family(family, 0.02, 13, Path(directory))
            connection = duckdb.connect(":memory:")
            for table in family.tables:
                connection.execute(table.ddl("duckdb"))
                connection.execute(
                    f"COPY {table.name} FROM '{directory}/{table.name}.csv' "
                    f"(FORMAT CSV, HEADER TRUE, NULL '\\N')")
            result = connection.execute(family.queries[0].sql).fetchall()
            self.assertTrue(result)
            self.assertGreater(result[0][0], 0)
            connection.close()

    @unittest.skipUnless(importlib.util.find_spec("duckdb"), "duckdb not installed")
    def test_every_query_executes_on_generated_data(self):
        import duckdb
        for family in ab.FAMILIES:
            with self.subTest(family=family.key), tempfile.TemporaryDirectory() as directory:
                ab.generate_family(family, 0.01, 11, Path(directory))
                connection = duckdb.connect(":memory:")
                connection.execute("SET threads=1")
                for table in family.tables:
                    connection.execute(table.ddl("duckdb"))
                    connection.execute(
                        f"COPY {table.name} FROM '{directory}/{table.name}.csv' "
                        f"(FORMAT CSV, HEADER TRUE, NULL '\\N')")
                for query in family.queries:
                    with self.subTest(query=query.name):
                        connection.execute(query.sql).fetchall()
                connection.close()


class ResultTest(unittest.TestCase):
    def test_compare_rows(self):
        self.assertIsNone(ab.compare_rows([(1, 1.0), (2, 2.0)], [(2, 2.0), (1, 1.0)]))
        self.assertIsNone(ab.compare_rows([(1, 1.0000001)], [(1, 1.0)]))
        self.assertIsNotNone(ab.compare_rows([(1,)], [(1,), (2,)]))
        self.assertIsNotNone(ab.compare_rows([(1,), (1,)], [(1,), (2,)]))
        self.assertIsNotNone(ab.compare_rows([(1, None)], [(1, 0)]))

    def test_clickhouse_value_parsing(self):
        self.assertEqual(ab._click_value(r"\N", "Nullable(Int64)"), None)
        self.assertEqual(ab._click_value("17", "UInt64"), 17)
        self.assertAlmostEqual(ab._click_value("1.5", "Float64"), 1.5)
        self.assertEqual(ab._click_value("abc", "String"), "abc")

    def test_large_multiset_comparison(self):
        left = [(i % 400, i % 100, float(i) / 7) for i in range(5000)]
        right = [(r[0], r[1], r[2] + 1e-9) for r in reversed(left)]
        self.assertIsNone(ab.compare_rows(left, right))
        self.assertIsNotNone(ab.compare_rows(left, right[:-1] + [(999, 999, 0.0)]))

    def test_tinylamb_literal_uses_column_type(self):
        self.assertEqual(ab._tiny_literal("1", "I"), "1")
        self.assertEqual(ab._tiny_literal("2.5", "F"), "2.5")
        # A numeric-looking string stays a string (ratings, tags, URLs).
        self.assertEqual(ab._tiny_literal("7.5", "S"), "'7.5'")
        self.assertEqual(ab._tiny_literal("a'b", "S"), "'a''b'")
        self.assertEqual(ab._tiny_literal(None, "I"), "NULL")


    def test_metric_table_sums_medians_per_family(self):
        env = {"engines": ["tinylamb", "duckdb", "clickhouse"],
               "families": ["job", "h2o"],
               "family_queries": {"job": ["J1", "J2"], "h2o": ["H1"]}}
        records = [
            {"family": "job", "query": "J1", "engine": "tinylamb", "status": "ok", "median_ms": 10.0},
            {"family": "job", "query": "J2", "engine": "tinylamb", "status": "ok", "median_ms": 5.0},
            {"family": "job", "query": "J1", "engine": "duckdb", "status": "ok", "median_ms": 4.0},
            {"family": "h2o", "query": "H1", "engine": "duckdb", "status": "ok", "median_ms": 3.0},
        ]
        table = ab.metric_table(records, env)
        self.assertEqual(table["job"]["tinylamb"], 15.0)
        self.assertIsNone(table["job"]["duckdb"])  # incomplete family
        self.assertIsNone(table["job"]["clickhouse"])
        self.assertEqual(table["h2o"]["duckdb"], 3.0)

    @unittest.skipUnless(importlib.util.find_spec("matplotlib"), "matplotlib not installed")
    def test_plot_from_saved_results(self):
        env = {"engines": ["tinylamb", "duckdb"], "families": ["job", "h2o"],
               "family_titles": {"job": "JOB (join order)", "h2o": "H2O db-benchmark"},
               "family_queries": {"job": ["J1"], "h2o": ["H1"]},
               "scale": 1.0, "repeats": 3, "available": ["tinylamb", "duckdb"],
               "unavailable": {"clickhouse": "not reachable"}}
        records = [
            {"family": "job", "query": "J1", "engine": "tinylamb", "status": "ok",
             "median_ms": 12.0, "answer_match": True},
            {"family": "job", "query": "J1", "engine": "duckdb", "status": "ok",
             "median_ms": 6.0, "answer_match": True},
            {"family": "h2o", "query": "H1", "engine": "tinylamb", "status": "ok",
             "median_ms": 30.0, "answer_match": True},
            {"family": "h2o", "query": "H1", "engine": "duckdb", "status": "ok",
             "median_ms": 9.0, "answer_match": True},
        ]
        with tempfile.TemporaryDirectory() as directory:
            out = Path(directory)
            (out / "environment.json").write_text(json.dumps(env))
            (out / "results.jsonl").write_text(
                "\n".join(json.dumps(r) for r in records) + "\n")
            ab.plot(out)
            for name in ("grouped_bars.png", "grouped_bars.svg",
                         "per_query.png", "per_query.svg", "summary.json"):
                self.assertTrue((out / name).is_file(), name)
            summary = json.loads((out / "summary.json").read_text())
            self.assertEqual(summary["family_totals_ms"]["job"]["duckdb"], 6.0)
            self.assertEqual(summary["fastest_family"]["h2o"], "duckdb")


class ClickHouseTest(unittest.TestCase):
    def _engine(self):
        args = argparse.Namespace(clickhouse_url="http://127.0.0.1:8123",
                                  clickhouse_database="bench", timeout=5)
        return ab.ClickHouseEngine(args, Path("."))

    def test_ddl_uses_mergetree_and_nullable(self):
        table = ab.Table("t", (ab.Column("id", "I"),
                               ab.Column("note", "S", True)), 10)
        self.assertEqual(table.ddl("clickhouse"),
                         "CREATE TABLE IF NOT EXISTS t (id Int64, "
                         "note Nullable(String)) ENGINE = MergeTree ORDER BY id")
        engine = self._engine()
        seen = []
        engine._request = lambda sql, payload=None: seen.append(sql)
        engine.create_table(table)
        self.assertEqual(seen, ["CREATE TABLE IF NOT EXISTS bench.t (id Int64, "
                                "note Nullable(String)) ENGINE = MergeTree ORDER BY id"])

    def test_auto_start_config_matches_existing_benchmark_profile(self):
        config = ab.CLICKHOUSE_CONFIG.format(root="/tmp/ch", http=28131,
                                             native=29008, threads=2)
        for token in ("<timezone>UTC</timezone>", "<http_port>28131</http_port>",
                      "<tcp_port>29008</tcp_port>", "<max_threads>2</max_threads>",
                      "<listen_host>127.0.0.1</listen_host>",
                      "<use_query_cache>0</use_query_cache>"):
            self.assertIn(token, config)

    def test_sql_is_sent_as_post_body(self):
        import urllib.request
        engine = self._engine()
        seen = {}

        class Response:
            def __enter__(self):
                return self
            def __exit__(self, *args):
                return False
            def read(self):
                return b"1\n"

        def fake_urlopen(request, timeout=None):
            seen["url"] = request.full_url
            seen["body"] = request.data
            return Response()
        original = urllib.request.urlopen
        urllib.request.urlopen = fake_urlopen
        try:
            engine._request("SELECT 1", query={"database": "default"})
            self.assertEqual(seen["body"], b"SELECT 1")
            self.assertIn("database=default", seen["url"])
            engine._request("INSERT INTO t FORMAT CSV", "1,a\n2,b\n")
            self.assertEqual(seen["body"], b"INSERT INTO t FORMAT CSV\n1,a\n2,b\n")
        finally:
            urllib.request.urlopen = original

    def test_readiness_and_setup_use_default_database(self):
        engine = self._engine()
        seen = []
        engine._request = lambda sql, payload=None, query=None: seen.append((sql, query))
        engine.available()
        engine.setup()
        self.assertEqual(seen, [
            ("SELECT 1", {"database": "default"}),
            ("CREATE DATABASE IF NOT EXISTS bench", {"database": "default"}),
        ])

    def test_query_result_parsing(self):
        engine = self._engine()
        engine._request = lambda sql, payload=None: (
            "n\ts\tf\nInt64\tString\tFloat64\n1\tabc\t1.5\n2\t\\N\t2.5\n")
        self.assertEqual(engine.query("SELECT 1"),
                         [(1, "abc", 1.5), (2, None, 2.5)])


class CliTest(unittest.TestCase):
    def test_list_modes(self):
        self.assertEqual(ab.main(["--list-engines"]), 0)
        self.assertEqual(ab.main(["--list-families"]), 0)

    def test_generate_only(self):
        with tempfile.TemporaryDirectory() as directory:
            out = Path(directory) / "run"
            self.assertEqual(ab.main(["--output", str(out), "--generate-only",
                                      "--scale", "0.01", "--families", "h2o"]), 0)
            self.assertTrue((out / "dataset" / "manifest.json").is_file())
            self.assertTrue((out / "dataset" / "h2o" / "x.csv").is_file())


if __name__ == "__main__":
    unittest.main()
