"""Offline checks for the generated SQL, dataset semantics and reference results."""
import bz2
import sqlite3
import tempfile
import unittest
from pathlib import Path

import cloud_bi_bench as bench


class CloudBIQueriesTest(unittest.TestCase):
    def check_fixture(self, fixture):
        tables, cases = fixture
        conn = sqlite3.connect(":memory:")
        try:
            for schema, rows in tables:
                cols = ", ".join(c.name + (" TEXT" if c.kind == "S" else " INTEGER")
                                 for c in schema.columns)
                conn.execute(f"CREATE TABLE {schema.name} ({cols})")
                slots = ", ".join("?" for _ in schema.columns)
                conn.executemany(f"INSERT INTO {schema.name} VALUES ({slots})", rows)
            for name, sql, expected in cases:
                with self.subTest(case=name):
                    bench.validate(conn.execute(sql).fetchall(), expected)
        finally:
            conn.close()

    def test_iceberg_delete_shapes(self):
        tables, cases = bench.iceberg_fixture()
        self.check_fixture((tables, cases))
        counts = {name: expected[0][0] for name, _, expected in cases}
        self.assertEqual(counts["position"], 96 * 45)  # 3 live target positions per file
        self.assertLess(counts["equality"], counts["base"])
        self.assertGreater(counts["both / part 0"], 0)

    def test_branch_factorization_including_join_free_and_join_only_shapes(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "trace.csv"
            for freq in ("0", "1", "0.125"):
                path.write_text(f"join\n{freq}\n")
                with self.subTest(join_fraction=freq):
                    self.check_fixture(bench.pbench_fixture(path))

    def test_mlb_duplicate_group_key_and_nullable_extrema(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "MLB.csv.bz2"
            with bz2.open(path, "wt") as data:
                for pos in ("Center Fielder", "Center Fielder", "Pitcher"):
                    row = ["null"] * 40
                    row[28], row[30], row[37], row[12] = "PCL", pos, "2015", "-0.125"
                    data.write("|".join(row) + "\n")
            tables, cases = bench.mlb_fixture(path)
            self.check_fixture((tables, cases))
            result = next(values for name, _, values in cases if name == "MLB Q108")
            self.assertEqual(result[0][:6], (None, None, None, None, -125, -125))

    def test_mlb_no_matching_q108_group(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "MLB.csv.bz2"
            row = ["null"] * 40
            row[28], row[30], row[37] = "IL", "Pitcher", "2015"
            path.write_bytes(bz2.compress(("|".join(row) + "\n").encode()))
            self.check_fixture(bench.mlb_fixture(path))

    def test_validate_distinguishes_null_and_text(self):
        with self.assertRaises(ValueError):
            bench.validate([(None,)], [("None",)])


if __name__ == "__main__":
    unittest.main()
