"""Answer verification must not hide wrong results behind LIMIT ties."""
import unittest
import json
import tempfile
from pathlib import Path
from unittest.mock import patch
import clickbench


class Client:
    def __init__(self, rows):
        self.rows = rows
        self.sql = None

    def query(self, sql):
        self.sql = sql
        return {'rows': self.rows}


class SessionTest(unittest.TestCase):
    def test_utc_probe_records_verified_setting(self):
        with tempfile.TemporaryDirectory() as directory:
            client = Client([['2000-01-02 00:00:00+00']])
            result = clickbench.check_native_utc(client, Path(directory))
            self.assertEqual(result['timezone'], 'UTC')
            self.assertIn('DAY', Path(client.sql).read_text())

    def test_utc_probe_rejects_other_zone_and_errors(self):
        with tempfile.TemporaryDirectory() as directory:
            for rows in [[['2000-01-01 08:00:00+00']], [], None]:
                with self.subTest(rows=rows), self.assertRaises(ValueError):
                    clickbench.check_native_utc(Client(rows), Path(directory))


class VerificationTest(unittest.TestCase):
    meta = [{'name': 'key', 'type': 'String'}, {'name': 'n', 'type': 'UInt64'}]
    sql = 'SELECT key, COUNT(*) AS n FROM hits GROUP BY key ORDER BY n DESC LIMIT 1'

    def verify(self, native, expected, complete, sql=None):
        client = Client(complete)
        result = clickbench.verify({'rows': native}, {'rows': expected, 'meta': self.meta},
                                  sql or self.sql, client)
        return result, client

    def test_legal_tie_requires_membership(self):
        result, client = self.verify([['b', 3]], [['a', 3]], [['b', 3]])
        self.assertIsNone(result)
        self.assertNotIn('LIMIT', client.sql)
        self.assertIsNotNone(self.verify([['bad', 3]], [['a', 3]], [])[0])

    def test_wrong_rank_is_rejected(self):
        result, client = self.verify([['b', 2]], [['a', 3]], [['b', 2]])
        self.assertIsNotNone(result)
        self.assertIsNone(client.sql)

    def test_duplicate_group_is_rejected(self):
        self.assertIsNotNone(self.verify([['b', 3], ['b', 3]], [['a', 3], ['c', 3]],
                                        [['b', 3]])[0])

    def test_missing_rows_are_rejected(self):
        self.assertIsNotNone(self.verify([], [['a', 3]], [])[0])

    def test_unordered_limit_checks_payload(self):
        sql = 'SELECT key, COUNT(*) AS n FROM hits GROUP BY key LIMIT 1'
        self.assertIsNone(self.verify([['b', 2]], [['a', 3]], [['b', 2]], sql)[0])
        self.assertIsNotNone(self.verify([['b', 99]], [['a', 3]], [['b', 2]], sql)[0])

    def test_no_limit_requires_exact_bag(self):
        self.assertIsNotNone(self.verify([['b', 3]], [['a', 3]], [['b', 3]],
                                         self.sql.replace(' LIMIT 1', ''))[0])

    def test_temporal_normalization_is_type_directed(self):
        row = [['2020-01-01 12:30:00+00', 'literal+00']]
        types = [{'type': 'DateTime'}, {'type': 'String'}]
        self.assertEqual(clickbench.normalize_rows(row, types),
                         [['2020-01-01T12:30:00+00:00', 'literal+00']])

    def test_integral_json_float_uses_declared_type(self):
        metadata = [{'name': 'a', 'type': 'Float64'}]
        self.assertIsNone(clickbench.verify({'rows': [[54675732684487872]]},
            {'rows': [[54675732684487870]], 'meta': metadata},
            'SELECT AVG(x) FROM hits', Client([])))
        self.assertIsNotNone(clickbench.verify({'rows': [[54675732684487872]]},
            {'rows': [[54675732684487870]], 'meta': [{'name': 'a', 'type': 'Int64'}]},
            'SELECT COUNT(*) FROM hits', Client([])))

    def test_dialect_preserves_byte_length_and_floating_average(self):
        ch, native = clickbench.translate('SELECT AVG(UserID), length(URL) FROM hits')
        self.assertIn('AVG(CAST(UserID AS DOUBLE))', ch)
        self.assertIn('BYTE_LENGTH(URL)', native)
        self.assertIn('AVG(CAST(UserID AS FLOAT64))', native)

    def test_rank_alias_and_expression(self):
        self.assertEqual(clickbench.order_slots(self.sql), [1])
        self.assertEqual(clickbench.order_slots(self.sql.replace('BY n DESC', 'BY COUNT(*) DESC')), [1])
        self.assertIsNone(clickbench.order_slots('SELECT key FROM hits ORDER BY hidden LIMIT 2'))



class DuckDBComparisonTest(unittest.TestCase):
    def test_byte_length_and_temporal_normalization(self):
        import duckdb
        connection = duckdb.connect()
        self.addCleanup(connection.close)
        sql = clickbench.duckdb_sql("SELECT LENGTH('é')")
        self.assertEqual(connection.execute(sql).fetchall(), [(2,)])
        self.assertEqual(clickbench.normalize_rows(
            [['2020-01-01T12:30:00', 'literal+00', 1]],
            [{'type': 'TIMESTAMP'}, {'type': 'VARCHAR'}, {'type': 'DOUBLE'}]),
            [['2020-01-01T12:30:00+00:00', 'literal+00', 1.0]])

    def test_duckdb_worker_verifies_ties_and_rejects_bad_payloads(self):
        import duckdb
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / 'reference.duckdb'
            with duckdb.connect(str(database)) as connection:
                connection.execute('CREATE TABLE hits(key VARCHAR)')
                connection.executemany('INSERT INTO hits VALUES (?)', [('é',), ('é',), ('a\\\n',), ('a\\\n',)])
            with (Path(directory) / 'worker.log').open('w') as log:
                client = clickbench.DuckDB(database, 1, 10, log)
                try:
                    sql = 'SELECT key,COUNT(*) AS n FROM hits GROUP BY key ORDER BY n DESC LIMIT 1'
                    reference = client.query(sql)
                    self.assertNotIn('error', reference)
                    other = 'é' if reference['rows'][0][0] != 'é' else 'a\\\n'
                    self.assertIsNone(clickbench.verify({'rows': [[other, 2]]}, reference, sql, client))
                    self.assertIsNotNone(clickbench.verify({'rows': [['missing', 2]]}, reference, sql, client))
                    self.assertIsNotNone(clickbench.verify({'rows': [[other, 3]]}, reference, sql, client))
                finally:
                    client.close()

    def test_prepare_uses_utc_independently_of_local_timezone(self):
        import argparse
        import duckdb
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            parquet = root / 'source.parquet'
            with duckdb.connect() as connection:
                connection.execute("COPY (SELECT 0::BIGINT AS EventTime, 0::INTEGER AS EventDate, "
                                   "encode('é') AS URL) TO ? (FORMAT PARQUET)", [str(parquet)])
            upstream = root / 'upstream'
            (upstream / 'clickhouse').mkdir(parents=True)
            (upstream / 'clickhouse' / 'queries.sql').write_text('SELECT COUNT(*) FROM hits;\n' * 43)
            (upstream / 'clickhouse' / 'create.sql').write_text('unused')
            dataset = root / 'dataset'
            args = argparse.Namespace(upstream=upstream, output=dataset, parquet=[parquet], queries_only=False)
            with patch('clickbench.subprocess.check_output', return_value=clickbench.COMMIT), \
                 patch.dict('os.environ', {'TZ': 'Asia/Tokyo'}):
                clickbench.prepare(args)
            fields = (dataset / 'hits.tsv').read_text().strip().split('\t')
            self.assertEqual(bytes.fromhex(fields[0]).decode(), '1970-01-01 00:00:00+00')
            self.assertEqual(fields[1], '1970-01-01')
            self.assertEqual(bytes.fromhex(fields[2]).decode(), 'é')
            self.assertEqual(json.loads((dataset / 'metadata.json').read_text())['timezone'], 'UTC')
            database = root / 'reference.duckdb'
            clickbench.init_duckdb(argparse.Namespace(dataset=dataset, duckdb=database, threads=1))
            with duckdb.connect(str(database), read_only=True) as connection:
                self.assertEqual(connection.execute('SELECT epoch(EventTime),EventDate,URL FROM hits').fetchone()[::2],
                                 (0.0, 'é'))

if __name__ == '__main__':
    unittest.main()
