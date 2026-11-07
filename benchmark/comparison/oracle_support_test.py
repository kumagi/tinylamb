"""Oracle dialect and transaction lifecycle regression checks (no server)."""

import datetime
from types import SimpleNamespace
import unittest

from oracle_support import Client, adapt_sql
from workloads import adapt_sql as portable_sql


class OracleSupportTest(unittest.TestCase):
    def test_types_limit_and_literals(self):
        self.assertEqual(adapt_sql('CREATE TABLE t (id BIGINT, v DOUBLE PRECISION, s VARCHAR(255));'),
                         'CREATE TABLE t (id NUMBER(19), v BINARY_DOUBLE, s VARCHAR2(255))')
        self.assertEqual(adapt_sql("SELECT 'BIGINT DOUBLE PRECISION LIMIT 5' AS s FROM t LIMIT 10;"),
                         "SELECT 'BIGINT DOUBLE PRECISION LIMIT 5' AS s FROM t FETCH FIRST 10 ROWS ONLY")
        self.assertEqual(adapt_sql("SELECT 'it''s VARCHAR' FROM t"), "SELECT 'it''s VARCHAR' FROM t")
        self.assertEqual(portable_sql("date_sub(date '1998-12-01', INTERVAL 74 day)", 'oracle'),
                         "DATE '1998-09-18'")

    def test_explicit_transactions_never_autocommit_intermediate_writes(self):
        calls = []
        c = Client.__new__(Client)
        c.connection = SimpleNamespace(autocommit=True, commit=lambda: calls.append('COMMIT'),
                                       rollback=lambda: calls.append('ROLLBACK'))
        c.cursor = SimpleNamespace(execute=lambda sql: calls.append((sql, c.connection.autocommit)), description=None)
        c.explicit_transaction = False
        c.execute('BEGIN')
        c.execute('UPDATE accounts SET balance = balance - 1 WHERE id = 1')
        c.execute('UPDATE accounts SET balance = balance + 1 WHERE id = 2')
        self.assertEqual([x[1] for x in calls], [False, False])
        c.execute('COMMIT')
        self.assertEqual(calls[-1], 'COMMIT')
        self.assertTrue(c.connection.autocommit)
        c.execute('BEGIN')
        with self.assertRaises(RuntimeError):
            c.execute('BEGIN')
        c.execute('ROLLBACK')
        self.assertEqual(calls[-1], 'ROLLBACK')
        self.assertTrue(c.connection.autocommit)

    def test_only_date_columns_with_zero_time_are_normalized(self):
        c = Client.__new__(Client)
        c.date_type = object()
        stamp = datetime.datetime(2026, 10, 31)
        c.cursor = SimpleNamespace(execute=lambda sql: None,
                                   description=[SimpleNamespace(type_code=c.date_type),
                                                SimpleNamespace(type_code=object())],
                                   fetchall=lambda: [(stamp, stamp)])
        self.assertEqual(c.execute('SELECT d, t FROM x'), [(stamp.date(), stamp)])


if __name__ == '__main__':
    unittest.main()
