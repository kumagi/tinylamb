"""Real Quack sessions: types, rollback, conflicts and multiple writer processes."""

import datetime
from decimal import Decimal
import importlib.util
import multiprocessing as mp
import os
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from duckdb_support import Client, Server
from workloads import equivalent


def write_disjoint(ports, key, result):
    try:
        client = Client(ports)
        for _ in range(10):
            client.execute(f'UPDATE multi_writer SET v=v+1 WHERE id={key}')
        client.close()
        result.put(None)
    except Exception as error:
        result.put(str(error))


@unittest.skipUnless(importlib.util.find_spec('duckdb'), 'install requirements-duckdb.txt')
class DuckDBSupportTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.environment = patch.dict(os.environ)
        cls.environment.start()
        cls.temporary = tempfile.TemporaryDirectory(prefix='quack-test-')
        cls.work = Path(cls.temporary.name)
        with socket.socket() as probe:
            probe.bind(('127.0.0.1', 0))
            port = probe.getsockname()[1]
        cpus = sorted(os.sched_getaffinity(0))[:2]
        cls.args = SimpleNamespace(duckdb_extensions=str(Path(__file__).resolve().parents[2] / 'build/benchmarks/duckdb-extensions-2.0.0'),
                                   server_cpus=','.join(map(str, cpus)), duckdb_port=port,
                                   duckdb_threads=len(cpus), duckdb_memory_mib=128)
        cls.ports = {'duckdb': port}
        cls.server = Server(cls.args, cls.work, cls.work, lambda _: None)
        cls.server.start()
        deadline = time.monotonic() + 20
        while True:
            try:
                client = Client(cls.ports)
                client.close()
                break
            except Exception:
                if time.monotonic() > deadline:
                    cls.server.close()
                    raise
                time.sleep(.1)

    @classmethod
    def tearDownClass(cls):
        cls.server.close()
        cls.environment.stop()
        cls.temporary.cleanup()

    def setUp(self):
        self.a, self.b = Client(self.ports), Client(self.ports)

    def tearDown(self):
        self.a.close()
        self.b.close()

    def test_lossless_types_order_and_null(self):
        actual = self.a.execute("SELECT 9223372036854775807::BIGINT, DATE '2024-02-29', 12.34::DECIMAL(6,2), NULL, 'a''b'")
        self.assertTrue(equivalent(actual, [(9223372036854775807, datetime.date(2024, 2, 29), Decimal('12.34'), None, "a'b")]))
        self.assertIsInstance(actual[0][1], datetime.date)
        self.assertEqual(self.a.execute('SELECT i FROM range(3) t(i) ORDER BY i DESC'), [(2,), (1,), (0,)])

    def test_explicit_rollback_and_visibility(self):
        self.a.execute('CREATE TABLE visibility_test(v BIGINT)')
        self.a.execute('INSERT INTO visibility_test VALUES (1)')
        self.a.execute('BEGIN')
        self.a.execute('UPDATE visibility_test SET v=2')
        self.assertEqual(self.b.execute('SELECT * FROM visibility_test'), [(1,)])
        self.a.execute('ROLLBACK')
        self.assertEqual(self.b.execute('SELECT * FROM visibility_test'), [(1,)])
        self.a.execute('UPDATE visibility_test SET v=3')
        self.assertEqual(self.b.execute('SELECT * FROM visibility_test'), [(3,)])

    def test_conflicts_reported_and_cleanup_allows_next_transaction(self):
        import duckdb
        self.a.execute('CREATE TABLE conflict_test(v BIGINT)')
        self.a.execute('INSERT INTO conflict_test VALUES (0)')
        self.a.execute('BEGIN')
        self.a.execute('UPDATE conflict_test SET v=1')
        with self.assertRaises(duckdb.TransactionException):
            self.b.execute('UPDATE conflict_test SET v=2')
        self.b.execute('ROLLBACK')  # implicit txn was already rolled back
        self.b.execute('BEGIN')
        with self.assertRaises(duckdb.TransactionException):
            self.b.execute('UPDATE conflict_test SET v=2')
        self.b.execute('ROLLBACK')  # failed explicit txn still needs cleanup
        self.a.execute('COMMIT')
        self.b.execute('UPDATE conflict_test SET v=v+1')
        self.assertEqual(self.b.execute('SELECT * FROM conflict_test'), [(2,)])

    def test_multiple_processes_write_disjoint_rows(self):
        self.a.execute('CREATE TABLE multi_writer(id BIGINT PRIMARY KEY, v BIGINT)')
        self.a.execute('INSERT INTO multi_writer VALUES (1,0),(2,0)')
        ctx = mp.get_context('spawn')
        results = ctx.Queue()
        workers = [ctx.Process(target=write_disjoint, args=(self.ports, key, results)) for key in (1, 2)]
        try:
            for worker in workers:
                worker.start()
            for _ in workers:
                self.assertIsNone(results.get(timeout=20))
            for worker in workers:
                worker.join(timeout=10)
                self.assertEqual(worker.exitcode, 0)
        finally:
            for worker in workers:
                if worker.is_alive():
                    worker.terminate()
                    worker.join()
            results.close()
        self.assertEqual(self.a.execute('SELECT * FROM multi_writer ORDER BY id'), [(1, 10), (2, 10)])

    def test_persistent_file_and_remote_server_settings(self):
        values = dict(self.a.execute("SELECT name,value FROM duckdb_settings() WHERE name IN ('threads','memory_limit')"))
        self.assertEqual(int(values['threads']), self.args.duckdb_threads)
        self.assertTrue((self.work / 'duckdb.db').exists())
        self.assertNotEqual(self.a.execute('SELECT current_database()'), [('memory',)])
        size = self.a.execute('PRAGMA database_size')
        self.assertTrue(size)
        self.assertTrue(any(row[0] == 'duckdb' for row in size))

    def test_server_preparation_keeps_mysql_c_driver_loadable(self):
        # A fresh process reproduces the static-TLS import ordering failure.
        code = '''
import os, socket, tempfile
from pathlib import Path
from types import SimpleNamespace
from duckdb_support import Server
with tempfile.TemporaryDirectory() as work:
    with socket.socket() as probe:
        probe.bind(('127.0.0.1', 0))
        port = probe.getsockname()[1]
    args = SimpleNamespace(duckdb_extensions=os.environ['TINYLAMB_BENCH_DUCKDB_EXTENSIONS'],
        server_cpus=str(min(os.sched_getaffinity(0))), duckdb_port=port,
        duckdb_threads=1, duckdb_memory_mib=128)
    server = Server(args, Path(work), Path(work), lambda _: None)
    try:
        server.start()
        import mysql.connector
        assert mysql.connector.HAVE_CEXT, 'MySQL C driver became unavailable'
    finally:
        server.close()
'''
        subprocess.run([sys.executable, '-c', code], cwd=Path(__file__).parent, check=True, timeout=30)


if __name__ == '__main__':
    unittest.main()
