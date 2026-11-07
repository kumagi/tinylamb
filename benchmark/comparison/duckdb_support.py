"""Optional DuckDB backend using the official Quack whole-session protocol.

Only the server owns the persistent database. Benchmark workers never open it
directly and never retry a transaction or change the benchmark SQL.
"""

import argparse
import hashlib
import os
from pathlib import Path
import secrets
import signal
import subprocess
import sys
import threading


def connect_local(threads=1, database=':memory:', memory_mib=None):
    import duckdb
    config = dict(threads=str(threads), extension_directory=os.environ['TINYLAMB_BENCH_DUCKDB_EXTENSIONS'])
    if memory_mib is not None:
        config['memory_limit'] = f'{memory_mib}MiB'
    connection = duckdb.connect(database, config=config)
    connection.execute('LOAD quack')
    return connection


class Client:
    def __init__(self, ports, timeout=30):
        self.connection = connect_local()
        self.cursor = self.connection
        try:
            token = os.environ['TINYLAMB_BENCH_DUCKDB_TOKEN']
            # Generated hex tokens contain no SQL metacharacters. CONNECT routes
            # the complete statement and transaction to the server, not a local
            # optimizer with partial remote pushdown.
            self.connection.execute(f"ATTACH 'quack:127.0.0.1:{ports['duckdb']}' AS remote_db (TOKEN '{token}')")
            self.connection.execute('CONNECT remote_db')
        except BaseException:
            self.connection.close()
            raise
        # The existing worker supervisor supplies the hard wall-clock deadline.

    def execute(self, sql):
        import duckdb
        try:
            return self.connection.execute(sql).fetchall()
        except duckdb.TransactionException as error:
            # An autocommit conflict has already aborted its implicit txn.
            # Match the other drivers' harmless cleanup ROLLBACK while keeping
            # every conflict visible to the caller and its failure counters.
            if sql.strip().rstrip(';').upper() == 'ROLLBACK' and 'cannot rollback - no transaction is active' in str(error):
                return []
            raise

    def close(self):
        self.connection.close()


class Server:
    def __init__(self, args, work, output, record):
        self.args, self.work, self.output, self.record = args, work, output, record
        self.process = self.log = None

    def start(self):
        extension_dir = Path(self.args.duckdb_extensions).resolve()
        extension_dir.mkdir(parents=True, exist_ok=True)
        os.environ['TINYLAMB_BENCH_DUCKDB_EXTENSIONS'] = str(extension_dir)
        os.environ['TINYLAMB_BENCH_DUCKDB_TOKEN'] = secrets.token_hex(32)
        # Installation is preparation, never part of a timed query/connection.
        # Loading Quack before MySQL's C driver can exhaust glibc's static TLS
        # block. Keep extension preparation in its own process; timed MySQL
        # workers never load DuckDB/Quack at all.
        subprocess.run([sys.executable, str(Path(__file__).resolve()), '--prepare-extensions'], check=True)
        self.extension_hashes = {str(p.relative_to(extension_dir)): hashlib.sha256(p.read_bytes()).hexdigest()
                                 for p in extension_dir.rglob('*.duckdb_extension')}
        invocation = ['taskset', '-c', self.args.server_cpus, sys.executable, str(Path(__file__).resolve()),
                      '--database', str(self.work / 'duckdb.db'), '--port', str(self.args.duckdb_port),
                      '--threads', str(self.args.duckdb_threads), '--memory-mib', str(self.args.duckdb_memory_mib)]
        self.record(invocation)
        self.log = (self.output / 'duckdb-server.log').open('w')
        self.process = subprocess.Popen(invocation, stdout=self.log, stderr=subprocess.STDOUT)

    def close(self):
        if self.process is not None:
            self.process.terminate()
            try:
                self.process.wait(timeout=30)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait()
        if self.log:
            self.log.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--prepare-extensions', action='store_true')
    parser.add_argument('--database')
    parser.add_argument('--port', type=int)
    parser.add_argument('--threads', type=int)
    parser.add_argument('--memory-mib', type=int)
    args = parser.parse_args()
    if args.prepare_extensions:
        import duckdb
        with duckdb.connect(config={'threads': '1', 'extension_directory': os.environ['TINYLAMB_BENCH_DUCKDB_EXTENSIONS']}) as connection:
            connection.execute('INSTALL httpfs')
            connection.execute('LOAD httpfs')
            connection.execute('INSTALL quack')
            connection.execute('LOAD quack')
        return
    if not all((args.database, args.port, args.threads, args.memory_mib)):
        parser.error('server database, port, threads and memory are required')
    stopped = threading.Event()
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, lambda *_: stopped.set())
    connection = connect_local(args.threads, args.database, args.memory_mib)
    uri = f'quack:127.0.0.1:{args.port}'
    token = os.environ['TINYLAMB_BENCH_DUCKDB_TOKEN']
    connection.execute(f"CALL quack_serve('{uri}', token='{token}')").fetchall()
    print('DuckDB Quack ready (persistent database; localhost only)', flush=True)
    try:
        stopped.wait()
    finally:
        connection.execute(f"CALL quack_stop('{uri}')")
        connection.close()


if __name__ == '__main__':
    main()
