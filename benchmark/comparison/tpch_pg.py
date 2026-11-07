#!/usr/bin/env python3
"""Matched TCP TPC-H snapshot using a private Tiny fixture and private PG."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import socket
import subprocess
import time
from types import SimpleNamespace

import run
from workloads import ROOT, adapt_sql, secondary_indexes, tpch_queries, tpch_schema


class PipeRows:
    """DBGEN trailing delimiter removed; keep all field text unchanged."""
    def __init__(self, stream):
        self.stream, self.pending = stream, ''

    def read(self, size):
        while len(self.pending) < size:
            line = self.stream.readline()
            if not line:
                break
            if not line.endswith('|\n'):
                raise ValueError('unexpected DBGEN record ending')
            self.pending += line[:-2] + '\n'
        result, self.pending = self.pending[:size], self.pending[size:]
        return result


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--output', type=Path, required=True)
    p.add_argument('--fixture', type=Path, required=True)
    p.add_argument('--data-dir', type=Path, required=True)
    p.add_argument('--server', type=Path, default=ROOT / 'build/tinylamb_server')
    p.add_argument('--scale', type=float, default=1)
    p.add_argument('--repeats', type=int, default=9)
    p.add_argument('--queries', nargs='+')
    p.add_argument('--secondary-indexes', action='store_true')
    p.add_argument('--fixture-indexed', action='store_true', help='Tiny fixture already has the common secondary indexes')
    p.add_argument('--bounded-duplicate-indexes', action='store_true', help='append identity columns to the two high-duplicate SF1 keys on BOTH engines')
    p.add_argument('--measure-gate', type=Path, help='wait for this file after preparation; keep builds/tests out of timings')
    p.add_argument('--server-cpus', default='0-15')
    p.add_argument('--timeout', type=int, default=60)
    p.add_argument('--postgres-image', default='postgres@sha256:979bccfde35445db618c65107d27d08c6c21339d8dc3ad4c830c6633947be0e4')
    a = p.parse_args()
    out = a.output.resolve(); out.mkdir(parents=True, exist_ok=False)
    work = ROOT / 'build/benchmarks' / out.name
    work.mkdir(parents=True, exist_ok=False)
    sockets = [socket.socket() for _ in range(2)]
    for s in sockets:
        s.bind(('127.0.0.1', 0))
    ports = dict(zip(('tinylamb', 'postgresql'), (s.getsockname()[1] for s in sockets)))
    for s in sockets:
        s.close()
    name = f'tinylamb-pg15-{os.getpid()}'
    tiny = None
    commands = []
    indexes = secondary_indexes('tpch')
    if a.bounded_duplicate_indexes:
        indexes = [s.replace('ON lineitem (l_shipdate)', 'ON lineitem (l_shipdate, l_orderkey, l_linenumber)')
                    .replace('ON customer (c_mktsegment)', 'ON customer (c_mktsegment, c_custkey)') for s in indexes]
    (out / 'indexes.json').write_text(json.dumps(indexes if a.secondary_indexes else [], indent=2))

    def call(argv):
        commands.append(dict(argv=argv))
        (out / 'commands.json').write_text(json.dumps(commands, indent=2))
        return subprocess.check_output(argv, text=True).strip()

    try:
        for f in a.fixture.parent.glob(a.fixture.name + '.*'):
            if f.is_file():
                shutil.copy2(f, work / ('database' + f.name[len(a.fixture.name):]))
        image = a.postgres_image
        call(['docker', 'run', '-d', '--name', name, '--network', 'host',
              '--cpuset-cpus', a.server_cpus, '-e', 'POSTGRES_HOST_AUTH_METHOD=trust',
              '-e', 'POSTGRES_USER=bench', '-e', 'POSTGRES_DB=bench', image,
              '-p', str(ports['postgresql']), '-c', 'listen_addresses=127.0.0.1',
              '-c', 'shared_buffers=4GB', '-c', 'work_mem=64MB',
              '-c', 'max_parallel_workers_per_gather=4', '-c', 'jit=on',
              '-c', 'fsync=on', '-c', 'synchronous_commit=on'])
        env = {k: v for k, v in os.environ.items() if not k.startswith('TINYLAMB_')}
        env.update(TINYLAMB_PAGE_POOL_BYTES=str(4 << 30), TINYLAMB_QUERY_MEMORY_BYTES=str(2 << 30))
        argv = ['taskset', '-c', a.server_cpus, str(a.server.resolve()), str(work / 'database'),
                '--port', str(ports['tinylamb']), '--read-workers', '8']
        commands.append(dict(argv=argv, env={k: v for k, v in env.items() if k.startswith('TINYLAMB_')}))
        (out / 'commands.json').write_text(json.dumps(commands, indent=2))
        with (out / 'tinylamb-server.log').open('w') as log:
            tiny = subprocess.Popen(argv, env=env, stdout=log, stderr=subprocess.STDOUT, cwd=ROOT)
            for engine in ports:
                deadline = time.monotonic() + 120
                while True:
                    try:
                        c = run.Client(engine, ports, a.timeout); c.close(); break
                    except Exception:
                        if time.monotonic() > deadline:
                            raise
                        time.sleep(.2)
            pg = run.Client('postgresql', ports, a.timeout)
            counts = {}
            for table, ddl, _ in tpch_schema():
                pg.execute(adapt_sql(ddl, 'postgresql'))
                with (a.data_dir / f'{table}.tbl').open() as stream:
                    pg.cursor.copy_expert(f"COPY {table} FROM STDIN WITH (FORMAT csv, DELIMITER '|', QUOTE E'\\x01')", PipeRows(stream))
                counts[table] = pg.execute(f'SELECT COUNT(*) FROM {table}')[0][0]
                print('loaded PG', table, counts[table], flush=True)
            for engine in ports:
                c = run.Client(engine, ports, a.timeout)
                if a.secondary_indexes and not (engine == 'tinylamb' and a.fixture_indexed):
                    for sql in indexes:
                        print('index', engine, sql, flush=True)
                        c.execute(sql)
                for table in counts:
                    actual = c.execute(f'SELECT COUNT(*) FROM {table}')[0][0]
                    assert actual == counts[table], (engine, table, actual)
                    c.execute(('VACUUM (ANALYZE) ' if engine == 'postgresql' else 'ANALYZE ') + table)
                c.close()
            settings = pg.execute("SELECT name, setting FROM pg_settings WHERE name IN ('server_version','shared_buffers','work_mem','max_parallel_workers_per_gather','jit','fsync','synchronous_commit','full_page_writes','wal_sync_method') ORDER BY name")
            meta = dict(args={k: str(v) if isinstance(v, Path) else v for k, v in vars(a).items()},
                        ports=ports, counts=counts, pg_settings=settings, load_start=os.getloadavg(),
                        image=call(['docker', 'image', 'inspect', image]),
                        source_sha256={str(f.relative_to(ROOT)): hashlib.file_digest(f.open('rb'), 'sha256').hexdigest() for f in ROOT.rglob('*.cpp') if not any(x.startswith('build') or x=='.git' for x in f.relative_to(ROOT).parts)},
                        server_sha256=hashlib.file_digest(a.server.open('rb'), 'sha256').hexdigest())
            (out / 'environment.json').write_text(json.dumps(meta, indent=2))
            pg.close()
            (out / 'prepared.json').write_text(json.dumps(dict(prepared=True, timestamp=time.time())))
            if a.measure_gate:
                print('prepared; waiting for measure gate', a.measure_gate, flush=True)
                while not a.measure_gate.exists():
                    time.sleep(.2)
            run.ENGINES = ('tinylamb', 'postgresql')
            queries = tpch_queries(a.scale)
            if a.queries:
                queries = {q: sql for q, sql in queries.items() if q in a.queries}
                assert set(queries) == set(a.queries)
            args = SimpleNamespace(timeout=a.timeout, repeats=a.repeats, seed=20261030, explain_analyze=True)
            run.run_olap(SimpleNamespace(ports=ports), args, out, a.scale, queries, 'tpch')
            records = [json.loads(s) for s in (out / 'results.jsonl').read_text().splitlines()]
            assert all(r['correct'] for r in records), 'incorrect or failed results; inspect records'
            (out / 'finished.json').write_text(json.dumps(dict(correct=len(records), load_end=os.getloadavg()), indent=2))
    finally:
        if tiny is not None:
            tiny.terminate()
            try:
                tiny.wait(timeout=30)
            except subprocess.TimeoutExpired:
                tiny.kill(); tiny.wait()
        with (out / 'postgres.log').open('w') as log:
            subprocess.run(['docker', 'logs', name], stdout=log, stderr=subprocess.STDOUT)
        subprocess.run(['docker', 'rm', '-fv', name], stdout=subprocess.DEVNULL)


if __name__ == '__main__':
    main()
