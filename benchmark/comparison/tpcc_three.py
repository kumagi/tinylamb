#!/usr/bin/env python3
"""W=1 TPC-C-derived comparison: shared C++ workload, Tiny/PG/MySQL TCP."""

import argparse
import csv
import hashlib
import json
import os
from pathlib import Path
import random
import re
import shutil
import socket
import subprocess
import time
import threading
from collections import Counter
from types import SimpleNamespace

from run import Client, Stack, append_json
from tpcc_pg import TABLES, KEYS, check, clone
from workloads import ROOT

ENGINES = ('tinylamb', 'postgresql', 'mysql')
ALLOWED_PIDS = {os.getpid()}


def competing_work():
    processes = subprocess.check_output(['ps', '-eo', 'pid=,comm=,args='], text=True).splitlines()
    blocked = {'cc1plus', 'cc1', 'clang', 'clang++', 'rustc', 'ninja', 'cmake',
               'ctest', 'valgrind', 'perf', 'ld', 'ld.lld', 'lto1', 'make', 'gmake'}
    found = Counter()
    for line in processes:
        fields = line.split(None, 2)
        if len(fields) != 3:
            continue
        pid, name, argv = fields
        if int(pid) in ALLOWED_PIDS:
            continue
        if name in blocked or name.endswith('_test'):
            found[name] += 1
        elif name.startswith('python') and 'benchmark/comparison/' in argv:
            found['other_comparison_driver'] += 1
        elif name.startswith(('tinylamb_tpcc', 'tinylamb_tpch', 'tpcc-tcp')):
            found['other_tpc_client'] += 1
    return dict(found)


def wait_quiet():
    announced = False
    quiet_since = None
    while True:
        if competing_work():
            quiet_since = None
            if not announced:
                print('WAIT competing build/test/profiler; preparation is complete', flush=True)
                announced = True
        else:
            if quiet_since is None:
                quiet_since = time.monotonic()
            if time.monotonic() - quiet_since >= 5:
                return
        time.sleep(1)


def schemas():
    source = (ROOT / 'benchmark/tpcc_workload.cpp').read_text()
    text = source.split('constexpr std::array<std::string_view, 9> schema = {', 1)[1].split('};', 1)[0]
    ddls = re.findall(r'((?:"[^"\n]*"\s*)+)(?:,|$)', text, re.M)
    result = [''.join(re.findall(r'"([^"\n]*)"', s)) for s in ddls]
    if len(result) != 9:
        raise ValueError('expected nine TPC-C tables')
    return result


def schema_sql(ddl, engine):
    ddl = re.sub(r'\bINT64\b', 'BIGINT', ddl)
    ddl = re.sub(r'\bNUMERIC\b', 'DOUBLE PRECISION', ddl)
    if engine == 'mysql':
        # Bin collation preserves the generator's case-sensitive name ordering.
        ddl = re.sub(r'\b(c_first|c_last) STRING', r'\1 VARCHAR(32) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin', ddl)
        ddl = re.sub(r'\b(c_middle|c_credit) STRING', r'\1 VARCHAR(2) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin', ddl)
        ddl = re.sub(r'\bSTRING\b', 'VARCHAR(500) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin', ddl)
        return ddl.rstrip(';') + ' ENGINE=InnoDB'
    return re.sub(r'\bSTRING\b', 'TEXT', ddl)


def typed_csv(path, ddl):
    fields = re.findall(r'\b\w+\s+(INT64|NUMERIC|STRING)\b', ddl)
    # QUOTE_NOTNULL (Python >=3.12) distinguishes quoted "" from bare NULL.
    with path.open(newline='') as stream:
        for row in csv.reader(stream, quoting=csv.QUOTE_NOTNULL):
            if len(row) != len(fields):
                raise ValueError('fixture width mismatch')
            yield tuple(None if v is None else int(v) if t == 'INT64' else float(v) if t == 'NUMERIC' else v
                        for v, t in zip(row, fields))


def fingerprint(rows):
    def cell(value):
        if value is None:
            return ['null']
        if isinstance(value, float):
            return ['float', value.hex()]
        return [type(value).__name__, value]
    lines = [json.dumps([cell(v) for v in row], ensure_ascii=True, separators=(',', ':')) for row in rows]
    lines.sort()
    return dict(rows=len(lines), sha256=hashlib.sha256('\n'.join(lines).encode()).hexdigest())


def load(client, output, ddls):
    client.execute(('TRUNCATE ' + ','.join(TABLES)) if client.engine == 'postgresql' else 'SET FOREIGN_KEY_CHECKS=1')
    for table, ddl in zip(TABLES, ddls):
        path = output / f'fixture-{table}.csv'
        if client.engine == 'postgresql':
            with path.open() as stream:
                client.cursor.copy_expert(f'COPY {table} FROM STDIN WITH (FORMAT csv)', stream)
            client.execute('VACUUM (ANALYZE) ' + table)
        else:
            client.execute('TRUNCATE TABLE ' + table)
            batch = []
            for row in typed_csv(path, ddl):
                batch.append(row)
                if len(batch) == 500:
                    placeholders = ','.join(['%s'] * len(row))
                    client.cursor.executemany(f'INSERT INTO {table} VALUES ({placeholders})', batch)
                    batch.clear()
            if batch:
                placeholders = ','.join(['%s'] * len(batch[0]))
                client.cursor.executemany(f'INSERT INTO {table} VALUES ({placeholders})', batch)
            client.execute('ANALYZE TABLE ' + table)


def wait_tiny(stack):
    deadline = time.monotonic() + 60
    while True:
        try:
            c = Client('tinylamb', stack.ports)
            c.close()
            return
        except Exception:
            if time.monotonic() > deadline:
                raise
            time.sleep(.1)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--server', type=Path, required=True)
    parser.add_argument('--client', type=Path, required=True)
    parser.add_argument('--fixture', type=Path, required=True)
    parser.add_argument('--seconds', type=int, default=30)
    parser.add_argument('--warmup', type=int, default=5)
    parser.add_argument('--trials', type=int, default=3)
    parser.add_argument('--clients', type=int, default=10)
    parser.add_argument('--server-cpus', default='0-15')
    parser.add_argument('--cases', nargs='+', help='targeted engine:trial repeats; original records stay unchanged')
    a = parser.parse_args()
    if min(a.seconds, a.trials, a.clients) < 1 or a.warmup < 0:
        parser.error('positive durations/trials/clients required')
    allowed = {f'{e}:{t}' for e in ENGINES for t in range(1, a.trials+1)}
    if a.cases and not set(a.cases) <= allowed:
        parser.error('cases must name selected engine:trial conditions')
    out = a.output.resolve()
    out.mkdir(parents=True, exist_ok=False)
    runtime = out / 'runtime-source'
    runtime.mkdir()
    for source in ['benchmark/tpcc_workload.cpp', 'benchmark/tpcc_workload.hpp',
                   'benchmark/tpcc_tcp_benchmark.cpp', 'benchmark/tpcc_mysql_session.hpp',
                   'benchmark/comparison/tpcc_three.py', 'benchmark/comparison/tpcc_pg.py',
                   'benchmark/comparison/run.py', 'benchmark/comparison/workloads.py', 'CMakeLists.txt']:
        shutil.copyfile(ROOT / source, runtime / Path(source).name)
    sockets = [socket.socket() for _ in ENGINES]
    for s in sockets:
        s.bind(('127.0.0.1', 0))
    ports = [s.getsockname()[1] for s in sockets]
    for s in sockets:
        s.close()
    args = SimpleNamespace(server=str(a.server.resolve()), scratch=str(ROOT/'build/benchmarks'),
        server_cpus=a.server_cpus, tiny_port=ports[0], pg_port=ports[1], mysql_port=ports[2],
        read_workers=8, tiny_query_memory_mib=256, data_cache_mib=512, server_metrics=True,
        postgres_image='postgres@sha256:979bccfde35445db618c65107d27d08c6c21339d8dc3ad4c830c6633947be0e4',
        mysql_image='mysql@sha256:a0225d1ea6631c8bc2385dc4c2faba17bbf1834ecf369e1659a9a382bd8d5dd7')
    stack = Stack(args, out)
    clone(a.fixture, stack.work / 'database')
    try:
        stack.start()
        meta = stack.metadata()
        meta['tpcc_args'] = {k: str(v) if isinstance(v, Path) else v for k, v in vars(a).items()}
        meta['client_sha256'] = hashlib.sha256(a.client.read_bytes()).hexdigest()
        meta['fixture_sha256'] = {p.name: hashlib.file_digest(p.open('rb'), 'sha256').hexdigest()
                                 for p in a.fixture.parent.glob(a.fixture.name + '.*') if p.is_file()}
        meta['runtime_source_sha256'] = {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in runtime.iterdir()}
        meta['mysql_c_library'] = subprocess.check_output(['ldd', str(a.client)], text=True)
        library = re.search(r'libmysqlclient\.so\.\d+ => (\S+)', meta['mysql_c_library'])
        if library:
            meta['mysql_c_library_sha256'] = hashlib.sha256(Path(library[1]).read_bytes()).hexdigest()
        meta['workload_note'] = 'current shared generator; batched SQL shapes may differ from older frozen clients'
        (out / 'environment.json').write_text(json.dumps(meta, indent=2))
        initial, expected = None, {}
        tiny = Client('tinylamb', stack.ports, 120)
        try:
            initial = check(tiny)
            if not initial['valid']:
                raise ValueError(initial)
            for table in TABLES:
                rows = tiny.execute(f'SELECT * FROM {table}')
                expected[table] = fingerprint(rows)
                with (out / f'fixture-{table}.csv').open('w') as stream:
                    for row in rows:
                        stream.write(','.join('' if v is None else '"' + v.replace('"','""') + '"' if isinstance(v,str) else str(v) for v in row) + '\n')
        finally:
            tiny.close()
        (out / 'initial-invariants.json').write_text(json.dumps(initial, indent=2, default=str))
        (out / 'fixture-fingerprints.json').write_text(json.dumps(expected, indent=2))
        ddls = schemas()
        for engine in ENGINES:
            if engine != 'tinylamb':
                c = Client(engine, stack.ports, 120)
                for ddl in ddls:
                    c.execute(schema_sql(ddl, engine))
                for i, (table, key, unique) in enumerate(KEYS):
                    c.execute(f'CREATE {"UNIQUE " if unique else ""}INDEX tpcc_{i} ON {table} ({key})')
                c.close()
            argv = [str(a.client.resolve()), '--port', str(stack.ports[engine]), '--transport-test', '1']
            if engine == 'mysql':
                argv += ['--mysql', '1']
            result = subprocess.run(argv, capture_output=True, text=True, timeout=60)
            (out / f'transport-{engine}.log').write_text(result.stdout + result.stderr)
            if result.returncode:
                raise RuntimeError(f'{engine} transport test failed: {result.stdout} {result.stderr}')
        for trial in range(1, a.trials + 1):
            if a.cases and not any(f'{e}:{trial}' in a.cases for e in ENGINES):
                continue
            stack.process.terminate()
            stack.process.wait(timeout=30)
            db = stack.work / f'database-{trial}'
            clone(a.fixture, db)
            env = {k:v for k,v in os.environ.items() if not k.startswith('TINYLAMB_')}
            env.update(TINYLAMB_PAGE_POOL_BYTES=str(512 << 20), TINYLAMB_QUERY_MEMORY_BYTES=str(256 << 20), TINYLAMB_SERVER_METRICS='1')
            argv = ['taskset', '-c', a.server_cpus, str(a.server.resolve()), str(db), '--port', str(ports[0]), '--read-workers', '8']
            append_json(out/'commands.jsonl', dict(argv=argv, env={k:v for k,v in env.items() if k.startswith('TINYLAMB_')}))
            stack.process = subprocess.Popen(argv, env=env, stdout=stack.log, stderr=subprocess.STDOUT, cwd=ROOT)
            wait_tiny(stack)
            for engine in ENGINES:
                c = Client(engine, stack.ports, 120)
                if engine != 'tinylamb':
                    load(c, out, ddls)
                state = check(c)
                actual = {table: fingerprint(c.execute('SELECT * FROM ' + table)) for table in TABLES}
                c.close()
                valid = state['valid'] and actual == expected
                append_json(out/'fixture-validation.jsonl', dict(engine=engine, trial=trial, valid=valid, fingerprints=actual, invariants=state))
                if not valid:
                    raise RuntimeError(f'{engine}: initial fixture mismatch')
                print('PREPARED', engine, trial, flush=True)
            order = list(ENGINES)
            random.Random(20261106 + trial).shuffle(order)
            for engine in order:
                if a.cases and f'{engine}:{trial}' not in a.cases:
                    continue
                wait_quiet()
                argv = [str(a.client.resolve()), '--port', str(stack.ports[engine]), '--clients', str(a.clients),
                        '--seconds', str(a.seconds), '--warmup', str(a.warmup), '--seed', '20260819']
                if engine == 'mysql':
                    argv += ['--mysql', '1']
                append_json(out/'commands.jsonl', dict(argv=argv, engine=engine, trial=trial))
                print('START', engine, trial, flush=True)
                activity, stopped = [], threading.Event()
                def monitor():
                    while True:
                        activity.append(dict(at=time.time(), competing=competing_work(), load=os.getloadavg()))
                        if stopped.wait(1):
                            return
                try:
                    with (out/f'{engine}-{trial}.log').open('w') as stream:
                        process = subprocess.Popen(argv, stdout=stream, stderr=subprocess.STDOUT)
                        ALLOWED_PIDS.add(process.pid)
                        watcher = threading.Thread(target=monitor)
                        watcher.start()
                        try:
                            process.wait(timeout=a.seconds+a.warmup+120)
                        except BaseException:
                            process.kill()
                            process.wait()
                            raise
                finally:
                    stopped.set()
                    if 'watcher' in locals():
                        watcher.join()
                    if 'process' in locals():
                        ALLOWED_PIDS.discard(process.pid)
                    (out/f'activity-{engine}-{trial}.json').write_text(json.dumps(activity, indent=2))
                text = (out/f'{engine}-{trial}.log').read_text()
                metrics = dict(re.findall(r'^([a-z_0-9.]+)=(.*)$', text, re.M))
                c = Client(engine, stack.ports, 120)
                state = check(c)
                c.close()
                valid = process.returncode == 0 and state['valid']
                append_json(out/'results.jsonl', dict(kind='tpcc', engine=engine, trial=trial, clients=a.clients,
                    seconds=a.seconds, warmup=a.warmup, metrics=metrics, invariants=state, exit_code=process.returncode,
                    timing_qualified=not any(s['competing'] for s in activity), valid=valid))
                print('DONE', engine, trial, metrics.get('tps'), 'valid=', valid, flush=True)
                if not valid:
                    raise RuntimeError(f'{engine}: measurement failed validation')
        (out/'finished.json').write_text(json.dumps(dict(trials=a.trials, engines=3, cases=a.cases, finished=time.strftime('%Y-%m-%dT%H:%M:%S%z'))))
    finally:
        stack.close()


if __name__ == '__main__':
    main()
