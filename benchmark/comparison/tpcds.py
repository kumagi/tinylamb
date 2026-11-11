#!/usr/bin/env python3
"""TPC-DS 99-query embedded comparison, using DuckDB's dsdgen data on both engines.

This is a reproducible engineering comparison, not a certified TPC-DS score.
DECIMAL storage/casts are mapped to binary64 on BOTH engines. Original SQL,
ported SQL, source/binary fingerprints, individual samples and answers survive.
"""
import argparse
import datetime
import hashlib
import json
import math
import os
import platform
from pathlib import Path
import selectors
import shlex
import shutil
import signal
import statistics
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[2]


def dump(path, obj):
    path.write_text(json.dumps(obj, indent=2, ensure_ascii=False, allow_nan=False) + '\n')


def sha(path):
    h = hashlib.sha256()
    with path.open('rb') as f:
        for block in iter(lambda: f.read(1024 * 1024), b''):
            h.update(block)
    return h.hexdigest()


def freeze_build_inputs(binary, cache_text, output):
    """Keep optional profile/allocator inputs needed to reproduce the build."""
    destination = output / 'build-inputs'
    destination.mkdir()
    inputs = {'profiles': [], 'libraries': []}
    profile_paths = set()
    for line in cache_text.splitlines():
        if line.startswith(('CMAKE_C_FLAGS:', 'CMAKE_CXX_FLAGS:')):
            for flag in shlex.split(line.split('=', 1)[1]):
                if flag.startswith('-fprofile-use='):
                    path = Path(flag.split('=', 1)[1])
                    profile_paths.add(path if path.is_absolute() else binary.parent / path)
    for index, path in enumerate(sorted(profile_paths)):
        frozen = destination / f'profile-{index}.profdata'
        shutil.copy2(path, frozen)
        inputs['profiles'].append({'source': str(path.resolve()),
                                   'file': str(frozen.relative_to(output)), 'sha256': sha(frozen)})
    linked = subprocess.check_output(['ldd', str(binary.resolve())], text=True)
    for line in linked.splitlines():
        if ' => /' not in line:
            continue
        path = Path(line.split(' => ', 1)[1].rsplit(' (', 1)[0].strip())
        entry = {'source': str(path.resolve()), 'sha256': sha(path)}
        if path.name.startswith(('libmimalloc', 'libjemalloc')):
            frozen = destination / path.name
            shutil.copy2(path, frozen)
            entry['file'] = str(frozen.relative_to(output))
        inputs['libraries'].append(entry)
    return inputs


def normalize(value):
    if isinstance(value, float) and not math.isfinite(value):
        return {'nonfinite': 'nan' if math.isnan(value) else 'inf' if value > 0 else '-inf'}
    if isinstance(value, (datetime.date, datetime.datetime)):
        return value.isoformat()
    if isinstance(value, (tuple, list)):
        return [normalize(v) for v in value]
    return value


def port_query(sql):
    import sqlglot
    from sqlglot import exp
    from sqlglot.dialects.bigquery import BigQuery

    class TinylambDialect(BigQuery):
        class Generator(BigQuery.Generator):
            # The parser/executor supports explicit null ordering for windows.
            NULL_ORDERING_SUPPORTED = True

    tree = sqlglot.parse_one(sql, read='duckdb')
    for dtype in tree.find_all(exp.DataType):
        if dtype.this == exp.DataType.Type.DECIMAL:
            dtype.replace(exp.DataType.build('DOUBLE'))
    # Explicit NULL placement is emitted by sqlglot where dialect defaults differ.
    return {engine: tree.sql(dialect=dialect, pretty=True)
            for engine, dialect in [('duckdb', 'duckdb'), ('tinylamb', TinylambDialect)]}


def prepare(directory, scale, threads):
    import duckdb
    import sqlglot
    directory.mkdir(parents=True, exist_ok=False)
    c = duckdb.connect(str(directory / 'reference.duckdb'))
    c.execute(f'SET threads={threads}')
    c.execute('INSTALL tpcds; LOAD tpcds')
    c.execute(f'CALL dsdgen(sf={scale})')
    for engine in ('original', 'duckdb', 'tinylamb'):
        (directory / engine).mkdir()
    for nr, query in c.execute('SELECT * FROM tpcds_queries()').fetchall():
        (directory / 'original' / f'q{nr}.sql').write_text(query)
        for engine, sql in port_query(query).items():
            (directory / engine / f'q{nr}.sql').write_text(sql)
    tables = {}
    with (directory / 'manifest.tsv').open('w') as manifest:
        names = [r[0] for r in c.execute('SHOW TABLES').fetchall()]
        for table in names:
            columns = c.execute(f'DESCRIBE {table}').fetchall()
            kinds, tiny_columns, projections = '', [], []
            for name, dtype, *_ in columns:
                if dtype.startswith('DECIMAL'):
                    kind, tiny, target = 'F', 'DOUBLE', 'DOUBLE'
                elif dtype in ('INTEGER', 'BIGINT'):
                    kind, tiny, target = 'I', 'INT64', 'BIGINT'
                elif dtype == 'DATE':
                    kind, tiny, target = 'D', 'DATE', 'DATE'
                elif dtype == 'VARCHAR':
                    kind, tiny, target = 'S', 'STRING', 'VARCHAR'
                else:
                    raise ValueError(dtype)
                kinds += kind
                tiny_columns.append(f'{name} {tiny}')
                projections.append(f'CAST({name} AS {target}) AS {name}')
            c.execute(f'CREATE TABLE converted AS SELECT {", ".join(projections)} FROM {table}')
            c.execute(f'DROP TABLE {table}')
            c.execute(f'ALTER TABLE converted RENAME TO {table}')
            count = 0
            cursor = c.execute(f'SELECT * FROM {table}')
            with (directory / f'{table}.tsv').open('w') as f:
                while batch := cursor.fetchmany(10000):
                    for row in batch:
                        f.write('\t'.join('\\N' if v is None else str(v).encode().hex()
                                          if k == 'S' else str(v)
                                          for k, v in zip(kinds, row, strict=True)) + '\n')
                    count += len(batch)
            ddl = f'CREATE TABLE {table} ({", ".join(tiny_columns)})'
            manifest.write(f'{table}\t{kinds}\t{count}\t{ddl}\n')
            tables[table] = {'rows': count, 'sha256': sha(directory / f'{table}.tsv')}
            print('generated', table, count, flush=True)
    c.execute('ANALYZE')
    c.execute('CHECKPOINT')
    c.close()
    dump(directory / 'dataset.json', {
        'scale_factor': scale, 'duckdb': duckdb.__version__, 'sqlglot': sqlglot.__version__,
        'generator': 'DuckDB tpcds dsdgen', 'numeric': 'binary64 on both engines',
        'tables': tables, 'queries': 99, 'query_parameter_scale': 1,
        'query_sha256': {str(p.relative_to(directory)): sha(p)
                         for d in ('original', 'duckdb', 'tinylamb') for p in (directory / d).glob('*.sql')},
        'manifest_sha256': sha(directory / 'manifest.tsv')})


def duck_worker(database, queries, threads):
    import duckdb
    c = duckdb.connect(database, read_only=True)
    c.execute(f'SET threads={threads}')
    print(json.dumps({'ready': True}), flush=True)
    for name in sys.stdin:
        try:
            sql = (Path(queries) / name.strip()).read_text()
            start = time.perf_counter_ns()
            rows = c.execute(sql).fetchall()
            ms = (time.perf_counter_ns() - start) / 1e6
            print(json.dumps({'ms': ms, 'rows': normalize(rows)}, allow_nan=False), flush=True)
        except Exception as error:
            print(json.dumps({'error': str(error)}), flush=True)


class Worker:
    def __init__(self, command, stderr, timeout):
        self.command, self.stderr, self.timeout = command, stderr, timeout
        self.process = None
        self.selector = selectors.DefaultSelector()

    def receive(self):
        if not self.selector.select(self.timeout):
            self.close()
            raise TimeoutError(f'query exceeded {self.timeout}s')
        line = self.process.stdout.readline()
        if not line:
            code = self.process.poll()
            self.close()
            raise RuntimeError(f'worker exited ({code})')
        return json.loads(line)

    def query(self, name):
        try:
            if self.process is None:
                self.process = subprocess.Popen(self.command, stdin=subprocess.PIPE,
                                                stdout=subprocess.PIPE, stderr=self.stderr,
                                                text=True, start_new_session=True)
                self.selector.register(self.process.stdout, selectors.EVENT_READ)
                if self.receive() != {'ready': True}:
                    raise RuntimeError('worker did not become ready')
            self.process.stdin.write(name + '\n')
            self.process.stdin.flush()
            return self.receive()
        except Exception as error:
            self.close()
            return {'error': str(error)}

    def close(self):
        process, self.process = self.process, None
        if process is None:
            return
        self.selector.unregister(process.stdout)
        if process.poll() is None:
            try:
                os.killpg(process.pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            os.killpg(process.pid, signal.SIGKILL)
            process.wait()
        process.stdin.close()
        process.stdout.close()


def cell_equal(a, b):
    if isinstance(a, int) and isinstance(b, int):
        return a == b
    if isinstance(a, (int, float)) and isinstance(b, (int, float)):
        return math.isclose(a, b, rel_tol=1e-9, abs_tol=1e-7)
    return a == b


def compare(a, b):
    # Bag comparison preserves duplicate multiplicities; ORDER BY ties are legal.
    # Strict LIMIT boundary discrepancies remain failures, never silently dropped.
    if len(a) != len(b):
        return f'row count {len(a)} != {len(b)}'
    unused = list(b)
    for row in a:
        for i, other in enumerate(unused):
            if len(row) == len(other) and all(cell_equal(x, y) for x, y in zip(row, other)):
                unused.pop(i)
                break
        else:
            return f'unmatched row {row!r}'
    return None


def report(out):
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    records = [json.loads(line) for line in (out / 'results.jsonl').read_text().splitlines()]
    records.sort(key=lambda record: record['query'])
    good = [r for r in records if r['status'] == 'ok']
    fig, axes = plt.subplots(3, 1, figsize=(20, 12), constrained_layout=True)
    for ax, start in zip(axes, (1, 34, 67)):
        subset = [r for r in records if start <= r['query'] < start + 33]
        for j, r in enumerate(subset):
            if r['status'] != 'ok':
                ax.text(j, .1, r['status'], rotation=90, fontsize=7, color='crimson')
                continue
            for offset, engine, color in [(-.19, 'tinylamb', '#2678b2'), (.19, 'duckdb', '#ec9b24')]:
                samples = r[engine]['samples_ms']
                median = statistics.median(samples)
                ax.bar(j + offset, median, width=.37, color=color, label=engine if j == 0 else '')
                ax.errorbar(j + offset, median, yerr=[[median - min(samples)], [max(samples) - median]],
                            color='black', capsize=1, linewidth=.5)
        ax.set_xticks(range(len(subset)), [f'Q{r["query"]}' for r in subset])
        ax.set_yscale('log'); ax.set_ylabel('Execute + collect (ms, log)')
        ax.grid(axis='y', alpha=.2)
    from matplotlib.patches import Patch
    axes[0].legend(handles=[Patch(color='#2678b2', label='tinylamb'), Patch(color='#ec9b24', label='DuckDB')])
    meta = json.loads((out / 'environment.json').read_text())
    fig.suptitle(f'TPC-DS derived / dsdgen SF={meta["dataset"]["scale_factor"]} / binary64 / '
                 f'{len(good)}/{len(records)} answers matched\nMedian with min/max; warm embedded APIs; not a certified TPC score')
    fig.savefig(out / 'latency.png', dpi=170)
    fig.savefig(out / 'latency.svg')
    plt.close(fig)
    ratios = [statistics.median(r['tinylamb']['samples_ms']) / statistics.median(r['duckdb']['samples_ms']) for r in good]
    summary = {'matched': len(good), 'attempted': len(records),
               'tinylamb_wins': sum(ratio < 1 for ratio in ratios),
               'duckdb_wins': sum(ratio > 1 for ratio in ratios),
               'tinylamb_nonoverlapping_wins': sum(max(r['tinylamb']['samples_ms']) < min(r['duckdb']['samples_ms']) for r in good),
               'empty_answers': [r['query'] for r in good if not r['tinylamb']['rows']],
               'nonempty_queries': sum(bool(r['tinylamb']['rows']) for r in good),
               'tinylamb_nonempty_wins': sum(bool(r['tinylamb']['rows']) and ratio < 1 for r, ratio in zip(good, ratios)),
               'comparison': 'multiset (duplicate-sensitive), exact integers/nulls, float rel_tol=1e-9 abs_tol=1e-7; output order not checked',
               'geomean_tinylamb_over_duckdb': math.exp(statistics.mean(map(math.log, ratios))) if ratios else None,
               'sum_median_ms': {e: sum(statistics.median(r[e]['samples_ms']) for r in good) for e in ('tinylamb', 'duckdb')},
               'failures': [{'query': r['query'], 'status': r['status'], 'detail': r.get('detail')} for r in records if r['status'] != 'ok']}
    dump(out / 'summary.json', summary)
    fig, ax = plt.subplots(figsize=(19, 5), constrained_layout=True)
    speedups = [1 / ratio for ratio in ratios]
    ax.bar(range(len(good)), [x - 1 for x in speedups], bottom=1,
           color=['#2678b2' if x > 1 else '#ec9b24' for x in speedups])
    ax.axhline(1, color='black', linewidth=1)
    ax.set_yscale('log')
    if speedups:
        ax.set_ylim(min(speedups) * .7, max(speedups) * 1.3)
    ax.set_xticks(range(len(good)), [str(r['query']) for r in good], fontsize=6)
    ax.set_xlabel('TPC-DS query')
    ax.set_ylabel('DuckDB / tinylamb median time (log)')
    ax.grid(axis='y', alpha=.2)
    ax.set_title(f'TPC-DS derived / SF={meta["dataset"]["scale_factor"]} / {meta["args"]["threads"]} threads / '
                 f'tinylamb faster on {summary["tinylamb_wins"]}/{len(records)} queries\n'
                 'Above 1: tinylamb faster; below 1: DuckDB faster. Binary64; not a certified TPC score.')
    fig.savefig(out / 'speedup.png', dpi=180)
    fig.savefig(out / 'speedup.svg')
    plt.close(fig)
    print(json.dumps(summary, indent=2))


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--output', type=Path)
    p.add_argument('--dataset', type=Path, help='reuse a generated dataset')
    p.add_argument('--fixture', type=Path, help='reuse a loaded native DB (no other process may open it)')
    p.add_argument('--binary', type=Path, default=ROOT / 'build/tinylamb_tpcds_compare')
    p.add_argument('--scale', type=float, default=.1)
    p.add_argument('--repeats', type=int, default=3)
    p.add_argument('--warmup', type=int, default=1)
    p.add_argument('--threads', type=int, default=2)
    p.add_argument('--cpus', help='comma-separated logical CPUs (default: first --threads allowed CPUs)')
    p.add_argument('--timeout', type=float, default=60)
    p.add_argument('--queries', nargs='+', type=int, default=list(range(1, 100)))
    p.add_argument('--prepare-only', action='store_true')
    p.add_argument('--profile', action='store_true', help='native plan timing and EXPLAIN dump; diagnostic run')
    p.add_argument('--report', type=Path)
    p.add_argument('--duck-worker', nargs=3, metavar=('DB', 'QUERIES', 'THREADS'))
    a = p.parse_args()
    if a.duck_worker:
        duck_worker(a.duck_worker[0], a.duck_worker[1], int(a.duck_worker[2])); return
    if a.report:
        report(a.report); return
    if not a.output or a.scale <= 0 or a.repeats < 1 or a.warmup < 0 or a.threads < 1:
        p.error('output, positive scale/repeats/threads and nonnegative warmup required')
    if len(set(a.queries)) != len(a.queries) or any(q < 1 or q > 99 for q in a.queries):
        p.error('queries must be distinct numbers 1..99')
    cpus = list(map(int, a.cpus.split(','))) if a.cpus else sorted(os.sched_getaffinity(0))[:a.threads]
    os.sched_setaffinity(0, cpus)
    out = a.output.resolve(); out.mkdir(parents=True, exist_ok=False)
    dataset = a.dataset.resolve() if a.dataset else out / 'dataset'
    if not a.dataset:
        prepare(dataset, a.scale, a.threads)
    metadata = json.loads((dataset / 'dataset.json').read_text())
    if sha(dataset / 'manifest.tsv') != metadata['manifest_sha256']:
        raise ValueError('dataset manifest changed')
    for table, info in metadata['tables'].items():
        if sha(dataset / f'{table}.tsv') != info['sha256']:
            raise ValueError('dataset changed: ' + table)
    for file, digest in metadata['query_sha256'].items():
        if sha(dataset / file) != digest:
            raise ValueError('query changed: ' + file)
    binary = out / 'tinylamb_tpcds_compare'
    shutil.copy2(a.binary, binary)
    (out / 'source.patch').write_bytes(subprocess.check_output(['git', 'diff', 'HEAD'], cwd=ROOT))
    sources = out / 'benchmark-source'
    sources.mkdir()
    for relative in ('benchmark/tpcds_compare.cpp', 'benchmark/comparison/tpcds.py',
                     'benchmark/comparison/tpcds_test.py', 'benchmark/comparison/requirements-tpcds.txt',
                     'benchmark/comparison/README-tpcds.md'):
        shutil.copy2(ROOT / relative, sources / Path(relative).name)
    cache_text = (a.binary.parent / 'CMakeCache.txt').read_text()
    (out / 'CMakeCache.txt').write_text(cache_text)
    build_inputs = freeze_build_inputs(a.binary, cache_text, out)
    additional_sources = out / 'additional-source'
    untracked = subprocess.check_output(
        ['git', 'ls-files', '--others', '--exclude-standard', '-z'], cwd=ROOT).decode().split('\0')
    for relative in untracked:
        path = Path(relative)
        if path.suffix not in ('.cpp', '.hpp') or path.parts[0] not in {
                'benchmark', 'common', 'database', 'executor', 'expression', 'index',
                'page', 'plan', 'query', 'recovery', 'server', 'table', 'transaction', 'type'}:
            continue
        frozen = additional_sources / path
        frozen.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(ROOT / path, frozen)
    compiler = next(line.split('=', 1)[1] for line in cache_text.splitlines()
                    if line.startswith('CMAKE_CXX_COMPILER:'))
    import duckdb
    dump(out / 'environment.json', {'args': vars(a) | {'output': str(out), 'dataset': str(dataset), 'binary': str(a.binary),
                                                       'fixture': str(a.fixture) if a.fixture else None},
                                   'dataset': metadata, 'duckdb': duckdb.__version__, 'cpus': cpus,
                                   'build_inputs': build_inputs,
                                   'runtime_environment': {key: value for key, value in sorted(os.environ.items())
                                                           if key.startswith(('TINYLAMB_', 'MIMALLOC_'))},
                                   'python': sys.version, 'platform': platform.platform(),
                                   'compiler': subprocess.check_output([compiler, '--version'], text=True).strip(),
                                   'load_average': os.getloadavg(),
                                   'cpu_model': Path('/proc/cpuinfo').read_text().split('model name')[1].splitlines()[0],
                                   'git_head': subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip(),
                                   'binary_sha256': sha(binary), 'started_utc': datetime.datetime.now(datetime.timezone.utc).isoformat(),
                                   'timing': 'embedded Execute + full result collection; serialization, transaction setup and load excluded'})
    fixture = a.fixture.resolve() if a.fixture else out / 'tinylamb'
    fixture_metadata = Path(str(fixture) + '.dataset.json')
    data_identity = {key: metadata[key] for key in ('scale_factor', 'numeric', 'manifest_sha256', 'tables')}
    if a.fixture:
        if not fixture_metadata.exists() or json.loads(fixture_metadata.read_text()) != data_identity:
            raise ValueError('native fixture has no matching dataset provenance; load a fresh fixture without --fixture')
    if not a.fixture:
        with (out / 'load.log').open('w') as log:
            subprocess.run([str(binary), str(fixture), '--load', str(dataset)], stdout=log, stderr=log, check=True)
        dump(fixture_metadata, data_identity)
    if a.prepare_only:
        print('prepared', dataset, fixture); return
    workers, logs = {}, []
    try:
        for e in ('tinylamb', 'duckdb'):
            log = (out / f'{e}-worker.log').open('w'); logs.append(log)
            command = ([str(binary), str(fixture), '--serve', str(dataset / e), str(a.threads)] + (['--profile'] if a.profile else []) if e == 'tinylamb' else
                       [sys.executable, str(Path(__file__).resolve()), '--duck-worker', str(dataset / 'reference.duckdb'), str(dataset / e), str(a.threads)])
            workers[e] = Worker(command, log, a.timeout)
        with (out / 'results.jsonl').open('w') as results:
            for q in a.queries:
                record = {'query': q, 'status': 'ok', 'load_average_before': os.getloadavg()}
                for e in (('tinylamb', 'duckdb') if q % 2 else ('duckdb', 'tinylamb')):
                    samples, answers, prepares = [], [], []
                    for i in range(a.warmup + a.repeats):
                        response = workers[e].query(f'q{q}.sql')
                        if 'error' in response:
                            record['status'] = 'error'; record['detail'] = e + ': ' + response['error']; break
                        answers.append(response['rows'])
                        if i >= a.warmup:
                            samples.append(response['ms'])
                            prepares.append(response.get('prepare_ms'))
                    record[e] = {'samples_ms': samples, 'prepare_ms': prepares, 'rows': answers[-1] if answers else None}
                    if answers and any(compare(answers[0], rows) for rows in answers[1:]):
                        record['status'] = 'unstable'; record['detail'] = e + ': answers changed between runs'
                if record['status'] == 'ok':
                    mismatch = compare(record['tinylamb']['rows'], record['duckdb']['rows'])
                    if mismatch:
                        record['status'] = 'mismatch'; record['detail'] = mismatch
                record['load_average_after'] = os.getloadavg()
                results.write(json.dumps(record, allow_nan=False) + '\n'); results.flush()
                print(q, record['status'], record.get('detail', ''), flush=True)
    finally:
        for worker in workers.values(): worker.close()
        for log in logs: log.close()
    report(out)
    if any(json.loads(line)['status'] != 'ok' for line in (out / 'results.jsonl').read_text().splitlines()):
        raise SystemExit(1)


if __name__ == '__main__':
    main()
