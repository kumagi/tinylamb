#!/usr/bin/env python3
"""Reproducible ClickBench subset/full-data comparison against DuckDB or ClickHouse.

Uses the manifest loader/JSON worker shared with TPC-DS. No query results are
cached by the harness. Run `--help` for preparation and measurement commands.
"""
import argparse
import datetime
import hashlib
import json
import math
import os
from pathlib import Path
import statistics
import shutil
import socket
import subprocess
import sys
import time
import re
from xml.sax.saxutils import escape
import urllib.parse
import urllib.request

from tpcds import Worker, cell_equal, compare, normalize, freeze_build_inputs

COMMIT = '0b1ff23f94d68b9790e910a4340e4ec020930ca8'
TIMESTAMPS = {'EventTime', 'ClientEventTime', 'LocalEventTime'}


def sha(path):
    with open(path, 'rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def dump(path, data):
    Path(path).write_text(json.dumps(data, indent=2, ensure_ascii=False) + '\n')


def translate(query):
    import sqlglot
    from sqlglot import exp
    tree = sqlglot.parse_one(query, read='duckdb')
    # ClickHouse AVG(Int64) wraps its signed accumulator on this data. Both
    # engines receive the same explicit floating input; retain original SQL.
    for avg in tree.find_all(exp.Avg):
        avg.set('this', exp.Cast(this=avg.this.copy(), to=exp.DataType.build('DOUBLE')))
    # ClickHouse regex functions make dot match line breaks by default. Spell
    # that class explicitly so GoogleSQL and both regex backends agree.
    for regex in tree.find_all(exp.RegexpReplace):
        pattern = regex.args.get('expression')
        if isinstance(pattern, exp.Literal) and pattern.is_string:
            text, in_class, rewritten = pattern.this, False, []
            for i, character in enumerate(text):
                escaped = i > 0 and text[i - 1] == '\\'
                if character == '[' and not escaped:
                    in_class = True
                if character == ']' and not escaped:
                    in_class = False
                rewritten.append(r'[\s\S]' if character == '.' and not escaped and not in_class else character)
            pattern.set('this', ''.join(rewritten))
    clickhouse = tree.sql(dialect='duckdb')
    for length in list(tree.find_all(exp.Length)):
        length.replace(exp.Anonymous(this='BYTE_LENGTH', expressions=[length.this.copy()]))
    return clickhouse, tree.sql(dialect='bigquery')


def prepare(args):
    import duckdb
    import sqlglot
    upstream = args.upstream.resolve()
    revision = subprocess.check_output(['git', '-C', str(upstream), 'rev-parse', 'HEAD'], text=True).strip()
    if revision != COMMIT:
        raise ValueError(f'expected ClickBench {COMMIT}, got {revision}')
    queries = (upstream / 'clickhouse/queries.sql').read_text().splitlines()
    if len(queries) != 43:
        raise ValueError('expected all 43 ClickBench queries')
    out = args.output.resolve()
    out.mkdir(parents=True, exist_ok=True)
    for engine in ('original', 'clickhouse', 'tinylamb'):
        (out / engine).mkdir(exist_ok=True)
    for number, original in enumerate(queries, 1):
        ch, native = translate(original)
        for engine, text in [('original', original), ('clickhouse', ch), ('tinylamb', native)]:
            (out / engine / f'q{number}.sql').write_text(text + '\n')
    (out / 'clickhouse-create.sql').write_text((upstream / 'clickhouse/create.sql').read_text())
    if args.queries_only:
        metadata = json.loads((out / 'metadata.json').read_text())
    else:
        if (out / 'hits.tsv').exists():
            raise ValueError('dataset exists; use --queries-only or a new output directory')
        paths = [str(p.resolve()) for p in args.parquet]
        if not paths:
            raise ValueError('provide --parquet with official hits_*.parquet files')
        con = duckdb.connect()
        con.execute('SET threads=4')
        con.execute("SET TimeZone='UTC'")
        cols = con.execute('DESCRIBE SELECT * FROM read_parquet(?)', [paths]).fetchall()
        rows = con.execute('SELECT count(*) FROM read_parquet(?)', [paths]).fetchall()[0][0]
        fields, kinds, definitions = [], [], []
        for name, kind, *_ in cols:
            ident = '"' + name + '"'
            sql_type, code, expression = 'INT64', 'I', ident
            if name == 'EventDate':
                sql_type, code = 'DATE', 'D'
                expression = f"strftime(DATE '1970-01-01' + CAST({ident} AS INTEGER), '%Y-%m-%d')"
            elif name in TIMESTAMPS:
                sql_type, code = 'TIMESTAMP', 'S'
                expression = f"hex(strftime(to_timestamp({ident}), '%Y-%m-%d %H:%M:%S+00'))"
            elif kind == 'BLOB':
                sql_type, code, expression = 'STRING', 'S', f'hex({ident})'
            fields.append(expression)
            kinds.append(code)
            definitions.append(f'`{name}` {sql_type} NOT NULL')
        con.from_parquet(paths).create_view('source_hits')
        destination = str(out / 'hits.tsv').replace("'", "''")
        con.execute(f"COPY (SELECT {', '.join(fields)} FROM source_hits) TO '{destination}' "
                    "(FORMAT CSV, DELIMITER '\t', HEADER false, QUOTE '', ESCAPE '', NULL '\\N')")
        con.close()
        (out / 'manifest.tsv').write_text('hits\t' + ''.join(kinds) + '\t' + str(rows) +
                                         '\tCREATE TABLE hits (' + ', '.join(definitions) + ')\n')
        metadata = {'rows': rows, 'columns': [c[0] for c in cols],
                    'sources': [{'path': p, 'sha256': sha(p)} for p in paths],
                    'duckdb': duckdb.__version__, 'sqlglot': sqlglot.__version__}
    metadata.update({'upstream_commit': revision, 'queries': 43, 'timezone': 'UTC',
                     'normalization': 'AVG inputs FLOAT64; LENGTH counts bytes; regex dot explicitly includes line breaks',
                     'query_sha256': {str(p.relative_to(out)): sha(p)
                                      for d in ('original', 'clickhouse', 'tinylamb')
                                      for p in (out / d).glob('*.sql')}})
    dump(out / 'metadata.json', metadata)


class ClickHouse:
    def __init__(self, url, database, threads, timeout):
        self.url = url.rstrip('/') + '/?'
        self.settings = {'database': database, 'max_threads': threads,
                         'output_format_json_quote_64bit_integers': 0,
                         'use_query_cache': 0, 'date_time_input_format': 'best_effort'}
        self.timeout = timeout

    def query(self, sql):
        request = urllib.request.Request(self.url + urllib.parse.urlencode(self.settings),
                                         data=(sql.strip().rstrip(';') + ' FORMAT JSONCompact').encode())
        try:
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                result = json.load(response)
            return {'ms': result['statistics']['elapsed'] * 1000, 'rows': result['data'],
                    'meta': result['meta']}
        except Exception as error:
            return {'error': str(error)}


def duckdb_sql(sql):
    """The existing common SQL counts bytes, as official DuckDB STRLEN does."""
    import sqlglot
    from sqlglot import exp
    tree = sqlglot.parse_one(sql, read='duckdb')
    for length in list(tree.find_all(exp.Length)):
        length.replace(exp.Anonymous(this='STRLEN', expressions=[length.this.copy()]))
    return tree.sql(dialect='duckdb')


def init_duckdb(args):
    import duckdb
    metadata = json.loads((args.dataset / 'metadata.json').read_text())
    sources = metadata.get('sources') or [
        {'path': metadata['source'], 'sha256': metadata['source_sha256']}]
    for source in sources:
        if sha(source['path']) != source['sha256']:
            raise ValueError('input checksum differs: ' + source['path'])
    database = args.duckdb.resolve()
    if database.exists():
        raise ValueError('DuckDB database exists; choose a new path')
    database.parent.mkdir(parents=True, exist_ok=True)
    paths = [source['path'] for source in sources]
    with duckdb.connect(str(database)) as connection:
        connection.execute(f'SET threads={args.threads}')
        connection.execute("SET TimeZone='UTC'")
        columns = connection.execute('DESCRIBE SELECT * FROM read_parquet(?)', [paths]).fetchall()
        expressions = []
        for name, kind, *_ in columns:
            ident = '"' + name.replace('"', '""') + '"'
            expression = ident
            if name == 'EventDate':
                expression = f"DATE '1970-01-01' + CAST({ident} AS INTEGER)"
            elif name in TIMESTAMPS:
                expression = f'CAST(to_timestamp({ident}) AS TIMESTAMP)'
            elif kind == 'BLOB':
                expression = f'decode({ident})'
            expressions.append(expression + ' AS ' + ident)
        connection.execute('CREATE TABLE hits AS SELECT ' + ','.join(expressions) +
                           ' FROM read_parquet(?)', [paths])
        rows = connection.execute('SELECT count(*) FROM hits').fetchone()[0]
        if rows != metadata['rows']:
            raise ValueError('DuckDB row count differs from manifest')
        connection.execute('ANALYZE hits')
        connection.execute('CHECKPOINT')
    dump(Path(str(database) + '.dataset.json'), {
        'rows': rows, 'sources': sources, 'duckdb_version': duckdb.__version__,
        'database_sha256': sha(database),
        'timestamp_representation': 'TIMESTAMP in UTC',
        'strings': 'UTF-8 decoded from the same Parquet BLOBs; queries count bytes'})


def duck_worker(args):
    import duckdb
    connection = duckdb.connect(str(args.duckdb.resolve()), read_only=True)
    connection.execute(f'SET threads={args.threads}')
    connection.execute("SET TimeZone='UTC'")
    print(json.dumps({'ready': True}), flush=True)
    try:
        for line in sys.stdin:
            try:
                sql = json.loads(line)
                start = time.perf_counter_ns()
                cursor = connection.execute(sql)
                rows = cursor.fetchall()
                elapsed = (time.perf_counter_ns() - start) / 1e6
                metadata = [{'name': column[0], 'type': str(column[1])}
                            for column in cursor.description]
                result = {'ms': elapsed, 'rows': normalize(rows), 'meta': metadata}
            except Exception as error:
                result = {'error': str(error)}
            print(json.dumps(result, allow_nan=False), flush=True)
    finally:
        connection.close()


class DuckDB:
    sql_dialect = 'duckdb'

    def __init__(self, database, threads, timeout, log):
        self.worker = Worker([sys.executable, str(Path(__file__).resolve()),
                              'duck-worker', '--duckdb', str(database.resolve()),
                              '--threads', str(threads)], log, timeout)

    def query(self, sql):
        return self.worker.query(json.dumps(sql))

    def close(self):
        self.worker.close()


def init_clickhouse(args):
    if not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*', args.database):
        raise ValueError('database must be a simple identifier')
    if args.threads < 1 or args.http_port == args.native_port:
        raise ValueError('threads must be positive and server ports distinct')
    for port in (args.http_port, args.native_port):
        with socket.socket() as reservation:
            reservation.bind(('127.0.0.1', port))
    metadata = json.loads((args.dataset / 'metadata.json').read_text())
    root = args.root.resolve()
    root.mkdir(parents=True, exist_ok=False)
    for directory in ('data', 'tmp', 'user_files'):
        (root / directory).mkdir()
    config = f"""<clickhouse>
<logger><level>warning</level><log>{escape(str(root / 'server.log'))}</log>
<errorlog>{escape(str(root / 'error.log'))}</errorlog></logger>
<listen_host>127.0.0.1</listen_host><http_port>{args.http_port}</http_port>
<tcp_port>{args.native_port}</tcp_port><path>{escape(str(root / 'data'))}/</path>
<tmp_path>{escape(str(root / 'tmp'))}/</tmp_path>
<user_files_path>{escape(str(root / 'user_files'))}/</user_files_path>
<mark_cache_size>536870912</mark_cache_size>
<background_schedule_pool_size>8</background_schedule_pool_size>
<background_buffer_flush_schedule_pool_size>4</background_buffer_flush_schedule_pool_size>
<background_pool_size>16</background_pool_size>
<profiles><default><max_threads>{args.threads}</max_threads>
<max_memory_usage>34359738368</max_memory_usage><use_query_cache>0</use_query_cache>
<date_time_input_format>best_effort</date_time_input_format></default></profiles>
<users><default><password></password><networks><ip>127.0.0.1</ip></networks>
<profile>default</profile><quota>default</quota><access_management>1</access_management>
</default></users><quotas><default><interval><duration>3600</duration>
<queries>0</queries><errors>0</errors><result_rows>0</result_rows><read_rows>0</read_rows>
<execution_time>0</execution_time></interval></default></quotas>
</clickhouse>"""
    (root / 'config.xml').write_text(config)
    with (root / 'stdout.log').open('w') as log:
        process = subprocess.Popen([str(args.binary.resolve()), 'server',
                                    '--config-file=' + str(root / 'config.xml')],
                                   stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
    try:
        (root / 'server.pid').write_text(str(process.pid) + '\n')
        client = ClickHouse(f'http://127.0.0.1:{args.http_port}', 'default', args.threads, 10)
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            if process.poll() is not None:
                raise RuntimeError('ClickHouse exited; inspect ' + str(root / 'stdout.log'))
            if client.query('SELECT 1').get('rows') == [[1]]:
                break
            time.sleep(.2)
        else:
            raise TimeoutError('ClickHouse startup exceeded 60 seconds')
        command = [str(args.binary.resolve()), 'client', '--port', str(args.native_port)]
        subprocess.run(command + ['--query', 'CREATE DATABASE ' + args.database], check=True)
        command += ['--database', args.database]
        subprocess.run(command + ['--query', (args.dataset / 'clickhouse-create.sql').read_text()], check=True)
        metadata = json.loads((args.dataset / 'metadata.json').read_text())
        sources = metadata.get('sources') or [{'path': metadata['source'], 'sha256': metadata['source_sha256']}]
        for source in sources:
            if sha(source['path']) != source['sha256']:
                raise ValueError('input checksum differs: ' + source['path'])
            with open(source['path'], 'rb') as data:
                subprocess.run(command + ['--query', 'INSERT INTO hits FORMAT Parquet'], stdin=data, check=True)
        print(f'ClickHouse PID {process.pid}; stop this local server with kill {process.pid}', flush=True)
    except Exception:
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()
        raise


def normalize_rows(rows, metadata):
    # Normalize only declared temporal outputs; strings that happen to end in
    # '+00' remain strings. ClickHouse JSON emits DateTime without a suffix.
    result = []
    for row in rows:
        converted = list(row)
        for i, column in enumerate(metadata):
            if ('Float' in column['type'] or column['type'] in ('DOUBLE', 'FLOAT', 'REAL')) and converted[i] is not None:
                converted[i] = float(converted[i])
            if ('DateTime' in column['type'] or 'TIMESTAMP' in column['type']) and converted[i] is not None:
                value = datetime.datetime.fromisoformat(converted[i].replace('Z', '+00:00'))
                if value.tzinfo is None:
                    value = value.replace(tzinfo=datetime.timezone.utc)
                converted[i] = value.astimezone(datetime.timezone.utc).isoformat()
        result.append(converted)
    return result


def order_slots(sql, metadata=None):
    import sqlglot
    from sqlglot import exp
    tree = sqlglot.parse_one(sql, read='duckdb')
    order = tree.args.get('order')
    if order is None:
        return []
    slots = []
    for term in order.expressions:
        key = term.this
        found = None
        if metadata is not None and any(isinstance(item, exp.Star) for item in tree.expressions):
            if isinstance(key, exp.Column):
                found = next((i for i, column in enumerate(metadata) if column['name'] == key.name), None)
            if found is None:
                return None
            slots.append(found)
            continue
        for i, item in enumerate(tree.expressions):
            value = item.this if isinstance(item, exp.Alias) else item
            if key == value or (isinstance(key, exp.Column) and item.alias and key.name == item.alias):
                found = i
                break
        if found is None:
            return None
        slots.append(found)
    return slots


def literal(value, sql_type, dialect='clickhouse'):
    if value is None:
        return 'NULL'
    if isinstance(value, str):
        # Hex literals preserve quotes, backslashes, control bytes and UTF-8.
        if dialect == 'duckdb':
            return "CAST(decode(from_hex('" + value.encode().hex() + "')) AS " + sql_type + ")"
        raw = "unhex('" + value.encode().hex() + "')"
        return f"CAST({raw}, '{sql_type.replace(chr(39), chr(39) * 2)}')"
    if isinstance(value, (int, float)) and math.isfinite(value):
        return repr(value)
    raise ValueError(f'unsupported verification value {value!r}')


def verify(native, reference, sql, client):
    """Prove legal LIMIT ties through rank keys plus untimed row membership.

    Membership is checked against the *complete* aggregate (no LIMIT/OFFSET),
    preserving payload correctness and duplicate multiplicity. It never turns
    an arbitrary mismatch into a success just because the query has ORDER BY.
    """
    import sqlglot
    metadata = reference['meta']
    if any(len(row) != len(metadata) for row in native['rows'] + reference['rows']):
        return 'output column count differs'
    actual = normalize_rows(native['rows'], metadata)
    expected = normalize_rows(reference['rows'], metadata)
    if len(actual) != len(expected):
        return f'row count {len(actual)} != {len(expected)}'
    slots = order_slots(sql, metadata)
    if slots is None and actual:
        # Hidden ORDER BY columns cannot be checked by comparing visible bags.
        # Obtain the reference rank at each output position, then prove that
        # each native payload can be paired with that rank in the full query.
        augmented = sqlglot.parse_one(sql, read='duckdb')
        order = augmented.args['order']
        extra = [term.this.copy().as_(f'__verify_order_{i}') for i, term in enumerate(order.expressions)]
        augmented.set('expressions', augmented.expressions + extra)
        augmented_sql = augmented.sql(dialect='duckdb')
        ranked = client.query(augmented_sql)
        if 'error' in ranked or len(ranked.get('rows', [])) != len(actual):
            return 'hidden ORDER BY reference failed'
        width = len(metadata)
        paired = [row + rank[width:] for row, rank in zip(native['rows'], ranked['rows'], strict=True)]
        return verify({'rows': paired}, ranked, augmented_sql, client)
    if slots:
        for a, b in zip(actual, expected, strict=True):
            if any(not cell_equal(a[i], b[i]) for i in slots):
                return 'ORDER BY key sequence differs'
    difference = compare(actual, expected)
    if difference is None:
        return None
    tree = sqlglot.parse_one(sql, read='duckdb')
    if tree.args.get('limit') is None or slots is None:
        return difference
    tree.set('limit', None)
    tree.set('offset', None)
    tree.set('order', None)
    dialect = getattr(client, 'sql_dialect', 'clickhouse')
    clauses = []
    for row in native['rows']:
        comparisons = []
        for column, value in zip(metadata, row, strict=True):
            name = '`' + column['name'].replace('`', '\\`') + '`'
            if dialect == 'duckdb':
                name = '"' + column['name'].replace('"', '""') + '"'
            if value is None:
                comparisons.append(f'{name} IS NULL' if dialect == 'duckdb' else f'isNull({name})')
            elif isinstance(value, float):
                tolerance = max(1e-7, abs(value) * 1e-9)
                comparisons.append(f'abs({name} - {literal(value, column["type"], dialect)}) <= {tolerance!r}')
            else:
                comparisons.append(f'{name} = {literal(value, column["type"], dialect)}')
        clauses.append('(' + ' AND '.join(comparisons) + ')')
    validation = client.query('SELECT * FROM (' + tree.sql(dialect='duckdb') +
                              ') AS checked WHERE ' + ' OR '.join(clauses))
    if 'error' in validation:
        return 'membership validation failed: ' + validation['error']
    available = normalize_rows(validation['rows'], metadata)
    for row in actual:
        for i, candidate in enumerate(available):
            if len(row) == len(candidate) and all(cell_equal(a, b) for a, b in zip(row, candidate, strict=True)):
                available.pop(i)
                break
        else:
            return 'row absent from complete query result: ' + repr(row)
    return None


def plot(records, out, rows, reference='clickhouse'):
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    valid = [r for r in records if r.get('correct')]
    fig, (ax, speed) = plt.subplots(2, 1, figsize=(16, 8), layout='constrained')
    x = list(range(1, 44))
    for engine, shift, color in [('tinylamb', -.19, '#1765a8'), (reference, .19, '#d89000')]:
        heights = [r.get(engine, {}).get('median_ms', float('nan')) for r in records]
        ax.bar([n + shift for n in x], heights, width=.38, label=engine, color=color)
    ax.set_yscale('log')
    ax.set_ylabel('Median query time (ms, log scale)')
    ax.legend()
    ratios = [r[reference]['median_ms'] / r['tinylamb']['median_ms'] if r.get('correct') else float('nan') for r in records]
    speed.bar(x, ratios, color=['#23856c' if r > 1 else '#b85450' for r in ratios])
    speed.set_yscale('log')
    speed.axhline(1, color='black', linewidth=.8)
    speed.set_ylabel(f'{reference} / tinylamb (higher is faster)')
    speed.set_xlabel('ClickBench query')
    for axes in (ax, speed):
        axes.set_xticks(x)
        axes.grid(axis='y', alpha=.2)
    title = f'ClickBench • {rows:,} rows • {len(valid)}/43 verified'
    if rows < 90_000_000:
        title += ' • subset, not the standard full dataset'
    fig.suptitle(title)
    for suffix in ('png', 'svg'):
        fig.savefig(out / f'comparison.{suffix}', dpi=150)
    plt.close(fig)


def check_native_utc(native, directory):
    # The timestamp is before midnight in Los Angeles: minute truncation
    # alone would miss a mismatched session because its result is invariant.
    probe = directory / 'native-timezone-probe.sql'
    probe.write_text("SELECT TIMESTAMP_TRUNC('2000-01-02 03:04:05+00', DAY)\n")
    result = native.query(str(probe.resolve()))
    if result.get('rows') != [['2000-01-02 00:00:00+00']]:
        raise ValueError('native worker must use a UTC session: ' + repr(result))
    return {'timezone': 'UTC', 'probe': probe.name, 'rows': result['rows']}


def run(args):
    dataset, out = args.dataset.resolve(), args.output.resolve()
    out.mkdir(parents=True, exist_ok=True)
    if (out / 'results.jsonl').exists():
        raise ValueError('output already contains a measurement; choose a new directory')
    metadata = json.loads((dataset / 'metadata.json').read_text())
    for name, digest in metadata['query_sha256'].items():
        if sha(dataset / name) != digest:
            raise ValueError('query changed after preparation: ' + name)
    cpus = {int(x) for x in args.cpus.split(',')}
    os.sched_setaffinity(0, cpus)
    if args.server_pid:
        # Pin all current server threads, and the main thread inherited by new
        # threads. This is a local benchmark server, not a remote service.
        for task in Path(f'/proc/{args.server_pid}/task').iterdir():
            try:
                os.sched_setaffinity(int(task.name), cpus)
            except ProcessLookupError:
                pass
    reference = args.reference
    reference_log = (out / f'{reference}.log').open('w')
    client = (DuckDB(args.duckdb, args.threads, args.timeout, reference_log)
              if reference == 'duckdb' else
              ClickHouse(args.url, args.database, args.threads, args.timeout))
    count = client.query('SELECT count(*) FROM hits')
    if count.get('rows') != [[metadata['rows']]]:
        raise ValueError(reference + ' row count differs from manifest: ' + repr(count))
    version = client.query('SELECT version()')
    binary = args.binary.resolve()
    manifest = {'dataset': metadata, 'binary_sha256': sha(binary),
                'threads': args.threads, 'cpus': sorted(cpus), 'server_pid': args.server_pid,
                'warmups': args.warmups, 'repeats': args.repeats,
                reference + '_version': version.get('rows'), 'reference': reference, 'reverse': args.reverse,
                'timer': ('native Execute+Collect; DuckDB execute+fetchall; both include planning' if reference == 'duckdb' else
                          'native Execute+Collect; ClickHouse statistics.elapsed; both include planning'),
                'query_result_cache': False, 'validation': 'untimed bag/rank/membership checks',
                'utc': datetime.datetime.now(datetime.timezone.utc).isoformat(),
                'git_head': subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip(),
                'tracked_diff_sha256': hashlib.sha256(subprocess.check_output(['git', 'diff', '--binary'])).hexdigest(),
                'harness_sha256': sha(Path(__file__))}
    cache = binary.parent / 'CMakeCache.txt'
    if cache.exists():
        names = {'CMAKE_BUILD_TYPE', 'CMAKE_CXX_COMPILER', 'CMAKE_CXX_FLAGS', 'CMAKE_CXX_FLAGS_RELEASE',
                 'TINYLAMB_ARCH', 'TINYLAMB_ENABLE_LTO', 'TINYLAMB_ENABLE_RE2',
                 'TINYLAMB_RE2_LIBRARY', 'TINYLAMB_ALLOCATOR'}
        configuration = {}
        for line in cache.read_text().splitlines():
            if '=' in line and ':' in line.split('=', 1)[0]:
                key, value = line.split('=', 1)
                key = key.split(':', 1)[0]
                if key in names:
                    configuration[key] = value
        manifest['build'] = configuration
        library = Path(configuration.get('TINYLAMB_RE2_LIBRARY', '/nonexistent'))
        if library.is_file():
            manifest['re2_library_sha256'] = sha(library)
    manifest['platform'] = list(os.uname())
    manifest['cpu_model'] = next((line.split(':', 1)[1].strip() for line in
                                  Path('/proc/cpuinfo').read_text().splitlines() if line.startswith('model name')), '')
    if reference == 'duckdb':
        if metadata.get('timezone') != 'UTC':
            raise ValueError('regenerate the native dataset with the UTC loader before comparing DuckDB')
        manifest['reference_database'] = json.loads(Path(str(args.duckdb) + '.dataset.json').read_text())
        sources = metadata.get('sources') or [{'path': metadata['source'], 'sha256': metadata['source_sha256']}]
        if [source['sha256'] for source in sources] != [source['sha256'] for source in manifest['reference_database']['sources']]:
            raise ValueError('DuckDB and native data came from different Parquet inputs')
        manifest['reference_database_sha256'] = sha(args.duckdb)
        if manifest['reference_database_sha256'] != manifest['reference_database']['database_sha256']:
            raise ValueError('DuckDB database changed after initialization')
    shutil.copy2(binary, out / 'tinylamb_tpcds_compare')
    (out / 'source.patch').write_bytes(subprocess.check_output(['git', 'diff', '--binary']))
    for source in ('clickbench.py', 'clickbench_test.py', 'tpcds.py', 'requirements-tpcds.txt'):
        shutil.copy2(Path(__file__).parent / source, out / source)
    if cache.exists():
        shutil.copy2(cache, out / 'CMakeCache.txt')
        manifest['build_inputs'] = freeze_build_inputs(binary, cache.read_text(), out)
    additional = out / 'additional-source'
    for relative in subprocess.check_output(['git', 'ls-files', '--others', '--exclude-standard', '-z']).decode().split('\0'):
        path = Path(relative)
        if path.suffix not in ('.cpp', '.hpp') or not path.parts or path.parts[0] not in {
                'benchmark', 'common', 'database', 'executor', 'expression', 'index',
                'page', 'plan', 'query', 'recovery', 'server', 'table', 'transaction', 'type'}:
            continue
        frozen = additional / path
        frozen.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(path, frozen)
    binary = out / 'tinylamb_tpcds_compare'
    dump(out / 'run.json', manifest)
    (out / 'queries').mkdir()
    records = []
    with (out / 'native.log').open('w') as log, (out / 'results.jsonl').open('w') as output:
        native = Worker([str(binary), str(args.native_db.resolve()), '--serve',
                         str(dataset / 'tinylamb'), str(args.threads)], log, args.timeout)
        try:
            if reference == 'duckdb':
                manifest['native_session'] = check_native_utc(native, out / 'queries')
                manifest['reference_session_timezone'] = 'UTC'
                dump(out / 'run.json', manifest)
            for q in range(1, 44):
                sql = (dataset / 'clickhouse' / f'q{q}.sql').read_text()
                if reference == 'duckdb':
                    sql = duckdb_sql(sql)
                (out / 'queries' / f'{reference}-q{q}.sql').write_text(sql + '\n')
                shutil.copy2(dataset / 'tinylamb' / f'q{q}.sql', out / 'queries' / f'tinylamb-q{q}.sql')
                record = {'query': q, 'reference_sql': sql}
                order = ['tinylamb', reference] if (q % 2 == 1) != args.reverse else [reference, 'tinylamb']
                for engine in order:
                    samples, answers, last = [], [], {}
                    for repeat in range(args.warmups + args.repeats):
                        last = native.query(f'q{q}.sql') if engine == 'tinylamb' else client.query(sql)
                        if 'error' in last:
                            break
                        answers.append(last['rows'])
                        if repeat >= args.warmups:
                            samples.append(last['ms'])
                    record[engine] = last | {'samples_ms': samples, 'answers': answers}
                    if len(samples) == args.repeats:
                        record[engine]['median_ms'] = statistics.median(samples)
                if all('median_ms' in record[e] for e in ('tinylamb', reference)):
                    try:
                        error = None
                        checked = set()
                        for engine in ('tinylamb', reference):
                            for answer in record[engine]['answers']:
                                fingerprint = json.dumps(answer, ensure_ascii=False)
                                if fingerprint in checked:
                                    continue
                                checked.add(fingerprint)
                                error = verify({'rows': answer}, record[reference], sql, client)
                                if error is not None:
                                    error = engine + ' sample: ' + error
                                    break
                            if error is not None:
                                break
                    except Exception as exception:
                        error = 'validation exception: ' + str(exception)
                else:
                    error = 'query failed or timed out'
                if native.process is not None:
                    status = Path(f'/proc/{native.process.pid}/status').read_text()
                    record['native_peak_rss_kib'] = next(int(line.split()[1]) for line in status.splitlines() if line.startswith('VmHWM:'))
                record['correct'] = error is None
                record['validation_error'] = error
                records.append(record)
                output.write(json.dumps(record, ensure_ascii=False) + '\n')
                output.flush()
                print(f'Q{q:02} verified={record["correct"]} ' +
                      ' '.join(f'{e}={record[e].get("median_ms", "ERROR")}' for e in order), flush=True)
        finally:
            native.close()
            if reference == 'duckdb':
                client.close()
            reference_log.close()
    passed = all(r['correct'] for r in records)
    totals = {e: sum(r[e]['median_ms'] for r in records) if all('median_ms' in r[e] for r in records) else None
              for e in ('tinylamb', reference)}
    dump(out / 'summary.json', {'verified': sum(r['correct'] for r in records), 'queries': 43,
                                'total_median_ms': totals, 'target_achieved': passed and (all(r['tinylamb']['median_ms'] < r[reference]['median_ms'] for r in records) if reference == 'duckdb' else totals['tinylamb'] < totals[reference]),
                                'wins': sum(r['correct'] and r['tinylamb']['median_ms'] < r[reference]['median_ms'] for r in records)})
    plot(records, out, metadata['rows'], reference)
    return 0 if passed else 1


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest='command', required=True)
    prep = commands.add_parser('prepare')
    prep.add_argument('--upstream', type=Path, required=True)
    prep.add_argument('--parquet', type=Path, nargs='*', default=[])
    prep.add_argument('--output', type=Path, required=True)
    prep.add_argument('--queries-only', action='store_true')
    init = commands.add_parser('init-clickhouse')
    init.add_argument('--binary', type=Path, required=True)
    init.add_argument('--root', type=Path, required=True)
    init.add_argument('--dataset', type=Path, required=True)
    init.add_argument('--database', default='clickbench')
    init.add_argument('--http-port', type=int, default=18123)
    init.add_argument('--native-port', type=int, default=19000)
    init.add_argument('--threads', type=int, default=2)
    for command in ('init-duckdb', 'duck-worker'):
        duck = commands.add_parser(command)
        duck.add_argument('--duckdb', type=Path, required=True)
        duck.add_argument('--threads', type=int, default=2)
        if command == 'init-duckdb':
            duck.add_argument('--dataset', type=Path, required=True)
    bench = commands.add_parser('run')
    bench.add_argument('--dataset', type=Path, required=True)
    bench.add_argument('--native-db', type=Path, required=True)
    bench.add_argument('--binary', type=Path, required=True)
    bench.add_argument('--output', type=Path, required=True)
    bench.add_argument('--reference', choices=('clickhouse', 'duckdb'), default='clickhouse')
    bench.add_argument('--duckdb', type=Path)
    bench.add_argument('--url', default='http://127.0.0.1:18123')
    bench.add_argument('--database', default='cb1m')
    bench.add_argument('--server-pid', type=int)
    bench.add_argument('--threads', type=int, default=2)
    bench.add_argument('--cpus', default='0,1')
    bench.add_argument('--warmups', type=int, default=1)
    bench.add_argument('--repeats', type=int, default=7)
    bench.add_argument('--timeout', type=float, default=180)
    bench.add_argument('--reverse', action='store_true')
    args = parser.parse_args()
    if args.command == 'prepare':
        prepare(args)
        return 0
    if args.command == 'init-clickhouse':
        init_clickhouse(args)
        return 0
    if args.command == 'init-duckdb':
        init_duckdb(args)
        return 0
    if args.command == 'duck-worker':
        duck_worker(args)
        return 0
    if args.reference == 'duckdb' and (not args.duckdb or args.server_pid):
        parser.error('DuckDB comparison requires --duckdb and does not use --server-pid')
    if args.threads < 1 or args.repeats < 1 or args.warmups < 0:
        parser.error('threads/repeats must be positive and warmups nonnegative')
    return run(args)


if __name__ == '__main__':
    sys.exit(main())
