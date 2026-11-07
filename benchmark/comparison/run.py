#!/usr/bin/env python3
"""OLAP/OLTP comparison, optionally including Oracle Free and DuckDB. Own test servers only.

Run from any directory. See README.md for profiles and measurement semantics.
"""

import argparse
import csv
import datetime
import hashlib
import importlib.metadata
import json
import multiprocessing as mp
import os
from pathlib import Path
import platform
import queue
import random
import socket
import statistics
import subprocess
import sys
import tempfile
import time
import traceback

from workloads import (ROOT, OLTP, accounts, adapt_sql, equivalent, insert_sql,
                       olap_queries, sales, schema, tpch_queries, tpch_schema,
                       transaction, secondary_indexes)


ENGINES = ("tinylamb", "postgresql", "mysql")


class Client:
    def __init__(self, engine, ports, timeout=30):
        self.engine = engine
        if engine == "oracle":
            from oracle_support import Client as OracleClient
            self.oracle = OracleClient(ports, timeout)
            self.connection, self.cursor = self.oracle.connection, self.oracle.cursor
            return
        if engine == "duckdb":
            from duckdb_support import Client as DuckDBClient
            self.duckdb = DuckDBClient(ports, timeout)
            self.connection, self.cursor = self.duckdb.connection, self.duckdb.cursor
            return
        if engine == "mysql":
            import mysql.connector
            self.connection = mysql.connector.connect(
                host="127.0.0.1", port=ports[engine], user="root", database="bench",
                autocommit=True, use_pure=False, ssl_disabled=True,
                connection_timeout=timeout, read_timeout=timeout, write_timeout=timeout)
        else:
            import psycopg2
            self.connection = psycopg2.connect(
                host="127.0.0.1", port=ports[engine], user="bench", dbname="bench",
                sslmode="disable", gssencmode="disable", connect_timeout=timeout,
                options=f"-c statement_timeout={timeout * 1000}" if engine == "postgresql" else "")
            self.connection.autocommit = True
            # Bound network stalls on libpq too; the supervising process remains the hard deadline.
        self.cursor = self.connection.cursor()
        if engine == "mysql":
            self.execute("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")
            self.execute(f"SET SESSION max_execution_time = {timeout * 1000}")
            self.execute(f"SET SESSION innodb_lock_wait_timeout = {timeout}")
        elif engine == "postgresql":
            self.execute("SET default_transaction_isolation = 'read committed'")

    def execute(self, sql):
        if self.engine == "oracle":
            return self.oracle.execute(sql)
        if self.engine == "duckdb":
            return self.duckdb.execute(sql)
        self.cursor.execute(sql)
        return self.cursor.fetchall() if self.cursor.description else []

    def close(self):
        if self.engine == "duckdb":
            self.duckdb.close()
            return
        self.cursor.close()
        self.connection.close()


def command(args, **kwargs):
    return subprocess.check_output(args, text=True, **kwargs).strip()


def append_json(path, record):
    with path.open("a") as stream:
        stream.write(json.dumps(record, default=str, ensure_ascii=False) + "\n")


def percentiles(samples):
    if not samples:
        return {"p50_ms": None, "p95_ms": None, "p99_ms": None, "max_ms": None}
    ordered = sorted(samples)
    def at(p):
        return ordered[max(0, min(len(ordered) - 1, math_ceil(p * len(ordered)) - 1))]
    return {"p50_ms": at(.5), "p95_ms": at(.95), "p99_ms": at(.99), "max_ms": ordered[-1]}


def math_ceil(value):
    return int(-(-value // 1))


def fingerprint(rows):
    return hashlib.sha256(json.dumps(rows, default=str, separators=(",", ":")).encode()).hexdigest()


class Stack:
    def __init__(self, args, output):
        self.args, self.output = args, output
        self.names = []
        self.process = None
        self.temporary = tempfile.TemporaryDirectory(prefix="tinylamb-comparison-", dir=args.scratch)
        self.work = Path(self.temporary.name)
        self.ports = dict(tinylamb=args.tiny_port, postgresql=args.pg_port, mysql=args.mysql_port)
        self.oracle = None
        self.duckdb = None
        if getattr(args, "include_duckdb", False):
            self.ports["duckdb"] = args.duckdb_port
        if getattr(args, "include_oracle", False):
            self.ports['oracle'] = args.oracle_port
        self.log = None

    def start(self):
        a = self.args
        cache_mib = getattr(a, 'data_cache_mib', 512)
        for port in self.ports.values():
            with socket.socket() as probe:
                probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                probe.bind(("127.0.0.1", port))
        common = ["--network", "host", "--cpuset-cpus", a.server_cpus]
        pg_name = f"tinylamb-comparison-pg-{os.getpid()}"
        mysql_name = f"tinylamb-comparison-mysql-{os.getpid()}"
        for name, image, env, settings in [
            (pg_name, a.postgres_image,
             ["-e", "POSTGRES_HOST_AUTH_METHOD=trust", "-e", "POSTGRES_USER=bench", "-e", "POSTGRES_DB=bench"],
             ["-p", str(a.pg_port), "-c", "listen_addresses=127.0.0.1", "-c", f"shared_buffers={cache_mib}MB",
              "-c", "work_mem=64MB", "-c", "max_parallel_workers_per_gather=4", "-c", "jit=on",
              "-c", "fsync=on", "-c", "synchronous_commit=on"]),
            (mysql_name, a.mysql_image,
             ["-e", "MYSQL_ALLOW_EMPTY_PASSWORD=yes", "-e", "MYSQL_ROOT_HOST=%", "-e", "MYSQL_DATABASE=bench"],
             [f"--port={a.mysql_port}", "--bind-address=127.0.0.1", "--mysqlx=OFF",
              f"--innodb-buffer-pool-size={cache_mib * 1024 * 1024}", "--innodb-flush-log-at-trx-commit=1",
              "--skip-log-bin", "--max-connections=128"]),
        ]:
            self.names.append(name)
            invocation = ["docker", "run", "-d", "--name", name, *common, *env, image, *settings]
            append_json(self.output / "commands.jsonl", {"argv": invocation})
            command(invocation)
        env = os.environ.copy()
        # Inherit no diagnostic switches from a developer shell.
        for key in list(env):
            if key.startswith("TINYLAMB_"):
                del env[key]
        if getattr(a, "server_metrics", False):
            env["TINYLAMB_SERVER_METRICS"] = "1"
        env.update(TINYLAMB_PAGE_POOL_BYTES=str(cache_mib * 1024 * 1024), TINYLAMB_QUERY_MEMORY_BYTES=str(a.tiny_query_memory_mib * 1024 * 1024))
        invocation = ["taskset", "-c", a.server_cpus, str(Path(a.server).resolve()), str(self.work / "database"),
                      "--port", str(a.tiny_port), "--read-workers", str(a.read_workers)]
        self.log = (self.output / "tinylamb-server.log").open("w")
        append_json(self.output / "commands.jsonl", {"argv": invocation, "env": {k: v for k, v in env.items() if k.startswith("TINYLAMB_")}})
        self.process = subprocess.Popen(invocation, env=env, cwd=ROOT, stdout=self.log, stderr=subprocess.STDOUT)
        if getattr(a, "include_oracle", False):
            from oracle_support import Container
            self.oracle = Container(a, self.work, lambda argv: append_json(self.output / "commands.jsonl", {"argv": argv}))
            self.names.append(self.oracle.name)
            self.oracle.start()
        if getattr(a, 'include_duckdb', False):
            from duckdb_support import Server
            self.duckdb = Server(a, self.work, self.output, lambda argv: append_json(self.output / 'commands.jsonl', {'argv': argv}))
            self.duckdb.start()
        for engine in ENGINES:
            deadline = time.monotonic() + 120
            first_failure = True
            while True:
                try:
                    client = Client(engine, self.ports)
                    client.execute("SELECT 1")
                    client.close()
                    break
                except Exception as error:
                    if first_failure:
                        print(f"waiting for {engine}: {error}", flush=True)
                        first_failure = False
                    if time.monotonic() >= deadline:
                        raise RuntimeError(f"{engine} did not start: {error}") from error
                    time.sleep(.3)

    def metadata(self):
        data = {"timestamp": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
                "git_head": command(["git", "rev-parse", "HEAD"], cwd=ROOT),
                "git_status": command(["git", "status", "--short"], cwd=ROOT),
                "tracked_diff_sha256": hashlib.sha256(subprocess.check_output(["git", "diff", "HEAD"], cwd=ROOT)).hexdigest(),
                "server_sha256": hashlib.file_digest(open(self.args.server, "rb"), "sha256").hexdigest(),
                "harness_sha256": {p.name: hashlib.file_digest(p.open("rb"), "sha256").hexdigest()
                                   for p in Path(__file__).parent.glob("*.py")},
                "platform": platform.platform(), "python": sys.version,
                "cpu": command(["lscpu"]), "memory": command(["free", "-b"]),
                "filesystem": command(["df", "-T", str(self.work)]),
                "load_start": os.getloadavg(), "args": {k: str(v) if isinstance(v, Path) else v for k, v in vars(self.args).items()},
                "drivers": {p: importlib.metadata.version(p) for p in ("psycopg2-binary", "mysql-connector-python")},
                "images": {image: json.loads(command(["docker", "image", "inspect", image]))[0]["RepoDigests"]
                           for image in (self.args.postgres_image, self.args.mysql_image)},
                "settings": {}}
        cache = ROOT / "build/CMakeCache.txt"
        if self.oracle:
            data['oracle_setup'] = self.oracle.setup_output
            data['drivers']['oracledb'] = importlib.metadata.version('oracledb')
            data['images'][self.args.oracle_image] = json.loads(command(['docker', 'image', 'inspect', self.args.oracle_image]))[0]['RepoDigests']
        if self.duckdb:
            import _duckdb
            data['drivers']['duckdb'] = importlib.metadata.version('duckdb')
            data['duckdb_engine_sha256'] = hashlib.sha256(Path(_duckdb.__file__).read_bytes()).hexdigest()
            data['duckdb_extensions_sha256'] = self.duckdb.extension_hashes
        data["build_configuration"] = [line for line in cache.read_text().splitlines()
                                      if line.startswith(("CMAKE_BUILD_TYPE:", "CMAKE_CXX_COMPILER:", "TINYLAMB_", "LLVM_DIR:"))] if cache.exists() else []
        for engine in ENGINES:
            client = Client(engine, self.ports)
            if engine == "postgresql":
                data["settings"][engine] = client.execute("SELECT name, setting FROM pg_settings WHERE name IN ('server_version','shared_buffers','work_mem','max_parallel_workers_per_gather','jit','fsync','synchronous_commit','default_transaction_isolation','full_page_writes','wal_sync_method') ORDER BY name")
            elif engine == "mysql":
                data["settings"][engine] = client.execute("SHOW VARIABLES WHERE Variable_name IN ('version','innodb_buffer_pool_size','innodb_flush_log_at_trx_commit','sync_binlog','log_bin','transaction_isolation','innodb_flush_method')")
                data["settings"][engine].append(("session_transaction_isolation", "READ-COMMITTED"))
            elif engine == 'oracle':
                data['settings'][engine] = dict(
                    version=client.execute('SELECT banner_full FROM v$version'),
                    parameters=client.execute("SELECT name, value FROM v$parameter WHERE name IN ('cpu_count','sga_target','sga_max_size','db_cache_size','pga_aggregate_target','pga_aggregate_limit','memory_target','commit_logging','commit_wait','optimizer_features_enable','parallel_degree_policy') ORDER BY name"),
                    logging=client.execute('SELECT log_mode, force_logging FROM v$database'),
                    sga=client.execute('SELECT name, bytes FROM v$sgainfo ORDER BY name'),
                    license_limits=dict(cpu_cores=2, database_ram_gb=2, user_data_gb=12),
                    driver_mode='thin; stmtcachesize=0; arraysize=100',
                    archive_log=False, force_logging=True)
                (self.output / 'oracle-profile.json').write_text(json.dumps(data['settings'][engine], indent=2))
                if data['settings'][engine]['logging'] != [('NOARCHIVELOG', 'YES')]:
                    raise RuntimeError('Oracle logging settings differ from the recorded benchmark profile')
            elif engine == 'duckdb':
                data['settings'][engine] = dict(
                    version=client.execute('SELECT version()'),
                    settings=client.execute("SELECT name, value FROM duckdb_settings() WHERE name IN ('threads','memory_limit','checkpoint_threshold','wal_autocheckpoint','access_mode','enable_external_access','storage_compatibility_version') ORDER BY name"),
                    extensions=client.execute("SELECT extension_name, extension_version, loaded FROM duckdb_extensions() WHERE loaded ORDER BY extension_name"),
                    transport='official Quack; CONNECT routes whole session; persistent file',
                    client_threads=1, isolation='snapshot isolation; optimistic write conflicts; no retries',
                    timeout='supervising worker wall-clock deadline', index_type='ART')
            else:
                data["settings"][engine] = {"page_pool_bytes": getattr(self.args, 'data_cache_mib', 512) * 1024 * 1024, "query_memory_bytes": self.args.tiny_query_memory_mib * 1024 * 1024,
                                            "synchronous_commit": "default true; docs/commit_durability.md", "read_workers": self.args.read_workers,
                                            "commit_driven_wal_sync": None, "io_cpu": None, "fixed_size_wal": None,
                                            "wal_sync_policy": "when commit-driven sync is enabled: requested barriers remain immediate; unrequested dirty WAL has a 200 ms backstop"}
                for line in (self.output / "tinylamb-server.log").read_text().splitlines():
                    if line.startswith("TINYLAMB_METRICS "):
                        metrics = json.loads(line[len("TINYLAMB_METRICS "):])
                        if "commit_driven_wal_sync" in metrics:
                            data["settings"][engine]["commit_driven_wal_sync"] = metrics["commit_driven_wal_sync"]
                        if "io_cpu" in metrics:
                            data["settings"][engine]["io_cpu"] = metrics["io_cpu"]
                        if "fixed_size_wal" in metrics:
                            data["settings"][engine]["fixed_size_wal"] = metrics["fixed_size_wal"]
            client.execute(adapt_sql("CREATE TABLE feature_probe (id BIGINT, v BIGINT)", engine))
            try:
                client.execute("CREATE INDEX feature_probe_v ON feature_probe(v)")
                index_capability = {"supported": True}
            except Exception as error:
                index_capability = {"supported": False, "error": str(error)}
            data.setdefault("capabilities", {})[engine] = {"sql_create_index": index_capability}
            client.execute("DROP TABLE feature_probe")
            client.close()
        return data

    def close(self):
        if self.duckdb:
            self.duckdb.close()
        if self.process is not None:
            self.process.terminate()
            try:
                self.process.wait(timeout=30)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait()
        if self.log:
            self.log.close()
        for name in self.names:
            subprocess.run(["docker", "logs", name], stdout=(self.output / f"{name}.log").open("w"), stderr=subprocess.STDOUT)
            subprocess.run(["docker", "rm", "-fv", name], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        self.temporary.cleanup()


def load_rows(client, table, rows, batch_size=500):
    batch = []
    count = 0
    for row in rows:
        batch.append(row)
        if len(batch) == batch_size:
            if client.engine == 'oracle':
                client.oracle.load_batch(table, batch)
            else:
                client.execute(insert_sql(table, batch))
            count += len(batch)
            batch = []
    if batch:
        if client.engine == 'oracle':
            client.oracle.load_batch(table, batch)
        else:
            client.execute(insert_sql(table, batch))
        count += len(batch)
    return count


def analyze(client, tables):
    if client.engine == "tinylamb":
        client.execute("ANALYZE")
    elif client.engine == 'oracle':
        for table in tables:
            client.oracle.analyze(table)
    else:
        for table in tables:
            client.execute(f"ANALYZE {'TABLE ' if client.engine == 'mysql' else ''}{table}")


def create_secondary_indexes(client, suite, output):
    statements = secondary_indexes(suite)
    start = time.perf_counter()
    for sql in statements:
        append_json(output / "commands.jsonl", {"kind": "create_index", "engine": client.engine, "sql": sql})
        try:
            client.execute(adapt_sql(sql, client.engine))
        except Exception as error:
            raise RuntimeError(f"{client.engine}: {sql}: {error}") from error
    append_json(output / "results.jsonl", {"kind": "index_build", "engine": client.engine,
                "suite": suite, "statements": statements, "seconds": time.perf_counter() - start})


def initialize(stack, args, output, rows):
    account_rows = min(rows, 10000)
    for engine in ENGINES:
        client = Client(engine, stack.ports, args.timeout)
        start = time.perf_counter()
        if getattr(stack, "initialized", False):
            for table in ("churn", "sales", "accounts"):
                client.execute(f"DROP TABLE {table}")
        for sql in schema():
            client.execute(sql)
        counts = {"accounts": load_rows(client, "accounts", accounts(account_rows)),
                  "sales": load_rows(client, "sales", sales(rows, account_rows))}
        loading = time.perf_counter() - start
        if args.secondary_indexes:
            create_secondary_indexes(client, "micro", output)
        analyze(client, counts)
        expected_sales = [(rows, rows * (rows - 1) // 2, sum(r[3] for r in sales(rows, account_rows)))]
        actual = client.execute("SELECT COUNT(*), SUM(id), SUM(amount) FROM sales")
        actual_accounts = client.execute("SELECT COUNT(*), SUM(balance) FROM accounts")
        valid = equivalent(actual, expected_sales) and equivalent(actual_accounts, [(account_rows, account_rows * 100000)])
        append_json(output / "results.jsonl", {"kind": "load", "engine": engine, "rows": rows,
                    "accounts": account_rows, "load_seconds": loading, "counts": counts, "valid": valid,
                    "checksums": actual})
        client.close()
        if not valid:
            raise RuntimeError(f"fixture validation failed: {engine}")
        print(f"loaded {engine} sales={rows} seconds={loading:.2f}", flush=True)
    stack.initialized = True


def query_child(engine, ports, sql, timeout, repeats, output, explain_analyze=False):
    client = None
    try:
        client = Client(engine, ports, timeout)
        start = time.perf_counter()
        first = client.execute(sql)
        first_ms = (time.perf_counter() - start) * 1000
        times = []
        stable = True
        for _ in range(repeats):
            start = time.perf_counter()
            result = client.execute(sql)
            times.append((time.perf_counter() - start) * 1000)
            stable &= equivalent(first, result)
        try:
            plan = client.execute("EXPLAIN " + sql)
        except Exception as error:
            plan = [[str(error)]]
        actual_plan = None
        if explain_analyze and engine == "tinylamb":
            try:
                actual_plan = client.execute("EXPLAIN ANALYZE " + sql)
            except Exception as error:
                actual_plan = [[str(error)]]
        output.put({"actual_plan": actual_plan, "status": "ok", "first_ms": first_ms, "samples_ms": times,
                    "median_ms": statistics.median(times), "stable": stable,
                    "result": first, "result_sha256": fingerprint(first), "plan": plan})
    except Exception as error:
        output.put({"status": "error", "error": str(error)})
    finally:
        if client:
            client.close()


def run_olap(stack, args, output, rows, queries, suite):
    ctx = mp.get_context("spawn")
    for name, query in queries.items():
        results = {}
        # Rotate engine order, keeping executions sequential on the same host.
        order = list(ENGINES)
        random.Random(args.seed + rows + list(queries).index(name)).shuffle(order)
        for engine in order:
            sql = adapt_sql(query, engine)
            result_queue = ctx.Queue()
            process = ctx.Process(target=query_child, args=(engine, stack.ports, sql, args.timeout, args.repeats, result_queue, args.explain_analyze))
            process.start()
            try:
                result = result_queue.get(timeout=args.timeout * (args.repeats + 2) + 10)
            except queue.Empty:
                result = {"status": "timeout", "error": "supervisor wall-clock deadline"}
            finally:
                process.join(timeout=2)
                if process.is_alive():
                    process.terminate()
                    process.join()
                result_queue.close()
            result.update(kind="olap", suite=suite, engine=engine, rows=rows, workload=name, sql=sql)
            results[engine] = result
        reference = results["postgresql"]
        for engine, result in results.items():
            result["correct"] = (result["status"] == reference["status"] == "ok"
                                  and result["stable"] and reference["stable"]
                                  and equivalent(result["result"], reference["result"]))
            # Save full result sets separately, so latency tables remain small.
            detail = output / "query-details"
            detail.mkdir(exist_ok=True)
            path = detail / f"{suite}-{rows}-{name}-{engine}.json"
            path.write_text(json.dumps({key: result[key] for key in ("result", "plan", "actual_plan") if key in result}, default=str, ensure_ascii=False, indent=2))
            result["detail"] = str(path.relative_to(output))
            append_json(output / "results.jsonl", {k: v for k, v in result.items() if k not in ("result", "plan", "actual_plan")})
            print(f"{suite} {rows} {name} {engine}: {result.get('median_ms', result['status'])} correct={result['correct']}", flush=True)


class ResultMismatch(RuntimeError):
    """A wrong result is a correctness failure, not a transaction conflict."""


def validate_read_result(name, result, statements, account_rows, sales_rows, role):
    try:
        if name == "noop" and not equivalent(result, [(1,)]):
            raise ResultMismatch("incorrect SELECT 1 result")
        if name == "point_read" and not equivalent(result, [(100000, "x" * 64)]):
            raise ResultMismatch("incorrect point lookup result")
        if name == "nonkey_read":
            bucket = int(statements[0].partition("bucket = ")[2].split()[0])
            expected = [(i, 100000) for i in range(bucket, account_rows, 100)][:10]
            if not equivalent(result, expected):
                raise ResultMismatch("incorrect ordered nonkey lookup result")
        if name == "range_read":
            bounds = statements[0].partition("BETWEEN ")[2].split(" AND ")
            low, high = map(int, bounds)
            expected = [(100000 * (min(high, account_rows - 1) - max(low, 0) + 1),)]
            if not equivalent(result, expected):
                raise ResultMismatch("incorrect range aggregate result")
        if role == "reader" and sales_rows % account_rows == 0:
            if sum(row[1] for row in result) != sales_rows * 100000:
                raise ResultMismatch("HTAP snapshot violates conserved balance")
    except ResultMismatch:
        raise
    except (TypeError, IndexError, ValueError) as error:
        raise ResultMismatch("malformed result: " + str(error)) from error


def trial_is_valid(collected, clients, invariant, failure):
    return (not failure and len(collected) == clients and invariant and
            all("fatal" not in w and not w.get("validation_failures", 0)
                for w in collected))


def oltp_child(engine, ports, name, worker, clients, account_rows, sales_rows, seed,
               timeout, warmup, seconds, start_time, ready, output):
    client = None
    try:
        client = Client(engine, ports, timeout)
        rng = random.Random(seed + worker)
        ready.put(True)
        while start_time.value == 0:
            time.sleep(.001)
        start = start_time.value
        measure_start, end = start + warmup, start + warmup + seconds
        while time.perf_counter() < start:
            time.sleep(.001)
        samples, errors, boundary_samples = [], [], []
        all_success = measured = attempted = delta_total = measured_errors = warmup_errors = crossing = 0
        sequence = validation_failures = 0
        role = "reader" if name == "htap" and worker == clients - 1 else "transaction"
        while time.perf_counter() < end:
            sequence += 1
            statements, delta = ((["SELECT a.bucket, SUM(a.balance) FROM sales s JOIN accounts a ON s.account_id = a.id GROUP BY a.bucket ORDER BY a.bucket"], 0)
                                 if role == "reader" else transaction(name, rng, worker, sequence, account_rows))
            begin = time.perf_counter()
            if begin >= measure_start:
                attempted += 1
            try:
                for sql in statements:
                    result = client.execute(sql)
                    if name in ("noop", "point_read", "nonkey_read", "range_read") or role == "reader":
                        validate_read_result(name, result, statements, account_rows, sales_rows, role)
                finish = time.perf_counter()
                all_success += 1
                delta_total += delta
                if begin >= measure_start and finish <= end:
                    measured += 1
                    samples.append((finish - begin) * 1000)
                elif finish > measure_start:
                    crossing += 1
                    boundary_samples.append({"begin": begin, "finish": finish,
                                             "latency_ms": (finish - begin) * 1000})
            except Exception as error:
                validation_failures += isinstance(error, ResultMismatch)
                if begin >= measure_start:
                    measured_errors += 1
                else:
                    warmup_errors += 1
                if len(errors) < 3:
                    errors.append(str(error))
                try:
                    client.execute("ROLLBACK")
                except Exception:
                    break
        output.put({"worker": worker, "role": role, "samples": samples, "completed": measured,
                    "all_success": all_success, "attempted": attempted, "errors": measured_errors,
                    "warmup_errors": warmup_errors,
                    "validation_failures": validation_failures,
                    "error_examples": errors, "delta": delta_total, "crossing": crossing,
                    "boundary_samples": boundary_samples})
    except Exception as error:
        ready.put(False)
        output.put({"fatal": str(error), "worker": worker})
    finally:
        if client:
            client.close()


def run_oltp(stack, args, output, rows):
    ctx = mp.get_context("spawn")
    account_rows = min(rows, 10000)
    for name in args.oltp_workloads:
        for clients in args.clients:
            if getattr(args, "oltp_cases", None) and f"{name}:{clients}" not in args.oltp_cases:
                continue
            for trial in range(args.trials):
                if getattr(args, "fresh_trials", False):
                    initialize(stack, args, output, rows)
                order = list(getattr(args, "oltp_engines", ENGINES))
                random.Random(args.seed + trial + clients).shuffle(order)
                for engine in order:
                    client = Client(engine, stack.ports, args.timeout)
                    client.execute("UPDATE accounts SET balance = 100000")
                    client.execute("DELETE FROM churn")
                    ready, results = ctx.Queue(), ctx.Queue()
                    start = ctx.Value("d", 0)
                    workers = [ctx.Process(target=oltp_child, args=(engine, stack.ports, name, i,
                               clients, account_rows, rows, args.seed + trial * 1000, args.timeout, args.warmup,
                               args.seconds, start, ready, results)) for i in range(clients)]
                    for worker in workers:
                        worker.start()
                    collected = []
                    failure = None
                    try:
                        if not all([ready.get(timeout=args.timeout + 10) for _ in workers]):
                            raise RuntimeError("worker could not connect")
                        start.value = time.perf_counter() + .1
                        deadline = start.value + args.warmup + args.seconds + args.timeout + 10
                        for _ in workers:
                            collected.append(results.get(timeout=max(1, deadline - time.perf_counter())))
                    except Exception as error:
                        failure = str(error) or type(error).__name__
                    finally:
                        for worker in workers:
                            worker.join(timeout=1)
                            if worker.is_alive():
                                worker.terminate()
                                worker.join()
                        ready.close()
                        results.close()
                    delta = sum(w.get("delta", 0) for w in collected)
                    actual = client.execute("SELECT COUNT(*), SUM(balance) FROM accounts")
                    churn = client.execute("SELECT COUNT(*) FROM churn")
                    invariant = (equivalent(actual, [(account_rows, account_rows * 100000 + delta)])
                                 and equivalent(churn, [(0,)]))
                    role_latency = {
                        role: percentiles([s for w in collected if w.get("role") == role
                                           for s in w.get("samples", [])])
                        for role in {w.get("role") for w in collected} - {None}}
                    samples = [s for w in collected for s in w.pop("samples", [])]
                    completed = sum(w.get("completed", 0) for w in collected)
                    errors = sum(w.get("errors", 0) for w in collected)
                    warmup_errors = sum(w.get("warmup_errors", 0) for w in collected)
                    valid = trial_is_valid(collected, clients, invariant, failure)
                    roles = {}
                    for role in {w.get("role") for w in collected} - {None}:
                        roles[role] = sum(w["completed"] for w in collected if w.get("role") == role) / args.seconds
                    record = {"kind": "oltp", "engine": engine, "rows": rows, "workload": name,
                              "clients": clients, "trial": trial, "seconds": args.seconds, "completed": completed,
                              "measure_start_monotonic": start.value + args.warmup,
                              "tps": completed / args.seconds, "errors": errors, "valid": valid,
                              "warmup_errors": warmup_errors,
                              "validation_failures": sum(w.get("validation_failures", 0) for w in collected),
                              "invariant": invariant, "account_state": actual, "churn_state": churn,
                              "workers": collected, "role_tps": roles, "role_latency": role_latency, "failure": failure, **percentiles(samples)}
                    append_json(output / "results.jsonl", record)
                    client.close()
                    print(f"oltp {rows} {name} c={clients} trial={trial} {engine}: {record['tps']:.1f} TPS errors={errors} valid={valid}", flush=True)


def load_tpch(stack, args, output):
    data = stack.work / "tpch-data"
    invocation = [str(ROOT / "build/tinylamb_tpch_benchmark"), str(stack.work / "unused"),
                  "--generate-only", "--scale-factor", str(args.tpch_scale), "--data-dir", str(data)]
    append_json(output / "commands.jsonl", {"argv": invocation})
    (output / "dbgen.txt").write_text(command(invocation))
    specs = tpch_schema()
    for engine in ENGINES:
        client = Client(engine, stack.ports, args.timeout)
        start = time.perf_counter()
        counts = {}
        for name, ddl, fields in specs:
            client.execute(adapt_sql(ddl, engine))
            def parsed():
                with (data / f"{name}.tbl").open() as stream:
                    for line in stream:
                        values = line.rstrip("\n").split("|")[:-1]
                        if len(values) != len(fields):
                            raise ValueError(f"invalid {name} row")
                        yield tuple(int(v) if f == "Integer" else float(v) if f == "Numeric" else datetime.date.fromisoformat(v) if f == "Date" else v for v, f in zip(values, fields))
            counts[name] = load_rows(client, name, parsed())
            actual = client.execute(f"SELECT COUNT(*) FROM {name}")
            if not equivalent(actual, [(counts[name],)]):
                raise RuntimeError(f"TPC-H row count mismatch: {engine} {name}")
        loading = time.perf_counter() - start
        if args.secondary_indexes:
            create_secondary_indexes(client, "tpch", output)
        analyze(client, counts)
        append_json(output / "results.jsonl", {"kind": "tpch_load", "engine": engine,
                    "scale": args.tpch_scale, "counts": counts, "load_seconds": loading})
        client.close()
        print(f"TPC-H loaded {engine} {counts} seconds={loading:.2f}", flush=True)
    (output / "tpch-manifest.txt").write_text((data / "tinylamb-tpch.manifest").read_text())
    (output / "tpch-data-sha256.json").write_text(json.dumps({p.name: hashlib.file_digest(p.open("rb"), "sha256").hexdigest() for p in data.glob("*.tbl")}, indent=2))


def summarize(output):
    rows = [json.loads(line) for line in (output / "results.jsonl").read_text().splitlines()]
    fields = ["kind", "suite", "rows", "workload", "engine", "clients", "trial", "median_ms",
              "first_ms", "tps", "p50_ms", "p95_ms", "p99_ms", "errors", "validation_failures", "correct", "valid", "status"]
    with (output / "summary.csv").open("w") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(r for r in rows if r["kind"] in ("olap", "oltp"))
    invalid = [r for r in rows if (r["kind"] == "olap" and not r["correct"])
               or (r["kind"] == "oltp" and (not r["valid"] or r["errors"] or r["warmup_errors"]))]
    (output / "failures.json").write_text(json.dumps(invalid, default=str, indent=2, ensure_ascii=False))
    return invalid


def main():
    global ENGINES
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True, help="new directory; never overwrite a run")
    parser.add_argument("--server", default=str(ROOT / "build/tinylamb_server"))
    parser.add_argument("--scratch", default="/var/tmp")
    parser.add_argument("--sizes", type=int, nargs="+", default=[10000, 100000, 1000000])
    parser.add_argument("--clients", type=int, nargs="+", default=[1, 4, 16])
    parser.add_argument("--oltp-workloads", nargs="+", choices=OLTP, default=list(OLTP))
    parser.add_argument("--seconds", type=float, default=5)
    parser.add_argument("--warmup", type=float, default=1)
    parser.add_argument("--trials", type=int, default=3)
    parser.add_argument("--repeats", type=int, default=5)
    parser.add_argument("--timeout", type=int, default=30)
    parser.add_argument("--seed", type=int, default=20261028)
    parser.add_argument("--read-workers", type=int)
    parser.add_argument("--tiny-query-memory-mib", type=int, default=64)
    parser.add_argument("--data-cache-mib", type=int, help="Tiny/PG/MySQL data buffers; default 512, or 1088 with Oracle; Oracle manages its own SGA")
    parser.add_argument("--olap-workloads", nargs="+", choices=list(olap_queries(100)))
    parser.add_argument("--explain-analyze", action="store_true", help="save tinylamb runtime plan after timing")
    parser.add_argument("--secondary-indexes", action="store_true", help="add the same explicit secondary keys on all engines")
    parser.add_argument("--server-cpus", help="server CPU affinity; default first 8 logical CPUs, or first 2 with Oracle Free")
    parser.add_argument("--tiny-port", type=int, default=55433)
    parser.add_argument("--pg-port", type=int, default=55432)
    parser.add_argument("--mysql-port", type=int, default=53306)
    parser.add_argument("--postgres-image", default="postgres:18")
    parser.add_argument("--mysql-image", default="mysql:8.4")
    parser.add_argument('--include-duckdb', action='store_true', help='also run DuckDB with official Quack over localhost')
    parser.add_argument('--duckdb-port', type=int, default=59494)
    parser.add_argument('--duckdb-threads', type=int, help='default number of selected server CPUs')
    parser.add_argument('--duckdb-memory-mib', type=int, help='DuckDB memory_limit; defaults to data-cache-mib, not total RSS')
    parser.add_argument('--duckdb-extensions', default=str(ROOT / 'build/benchmarks/duckdb-extensions-2.0.0'))
    parser.add_argument("--include-oracle", action="store_true", help="also run the optional official Oracle Free container")
    parser.add_argument("--oracle-image", default="container-registry.oracle.com/database/free@sha256:f988b0c04c4c386cd306a2a914c0d7a9702d83acc31b064a28ad8eb6278a8fba")
    parser.add_argument("--oracle-port", type=int, default=51521)
    parser.add_argument("--tpch-scale", type=float, default=.01, help="0 skips TPC-H; fractional scales are engineering tests")
    parser.add_argument("--skip-oltp", action="store_true")
    parser.add_argument("--skip-olap", action="store_true")
    parser.add_argument("--server-metrics", action="store_true")
    parser.add_argument("--htap-reader-baseline", action="store_true", help="time the exact HTAP reader SQL without writers in OLAP")
    parser.add_argument("--fresh-trials", action="store_true", help="reload all fixtures before each OLTP trial")
    parser.add_argument("--oltp-cases", nargs="+", help="restrict workload:clients pairs")
    parser.add_argument("--oltp-engines", choices=(*ENGINES, 'oracle', 'duckdb'), nargs="+")
    args = parser.parse_args()
    if args.server_cpus is None:
        args.server_cpus = ','.join(map(str, sorted(os.sched_getaffinity(0))[:2 if args.include_oracle else 8]))
    if args.read_workers is None:
        args.read_workers = 2 if args.include_oracle else 8
    if args.data_cache_mib is None:
        args.data_cache_mib = 1088 if args.include_oracle else 512
    if args.include_oracle:
        ENGINES = (*ENGINES, 'oracle')
    if args.include_duckdb:
        ENGINES = (*ENGINES, 'duckdb')
    if args.duckdb_threads is None:
        selected_cpus = set()
        for part in args.server_cpus.split(','):
            limits = list(map(int, part.split('-')))
            selected_cpus.update(range(limits[0], limits[-1] + 1))
        args.duckdb_threads = len(os.sched_getaffinity(0) & selected_cpus)
    if args.duckdb_memory_mib is None:
        args.duckdb_memory_mib = args.data_cache_mib
    if min(args.duckdb_threads, args.duckdb_memory_mib) < 1:
        parser.error('DuckDB threads and memory must be >=1')
    if args.oltp_engines is None:
        args.oltp_engines = list(ENGINES)
    if not set(args.oltp_engines) <= set(ENGINES):
        parser.error('optional workloads require --include-oracle / --include-duckdb')
    if min(args.sizes) < 100 or min(args.clients) < 1 or min(args.seconds, args.trials, args.repeats, args.timeout) <= 0 or args.warmup < 0 or args.tpch_scale < 0:
        parser.error("sizes >=100, clients/repetitions/durations >0, warmup/scale >=0 required")
    if args.tiny_query_memory_mib < 1 or args.data_cache_mib < 1:
        parser.error("tiny-query-memory-mib and data-cache-mib must be >= 1")
    if len({args.tiny_port, args.pg_port, args.mysql_port}) != 3:
        parser.error("ports must be distinct")
    if args.include_oracle and args.oracle_port in (args.tiny_port, args.pg_port, args.mysql_port):
        parser.error('Oracle port must be distinct')
    selected_ports = [args.tiny_port, args.pg_port, args.mysql_port]
    if args.include_oracle:
        selected_ports.append(args.oracle_port)
    if args.include_duckdb:
        selected_ports.append(args.duckdb_port)
    if len(selected_ports) != len(set(selected_ports)):
        parser.error('all selected engine ports must be distinct')
    if args.oltp_cases:
        allowed_cases = {f"{name}:{clients}" for name in args.oltp_workloads for clients in args.clients}
        if not set(args.oltp_cases) <= allowed_cases:
            parser.error("oltp-cases must refer to selected workload:clients pairs")
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    runtime = output / 'runtime-harness'
    runtime.mkdir()
    for source in Path(__file__).parent.iterdir():
        if source.suffix == '.py' or source.name.startswith('requirements') or source.name == 'README.md':
            (runtime / source.name).write_bytes(source.read_bytes())
    stack = Stack(args, output)
    try:
        stack.start()
        metadata = stack.metadata()
        (output / "environment.json").write_text(json.dumps(metadata, indent=2, ensure_ascii=False))
        (output / "tracked-changes.patch").write_bytes(subprocess.check_output(["git", "diff", "HEAD"], cwd=ROOT))
        for rows in args.sizes:
            initialize(stack, args, output, rows)
            queries = olap_queries(rows)
            if args.olap_workloads:
                queries = {k: v for k, v in queries.items() if k in args.olap_workloads}
            if args.htap_reader_baseline:
                queries["htap_reader"] = "SELECT a.bucket, SUM(a.balance) FROM sales s JOIN accounts a ON s.account_id = a.id GROUP BY a.bucket ORDER BY a.bucket"
            if not args.skip_olap:
                run_olap(stack, args, output, rows, queries, "micro")
            # The account fixture is capped at 10k, so repeat OLTP only for
            # the largest sales fixture; other scales add no account coverage.
            if not args.skip_oltp and rows == args.sizes[-1]:
                run_oltp(stack, args, output, rows)
        if args.tpch_scale:
            load_tpch(stack, args, output)
            run_olap(stack, args, output, args.tpch_scale, tpch_queries(args.tpch_scale), "tpch")
        metadata["load_end"] = os.getloadavg()
        if args.include_oracle:
            client = Client('oracle', stack.ports, args.timeout)
            metadata['oracle_schema_segments_bytes'] = client.execute('SELECT segment_type, SUM(bytes) FROM user_segments GROUP BY segment_type ORDER BY segment_type')
            metadata['oracle_sga_end'] = client.execute('SELECT name, bytes FROM v$sgainfo ORDER BY name')
            client.close()
        if args.include_duckdb:
            client = Client('duckdb', stack.ports, args.timeout)
            metadata['duckdb_database_size'] = client.execute('PRAGMA database_size')
            metadata['duckdb_file_sizes'] = {p.name: p.stat().st_size for p in stack.work.glob('duckdb.db*') if p.is_file()}
            client.close()
        metadata["finished"] = time.strftime("%Y-%m-%dT%H:%M:%S%z")
        (output / "environment.json").write_text(json.dumps(metadata, indent=2, ensure_ascii=False))
        invalid = summarize(output)
        print(f"finished; {len(invalid)} failed/incorrect/error-bearing measurements; results: {output}", flush=True)
        return 1 if invalid else 0
    finally:
        stack.close()


if __name__ == "__main__":
    sys.exit(main())
