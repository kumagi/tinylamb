#!/usr/bin/env python3
"""Quiet single-SQL diagnostics on a private W=1 clone; not a TPS comparison."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import socket
import statistics
import subprocess
import time

from run import Client, append_json
from tpcc_pg import clone
from workloads import ROOT


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--output', type=Path, required=True)
    p.add_argument('--server', type=Path, required=True)
    p.add_argument('--fixture', type=Path, required=True)
    p.add_argument('--plans-only', action='store_true', help='no latency comparison; inspect structure/counts during validation')
    p.add_argument('--disable-index-prefetch', action='store_true', help='diagnostic A/B only; never a final comparison profile')
    p.add_argument('--mixed-client', type=Path, help='diagnostic mixed writers; requires --plans-only, all timings unqualified')
    p.add_argument('--trace-sql', action='store_true', help='ask diagnostic mixed client to trace worker 0 SQL; not a performance comparison')
    p.add_argument('--callgrind', action='store_true', help='instrument the ready server before client startup; skips extra plan probes')
    p.add_argument('--mixed-transaction', type=int, choices=range(5), help='diagnostic transaction type only')
    p.add_argument('--mixed-clients', type=int, default=10)
    a = p.parse_args()
    if a.mixed_client and not a.plans_only:
        p.error('--mixed-client requires --plans-only')
    if a.callgrind and not a.mixed_client:
        p.error('--callgrind requires --mixed-client')
    out = a.output.resolve(); out.mkdir(parents=True, exist_ok=False)
    work = ROOT / 'build/benchmarks' / out.name
    work.mkdir(parents=True, exist_ok=False)
    clone(a.fixture, work / 'database')
    with socket.socket() as s:
        s.bind(('127.0.0.1', 0)); port = s.getsockname()[1]
    env = {k: v for k, v in os.environ.items() if not k.startswith('TINYLAMB_')}
    env.update(TINYLAMB_PAGE_POOL_BYTES=str(512 << 20), TINYLAMB_QUERY_MEMORY_BYTES=str(256 << 20))
    if a.disable_index_prefetch:
        env['TINYLAMB_DISABLE_INDEX_SCAN_PREFETCH'] = '1'
    argv = ['taskset', '-c', '0-15', str(a.server.resolve()), str(work / 'database'),
            '--port', str(port), '--read-workers', '8']
    (out / 'environment.json').write_text(json.dumps(dict(argv=argv, fixture=str(a.fixture),
        environment={k: v for k, v in env.items() if k.startswith('TINYLAMB_')},
        server_sha256=hashlib.file_digest(a.server.open('rb'), 'sha256').hexdigest(),
        plans_only=a.plans_only,
        load_start=os.getloadavg()), indent=2))
    client = None
    mixed = None
    with (out / 'server.log').open('w') as log:
        server_argv = argv
        if a.callgrind:
            server_argv = ['valgrind', '--tool=callgrind', '--fair-sched=yes', '--instr-atstart=no',
                           '--callgrind-out-file=' + str(out / 'callgrind.out'), *argv[3:]]
            # taskset must wrap valgrind, not become the instrumented program.
            server_argv = argv[:3] + server_argv
        process = subprocess.Popen(server_argv, env=env, stdout=log, stderr=subprocess.STDOUT, cwd=ROOT)
        (out / 'server-command.json').write_text(json.dumps(server_argv, indent=2))
        try:
            deadline = time.monotonic() + 120
            while client is None:
                try:
                    client = Client('tinylamb', {'tinylamb': port}, 60)
                except Exception:
                    if time.monotonic() > deadline or process.poll() is not None:
                        raise
                    time.sleep(.1)
            last = client.execute('SELECT c_last FROM customer WHERE c_w_id=1 AND c_d_id=1 AND c_id=1')[0][0]
            order = client.execute('SELECT o_id FROM orders WHERE o_w_id=1 AND o_d_id=1 AND o_c_id=1 ORDER BY o_id DESC LIMIT 1')[0][0]
            next_id = client.execute('SELECT d_next_o_id FROM district WHERE d_w_id=1 AND d_id=1')[0][0]
            line_sql = f'SELECT ol_i_id FROM order_line WHERE ol_w_id=1 AND ol_d_id=1 AND ol_o_id>={next_id-20} AND ol_o_id<{next_id}'
            items = ','.join(str(x) for x in sorted({r[0] for r in client.execute(line_sql)}))
            queries = dict(
                customer_id='SELECT c_id, c_first, c_balance FROM customer WHERE c_w_id=1 AND c_d_id=1 AND c_id=1',
                customer_name="SELECT c_id, c_first, c_balance FROM customer WHERE c_w_id=1 AND c_d_id=1 AND c_last='" + last.replace("'", "''") + "' ORDER BY c_first",
                latest_order='SELECT o_id, o_entry_d, o_carrier_id FROM orders WHERE o_w_id=1 AND o_d_id=1 AND o_c_id=1 ORDER BY o_id DESC LIMIT 1',
                order_lines=f'SELECT ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_delivery_d FROM order_line WHERE ol_w_id=1 AND ol_d_id=1 AND ol_o_id={order} AND ol_number>=1 AND ol_number<=15 ORDER BY ol_number',
                stock_lines=line_sql,
                stock_in=f'SELECT s_i_id FROM stock WHERE s_w_id=1 AND s_quantity<20 AND s_i_id IN ({items})',
                oldest_new_order='SELECT no_o_id FROM new_order WHERE no_w_id=1 AND no_d_id=1 ORDER BY no_o_id LIMIT 1')
            if a.mixed_client:
                if a.callgrind:
                    subprocess.run(['callgrind_control', '-i', 'on', str(process.pid)], check=True)
                mixed_argv = [str(a.mixed_client.resolve()), '--port', str(port),
                              '--clients', str(a.mixed_clients), '--seconds', '20', '--warmup', '1', '--seed', '20260819']
                if a.trace_sql:
                    mixed_argv += ['--trace-sql', '1']
                if a.mixed_transaction is not None:
                    mixed_argv += ['--transaction', str(a.mixed_transaction)]
                (out / 'mixed-client-provenance.json').write_text(json.dumps(dict(argv=mixed_argv,
                    sha256=hashlib.file_digest(a.mixed_client.open('rb'), 'sha256').hexdigest(),
                    comparison_timings_qualified=False), indent=2))
                with (out / 'mixed-client.log').open('w') as mixed_log:
                    mixed = subprocess.Popen(mixed_argv,
                        stdout=mixed_log, stderr=subprocess.STDOUT)
                time.sleep(1)
            cases = [] if a.callgrind else [(sample, name, sql) for sample in range(8 if mixed else 1) for name, sql in queries.items()]
            for sample, name, sql in cases:
                client.execute('BEGIN')
                first = client.execute(sql)
                times = []
                for _ in range(0 if a.plans_only else 9):
                    start = time.perf_counter(); result = client.execute(sql)
                    times.append((time.perf_counter() - start) * 1000)
                    assert first == result
                plan = client.execute('EXPLAIN ANALYZE ' + sql)
                client.execute('COMMIT')
                record = dict(name=name, sample=sample, sql=sql, rows=len(first), samples_ms=times,
                              median_ms=statistics.median(times) if times else None, actual_plan=plan)
                append_json(out / 'results.jsonl', record)
                print(name, record['median_ms'], len(first), flush=True)
            if mixed:
                mixed.wait(timeout=60)
                (out / 'mixed-client-exit.json').write_text(json.dumps(dict(exit_code=mixed.returncode)))
                if a.callgrind:
                    subprocess.run(['callgrind_control', '-d', str(process.pid)], check=True)
                    subprocess.run(['callgrind_control', '-i', 'off', str(process.pid)], check=True)
                if mixed.returncode:
                    raise RuntimeError('diagnostic client failed; do not treat partial profile as successful workload')
        finally:
            if mixed and mixed.poll() is None:
                mixed.terminate(); mixed.wait(timeout=30)
            if client:
                client.close()
            process.terminate()
            try:
                process.wait(timeout=30)
            except subprocess.TimeoutExpired:
                process.kill(); process.wait()


if __name__ == '__main__':
    main()
