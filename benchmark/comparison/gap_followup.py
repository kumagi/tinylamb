#!/usr/bin/env python3
"""Recheck the A-H gap plan, with isolated 60s x 5 write/HTAP windows.

Uses the comparison runner's owned, disposable stack. Run profiles sequentially.
"""
import argparse
import copy
import json
import os
from pathlib import Path
import subprocess
import time

from run import Stack, initialize, load_tpch, run_olap, run_oltp, summarize
from workloads import ROOT, olap_queries, tpch_queries


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--output', required=True, type=Path)
    p.add_argument('--secondary-indexes', action='store_true')
    p.add_argument('--seed', type=int, default=20261028)
    p.add_argument('--server', default=str(ROOT / 'build/tinylamb_server'))
    p.add_argument('--phase', choices=('all', 'olap', 'concurrent'), default='all')
    p.add_argument('--seconds', type=float, default=60)
    p.add_argument('--trials', type=int, default=5)
    p.add_argument('--warmup', type=float, default=10)
    p.add_argument('--only-workloads', nargs='+', choices=('nonkey_read','hot_update','update','transfer','insert_delete','htap'))
    p.add_argument('--history', choices=('fresh', 'continued'), default='fresh')
    a = p.parse_args()
    if min(a.seconds, a.trials) <= 0 or a.warmup < 0:
        p.error('positive duration/trials and nonnegative warmup required')
    a.output = a.output.resolve()
    a.output.mkdir(parents=True, exist_ok=False)
    a.scratch = '/var/tmp'
    a.server_cpus = ','.join(map(str, sorted(os.sched_getaffinity(0))[:8]))
    a.tiny_port, a.pg_port, a.mysql_port = 55433, 55432, 53306
    a.postgres_image = 'postgres@sha256:979bccfde35445db618c65107d27d08c6c21339d8dc3ad4c830c6633947be0e4'
    a.mysql_image = 'mysql@sha256:a0225d1ea6631c8bc2385dc4c2faba17bbf1834ecf369e1659a9a382bd8d5dd7'
    a.tiny_query_memory_mib, a.read_workers = 64, 8
    a.server_metrics, a.explain_analyze = True, True
    a.timeout, a.repeats, a.tpch_scale = 60, 5, .01
    a.oltp_engines = ['tinylamb', 'postgresql']
    a.fresh_trials = a.history == 'fresh'
    stack = Stack(a, a.output)
    protocol = []
    try:
        stack.start()
        metadata = stack.metadata()
        (a.output / 'environment.json').write_text(json.dumps(metadata, indent=2))
        (a.output / 'tracked-changes.patch').write_bytes(subprocess.check_output(['git', 'diff', 'HEAD'], cwd=ROOT))
        if a.phase in ('all', 'olap'):
            for rows in (10000, 100000, 1000000):
                initialize(stack, a, a.output, rows)
                queries = olap_queries(rows)
                if rows == 1000000:
                    queries['htap_reader'] = 'SELECT a.bucket, SUM(a.balance) FROM sales s JOIN accounts a ON s.account_id = a.id GROUP BY a.bucket ORDER BY a.bucket'
                run_olap(stack, a, a.output, rows, queries, 'micro')
            load_tpch(stack, a, a.output)
            run_olap(stack, a, a.output, a.tpch_scale, tpch_queries(a.tpch_scale), 'tpch')
        else:
            initialize(stack, a, a.output, 1000000)
        if a.phase in ('all', 'concurrent'):
            cases = ([('transfer', 16), ('insert_delete', 16)] if a.secondary_indexes
                     else [('hot_update', 1), ('update', 16)])
            # Read-only ordered nonkey scans need concurrency coverage, but
            # not a minute of WAL history. Keep their protocol explicit.
            if not a.secondary_indexes:
                cases = [('nonkey_read', 4), ('nonkey_read', 16)] + cases
            cases += [('htap', 4), ('htap', 16)]
            if a.only_workloads:
                cases = [(name, clients) for name, clients in cases if name in a.only_workloads]
                if not cases:
                    raise ValueError('no selected workload applies to this profile')
            for name, clients in cases:
                case = copy.copy(a)
                case.oltp_workloads, case.clients, case.oltp_cases = [name], [clients], None
                if name == 'nonkey_read':
                    case.seconds, case.warmup, case.trials = 10, 2, 3
                protocol.append(dict(workload=name, clients=clients, seconds=case.seconds,
                                     warmup=case.warmup, trials=case.trials, history=case.history,
                                     engines=case.oltp_engines, seed=case.seed))
                (a.output / 'protocol.json').write_text(json.dumps(protocol, indent=2))
                run_oltp(stack, case, a.output, 1000000)
        invalid = summarize(a.output)
        metadata.update(finished=time.strftime('%Y-%m-%dT%H:%M:%S%z'), load_end=os.getloadavg())
        (a.output / 'environment.json').write_text(json.dumps(metadata, indent=2))
        hard = [r for r in invalid if (r['kind'] == 'olap' or not r.get('valid'))]
        print(f'finished: {len(hard)} incorrect/invalid measurements, {len(invalid)-len(hard)} valid trials with transaction errors', flush=True)
        return bool(hard)
    finally:
        stack.close()


if __name__ == '__main__':
    raise SystemExit(main())
