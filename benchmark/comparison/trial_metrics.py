#!/usr/bin/env python3
"""Align cumulative server counters to the interior of each timed trial."""
import argparse
import json
from pathlib import Path


def window_metrics(points, start, duration):
    inside = [p for p in points if start <= p['time_ns'] / 1e9 <= start + duration]
    if len(inside) < 2:
        return None
    first, last = inside[0], inside[-1]
    elapsed = (last['time_ns'] - first['time_ns']) / 1e9
    def delta(name): return last[name] - first[name]
    def per(name, count):
        return delta(name) / delta(count) / 1e6 if delta(count) else None
    return dict(interior_seconds=elapsed,
                commands_per_second=delta('tasks') / elapsed,
                queue_ms_per_command=per('queue_ns', 'tasks'),
                execution_ms_per_command=per('execution_ns', 'tasks'),
                completion_ms_per_command=per('completion_ns', 'tasks') if 'completion_ns' in first else None,
                deferred_wait_ms_per_command=per('durability_wait_ns', 'durability_commands') if 'durability_commands' in first else None,
                commands_per_durability_batch=delta('durability_commands') / delta('durability_batches') if first.get('durability_batches') is not None and delta('durability_batches') else None,
                own_wal_wait_ms_per_commit=per('wal_wait_ns', 'wal_wait_count'),
                sync_ms_per_call=per('wal_sync_ns', 'wal_sync_count'),
                commits_per_sync=delta('wal_wait_count') / delta('wal_sync_count') if delta('wal_sync_count') else None,
                deferred_replies_per_sync=delta('durability_commands') / delta('wal_sync_count') if 'durability_commands' in first and delta('wal_sync_count') else None,
                wal_bytes_per_second=delta('wal_buffered_bytes') / elapsed,
                max_pending_tasks=max(p['pending_tasks'] for p in inside),
                max_version_chains=max(p['version_chains'] for p in inside),
                max_snapshot_lag=max(p['current_commit_ts']-p['oldest_snapshot_ts']
                                     if p['oldest_snapshot_ts'] else 0 for p in inside))


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('directory', type=Path)
    a = p.parse_args()
    points = []
    for line in (a.directory / 'tinylamb-server.log').read_text().splitlines():
        if line.startswith('TINYLAMB_METRICS '):
            points.append(json.loads(line.partition(' ')[2]))
    records = []
    for line in (a.directory / 'results.jsonl').read_text().splitlines():
        r = json.loads(line)
        if r['kind'] != 'oltp' or r['engine'] != 'tinylamb':
            continue
        entry = {k:r[k] for k in ('workload','clients','trial','seconds','tps','p50_ms','p95_ms','p99_ms','valid','errors')}
        entry['metrics'] = window_metrics(points, r['measure_start_monotonic'], r['seconds'])
        entry['roles'] = r['role_tps']
        entry['role_latency'] = r.get('role_latency')
        entry['crossing'] = {role:sum(w.get('crossing',0) for w in r['workers'] if w['role']==role)
                             for role in r['role_tps']}
        records.append(entry)
    (a.directory / 'trial-metrics.json').write_text(json.dumps(records, indent=2))
    print(json.dumps(records, indent=2))


if __name__ == '__main__':
    main()
