#!/usr/bin/env python3
"""Run the isolated aggregation experiment, verify every sample, and plot medians.

Requires a built tinylamb_spsc_aggregation_benchmark and matplotlib for plotting.
Optional --q67-input accepts the 16-byte (int64 key ID, int64 value) row fixture.
This is an integer aggregate microbenchmark, not a SQL engine comparison.
"""
import argparse
from collections import defaultdict
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import statistics
import subprocess

ROOT = Path(__file__).resolve().parents[2]
MODES = ('barrier_n1', 'spsc_n1', 'partitioned_merge')
LABELS = ('Barrier + serial merge', 'SPSC N:1 pipeline', 'Partitioned parallel merge')


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def cases(q67):
    result = []
    def add(name, args, threads=(2, 8, 16), reverse=False, cpus=None):
        for t in threads:
            result.append(dict(name=name, threads=t, args=args, reverse=reverse,
                               cpus=cpus or list(range(t))))
    for rows in (1_000_000, 8_000_000):
        for groups in (64, 65536, 1048576):
            add(f'{rows//1000000}M_g{groups}', ['--rows', str(rows), '--groups', str(groups)])
    for groups in (64, 1048576):
        add(f'32M_g{groups}', ['--rows', '32000000', '--groups', str(groups)])
    add('8M_skew90', ['--rows', '8000000', '--groups', '1048576', '--skew'])
    add('8M_work16', ['--rows', '8000000', '--groups', '1048576', '--work', '16'])
    if q67:
        add('Q67_keys', ['--input', str(q67), '--groups', '342282'])
    for depth, batch in ((2, 256), (64, 256), (8, 32)):
        add(f'8M_g1048576_depth{depth}_batch{batch}',
            ['--rows', '8000000', '--groups', '1048576', '--ring-batches', str(depth),
             '--batch-entries', str(batch)], threads=(8,))
    for local in (512, 32768):
        add(f'8M_g1048576_local{local}', ['--rows', '8000000', '--groups', '1048576',
                                      '--local-groups', str(local)], threads=(8,))
    for groups in (64, 1048576):
        add(f'8M_g{groups}_reverse', ['--rows', '8000000', '--groups', str(groups)],
            threads=(8, 16), reverse=True)
    add('8M_g1048576_other_l3', ['--rows', '8000000', '--groups', '1048576'],
        threads=(8,), cpus=list(range(8, 16)))
    return result


def summarize(output):
    manifest = json.loads((output / 'manifest.json').read_text())
    records = []
    for case in manifest['cases']:
        path = output / f"{case['name']}_t{case['threads']}.jsonl"
        data = [json.loads(line) for line in path.read_text().splitlines()]
        assert len(data) == 18 and all(r['verified'] for r in data)
        for mode in MODES:
            rows = [r for r in data if r['mode'] == mode and not r['warmup']]
            assert len(rows) == 5
            record = dict(case=case['name'], threads=case['threads'], mode=mode)
            for key in rows[0]:
                if isinstance(rows[0][key], (int, float)) and not isinstance(rows[0][key], bool):
                    record[key] = statistics.median(r[key] for r in rows)
            record['wall_min_ms'] = min(r['wall_ms'] for r in rows)
            record['wall_max_ms'] = max(r['wall_ms'] for r in rows)
            records.append(record)
    (output / 'summary.json').write_text(json.dumps(records, indent=2) + '\n')
    table = ['| Input | Threads | Barrier ms | SPSC ms | Partitioned ms | SPSC / barrier |',
             '|---|---:|---:|---:|---:|---:|']
    grouped = defaultdict(dict)
    for r in records:
        grouped[r['case'], r['threads']][r['mode']] = r
    for (name, threads), modes in grouped.items():
        a, b, c = (modes[m]['wall_ms'] for m in MODES)
        table.append(f'| {name} | {threads} | {a:.3f} | {b:.3f} | {c:.3f} | {b/a:.2f}x |')
    (output / 'results.md').write_text('\n'.join(table) + '\n')
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    names = ['8M_g64', '8M_g65536', '8M_g1048576', '8M_skew90', '8M_work16']
    if any(r['case'] == 'Q67_keys' for r in records):
        names.append('Q67_keys')
    fig, axes = plt.subplots(2, 3, figsize=(14, 8))
    for ax, name in zip(axes.flat, names):
        for mode, label in zip(MODES, LABELS):
            rows = sorted((r for r in records if r['case'] == name and r['mode'] == mode),
                          key=lambda r: r['threads'])
            ax.plot([r['threads'] for r in rows], [r['wall_ms'] for r in rows], 'o-', label=label)
        ax.set_title({'8M_g64': '8M rows / 64 groups', '8M_g65536': '8M rows / 65K groups', '8M_g1048576': '8M rows / 1M groups', '8M_skew90': '8M rows / 90% hot keys', '8M_work16': '8M rows / heavier producers', 'Q67_keys': 'TPC-DS Q67 key frequencies only'}[name])
        ax.set_xticks([2, 8, 16])
        ax.set_xlabel('Total physical cores')
        ax.set_ylabel('Median wall time, ms (lower is better)')
        ax.set_yscale('log')
        ax.grid(alpha=.2)
    for ax in list(axes.flat)[len(names):]:
        ax.set_visible(False)
    handles, labels = axes.flat[0].get_legend_handles_labels()
    fig.legend(handles, labels, loc='upper center', bbox_to_anchor=(.5, .95), ncol=3)
    fig.suptitle('INT64 COUNT/SUM | SPSC uses T-1 producers + 1 consumer | 1 warmup + 5 trials', y=.99)
    fig.tight_layout(rect=(0, 0, 1, .90))
    for ext in ('png', 'svg'):
        fig.savefig(output / f'comparison.{ext}', dpi=160, bbox_inches='tight')
    plt.close(fig)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', type=Path, default=ROOT / 'build-tpcds-release/tinylamb_spsc_aggregation_benchmark')
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--q67-input', type=Path)
    parser.add_argument('--summarize-only', action='store_true')
    opts = parser.parse_args()
    output = opts.output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    if not opts.summarize_only:
        if (output / 'manifest.json').exists():
            raise SystemExit('Use a fresh output directory; existing results will not be overwritten')
        run_cases = cases(opts.q67_input.resolve() if opts.q67_input else None)
        manifest = dict(started=datetime.now(timezone.utc).isoformat(), binary=str(opts.binary.resolve()),
                        binary_sha256=digest(opts.binary), source_sha256=digest(ROOT / 'benchmark/spsc_aggregation_benchmark.cpp'),
                        git_head=subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip(),
                        cases=run_cases, repeats=5, local_groups=4096, ring_batches=8, batch_entries=256,
                        morsel_rows=65536, note='Dirty tree; excluded setup and validation; no SQL execution')
        if opts.q67_input:
            manifest['q67_sha256'] = digest(opts.q67_input)
        (output / 'manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')
        (output / 'lscpu.txt').write_text(subprocess.check_output(['lscpu'], text=True))
        for i, case in enumerate(run_cases):
            command = [str(opts.binary.resolve()), '--threads', str(case['threads']), '--cpus',
                       ','.join(map(str, case['cpus'])), '--repeats', '5', *case['args']]
            if case['reverse']:
                command.append('--reverse')
            case['command'] = command
            print(f"[{i+1}/{len(run_cases)}] {case['name']} t{case['threads']}", flush=True)
            with (output / f"{case['name']}_t{case['threads']}.jsonl").open('w') as out:
                subprocess.run(command, stdout=out, check=True, timeout=600)
        manifest['finished'] = datetime.now(timezone.utc).isoformat()
        assert manifest['binary_sha256'] == digest(opts.binary), 'Binary changed during measurements'
        (output / 'manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')
    summarize(output)


if __name__ == '__main__':
    main()
