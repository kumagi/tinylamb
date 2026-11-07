#!/usr/bin/env python3
"""Optional report figures from the saved, complete 41-metric comparison."""
import argparse
import json
from pathlib import Path


PLANS = {
    'count': 'A', 'sum': 'A', 'filter_1pct': 'A', 'distinct': 'A', 'Q06': 'A',
    'top_100': 'B', 'exists': 'C', 'join_group': 'D', 'left_join': 'E',
    'nonkey_read': 'F', 'hot_update': 'G', 'update': 'G', 'transfer': 'G',
    'insert_delete': 'G', 'htap': 'H',
}
NAMES = {
    'A': 'Index scans', 'B': 'Ordered Top N', 'C': 'Correlated EXISTS',
    'D': 'Grouped join', 'E': 'LEFT JOIN', 'F': 'Nonkey read',
    'G': 'Durable writes', 'H': 'HTAP reader',
}


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('comparison', type=Path)
    p.add_argument('unindexed_results', type=Path)
    p.add_argument('--output', type=Path, required=True)
    a = p.parse_args()
    rows = json.loads(a.comparison.read_text())
    if len(rows) != 41 or any(r['after'] is None for r in rows):
        p.error('a complete comparison of the original 41 metrics is required')
    groups = {name: [] for name in NAMES}
    for row in rows:
        groups[PLANS[row['before']['workload']]].append(row)
    summary = {
        name: dict(count=len(items),
                   before=max(r['before']['loss_factor'] for r in items),
                   current_pg=max(r['after']['loss_factor'] for r in items),
                   historical_pg=max(r['loss_against_original_pg'] for r in items))
        for name, items in groups.items()
    }
    records = [json.loads(line) for line in a.unindexed_results.read_text().splitlines()]
    hot = {engine: sorted(
        (r for r in records if r['kind'] == 'oltp' and
         r['workload'] == 'hot_update' and r['clients'] == 1 and
         r['engine'] == engine), key=lambda r: r['trial'])
        for engine in ('tinylamb', 'postgresql')}
    if any([r['trial'] for r in items] != list(range(5)) for items in hot.values()):
        p.error('five complete hot_update trials per engine are required')
    reference = next(r['before']['postgresql'] for r in rows
                     if r['before']['profile'] == 'unindexed' and
                     r['before']['workload'] == 'hot_update' and
                     r['before']['metric'] == 'p50_ms')
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt

    fig, (left, right) = plt.subplots(1, 2, figsize=(13.5, 5.6),
                                     gridspec_kw={'width_ratios': [1.45, 1]},
                                     layout='constrained')
    colors = {'before': '#777777', 'current_pg': '#0072B2',
              'historical_pg': '#D55E00'}
    labels = {'before': 'Before / historical PG', 'current_pg': 'After / current PG',
              'historical_pg': 'After / historical PG'}
    for shift, key, marker in ((-.17, 'before', 'o'),
                               (0, 'current_pg', 's'),
                               (.17, 'historical_pg', 'D')):
        left.scatter([summary[name][key] for name in NAMES],
                     [i + shift for i in range(8)], color=colors[key],
                     marker=marker, s=44, label=labels[key], zorder=3)
    left.set_xscale('log')
    left.set_xlim(.5, 15000)
    left.set_yticks(range(8), [f'{name}: {NAMES[name]} ({summary[name]["count"]})'
                             for name in NAMES])
    left.invert_yaxis()
    left.axvline(10, color='#333333', linestyle='--', linewidth=1)
    left.grid(axis='x', alpha=.25)
    left.set_xlabel('Maximum loss factor within each plan (lower is better)')
    left.set_title('All 41 flagged metrics, including p95 / p99')
    left.legend(loc='lower center', bbox_to_anchor=(.5, -.26),
                fontsize=8.5, ncol=2, frameon=False)
    for engine, color in (('tinylamb', '#0072B2'), ('postgresql', '#009E73')):
        right.plot(range(1, 6), [r['p50_ms'] for r in hot[engine]],
                   marker='o', color=color, label=engine)
    right.axhline(reference * 10, color='#D55E00', linestyle='--',
                  label='10x historical PG target')
    right.set_xticks(range(1, 6))
    right.set_ylim(bottom=0)
    right.set_xlabel('Trial (10 s warmup + 60 s measurement)')
    right.set_ylabel('Successful request p50 (ms)')
    right.set_title('Hot update, one client: variation between trials')
    right.grid(alpha=.25)
    right.legend(fontsize=9, frameon=False)
    fig.suptitle('tinylamb PostgreSQL gap follow-up: 2026-10-29', fontsize=15)
    a.output.mkdir(parents=True, exist_ok=True)
    for suffix in ('png', 'svg'):
        fig.savefig(a.output / f'comparison.{suffix}', dpi=160)
    (a.output / 'figure-data.json').write_text(json.dumps(summary, indent=2))
    plt.close(fig)


if __name__ == '__main__':
    main()
