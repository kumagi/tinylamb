#!/usr/bin/env python3
"""Join two verified ClickBench comparisons without hiding losing queries."""
import argparse
import json
import math
from pathlib import Path
import statistics


def read_run(directory, reference):
    manifest = json.loads((directory / 'run.json').read_text())
    records = [json.loads(line) for line in (directory / 'results.jsonl').read_text().splitlines()]
    if manifest['reference'] != reference:
        raise ValueError(f'{directory}: expected {reference}')
    if len(records) != 43 or sorted(r['query'] for r in records) != list(range(1, 44)):
        raise ValueError('all 43 distinct queries are required')
    if not all(r.get('correct') for r in records):
        raise ValueError('cannot report performance success with incorrect answers')
    for record in records:
        for engine in ('tinylamb', reference):
            samples = record[engine]['samples_ms']
            if len(samples) != manifest['repeats'] or any(not isinstance(s, (int, float)) or not math.isfinite(s) or s <= 0 for s in samples):
                raise ValueError('invalid or missing measurement samples')
            if not math.isclose(statistics.median(samples),
                                record[engine]['median_ms'], rel_tol=1e-12):
                raise ValueError('median does not match raw samples')
    return manifest, sorted(records, key=lambda r: r['query'])


def report(duck, click, output):
    dm, dr = read_run(duck, 'duckdb')
    cm, cr = read_run(click, 'clickhouse')
    for field in ('binary_sha256', 'threads', 'cpus', 'warmups', 'repeats',
                  'reverse', 'reference_session'):
        # Both runs must have followed the same query order: --reverse swaps
        # which engine runs first on odd/even queries, which is exactly the
        # cache/warm-up bias this report is supposed to control for.
        if dm.get(field) != cm.get(field):
            raise ValueError('comparison conditions differ: ' + field)
    if dm.get('reference_session_timezone') != cm.get('reference_session_timezone'):
        raise ValueError('comparison conditions differ: reference_session_timezone')
    for field in ('rows', 'sources', 'query_sha256', 'timezone', 'upstream_commit'):
        if dm['dataset'][field] != cm['dataset'][field]:
            raise ValueError('datasets differ: ' + field)
    output.mkdir(parents=True, exist_ok=True)
    totals = {'tinylamb_vs_duckdb': sum(r['tinylamb']['median_ms'] for r in dr),
              'tinylamb_vs_clickhouse': sum(r['tinylamb']['median_ms'] for r in cr),
              'duckdb': sum(r['duckdb']['median_ms'] for r in dr),
              'clickhouse': sum(r['clickhouse']['median_ms'] for r in cr)}
    wins = {e: sum(r['tinylamb']['median_ms'] < r[e]['median_ms'] for r in rows)
            for e, rows in [('duckdb', dr), ('clickhouse', cr)]}
    summary = {'rows': dm['dataset']['rows'], 'totals_ms': totals, 'wins': wins,
               'all_queries_verified': True,
               'all_queries_faster_than_both': all(v == 43 for v in wins.values()),
               'inputs': [str(duck.resolve()), str(click.resolve())]}
    (output / 'summary.json').write_text(json.dumps(summary, indent=2) + '\n')
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    import numpy as np
    fig, axes = plt.subplots(2, 1, figsize=(16, 9), gridspec_kw={'height_ratios': [1, 2]})
    native = [totals['tinylamb_vs_duckdb'], totals['tinylamb_vs_clickhouse']]
    heights = [statistics.mean(native), totals['duckdb'], totals['clickhouse']]
    bars = axes[0].bar(['Tinylamb', 'DuckDB', 'ClickHouse'], [v / 1000 for v in heights],
                       color=['#367bd1', '#dcae32', '#cc6054'])
    axes[0].errorbar(0, heights[0] / 1000, yerr=(max(native) - min(native)) / 2000,
                    color='black', capsize=6)
    for bar, value in zip(bars, heights):
        axes[0].text(bar.get_x() + bar.get_width()/2, value / 1000, f'{value/1000:.3f} s',
                     ha='center', va='bottom')
    axes[0].set_ylabel('Sum of query medians (s)\nLower is better')
    axes[0].set_ylim(0, max(heights) / 1000 * 1.18)
    x = np.arange(1, 44)
    for shift, engine, rows, color in [(-.2, 'duckdb', dr, '#dcae32'), (.2, 'clickhouse', cr, '#cc6054')]:
        ratios = [r[engine]['median_ms'] / r['tinylamb']['median_ms'] for r in rows]
        axes[1].bar(x + shift, ratios, width=.4, label=engine, color=color)
    axes[1].axhline(1, color='black', linewidth=1)
    axes[1].set_yscale('log')
    axes[1].set_xticks(x)
    axes[1].set_xticklabels([f'Q{i}' for i in x], rotation=90)
    axes[1].set_ylabel('Reference / Tinylamb elapsed time\nAbove 1 = Tinylamb faster')
    axes[1].legend()
    fig.suptitle(f'ClickBench {dm["dataset"]["rows"]:,} rows | {dm["threads"]} threads | '
                 f'all 43 answers verified\nTinylamb wins: DuckDB {wins["duckdb"]}/43, ClickHouse {wins["clickhouse"]}/43')
    fig.text(.5, .01, 'Warm query repetitions; no result cache. Tinylamb total: mean and range of the two paired runs. '
             'Includes planning; first-use column construction excluded.', ha='center', fontsize=9)
    fig.tight_layout(rect=[0, .035, 1, .96])
    for suffix in ('png', 'svg'):
        fig.savefig(output / ('comparison.' + suffix), dpi=160)
    plt.close(fig)
    lines = ['# ClickBench comparison', '',
             f'{dm["dataset"]["rows"]:,} rows, {dm["threads"]} threads, CPUs {dm["cpus"]}. '
             f'Warmups {dm["warmups"]}; measured repetitions {dm["repeats"]}.', '',
             f'CPU: {dm.get("cpu_model", "unrecorded")}. '
             f'DuckDB: {dm.get("duckdb_version")}; ClickHouse: {cm.get("clickhouse_version")}.', '',
             f'Tinylamb binary SHA256: `{dm["binary_sha256"]}`.', '',
             'All 43 queries verified in each paired run. Results are warm repetitions; '
             'first-use column construction is outside the medians. No query result cache.', '',
             '| Engine/run | Sum of medians (ms) |', '|---|---:|']
    lines += [f'| {k} | {v:.3f} |' for k, v in totals.items()]
    lines += ['', f'Tinylamb wins: DuckDB {wins["duckdb"]}/43; ClickHouse {wins["clickhouse"]}/43.', '',
              f'All-query performance target achieved: {summary["all_queries_faster_than_both"]}.', '',
              '![Measured comparison](comparison.png)', '',
              '| Query | Tinylamb vs DuckDB (ms) | DuckDB (ms) | Tinylamb vs ClickHouse (ms) | ClickHouse (ms) |',
              '|---|---:|---:|---:|---:|']
    lines += [f'| Q{d["query"]} | {d["tinylamb"]["median_ms"]:.3f} | {d["duckdb"]["median_ms"]:.3f} | '
              f'{c["tinylamb"]["median_ms"]:.3f} | {c["clickhouse"]["median_ms"]:.3f} |' for d, c in zip(dr, cr)]
    lines += ['', 'Raw runs: ' + ', '.join(summary['inputs']), '',
              'ClickHouse uses server statistics.elapsed; Tinylamb uses Execute+Collect; '
              'DuckDB uses execute+fetchall. All include planning, but transport boundaries differ.', '',
              'Memory limits are not equal: Tinylamb query working memory defaults to 8 GiB '
              '(column caches and storage are separate); this ClickHouse server uses 32 GiB; '
              'DuckDB uses its automatic default. This is a same-CPU comparison, not an equal-RSS experiment.', '']
    (output / 'REPORT.md').write_text('\n'.join(lines))
    return summary


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--duckdb-run', type=Path, required=True)
    parser.add_argument('--clickhouse-run', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(report(args.duckdb_run, args.clickhouse_run, args.output), indent=2))
