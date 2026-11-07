#!/usr/bin/env python3
"""Match all originally flagged gaps to later, independently saved measurements."""
import argparse
import json
import math
from pathlib import Path

from gaps import collect, loss_factor


def key(row):
    return (row['profile'], row['domain'], row.get('suite'), row.get('rows'),
            row.get('clients'), row['workload'], row['metric'],
            row.get('reference_engine', 'postgresql'))


def compare(baseline, following, max_loss=10, inclusive=False):
    if not math.isfinite(max_loss) or max_loss <= 1:
        raise ValueError('max_loss must be finite and > 1')
    def passes(value):
        return value <= max_loss if inclusive else value < max_loss
    actual = {key(row): row for row in following}
    rows = []
    for before in baseline:
        after = actual.get(key(before))
        entry = dict(before=before, after=after, passed=False)
        if after is not None:
            reference = before.get('reference_engine', 'postgresql')
            original_ratio = loss_factor(after['tinylamb'], before[reference],
                                         after['metric'] in ('tps', 'reader_tps', 'transaction_tps'))
            entry['loss_against_original_reference'] = original_ratio
            if reference == 'postgresql':
                entry['loss_against_original_pg'] = original_ratio
            entry['passed'] = (passes(original_ratio) and after['loss_factor'] is not None
                               and passes(after['loss_factor']))
        rows.append(entry)
    return rows


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--baseline', type=Path, default=Path(__file__).parent / 'results/2026-10-29-gap-plan/gaps.json')
    p.add_argument('--run', action='append', required=True, help='profile=directory; later runs replace matching conditions')
    p.add_argument('--output', type=Path, required=True)
    p.add_argument('--reference-engine', choices=('postgresql', 'mysql'), default='postgresql')
    p.add_argument('--max-loss', type=float, default=10,
                   help='maximum disadvantage ratio, default strictly below 10')
    p.add_argument('--inclusive', action='store_true',
                   help='also pass equality, e.g. --max-loss 2 --inclusive')
    a = p.parse_args()
    if not math.isfinite(a.max_loss) or a.max_loss <= 1:
        p.error('max-loss must be finite and > 1')
    subsequent = {}
    for spec in a.run:
        profile, directory = spec.split('=', 1)
        source = Path(directory)
        records = [json.loads(s) for s in (source / 'results.jsonl').read_text().splitlines()]
        for row in collect(records, profile, reference_engine=a.reference_engine):
            row['source'] = str(source)
            subsequent[key(row)] = row
    baseline = json.loads(a.baseline.read_text())['qualified']
    if any(r.get('reference_engine', 'postgresql') != a.reference_engine for r in baseline):
        p.error('baseline reference engine does not match --reference-engine')
    rows = compare(baseline, subsequent.values(), a.max_loss, a.inclusive)
    a.output.mkdir(parents=True, exist_ok=True)
    (a.output / 'comparison.json').write_text(json.dumps(rows, ensure_ascii=False, indent=2, allow_nan=False))
    label = 'PG' if a.reference_engine == 'postgresql' else 'MySQL'
    lines = [f'| 追加索引 | 負荷 | 規模/接続 | 指標 | 変更前tiny | 旧{label} | 変更後tiny | 同時期{label} | 新{label}比 | 旧{label}比 | 判定 |',
             '| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |']
    for row in rows:
        b, n = row['before'], row['after']
        prefix = [b['profile'], b['workload'], str(b.get('rows', b.get('clients'))), b['metric'], f"{b['tinylamb']:.6f}", f"{b[a.reference_engine]:.6f}"]
        if n:
            prefix += [f"{n['tinylamb']:.6f}", f"{n[a.reference_engine]:.6f}",
                       f"{n['loss_factor']:.2f}×" if n['loss_factor'] is not None else '∞',
                       f"{row['loss_against_original_reference']:.2f}×", '通過' if row['passed'] else '未達']
        else:
            prefix += ['—', '—', '—', '—', '未測定']
        lines.append('| ' + ' | '.join(prefix) + ' |')
    (a.output / 'comparison.md').write_text('\n'.join(lines) + '\n')
    for domain in ('primary', 'tail'):
        selection = [r for r in rows if (r['before']['domain'] == 'tail') == (domain == 'tail')]
        print(domain, 'passed', sum(r['passed'] for r in selection), '/', len(selection),
              'measured', sum(r['after'] is not None for r in selection))


if __name__ == '__main__':
    main()
