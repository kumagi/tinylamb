#!/usr/bin/env python3
"""Find median performance gaps against a reference engine in completed runs."""
import argparse
from collections import Counter, defaultdict
import json
import math
from pathlib import Path
import statistics


def loss_factor(tiny, postgres, higher_is_better=False):
    """Always >=10 for a tenfold disadvantage, independent of metric direction."""
    if tiny < 0 or postgres < 0 or not all(math.isfinite(v) for v in (tiny, postgres)):
        raise ValueError('performance values must be finite and nonnegative')
    numerator, denominator = (postgres, tiny) if higher_is_better else (tiny, postgres)
    return numerator / denominator if denominator else (math.inf if numerator else 1.0)


def collect(records, profile, threshold=10, reference_engine="postgresql"):
    if reference_engine not in ("postgresql", "mysql"):
        raise ValueError("reference engine must be postgresql or mysql")
    olap = defaultdict(dict)
    oltp = defaultdict(lambda: defaultdict(list))
    for r in records:
        if r['engine'] not in ('tinylamb', reference_engine):
            continue
        if r['kind'] == 'olap':
            if not r['correct'] or r['status'] != 'ok':
                raise ValueError('incorrect OLAP results must not be ranked')
            key=(r['suite'],r['rows'],r['workload'])
            if r['engine'] in olap[key]:
                raise ValueError('duplicate OLAP condition')
            olap[key][r['engine']]=r
        elif r['kind'] == 'oltp':
            if not r['valid']:
                raise ValueError('invalid OLTP trials must not be ranked')
            oltp[(r['workload'],r['clients'])][r['engine']].append(r)
    comparisons=[]
    def emit(domain,workload,metric,tiny_values,pg_values,**extra):
        if not tiny_values or not pg_values:
            raise ValueError('missing matched measurements')
        tiny,pg=statistics.median(tiny_values),statistics.median(pg_values)
        higher=metric in ('tps','reader_tps','transaction_tps')
        ratio=loss_factor(tiny,pg,higher)
        record=dict(profile=profile,domain=domain,workload=workload,metric=metric,
                    unit='query/s' if metric=='reader_tps' or (metric=='tps' and workload in ('noop','point_read','nonkey_read','range_read')) else 'txn/s' if higher else 'ms',
                    tinylamb=tiny,reference_engine=reference_engine,loss_factor=ratio if math.isfinite(ratio) else None,
                    unbounded=not math.isfinite(ratio),qualified=ratio>=threshold,
                    samples_tinylamb=tiny_values,
                    tinylamb_min=min(tiny_values),tinylamb_max=max(tiny_values),
                    **{reference_engine:pg, "samples_"+reference_engine:pg_values,
                       reference_engine+"_min":min(pg_values),reference_engine+"_max":max(pg_values)},
                    **extra)
        comparisons.append(record)
    for (suite,rows,name),engines in olap.items():
        if set(engines)!= {'tinylamb',reference_engine}:
            raise ValueError('missing OLAP counterpart')
        t,p=(engines[e] for e in ('tinylamb',reference_engine))
        if not math.isclose(statistics.median(t['samples_ms']),t['median_ms'],rel_tol=1e-12):
            raise ValueError('stored tinylamb median differs from samples')
        if not math.isclose(statistics.median(p['samples_ms']),p['median_ms'],rel_tol=1e-12):
            raise ValueError(f'stored {reference_engine} median differs from samples')
        emit('olap',name,'median_ms',t['samples_ms'],p['samples_ms'],suite=suite,rows=rows,
             detail_tinylamb=t['detail'],**{'detail_'+reference_engine:p['detail']})
    for (name,clients),engines in oltp.items():
        if set(engines)!= {'tinylamb',reference_engine}:
            raise ValueError('missing OLTP counterpart')
        t,p=(engines[e] for e in ('tinylamb',reference_engine))
        trial_ids=[{r['trial'] for r in trials} for trials in (t,p)]
        if trial_ids[0]!=trial_ids[1] or any(len(ids)!=len(trials) for ids,trials in zip(trial_ids,(t,p))):
            raise ValueError('OLTP trial IDs must match and must not repeat')
        for metric in ('tps','p50_ms','p95_ms','p99_ms'):
            emit('tail' if metric in ('p95_ms','p99_ms') else 'oltp',name,metric,
                 [r[metric] for r in t if r[metric] is not None],
                 [r[metric] for r in p if r[metric] is not None],clients=clients,
                 measured_errors_tinylamb=sum(r['errors'] for r in t),
                 **{"measured_errors_"+reference_engine:sum(r['errors'] for r in p)})
        if name=='htap':
            for role in ('reader','transaction'):
                if clients == 1 and role == 'transaction':
                    continue
                emit('htap',name,role+'_tps',[r['role_tps'][role] for r in t],
                     [r['role_tps'][role] for r in p],clients=clients)
                # Mixed-request latency can hide a slow reader behind many
                # short writer requests. Older saved runs may lack this field.
                if clients > 1 and any('role_latency' in r for r in t+p):
                    for metric in ('p50_ms','p95_ms','p99_ms'):
                        values=[]
                        for trials in (t,p):
                            values.append([r['role_latency'][role][metric]
                                           for r in trials
                                           if r.get('role_latency',{}).get(role,{}).get(metric) is not None])
                        emit('tail' if metric != 'p50_ms' else 'htap',name,
                             role+'_'+metric,*values,clients=clients)
    return comparisons


def markdown(rows, reference_engine="postgresql"):
    reference_label={"postgresql":"PostgreSQL", "mysql":"MySQL"}[reference_engine]
    header=['条件','負荷','規模／接続','指標','tinylamb',reference_label,'不利倍率']
    lines=['| '+' | '.join(header)+' |','| '+' | '.join(['---']*len(header))+' |']
    for r in rows:
        scope=str(r.get('rows',str(r.get('clients'))+'接続'))
        if r.get('suite')=='tpch':scope='SF '+scope
        ratio='∞' if r['unbounded'] else f"{r['loss_factor']:.2f}×"
        lines.append('| '+' | '.join([r['profile'],r['workload'],scope,r['metric'],
                                     f"{r['tinylamb']:.6f}",f"{r[reference_engine]:.6f}",ratio])+' |')
    return '\n'.join(lines)+'\n'


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('directory',type=Path)
    parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--reference-engine', choices=('postgresql','mysql'), default='postgresql')
    parser.add_argument('--threshold',type=float,default=10)
    parser.add_argument('--overwrite',action='store_true',help='replace only derived analysis files')
    args=parser.parse_args()
    if not math.isfinite(args.threshold) or args.threshold<=1:
        parser.error('threshold must be finite and >1')
    comparisons=[]
    for profile in ('unindexed','indexed'):
        path=args.directory/profile/'results.jsonl'
        comparisons.extend(collect([json.loads(line) for line in path.read_text().splitlines()],profile,args.threshold,args.reference_engine))
    selected=[r for r in comparisons if r['qualified']]
    result={'source':str(args.directory),'reference_engine':args.reference_engine,'threshold':args.threshold,
            'comparison_count':len(comparisons),'qualified_count':len(selected),
            'counts_by_domain':dict(Counter(r['domain'] for r in selected)),
            'qualified':selected,'all_comparisons':comparisons}
    args.output.mkdir(parents=True,exist_ok=args.overwrite)
    (args.output/'gaps.json').write_text(json.dumps(result,ensure_ascii=False,indent=2,allow_nan=False))
    for domain in ('olap','oltp','htap','tail'):
        (args.output/f'{domain}.md').write_text(markdown([r for r in selected if r['domain']==domain],args.reference_engine))
    print(json.dumps({k:result[k] for k in ('comparison_count','qualified_count','counts_by_domain')}))


if __name__=='__main__':
    main()
