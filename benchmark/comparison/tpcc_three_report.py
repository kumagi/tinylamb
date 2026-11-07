#!/usr/bin/env python3
"""Audit and report the shared three-engine TPC-C-derived run."""
import argparse
import csv
import hashlib
import json
import math
from pathlib import Path
import statistics

from analyze import markdown

ENGINES = ('tinylamb', 'postgresql', 'mysql')
LABELS = ('Tinylamb', 'PostgreSQL', 'MySQL')
TYPES = ('new_order', 'payment', 'order_status', 'delivery', 'stock_level')


def load(directory):
    finished = json.loads((directory / 'finished.json').read_text())
    env = json.loads((directory / 'environment.json').read_text())
    args = env['tpcc_args']
    records = [json.loads(l) for l in (directory / 'results.jsonl').read_text().splitlines()]
    fixtures = [json.loads(l) for l in (directory / 'fixture-validation.jsonl').read_text().splitlines()]
    expected = {(e, t) for e in ENGINES for t in range(1, args['trials'] + 1)}
    for collection in (records, fixtures):
        keys = [(r['engine'], r['trial']) for r in collection]
        if set(keys) != expected or len(keys) != len(expected) or not all(r['valid'] for r in collection):
            raise ValueError('incomplete, duplicate or invalid trial/fixture')
    for r in records:
        r['_directory'] = str(directory)
    selection = directory/'replacement-selection.json'
    if selection.exists():
        manifest = json.loads(selection.read_text())
        if hashlib.sha256((directory/'results.jsonl').read_bytes()).hexdigest() != manifest['original_results_sha256']:
            raise ValueError('original results changed after replacement selection')
        replaced = set()
        for entry in manifest['replacements']:
            key = (entry['engine'],entry['trial'])
            old = next(r for r in records if (r['engine'],r['trial']) == key)
            if old['timing_qualified'] or key in replaced:
                raise ValueError('replacement allowed only for a recorded competing-work overlap')
            extra = (directory/entry['directory']).resolve()
            if not (extra/'finished.json').exists():
                raise ValueError('incomplete replacement run')
            for name,digest in entry['sha256'].items():
                if hashlib.sha256((extra/name).read_bytes()).hexdigest() != digest:
                    raise ValueError('replacement artifact changed')
            extra_env=json.loads((extra/'environment.json').read_text())
            for field in ['server_sha256','client_sha256','fixture_sha256','mysql_c_library_sha256']:
                if extra_env[field] != env[field]:
                    raise ValueError('replacement binary/fixture mismatch')
            for field in ['server_cpus','read_workers','tiny_query_memory_mib','data_cache_mib','postgres_image','mysql_image']:
                if extra_env['args'][field] != env['args'][field]:
                    raise ValueError('replacement server profile mismatch')
            for field in ['seconds','warmup','clients','trials']:
                if extra_env['tpcc_args'][field] != args[field]:
                    raise ValueError('replacement client profile mismatch')
            for name,digest in extra_env['runtime_source_sha256'].items():
                if hashlib.sha256((extra/'runtime-source'/name).read_bytes()).hexdigest() != digest:
                    raise ValueError('replacement runtime source changed')
            extras=[json.loads(l) for l in (extra/'results.jsonl').read_text().splitlines()]
            matches=[r for r in extras if (r['engine'],r['trial']) == key]
            if len(matches) != 1 or not matches[0]['valid']:
                raise ValueError('replacement result missing or invalid')
            replacement_fixture=[json.loads(l) for l in (extra/'fixture-validation.jsonl').read_text().splitlines()]
            fresh=next(r for r in replacement_fixture if (r['engine'],r['trial']) == key)
            original=next(r for r in fixtures if (r['engine'],r['trial']) == key)
            if not fresh['valid'] or fresh['fingerprints'] != original['fingerprints']:
                raise ValueError('replacement initial data mismatch')
            replacement=matches[0]
            replacement['_directory']=str(extra)
            records[records.index(old)]=replacement
            replaced.add(key)
    if finished['engines'] != 3 or finished['trials'] != args['trials']:
        raise ValueError('completion metadata mismatch')
    for r in records:
        if not r['timing_qualified'] or not r['invariants']['valid'] or r['exit_code']:
            raise ValueError('unqualified measurement')
        activity = json.loads((Path(r['_directory'])/f"activity-{r['engine']}-{r['trial']}.json").read_text())
        if not activity or any(s['competing'] for s in activity):
            raise ValueError('build/test/profiler overlaps measurement')
        for t in TYPES:
            m = r['metrics']
            counts = [int(m[t + '.' + k]) for k in ('attempted', 'committed', 'user_rollback', 'engine_aborts')]
            if counts[0] != sum(counts[1:]) or not counts[0] or min(counts) < 0:
                raise ValueError('transaction accounting mismatch')
            if not math.isclose(float(m[t+'.tps']), counts[1]/r['seconds'], rel_tol=1e-5, abs_tol=.01):
                raise ValueError('TPS mismatch')
            latency = [float(m[t + '.' + p + '_ms']) for p in ('p50','p95','p99')]
            if latency != sorted(latency) or any(not math.isfinite(v) or v < 0 for v in latency):
                raise ValueError('invalid percentiles')
        if not math.isclose(float(m['tps']), sum(int(m[t+'.committed']) for t in TYPES)/r['seconds'], rel_tol=1e-5, abs_tol=.01):
            raise ValueError('total TPS mismatch')
    for name, digest in env['runtime_source_sha256'].items():
        if hashlib.sha256((directory/'runtime-source'/name).read_bytes()).hexdigest() != digest:
            raise ValueError('runtime source changed')
    for engine in ENGINES:
        if 'transport_test=passed' not in (directory/f'transport-{engine}.log').read_text():
            raise ValueError('transport test missing')
    return env, records


def generate(directory, output):
    env, records = load(directory)
    def trials(e):
        return [r for r in records if r['engine'] == e]
    def values(e, key):
        return [float(r['metrics'][key]) for r in trials(e)]
    def median(e, key):
        return statistics.median(values(e, key))
    totals, error_rows, trial_rows = [], [], []
    for e, label in zip(ENGINES, LABELS):
        attempts = sum(int(r['metrics'][t+'.attempted']) for r in trials(e) for t in TYPES)
        aborted = sum(int(r['metrics'][t+'.engine_aborts']) for r in trials(e) for t in TYPES)
        user = sum(int(r['metrics'][t+'.user_rollback']) for r in trials(e) for t in TYPES)
        totals.append([label, f"{median(e,'tps'):,.2f} [{min(values(e,'tps')):,.2f}–{max(values(e,'tps')):,.2f}]",
                       f"{median(e,'new_order.tps'):,.2f}", f'{aborted/attempts*100:.2f}%', f'{user/attempts*100:.2f}%'])
        for t in TYPES:
            errors = sum(int(r['metrics'][t+'.engine_aborts']) for r in trials(e))
            examples = list(dict.fromkeys(r['metrics'].get(t+'.first_error','') for r in trials(e)))
            error_rows.append([label, t, errors, next((s for s in examples if s), 'なし').replace('|','\\|')])
        for r in trials(e):
            m=r['metrics']
            trial_rows.append([label, r['trial'], m['tps'], sum(int(m[t+'.committed']) for t in TYPES),
                               sum(int(m[t+'.engine_aborts']) for t in TYPES), sum(int(m[t+'.user_rollback']) for t in TYPES)])
    primary = markdown(['製品','合計成功TPS 中央値 [最小–最大]','New-Order成功TPS','engine abort率','意図したrollback率'],totals)
    latency_rows, tail_rows = [], []
    for t in TYPES:
        latency_rows.append([t, *[f"{median(e,t+'.tps'):,.2f} / {median(e,t+'.p50_ms'):.3f}" for e in ENGINES]])
        tail_rows.append([t, *[f"{median(e,t+'.p95_ms'):.3f} / {median(e,t+'.p99_ms'):.3f}" for e in ENGINES]])
    latency = markdown(['処理', *[l+' 成功TPS / p50 ms' for l in LABELS]],latency_rows)
    tail = markdown(['処理', *[l+' p95 / p99 ms' for l in LABELS]],tail_rows)
    table = markdown(['製品','試行','合計成功TPS','成功件数','engine abort','意図したrollback'],trial_rows)
    errors = markdown(['製品','処理','3試行のengine abort合計','代表的な最初の失敗'],error_rows)
    analysis = directory/'analysis'
    analysis.mkdir(exist_ok=True)
    (analysis/'errors.md').write_text(errors)
    with (analysis/'metrics.csv').open('w',newline='') as stream:
        writer=csv.writer(stream)
        writer.writerow(['engine','trial','type','attempted','committed','engine_aborts','user_rollback','tps','p50_ms','p95_ms','p99_ms'])
        for r in records:
            for t in TYPES:
                writer.writerow([r['engine'],r['trial'],t,*[r['metrics'][t+'.'+k] for k in ['attempted','committed','engine_aborts','user_rollback','tps','p50_ms','p95_ms','p99_ms']]])
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    colors=['#d67824','#326690','#38866a']
    fig,axes=plt.subplots(1,2,figsize=(10,4))
    for ax,key,title in zip(axes,['tps','new_order.tps'],['All five transaction types','New-Order']):
        for i,(e,label,color) in enumerate(zip(ENGINES,LABELS,colors)):
            vals=values(e,key); middle=statistics.median(vals)
            ax.bar(i,middle,color=color)
            ax.errorbar(i,middle,yerr=[[middle-min(vals)],[max(vals)-middle]],fmt='none',color='black',capsize=5)
        ax.set_xticks(range(3),LABELS);ax.set_title(title);ax.set_ylabel('Successful transactions/s');ax.grid(axis='y',alpha=.2);ax.set_axisbelow(True)
    fig.suptitle('TPC-C-derived W=1 / 10 clients: median and min/max of three trials')
    fig.tight_layout();fig.savefig(analysis/'throughput.png',dpi=160);fig.savefig(analysis/'throughput.svg');plt.close(fig)
    fig,axes=plt.subplots(1,3,figsize=(13,4))
    for ax,p in zip(axes,['p50','p95','p99']):
        for i,(e,label,color) in enumerate(zip(ENGINES,LABELS,colors)):
            ax.bar([j+(i-1)*.25 for j in range(5)],[median(e,t+'.'+p+'_ms') for t in TYPES],width=.24,color=color,label=label)
        ax.set_xticks(range(5),['NewOrder','Payment','OrderStatus','Delivery','StockLevel'],rotation=35,ha='right')
        ax.set_yscale('log');ax.set_ylabel('Successful latency (ms; log)');ax.set_title(p);ax.grid(axis='y',alpha=.2);ax.set_axisbelow(True)
    handles, labels = axes[0].get_legend_handles_labels()
    fig.legend(handles, labels, loc='upper center', bbox_to_anchor=(.5,.93), ncol=3)
    fig.suptitle('Median of per-trial successful transaction percentiles; aborts reported separately')
    fig.tight_layout(rect=(0,0,1,.85));fig.savefig(analysis/'latency.png',dpi=160);fig.savefig(analysis/'latency.svg');plt.close(fig)
    args=env['tpcc_args'];rel='../benchmark/comparison/results/'+directory.name
    pg=dict(env['settings']['postgresql']);mysql=dict(env['settings']['mysql'])
    mysql_tiny = median('mysql','tps')/median('tinylamb','tps')
    mysql_pg = median('mysql','tps')/median('postgresql','tps')
    tiny_pg = median('tinylamb','tps')/median('postgresql','tps')
    doc=f'''# TPC-C由来W=1の3製品比較（2026-11-06）

DuckDB・Oracleを外し、Tinylamb C8・PostgreSQL {pg['server_version']}・MySQL {mysql['version']}を再測定した。
同じC++トランザクション生成器、同じW=1 snapshot、同じSQL生成規則を使用した全9試行で、事後不変条件を通過した。
各試行前に全9表の全行・型・値のfingerprint一致も検証した。

## 条件と比較範囲

- TPC-Cは倉庫数W=1。1倉庫・10district・30,000顧客・100,000商品／stockが基礎規模。
  SF=1は直前のTPC-Hの指定で、今回Oracleを外した理由を「W=1が容量制限に入らない」と確認したわけではない。
  ユーザー指定の3製品枠としてDuckDB／Oracleを除外した。
- {args['clients']}接続、warmup {args['warmup']}秒＋計測{args['seconds']}秒、{args['trials']}試行。
  seed=20260819、New-Order／Payment／Order-Status／Delivery／Stock-Levelの要求比率45／43／4／4／4。
  これは入力要求の比率であり、中断により成功した処理の構成比は変わる。
  think time・keying time・自動再試行・stored procedureを加えていない。TPC認証スコア／tpmCではない。
- 全サーバーCPU {args['server_cpus']}、データバッファ512 MiB。Tinyクエリ予算256 MiB、PG work_mem64 MiB。
  全プロセスの総メモリを同一にした比較ではない。同期commit、PG fsync有効、MySQL redo fsync=1・binlog無効。
- 同じ実行ファイルでTiny／PGはSimple Query、MySQLは公式C API 8.4.11のCOM_QUERY。
  literal SQLとtext結果、prepared statementなし。型変換・往復・取得を含む端末側時間である。
  MySQLはREAD COMMITTED、文字列列はutf8mb4_bin。複合索引キーとunique指定を合わせた。
- 各試行は3製品とも元snapshotへ復元する。PGはCOPY後VACUUM ANALYZE、MySQLは投入後ANALYZE。
  投入方法が違うためロード時間を製品性能として比較しない。実行順は試行ごとにseed付きshuffle。
- 使用snapshotは以前の共通W=1 fixtureを固定したもので、初期化後の事前トランザクションを少量含む。
  正確な初期行数・残高・SHA-256は[初期状態]({rel}/initial-invariants.json)と環境記録に残す。
- **現在の生成器にはSQLをまとめて発行する箇所がある。旧clientとの同一性を今回検証していないため、過去のTPS差をDB改善とは扱わない。**
  今回の3製品間では同じソース・実行ファイル・生成器を使用する。
- 実行開始前に他のbuild／test／profiler／比較ベンチマークを待ち、各計測中は1秒ごとにprocessを監視した。
  採用した全9試行の監視では重なりを検出しなかった。
  初回シリーズとその部分再測定は参考として保存し、最終表には監視を強化した新しいシリーズを使った。
  このシリーズでも別TPC実行を検出したTiny trial3／PG trial3は、同一条件の再測定へ置換した。
  採用元と旧セットの扱いは[採用記録]({rel}/replacement-selection.json)に残す。
  遅さを理由に試行を除外していない。通常のホストアプリや他の待機中DBは停止しておらず、CPU／I/O完全占有の試験ではない。
- クライアントは計測区間内に開始した処理を数えるため、終了時刻をまたぐ最後の処理も含み得る。
  各処理は始終を計時し、成功したトランザクションのみ遅延分位点へ含める。

## スループットと中断

{primary}
今回の中央値ではMySQLの合計成功TPSがTinyの{mysql_tiny:.2f}倍、PGの{mysql_pg:.2f}倍だった。
TinyはPGの{tiny_pg:.2f}倍だが、中断率も大きい。成功TPSと中断率を併せて評価する必要がある。
高い中断率では成功処理の構成も変わるため、合計TPSだけで固定された業務mixの処理能力を断定しない。
試行間のばらつきも上表に残した。速さ・遅さを理由に試行を除外しておらず、
この測定だけから単一の安定した倍率を保証するものではない。

合計成功TPSは5種別の合算。engine abort率と意図したNew-Order rollback率は要求件数を分母に、
3試行の件数を合算して計算した。成功TPSの中央値と失敗率は同一の集計統計ではない。
件数と分位点は計測区間のもので、warmup中の中断数はこのクライアントでは別集計していない。
Deliveryの成功1件は最大10districtへの配送を含む1トランザクションで、配送注文数ではない。
出力名`engine_aborts`にはSQL失敗に加え、生成器の更新行数検査により中断したものも含む。
PG／MySQLの中断はDeliveryに集中し、保存した代表例はキューDELETEの行数不足だった。
共通生成器はキューのSELECTとDELETEを分け、FOR UPDATE／SKIP LOCKEDを加えていない。
このドライバの競合挙動も含めた比較である。

![throughput]({rel}/analysis/throughput.png)

## 処理別の成功TPSと遅延

各値は3試行の中央値。遅延は各試行の成功処理だけの分位点を求め、その中央値を取った。
中断した処理の遅延を成功処理へ混ぜていない。

{latency}
{tail}
![latency]({rel}/analysis/latency.png)

## 全試行と検証

{table}
初期化前後の9表行数、district next-order-id合計とorders行数の関係、ordersの明細数合計と
order_line行数、warehouse／district／history金額合計の一致を確認した。
これは完全なTPC-C serial oracleや全12整合性条件の監査ではない。
engine abortの代表原因と全件数は[失敗一覧]({rel}/analysis/errors.md)に保存した。

MySQL接続層は[公式C API](https://dev.mysql.com/doc/c-api/8.4/en/mysql-affected-rows.html)を使い、
更新件数・NULL・64bit整数・小数・文字列・commit／rollback・unique違反後の復帰を全3製品で実接続検査した。
CSV投入では引用された空文字と裸のNULLを区別し、名前の照合順も検証する。
比較器の60回帰テスト、include-layer lint、C++変更箇所のformat確認も通過した。

## 再現と保存

- [全生データと監査]({rel}/)、[全指標CSV]({rel}/analysis/metrics.csv)、[全環境設定]({rel}/environment.json)。
- client／Tiny C8／fixture／MySQL C libraryのSHA-256、Docker digest、実行コマンド、全試行ログ、
  試行前の9表fingerprint、process監視ログ、実行時ソースを保存した。
- [事前検証](../benchmark/comparison/results/2026-11-06-tpcc-three-preflight2/)は2秒1試行であり本表へ混ぜていない。
- 現在の作業ツリーのストレージ層に今回と別のビルドエラーがあったため、clientはDB本体から分離してビルドした。
  サーバーには前回検証済みC8を固定しており、作業中のLStable変更を測った結果ではない。
  C8以降の現在ソース全体をビルド・全回帰できたという意味ではない。
- [実行手順](../benchmark/comparison/README.md)。終了時に自分のサーバー・コンテナ・一時DBを削除する。
'''
    output.write_text(doc)
    audit=dict(complete=True,valid_trials=len(records),fixtures=len(records),engines=3,
               selected_sources=[dict(engine=r['engine'],trial=r['trial'],directory=r['_directory']) for r in records],
               all_timings_qualified=True,report_generator_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest())
    (directory/'audit.json').write_text(json.dumps(audit,indent=2))
    print(json.dumps(audit,indent=2))


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('directory',type=Path)
    p.add_argument('--output',type=Path,required=True)
    args=p.parse_args()
    generate(args.directory,args.output)


if __name__=='__main__':
    main()
