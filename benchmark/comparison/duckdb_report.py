#!/usr/bin/env python3
"""Audit the complete DuckDB comparison matrix and generate its repository report."""

import argparse
from collections import Counter
import hashlib
import json
from pathlib import Path
import statistics

from analyze import markdown, selected_engines
from workloads import OLTP, olap_queries, tpch_queries


def audit(directory):
    env = json.loads((directory / 'environment.json').read_text())
    recovery = None
    if not env.get('finished'):
        sidecar = directory / 'completion-recovery.json'
        if not sidecar.exists():
            raise ValueError('incomplete run: no finished timestamp')
        recovery = json.loads(sidecar.read_text())
        if recovery.get('stage') != 'post-measurement-capacity-metadata':
            raise ValueError('unexpected recovery stage')
        for name, digest in recovery['preserved_files_sha256'].items():
            if hashlib.sha256((directory / name).read_bytes()).hexdigest() != digest:
                raise ValueError('recovered source artifact changed: ' + name)
        if set(recovery['preserved_files_sha256']) != {'environment.json', 'results.jsonl', 'run-finalization.log'}:
            raise ValueError('incomplete recovery evidence')
    args = env['args']
    records = [json.loads(line) for line in (directory / 'results.jsonl').read_text().splitlines()]
    expected_engines = ['tinylamb', 'postgresql', 'mysql']
    if args['include_oracle']:
        expected_engines.append('oracle')
    if not args['include_duckdb']:
        raise ValueError('DuckDB was not selected')
    expected_engines.append('duckdb')
    engines, labels, _ = selected_engines([{'engine': e} for e in expected_engines])
    if {r.get('engine') for r in records if 'engine' in r} != set(engines):
        raise ValueError('selected backend is missing or unexpected backend is present')
    for name, digest in env['harness_sha256'].items():
        source = directory / 'runtime-harness' / name
        if not source.exists() or hashlib.sha256(source.read_bytes()).hexdigest() != digest:
            raise ValueError('runtime harness source is missing or changed: ' + name)
    names = list(olap_queries(10000))
    if args['olap_workloads']:
        names = [n for n in names if n in args['olap_workloads']]
    if args['htap_reader_baseline']:
        names.append('htap_reader')
    expected_olap = set()
    if not args['skip_olap']:
        expected_olap.update(('micro', scale, q, e) for scale in args['sizes'] for q in names for e in engines)
    if args['tpch_scale']:
        expected_olap.update(('tpch', args['tpch_scale'], q, e) for q in tpch_queries(args['tpch_scale']) for e in engines)
    expected_oltp = set()
    if not args['skip_oltp']:
        expected_oltp.update((q, c, t, e) for q in args['oltp_workloads'] for c in args['clients']
                             if not args['oltp_cases'] or f'{q}:{c}' in args['oltp_cases']
                             for t in range(args['trials']) for e in args['oltp_engines'])
    olap = [r for r in records if r['kind'] == 'olap']
    oltp = [r for r in records if r['kind'] == 'oltp']
    keys_olap = [(r['suite'], r['rows'], r['workload'], r['engine']) for r in olap]
    keys_oltp = [(r['workload'], r['clients'], r['trial'], r['engine']) for r in oltp]
    for keys, expected in ((keys_olap, expected_olap), (keys_oltp, expected_oltp)):
        if set(keys) != expected or len(keys) != len(expected):
            raise ValueError('missing, extra or duplicate measurement conditions')
    if not all(r['correct'] and r['stable'] and r['status'] == 'ok'
               and len(r['samples_ms']) == args['repeats'] for r in olap):
        raise ValueError('incorrect, failed or incomplete OLAP measurement')
    if not all(r['valid'] and not r['validation_failures'] for r in oltp):
        raise ValueError('OLTP validation failed')
    loads = [r for r in records if r['kind'] == 'load']
    load_keys = [(r['rows'], r['engine']) for r in loads]
    expected_loads = {(scale, e) for scale in args['sizes'] for e in engines}
    if set(load_keys) != expected_loads or len(load_keys) != len(expected_loads) or not all(r['valid'] for r in loads):
        raise ValueError('missing, duplicate or invalid micro fixture')
    tpch_loads = [r for r in records if r['kind'] == 'tpch_load']
    if args['tpch_scale']:
        if Counter(r['engine'] for r in tpch_loads) != Counter(engines):
            raise ValueError('missing or duplicate TPC-H fixture')
        if any(r['counts'] != tpch_loads[0]['counts'] for r in tpch_loads):
            raise ValueError('inconsistent TPC-H fixture counts')
    if args['secondary_indexes']:
        expected_indexes = Counter({('micro', e): len(args['sizes']) for e in engines})
        if args['tpch_scale']:
            expected_indexes.update({('tpch', e): 1 for e in engines})
        if Counter((r['suite'], r['engine']) for r in records if r['kind'] == 'index_build') != expected_indexes:
            raise ValueError('incomplete secondary index setup')
    errors = {e: dict(measured=sum(r['errors'] for r in oltp if r['engine'] == e),
                      warmup=sum(r['warmup_errors'] for r in oltp if r['engine'] == e),
                      error_bearing_trials=sum(bool(r['errors'] or r['warmup_errors']) for r in oltp if r['engine'] == e))
              for e in engines}
    result = dict(complete=True, correct_olap=len(olap), valid_oltp=len(oltp), errors=errors,
                  records=dict(Counter(r['kind'] for r in records)),
                  finalization='post-measurement failure; recovered from complete immutable records' if recovery else 'normal',
                  report_generator_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest())
    return env, records, engines, labels, result


def report(directory):
    env, records, engines, labels, result = audit(directory)
    args = env['args']
    if (set(engines) != {'tinylamb', 'postgresql', 'mysql', 'oracle', 'duckdb'}
            or set(args['sizes']) != {10000, 100000, 1000000}
            or set(args['clients']) != {1, 4, 16}
            or set(args['oltp_workloads']) != set(OLTP)
            or set(args['oltp_engines']) != set(engines)
            or args['olap_workloads'] or args['oltp_cases']
            or args['skip_olap'] or args['skip_oltp']
            or not args['htap_reader_baseline'] or not args['secondary_indexes']
            or args['tpch_scale'] != .01):
        raise ValueError('this report template requires the full indexed five-engine profile')
    scale = max(args['sizes'])
    olap = [r for r in records if r['kind'] == 'olap']
    oltp = [r for r in records if r['kind'] == 'oltp']
    def value(suite, q, engine):
        return next(r['median_ms'] for r in olap if r['suite'] == suite and r['workload'] == q
                    and r['engine'] == engine and r['rows'] == (scale if suite == 'micro' else args['tpch_scale']))
    selected = ['count', 'sum', 'group_100', 'group_10000', 'distinct', 'top_100', 'join_group', 'exists', 'window']
    micro = markdown(['処理', *[label + ' ms' for label in labels]],
                     [[q, *[f'{value("micro", q, e):.3f}' for e in engines]] for q in selected])
    totals = {e: sum(r['median_ms'] for r in olap if r['suite'] == 'tpch' and r['engine'] == e) for e in engines}
    tpch = markdown(['製品', '22問の中央値合計 ms'], [[label, f'{totals[e]:.3f}'] for e, label in zip(engines, labels)])
    wins = {other: sum(value('micro', q, 'duckdb') < value('micro', q, other)
                       for q in [*olap_queries(scale), 'htap_reader']) for other in ('tinylamb', 'postgresql')}
    tpch_wins = {other: sum(value('tpch', q, 'duckdb') < value('tpch', q, other)
                            for q in tpch_queries(args['tpch_scale'])) for other in ('tinylamb', 'postgresql')}
    gaps = sorted([(value('micro', q, 'tinylamb') / value('micro', q, 'duckdb'), q)
                   for q in [*olap_queries(scale), 'htap_reader']], reverse=True)
    gap_table = markdown(['処理', 'Tiny / DuckDB 時間比'], [[q, f'{ratio:.2f}'] for ratio, q in gaps[:6]])
    tps_rows = []
    for name in ['noop', 'point_read', 'range_read', 'update', 'hot_update', 'transfer', 'rollback']:
        for clients in [1, 16]:
            values = []
            for e in engines:
                trials = [r for r in oltp if r['workload'] == name and r['clients'] == clients and r['engine'] == e]
                values.append(f"{statistics.median(r['tps'] for r in trials):,.1f}")
            tps_rows.append([name, clients, *values])
    tps = markdown(['処理', '接続', *[label + ' 成功TPS' for label in labels]], tps_rows)
    errors = markdown(['製品', '計測中SQL失敗', 'warmup失敗', '失敗を含む試行'],
                      [[label, *result['errors'][e].values()] for e, label in zip(engines, labels)])
    relative = '../benchmark/comparison/results/' + directory.name
    duck_settings = dict(env['settings']['duckdb']['settings'])
    mysql_cache = int(dict(env['settings']['mysql'])['innodb_buffer_pool_size']) / 2**20
    oracle_cache_start = dict(env['settings']['oracle']['sga'])['Buffer Cache Size'] / 2**20
    oracle_cache_end = (f"{dict(env['oracle_sga_end'])['Buffer Cache Size'] / 2**20:,.0f}"
                        if 'oracle_sga_end' in env else '未取得')
    pg_version = dict(env['settings']['postgresql'])['server_version']
    mysql_version = dict(env['settings']['mysql'])['version']
    oracle_version = env['settings']['oracle']['version'][0][0].replace('\n', ' / ') if 'oracle' in engines else '未選択'
    noop = {e: statistics.median(r['p50_ms'] for r in oltp if r['engine'] == e
                                 and r['workload'] == 'noop' and r['clients'] == 1) for e in engines}
    recovered = result['finalization'] != 'normal'
    finalization_note = f'''測定終了後、容量取得の`duckdb_database_size()`がDuckDB 2.0で存在せず、最終メタデータ保存だけが失敗した。
全{result['correct_olap']} OLAP・{result['valid_oltp']} OLTPの記録はその前に書き込み済みで、欠落・重複・結果・反復数・fixtureを独立監査した。
元のenvironment／results／終了ログを改変せず、そのSHA-256付き[回復記録]({relative}/completion-recovery.json)を添えた。
終了時のOracle SGA／segment容量、DuckDBファイル容量は未取得として扱い、別実行の値を流用していない。
実行時のソースはそのまま保存し、現行比較器は`PRAGMA database_size`へ修正。
修正後の5製品・100行の別スモーク実行で、最終保存と後始末まで確認した。性能表には混ぜていない。''' if recovered else '終了時メタデータの保存と通常の後始末まで完了した。'
    exit_note = ('今回の終了コード1の直接原因は、上述の終了時容量メタデータ取得である。'
                 if recovered else '比較器の終了コード1はwarmupを含むSQL失敗のある試行も表す。')
    return result, f'''# DuckDBを追加した5製品比較（2026-11-05）

DuckDB 2.0.0を既存比較器へ追加し、Tinylamb C8・PostgreSQL・MySQL・Oracle Freeと
同じ入力／SQLで再測定した。OLAP {result['correct_olap']}測定は全行・値・順序がPGと一致し、
OLTP／HTAP {result['valid_oltp']}試行はすべて残高・行数・読み取り検査を通過した。
測定条件の欠落・重複も監査した。前回のOracle結果を流用した表ではない。

{finalization_note}

## 測定条件

- PostgreSQL {pg_version}、MySQL {mysql_version}、{oracle_version}。
  Tinyは前回の改善済みC8 binaryを固定し、今回の追加でエンジン本体は変更していない。
- 全サーバーをCPU {args['server_cpus']}へ固定し、負荷は製品ごとに順次実行。
  DuckDBのサーバーthreads={duck_settings['threads']}、クライアント側は各1スレッド。
- sales={args['sizes']}、accounts最大10,000行。共通二次索引キーあり。
  DuckDBはART＋zone map・固有の圧縮を使うため、B-treeや物理配置を同一にした比較ではない。
- Tiny／PGのデータバッファ{args['data_cache_mib']:,} MiB、MySQL実測{mysql_cache:,.0f} MiB。
  Oracle実Buffer Cache開始{oracle_cache_start:,.0f} MiB／終了{oracle_cache_end}。
  DuckDB memory_limit指定{args['duckdb_memory_mib']:,} MiB（設定表示{duck_settings['memory_limit']}）。その適用範囲は他製品のデータバッファと異なり、
  総RSS上限でもない。OracleのSGA／PGAや他製品のクエリ予算も同一ではない。
- OLAPは最初の実行を別記し、その後{args['repeats']}反復の中央値。cold I/O性能ではない。
  SQL送信から全結果取得・Python型変換までを含む。
  整数・NULL・文字列・行順を厳密比較し、浮動小数点は相対1e-7／絶対0.01の許容誤差で照合する。
- OLTPは{args['clients']}接続、warmup {args['warmup']}秒＋計測{args['seconds']}秒、
  {args['trials']}試行。表は試行中央値、全表にはmin/max・p95/p99・失敗率も残す。
  初回比較の短窓であり、僅差・tail latencyや以前のPG比1.5倍目標の達成判定には使わない。
- TPC-H由来22問はSF={args['tpch_scale']}・同一DBGEN入力・binary64。
  TPCの監査を受けたスコアではない。TPC-Cの既存クライアントはPG Simple Query専用で、
  DuckDB／OracleのTPC-C、および今回のDuckDB SF=1は未測定。
- 投入時だけOracleはarray DML、他製品は500行単位のINSERTを使う。
  ロード時間を同じ投入手法のエンジン比較として順位付けしない。
- ビルド・テスト・プロファイラを本測定と重ねていない。ホスト上の通常アプリは停止していない。

## DuckDBの接続と更新競合

公式[Quack](https://duckdb.org/docs/2.0/quack/overview)を使い、localhost HTTP上で
`ATTACH`＋`CONNECT`によりセッション全体をサーバーへ送る。
永続ファイルを開くのは専用サーバープロセスだけで、クライアントのローカル最適化への
部分pushdownを使わない。公式Python clientとQuackの通信・型変換費用も含むため、
組み込みAPIを直接呼ぶDuckDBの速度はこの数値から断定できない。
PG/TinyはSimple Query、MySQLはCOM_QUERY、OracleはThin＋非キャッシュstatement。
OracleのみDockerのlocalhostポート転送が入り、他はホスト内の直接TCPである。

DuckDBは[snapshot isolationと楽観的並行制御](https://duckdb.org/docs/2.0/connect/concurrency)を使う。
PG／MySQL／OracleのREAD COMMITTEDと同一ではなく、同一行の同時更新では競合エラーが起きる。
自動再試行は追加していない。成功したトランザクションだけをTPSへ数え、失敗は別に残す。
暗黙transactionがabort済みのときだけ、後始末ROLLBACKの「transactionなし」を無害として扱う。
元の更新エラーは隠さず計上する。

DuckDBはin-memory DBを使わず、永続ファイルと標準WAL設定を使う。
DuckDBの[耐久化方式](https://duckdb.org/2024/10/30/analytics-optimized-concurrent-transactions)はcommit時WALとfsync。
PGはfsync／synchronous_commit有効、MySQLはflush_log_at_trx_commit=1・binlog無効、
Tinyは同期commit、OracleはWAIT／IMMEDIATE・FORCE LOGGING・NOARCHIVELOG。
今回の比較では電源断試験を追加していない。

## 読み取り結果

100万行での抜粋。時間は小さいほど速い。

{micro}
DuckDBは100万行の19種目中、Tinyより{wins['tinylamb']}種目、PGより{wins['postgresql']}種目で速かった。
TinyがDuckDBへ大きく負けた側の時間比は次のとおり。

{gap_table}
これらは共通SQLを送信して全結果を受け取るまでの差であり、個々の実行器の原因を
profileした結果ではない。走査・集約と短い点検索を分けて評価する必要がある。

TPC-H由来22問の各中央値の合計は次のとおり。合計そのものを連続実行して計測した時間ではない。

{tpch}
DuckDBが速い問はTiny比{tpch_wins['tinylamb']}/22、PG比{tpch_wins['postgresql']}/22。
全22問と全3規模のmicro表は下記の生成表に保存した。

![100万行OLAP]({relative}/analysis/micro_1m.png)

![TPC-H由来22問]({relative}/analysis/tpch.png)

## トランザクション結果

成功TPSの抜粋。失敗した要求を成功数へ含めないため、次の失敗件数と併せて読む。

{tps}
`SELECT 1`だけの1接続p50も、Tiny {noop['tinylamb']:.4f} ms、PG {noop['postgresql']:.4f} ms、
DuckDB {noop['duckdb']:.4f} msと差がある。短いSQLの比較には通信・クライアント・実行準備の
固定費も含まれる。今回の数値をエンジン内部の演算時間だけと解釈しない。

{errors}
失敗を含む試行でも、全件の事後不変条件は通過した。整合性の成功とSQL失敗ゼロは
別の項目である。`failures.json`はwarmup中を含むSQL失敗のある試行も収録するため、
SQL失敗を含む試行をそのまま「結果不一致」と解釈しない。{exit_note}
HTAPは1接続でreaderのみ、4／16接続では最後の1接続がreaderとなる。

![OLTP scaling]({relative}/analysis/oltp_scaling.png)

## 保存先と再現

- [生データ・実測設定・監査]({relative}/)、[監査結果]({relative}/audit.json)、
  [全測定CSV]({relative}/summary.csv)、[初回実行・全反復・SQL失敗]({relative}/results.jsonl)。
- [1万行]({relative}/analysis/micro_10000.md)、[10万行]({relative}/analysis/micro_100000.md)、
  [100万行]({relative}/analysis/micro_1000000.md)、[TPC-H全22問]({relative}/analysis/tpch.md)。
- [全OLTP TPS・min/max・失敗率]({relative}/analysis/oltp_tps.md)、
  [p95/p99]({relative}/analysis/oltp_latency.md)、[HTAP reader/writer別]({relative}/analysis/htap.md)。
- [実行時の比較器]({relative}/runtime-harness/)、[完全な実行引数]({relative}/environment.json)。
  DuckDB 2.0.0、Quack拡張・エンジンbinaryのSHA-256、Docker image digest、
  frozen Tiny C8 serverのSHA-256、追跡差分を保存。
  実行方法は[比較器README](../benchmark/comparison/README.md)。

事前検証は別ディレクトリ`2026-11-05-duckdb-preflight3`に分離し、本表へ混ぜていない。
初期の起動失敗2回も採用していない。Quackを親プロセスへ先にLOADするとMySQL Cドライバの
static TLS確保が失敗する問題を再現し、拡張の準備を別プロセスへ分離した。
この読み込み順・容量取得・集計監査も含め比較器の51回帰テストが通過した。エンジン本体のコード変更はない。
終了時に比較用サーバー・コンテナ・一時DBを削除し、公式imageと拡張cacheは再測定用に残す。
'''


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('directory', type=Path)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    result, text = report(args.directory)
    (args.directory / 'audit.json').write_text(json.dumps(result, indent=2))
    args.output.write_text(text)
    print(json.dumps(result, indent=2))


if __name__ == '__main__':
    main()
