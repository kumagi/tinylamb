#!/usr/bin/env python3
"""Audit and describe the completed four-engine Oracle Free addition."""

import argparse
from collections import Counter
import hashlib
import json
from pathlib import Path
import statistics

from analyze import markdown
from workloads import olap_queries, tpch_queries


ENGINES = ('tinylamb', 'postgresql', 'mysql', 'oracle')
LABELS = ('Tinylamb', 'PostgreSQL', 'MySQL', 'Oracle Free')


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('directory', type=Path)
    p.add_argument('--output', type=Path, required=True)
    a = p.parse_args()
    directory = a.directory.resolve()
    env = json.loads((directory / 'environment.json').read_text())
    assert env.get('finished'), 'incomplete measurement: no finished timestamp'
    args = env['args']
    records = [json.loads(s) for s in (directory / 'results.jsonl').read_text().splitlines()]
    olap = [r for r in records if r['kind'] == 'olap']
    oltp = [r for r in records if r['kind'] == 'oltp']
    queries = [*olap_queries(10000), 'htap_reader']
    expected_olap = {(suite, scale, q, e)
                     for suite, scales, names in [('micro', args['sizes'], queries),
                                                   ('tpch', [args['tpch_scale']], list(tpch_queries(args['tpch_scale'])))]
                     for scale in scales for q in names for e in ENGINES}
    expected_oltp = {(q, c, t, e) for q in args['oltp_workloads'] for c in args['clients']
                     for t in range(args['trials']) for e in ENGINES}
    actual_olap = [(r['suite'], r['rows'], r['workload'], r['engine']) for r in olap]
    actual_oltp = [(r['workload'], r['clients'], r['trial'], r['engine']) for r in oltp]
    assert set(actual_olap) == expected_olap and len(actual_olap) == len(expected_olap)
    assert set(actual_oltp) == expected_oltp and len(actual_oltp) == len(expected_oltp)
    assert all(r['correct'] and r['stable'] and len(r['samples_ms']) == args['repeats'] for r in olap)
    assert all(r['valid'] and not r['validation_failures'] for r in oltp)
    errors = {e: dict(measured=sum(r['errors'] for r in oltp if r['engine'] == e),
                      warmup=sum(r['warmup_errors'] for r in oltp if r['engine'] == e)) for e in ENGINES}
    audit = dict(complete=True, correct_olap=len(olap), valid_oltp=len(oltp), errors=errors,
                 records=dict(Counter(r['kind'] for r in records)),
                 report_generator_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest())
    (directory / 'audit.json').write_text(json.dumps(audit, indent=2))

    largest = max(args['sizes'])
    def latency(q, e):
        return next(r['median_ms'] for r in olap if r['suite'] == 'micro' and r['rows'] == largest
                    and r['workload'] == q and r['engine'] == e)
    selected = ['count', 'sum', 'group_100', 'group_10000', 'distinct', 'top_100', 'join_group', 'exists', 'union_all']
    micro_table = markdown(['処理', *[e + ' ms' for e in LABELS]],
                           [[q, *[f'{latency(q,e):.3f}' for e in ENGINES]] for q in selected])
    tpch_table = markdown(['製品', '22問の中央値合計 ms'],
                          [[label, f"{sum(r['median_ms'] for r in olap if r['suite']=='tpch' and r['engine']==e):.3f}"]
                           for e, label in zip(ENGINES, LABELS)])
    tps_rows = []
    for name in ['noop', 'point_read', 'range_read', 'update', 'hot_update', 'transfer', 'rollback']:
        for clients in [1, 16]:
            tps_rows.append([name, clients, *[f"{statistics.median(r['tps'] for r in oltp if r['workload']==name and r['clients']==clients and r['engine']==e):,.1f}" for e in ENGINES]])
    tps_table = markdown(['処理', '接続', *[e + ' 成功TPS' for e in LABELS]], tps_rows)
    relative = '../benchmark/comparison/results/' + directory.name
    oracle = env['settings']['oracle']
    cache_start = dict(oracle['sga'])['Buffer Cache Size'] / 2**20
    cache_end = dict(env['oracle_sga_end'])['Buffer Cache Size'] / 2**20
    allocated = sum(row[1] for row in env['oracle_schema_segments_bytes']) / 2**20
    mysql_cache = int(dict(env['settings']['mysql'])['innodb_buffer_pool_size']) / 2**20
    version = oracle['version'][0][0].replace('\n', ' / ')
    report = f'''# Oracle Freeを追加した4製品比較（2026-10-31）

Oracle Freeを既存の比較器に追加し、共通2コア枠でOLAP {len(olap)}測定、OLTP／HTAP {len(oltp)}試行を完了した。
OLAPは全行・値・順序がPGと一致し、全OLTP試行で残高・行数等の事後検査を通過した。
通常の8／16コア測定、TPC-CのPG Simple Query専用クライアントとは別枠である。

## 製品と測定条件

- Oracle: {version}。公式Fullイメージをdigestで固定。Free Use Terms配布。
- [Freeの上限](https://docs.oracle.com/en/database/oracle/oracle-database/26/xeinl/licensing-restrictions.html)は2コア・DB RAM 2GB・ユーザーデータ12GB。この結果はEnterprise Editionの性能を表さない。
- 全4製品を物理コア0／1に対応する論理CPU 0,1へ固定。負荷は製品ごとに順次実行。
- Tiny／PGはデータバッファ1,088 MiB。MySQLは同じ1,088 MiB指定に対し、実測値は{mysql_cache:,.0f} MiB。
  Oracleの実Buffer Cacheは開始{cache_start:,.0f}／終了{cache_end:,.0f} MiB。
  SGA内配分・共有プール・PGA・プロセス全体のメモリは同一ではない。全SGA項目を生データへ保存。
- 共通二次索引あり。salesは10,000／100,000／1,000,000行、accountsは最大10,000行。
  各規模19 OLAP、TPC-H由来22問はSF=0.01・共通DBGEN入力・binary64。更新関数や監査を含まない非認証試験。
- OLAPは最初の実行と、その後9反復の中央値を区別する。cold I/O測定ではない。
- OLTPは1／4／16接続、0.5秒warmup＋2秒、3試行。表は試行中央値。短窓の初期比較であり、僅差やtailの達成判定には使わない。
  全11種目・min/max・p95/p99・失敗率は別表へ保存。HTAPの1接続はreaderのみ、4／16接続は最後の1接続がreader。
- OracleはREAD COMMITTED・WAIT／IMMEDIATEの同期commit・FORCE LOGGING YES・NOARCHIVELOG。
  PGはfsync／synchronous_commit有効、MySQLはinnodb_flush_log_at_trx_commit=1・binlog無効、Tinyは同期commit。
  アーカイブ・レプリケーション性能の比較ではない。
- 全結果取得までのクライアント時間。Oracleは公式Python Thin、statement cache無効。
  SQL方言・型変換も含む。PG／TinyはSimple Query、MySQLはCOM_QUERYであり、prepared／bindの最高性能を測っていない。
  OracleだけDockerのlocalhostポート公開、他製品はhost networkなのでネットワーク経路も完全同一ではない。
  SELECT 1の差をエンジン内部の実行速度と解釈しない。
- Oracleは投入時だけarray DML。他製品の複数行SQL投入と異なるため、ロード時間を純粋なエンジン性能として順位付けしない。
- 今回のOracleスキーマの割当セグメント計{allocated:,.1f} MiB。12GB上限内で動作したが、SF=1／より大きな規模は今回未測定。

## OLAPの結果

100万行から代表種目を抜粋。全19種目×3規模は[全OLAP表]({relative}/analysis/)に保存。

{micro_table}
![100万行OLAP、4製品の中央値]({relative}/analysis/micro_1m.png)

Oracleにも得意・不得意があり、件数集計と上位件数取得で挙動が異なる。索引・計画・通信固定費を含むこの条件の比較であり、製品の一般的な序列とは扱わない。
TinyのHTAP readerは不変データの前計算・キャッシュ再利用を含む。更新と組み合わせた結果も別表で確認する。

## TPC-H由来22問

9反復の問別中央値を足した参考値。連続して22問を実行した時間や公式TPC-Hスコアではない。
全問の値と計画は[問別表]({relative}/analysis/tpch.md)・[全結果]({relative}/query-details/)へ保存。

{tpch_table}
![TPC-H由来22問、4製品の中央値]({relative}/analysis/tpch.png)

## OLTPの結果

成功TPSの3試行中央値。成功時の遅延と競合中断を併読する。

{tps_table}
測定窓／warmup中のSQL失敗数は、順にTinylamb {errors['tinylamb']['measured']}／{errors['tinylamb']['warmup']}、
PG {errors['postgresql']['measured']}／{errors['postgresql']['warmup']}、MySQL {errors['mysql']['measured']}／{errors['mysql']['warmup']}、
Oracle {errors['oracle']['measured']}／{errors['oracle']['warmup']}。
SQL失敗と値の不一致は別であり、値不一致・事後検査失敗は全製品0件だった。

![成功TPSの接続数別推移]({relative}/analysis/oltp_scaling.png)

[全TPS・min/max・失敗率]({relative}/analysis/oltp_tps.md)、
[成功時p95／p99]({relative}/analysis/oltp_latency.md)、[HTAP役割別]({relative}/analysis/htap.md)。

## 再現性と残作業

起動コマンド・SQL・入力hash・Docker digest・全計時・全結果・実行計画・不変条件は[測定ディレクトリ]({relative}/)に保存。
`runtime-harness/`は実行中の3ソースのhash一致を確認したコピー。解析コードのhashは`audit.json`に別記する。
TinylambバイナリSHA256: `{env['server_sha256']}`。

OracleのARCHIVELOGが残った最初の探索試行は不採用。キャッシュを厳密512／1,088 MiBへ固定しようとした準備失敗も本結果に含めない。
採用対象はこのfinalディレクトリだけで、設定の異なる値を継ぎ足していない。

- OracleのTPC-Cは未対応。PG専用クライアントをOracleの測定済みとして扱わない。
- 追加索引なし、SF=1、bind／prepared、長時間の同期writeとtail、実運用サイズの評価は追加課題。
- 進行中のPG比1.5倍改善は[既存計画](postgresql_1_5_plan_2026-10-30.md)に保持。この2コア枠を8／16コアの達成判定へ混ぜない。

公式資料: [配布イメージ](https://container-registry.oracle.com/ords/ocr/ba/database/free)、
[リソース制限](https://docs.oracle.com/en/database/oracle/oracle-database/26/xeinl/licensing-restrictions.html)、
[Free Use Terms](https://www.oracle.com/downloads/licenses/oracle-free-license.html)。
'''
    a.output.write_text(report)
    print(json.dumps(audit, ensure_ascii=False))


if __name__ == '__main__':
    main()
