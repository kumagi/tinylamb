# Oracle Freeを追加した4製品比較（2026-10-31）

Oracle Freeを既存の比較器に追加し、共通2コア枠でOLAP 316測定、OLTP／HTAP 396試行を完了した。
OLAPは全行・値・順序がPGと一致し、全OLTP試行で残高・行数等の事後検査を通過した。
通常の8／16コア測定、TPC-CのPG Simple Query専用クライアントとは別枠である。

## 製品と測定条件

- Oracle: Oracle AI Database 26ai Free Release 23.26.3.0.0 - Develop, Learn, and Run for Free / Version 23.26.3.0.0。公式Fullイメージをdigestで固定。Free Use Terms配布。
- [Freeの上限](https://docs.oracle.com/en/database/oracle/oracle-database/26/xeinl/licensing-restrictions.html)は2コア・DB RAM 2GB・ユーザーデータ12GB。この結果はEnterprise Editionの性能を表さない。
- 全4製品を物理コア0／1に対応する論理CPU 0,1へ固定。負荷は製品ごとに順次実行。
- Tiny／PGはデータバッファ1,088 MiB。MySQLは同じ1,088 MiB指定に対し、実測値は1,536 MiB。
  Oracleの実Buffer Cacheは開始1,088／終了1,008 MiB。
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
- 今回のOracleスキーマの割当セグメント計126.4 MiB。12GB上限内で動作したが、SF=1／より大きな規模は今回未測定。

## OLAPの結果

100万行から代表種目を抜粋。全19種目×3規模は[全OLAP表](../benchmark/comparison/results/2026-10-31-oracle-2core-indexed-final/analysis/)に保存。

| 処理 | Tinylamb ms | PostgreSQL ms | MySQL ms | Oracle Free ms |
| --- | --- | --- | --- | --- |
| count | 6.886 | 14.401 | 116.056 | 2.639 |
| sum | 8.161 | 21.087 | 150.911 | 28.277 |
| group_100 | 18.720 | 42.070 | 148.338 | 54.639 |
| group_10000 | 33.616 | 50.164 | 684.065 | 153.129 |
| distinct | 4.207 | 158.583 | 4.886 | 21.734 |
| top_100 | 0.066 | 0.050 | 188.995 | 26.333 |
| join_group | 12.679 | 61.350 | 736.740 | 31.756 |
| exists | 2.366 | 3.533 | 8.070 | 8.381 |
| union_all | 13.000 | 4.851 | 26.044 | 3.033 |

![100万行OLAP、4製品の中央値](../benchmark/comparison/results/2026-10-31-oracle-2core-indexed-final/analysis/micro_1m.png)

Oracleにも得意・不得意があり、件数集計と上位件数取得で挙動が異なる。索引・計画・通信固定費を含むこの条件の比較であり、製品の一般的な序列とは扱わない。
TinyのHTAP readerは不変データの前計算・キャッシュ再利用を含む。更新と組み合わせた結果も別表で確認する。

## TPC-H由来22問

9反復の問別中央値を足した参考値。連続して22問を実行した時間や公式TPC-Hスコアではない。
全問の値と計画は[問別表](../benchmark/comparison/results/2026-10-31-oracle-2core-indexed-final/analysis/tpch.md)・[全結果](../benchmark/comparison/results/2026-10-31-oracle-2core-indexed-final/query-details/)へ保存。

| 製品 | 22問の中央値合計 ms |
| --- | --- |
| Tinylamb | 74.464 |
| PostgreSQL | 68.407 |
| MySQL | 262.573 |
| Oracle Free | 59.653 |

![TPC-H由来22問、4製品の中央値](../benchmark/comparison/results/2026-10-31-oracle-2core-indexed-final/analysis/tpch.png)

## OLTPの結果

成功TPSの3試行中央値。成功時の遅延と競合中断を併読する。

| 処理 | 接続 | Tinylamb 成功TPS | PostgreSQL 成功TPS | MySQL 成功TPS | Oracle Free 成功TPS |
| --- | --- | --- | --- | --- | --- |
| noop | 1 | 135,509.5 | 102,074.0 | 71,151.5 | 37,269.0 |
| noop | 16 | 318,363.0 | 216,603.0 | 224,327.5 | 174,356.5 |
| point_read | 1 | 61,486.0 | 62,532.5 | 51,335.0 | 2,661.5 |
| point_read | 16 | 125,268.5 | 127,539.0 | 157,967.5 | 5,237.0 |
| range_read | 1 | 28,894.0 | 35,435.0 | 32,532.5 | 2,587.0 |
| range_read | 16 | 70,419.0 | 71,889.0 | 88,049.0 | 4,942.0 |
| update | 1 | 6,077.5 | 7,260.0 | 6,682.0 | 1,850.0 |
| update | 16 | 8,641.5 | 7,465.5 | 27,580.0 | 4,533.5 |
| hot_update | 1 | 5,961.0 | 952.5 | 6,660.0 | 6,013.5 |
| hot_update | 16 | 8,623.0 | 4,538.5 | 7,679.0 | 7,319.0 |
| transfer | 1 | 894.0 | 4,492.5 | 786.5 | 535.5 |
| transfer | 16 | 7,637.0 | 7,400.0 | 7,654.5 | 2,400.0 |
| rollback | 1 | 17,733.0 | 33,347.5 | 5,420.5 | 1,695.0 |
| rollback | 16 | 44,386.5 | 60,795.0 | 19,859.5 | 4,679.5 |

測定窓／warmup中のSQL失敗数は、順にTinylamb 2304／528、
PG 0／0、MySQL 0／0、
Oracle 0／0。
SQL失敗と値の不一致は別であり、値不一致・事後検査失敗は全製品0件だった。

![成功TPSの接続数別推移](../benchmark/comparison/results/2026-10-31-oracle-2core-indexed-final/analysis/oltp_scaling.png)

[全TPS・min/max・失敗率](../benchmark/comparison/results/2026-10-31-oracle-2core-indexed-final/analysis/oltp_tps.md)、
[成功時p95／p99](../benchmark/comparison/results/2026-10-31-oracle-2core-indexed-final/analysis/oltp_latency.md)、[HTAP役割別](../benchmark/comparison/results/2026-10-31-oracle-2core-indexed-final/analysis/htap.md)。

## 再現性と残作業

起動コマンド・SQL・入力hash・Docker digest・全計時・全結果・実行計画・不変条件は[測定ディレクトリ](../benchmark/comparison/results/2026-10-31-oracle-2core-indexed-final/)に保存。
`runtime-harness/`は実行中の3ソースのhash一致を確認したコピー。解析コードのhashは`audit.json`に別記する。
TinylambバイナリSHA256: `347d3f26348c8308db5d7b24cdbdb066e2ad15e3cec47c8f13ef753cce4af55c`。

OracleのARCHIVELOGが残った最初の探索試行は不採用。キャッシュを厳密512／1,088 MiBへ固定しようとした準備失敗も本結果に含めない。
採用対象はこのfinalディレクトリだけで、設定の異なる値を継ぎ足していない。

- OracleのTPC-Cは未対応。PG専用クライアントをOracleの測定済みとして扱わない。
- 追加索引なし、SF=1、bind／prepared、長時間の同期writeとtail、実運用サイズの評価は追加課題。
- 進行中のPG比1.5倍改善は[既存計画](postgresql_1_5_plan_2026-10-30.md)に保持。この2コア枠を8／16コアの達成判定へ混ぜない。

公式資料: [配布イメージ](https://container-registry.oracle.com/ords/ocr/ba/database/free)、
[リソース制限](https://docs.oracle.com/en/database/oracle/oracle-database/26/xeinl/licensing-restrictions.html)、
[Free Use Terms](https://www.oracle.com/downloads/licenses/oracle-free-license.html)。
