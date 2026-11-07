# TPC-C由来W=1の3製品比較（2026-11-06）

DuckDB・Oracleを外し、Tinylamb C8・PostgreSQL 18.6 (Debian 18.6-1.pgdg13+2)・MySQL 8.4.13を再測定した。
同じC++トランザクション生成器、同じW=1 snapshot、同じSQL生成規則を使用した全9試行で、事後不変条件を通過した。
各試行前に全9表の全行・型・値のfingerprint一致も検証した。

## 条件と比較範囲

- TPC-Cは倉庫数W=1。1倉庫・10district・30,000顧客・100,000商品／stockが基礎規模。
  SF=1は直前のTPC-Hの指定で、今回Oracleを外した理由を「W=1が容量制限に入らない」と確認したわけではない。
  ユーザー指定の3製品枠としてDuckDB／Oracleを除外した。
- 10接続、warmup 5秒＋計測30秒、3試行。
  seed=20260819、New-Order／Payment／Order-Status／Delivery／Stock-Levelの要求比率45／43／4／4／4。
  これは入力要求の比率であり、中断により成功した処理の構成比は変わる。
  think time・keying time・自動再試行・stored procedureを加えていない。TPC認証スコア／tpmCではない。
- 全サーバーCPU 0-15、データバッファ512 MiB。Tinyクエリ予算256 MiB、PG work_mem64 MiB。
  全プロセスの総メモリを同一にした比較ではない。同期commit、PG fsync有効、MySQL redo fsync=1・binlog無効。
- 同じ実行ファイルでTiny／PGはSimple Query、MySQLは公式C API 8.4.11のCOM_QUERY。
  literal SQLとtext結果、prepared statementなし。型変換・往復・取得を含む端末側時間である。
  MySQLはREAD COMMITTED、文字列列はutf8mb4_bin。複合索引キーとunique指定を合わせた。
- 各試行は3製品とも元snapshotへ復元する。PGはCOPY後VACUUM ANALYZE、MySQLは投入後ANALYZE。
  投入方法が違うためロード時間を製品性能として比較しない。実行順は試行ごとにseed付きshuffle。
- 使用snapshotは以前の共通W=1 fixtureを固定したもので、初期化後の事前トランザクションを少量含む。
  正確な初期行数・残高・SHA-256は[初期状態](../benchmark/comparison/results/2026-11-06-tpcc-three-quiet/initial-invariants.json)と環境記録に残す。
- **現在の生成器にはSQLをまとめて発行する箇所がある。旧clientとの同一性を今回検証していないため、過去のTPS差をDB改善とは扱わない。**
  今回の3製品間では同じソース・実行ファイル・生成器を使用する。
- 実行開始前に他のbuild／test／profiler／比較ベンチマークを待ち、各計測中は1秒ごとにprocessを監視した。
  採用した全9試行の監視では重なりを検出しなかった。
  初回シリーズとその部分再測定は参考として保存し、最終表には監視を強化した新しいシリーズを使った。
  このシリーズでも別TPC実行を検出したTiny trial3／PG trial3は、同一条件の再測定へ置換した。
  採用元と旧セットの扱いは[採用記録](../benchmark/comparison/results/2026-11-06-tpcc-three-quiet/replacement-selection.json)に残す。
  遅さを理由に試行を除外していない。通常のホストアプリや他の待機中DBは停止しておらず、CPU／I/O完全占有の試験ではない。
- クライアントは計測区間内に開始した処理を数えるため、終了時刻をまたぐ最後の処理も含み得る。
  各処理は始終を計時し、成功したトランザクションのみ遅延分位点へ含める。

## スループットと中断

| 製品 | 合計成功TPS 中央値 [最小–最大] | New-Order成功TPS | engine abort率 | 意図したrollback率 |
| --- | --- | --- | --- | --- |
| Tinylamb | 1,712.30 [1,701.93–1,825.40] | 993.43 | 41.23% | 0.34% |
| PostgreSQL | 1,373.27 [1,343.87–1,454.60] | 617.23 | 0.46% | 0.42% |
| MySQL | 4,376.43 [4,347.30–4,650.57] | 1,981.73 | 1.21% | 0.45% |

今回の中央値ではMySQLの合計成功TPSがTinyの2.56倍、PGの3.19倍だった。
TinyはPGの1.25倍だが、中断率も大きい。成功TPSと中断率を併せて評価する必要がある。
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

![throughput](../benchmark/comparison/results/2026-11-06-tpcc-three-quiet/analysis/throughput.png)

## 処理別の成功TPSと遅延

各値は3試行の中央値。遅延は各試行の成功処理だけの分位点を求め、その中央値を取った。
中断した処理の遅延を成功処理へ混ぜていない。

| 処理 | Tinylamb 成功TPS / p50 ms | PostgreSQL 成功TPS / p50 ms | MySQL 成功TPS / p50 ms |
| --- | --- | --- | --- |
| new_order | 993.43 / 4.371 | 617.23 / 3.066 | 1,981.73 / 2.017 |
| payment | 417.27 / 5.997 | 595.40 / 8.433 | 1,911.90 / 2.013 |
| order_status | 117.30 / 0.604 | 56.60 / 0.181 | 179.27 / 0.177 |
| delivery | 64.83 / 8.820 | 47.63 / 3.248 | 125.20 / 3.996 |
| stock_level | 119.47 / 1.689 | 56.40 / 0.727 | 178.33 / 0.629 |

| 処理 | Tinylamb p95 / p99 ms | PostgreSQL p95 / p99 ms | MySQL p95 / p99 ms |
| --- | --- | --- | --- |
| new_order | 7.420 / 10.147 | 16.779 / 28.965 | 3.627 / 11.146 |
| payment | 9.367 / 12.957 | 23.575 / 37.122 | 3.096 / 7.611 |
| order_status | 1.593 / 2.139 | 0.247 / 0.326 | 0.251 / 0.359 |
| delivery | 11.928 / 15.822 | 4.443 / 6.012 | 7.152 / 14.519 |
| stock_level | 3.832 / 5.054 | 0.979 / 1.156 | 0.884 / 1.132 |

![latency](../benchmark/comparison/results/2026-11-06-tpcc-three-quiet/analysis/latency.png)

## 全試行と検証

| 製品 | 試行 | 合計成功TPS | 成功件数 | engine abort | 意図したrollback |
| --- | --- | --- | --- | --- | --- |
| Tinylamb | 1 | 1825.4 | 54762 | 38581 | 332 |
| Tinylamb | 2 | 1701.93 | 51058 | 36010 | 290 |
| Tinylamb | 3 | 1712.3 | 51369 | 36341 | 300 |
| PostgreSQL | 1 | 1454.6 | 43638 | 221 | 183 |
| PostgreSQL | 2 | 1373.27 | 41198 | 173 | 179 |
| PostgreSQL | 3 | 1343.87 | 40316 | 183 | 170 |
| MySQL | 1 | 4650.57 | 139517 | 1771 | 653 |
| MySQL | 2 | 4376.43 | 131293 | 1589 | 602 |
| MySQL | 3 | 4347.3 | 130419 | 1587 | 601 |

初期化前後の9表行数、district next-order-id合計とorders行数の関係、ordersの明細数合計と
order_line行数、warehouse／district／history金額合計の一致を確認した。
これは完全なTPC-C serial oracleや全12整合性条件の監査ではない。
engine abortの代表原因と全件数は[失敗一覧](../benchmark/comparison/results/2026-11-06-tpcc-three-quiet/analysis/errors.md)に保存した。

MySQL接続層は[公式C API](https://dev.mysql.com/doc/c-api/8.4/en/mysql-affected-rows.html)を使い、
更新件数・NULL・64bit整数・小数・文字列・commit／rollback・unique違反後の復帰を全3製品で実接続検査した。
CSV投入では引用された空文字と裸のNULLを区別し、名前の照合順も検証する。
比較器の60回帰テスト、include-layer lint、C++変更箇所のformat確認も通過した。

## 再現と保存

- [全生データと監査](../benchmark/comparison/results/2026-11-06-tpcc-three-quiet/)、[全指標CSV](../benchmark/comparison/results/2026-11-06-tpcc-three-quiet/analysis/metrics.csv)、[全環境設定](../benchmark/comparison/results/2026-11-06-tpcc-three-quiet/environment.json)。
- client／Tiny C8／fixture／MySQL C libraryのSHA-256、Docker digest、実行コマンド、全試行ログ、
  試行前の9表fingerprint、process監視ログ、実行時ソースを保存した。
- [事前検証](../benchmark/comparison/results/2026-11-06-tpcc-three-preflight2/)は2秒1試行であり本表へ混ぜていない。
- 現在の作業ツリーのストレージ層に今回と別のビルドエラーがあったため、clientはDB本体から分離してビルドした。
  サーバーには前回検証済みC8を固定しており、作業中のLStable変更を測った結果ではない。
  C8以降の現在ソース全体をビルド・全回帰できたという意味ではない。
- [実行手順](../benchmark/comparison/README.md)。終了時に自分のサーバー・コンテナ・一時DBを削除する。
