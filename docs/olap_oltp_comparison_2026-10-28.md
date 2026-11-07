# tinylamb PostgreSQL MySQLのOLAP OLTP性能比較 2026年10月28日

修正後の全条件を取り直した最新結果は[10月29日の再測定](olap_oltp_comparison_2026-10-29.md)を参照。

前半の表と結論は同日21:11～21:34の修正前測定を保存したもの。
その後[TODO](../TODO.md)の6項目を順に実装した。Q6は現在一致し、
INSERT＋DELETEの不要な競合、書き込みワーカ、結合・集計・Top N、SQL索引を改善した。
修正後の数値と残る制約は末尾の「修正後の確認」に分けて記載する。

## 結論

tinylambは、今回の条件ではグループ集計や一部の複雑な分析SQLに強みがある。一方、単純な全件集計、結合の構築側の選択、クライアント経由の並列書き込みに改善余地が大きい。100万行の結合集計はtinylamb 980 ms、PostgreSQL 31.1 ms、MySQL 732 msだった。100グループの集計はそれぞれ14.7 ms、20.7 ms、236 msであり、SQLの形によって評価が変わる。

主キー読み取りは16接続でtinylamb約20.2万query/s、PostgreSQL約37.6万、MySQL約22.2万だった。2行送金はそれぞれ494、8,175、8,222成功txn/sで、tinylambは接続数を増やしても伸びなかった。INSERT＋DELETEではtinylambの3秒窓内の成功数が非常に少なく、16接続の成功TPS中央値は0だった。これは有限の測定窓の値であり、長時間の処理能力が厳密に0という意味ではない。

**正しさではQ6に問題が見つかった。** 同じ入力でPostgreSQLとMySQLは1,787,210.8441を返したが、tinylambはNULLを返した。このクエリは性能順位から除外した。OLTP／HTAPの最終状態の不変条件は全試行で満たされたが、tinylambには競合等による中断があった。

これはローカルTCP、キャッシュを使用するデータ量、追加索引なし、同期commit有効での実測である。PostgreSQLは18.6、MySQLは8.4.13、tinylambはHEAD `d9315a8277650d96a5f535f5b85a7a964ce86fa5`と測定開始時の未コミット変更を含むビルドを用いた。HEADだけの性能を表す値ではない。

## 実装したベンチマークセット

再実行可能なコードは[`benchmark/comparison`](../benchmark/comparison/README.md)に置いた。共通の決定的データを使い、製品を順番に測定する。ロード、ウォームアップ、計測、全結果取得、正しさの検査、EXPLAINの保存、後始末を一つのコマンドで実行できる。

| 分類 | 今回の測定範囲 |
| --- | --- |
| 共通OLAP | 18クエリ × sales 1万／10万／100万行 × 3製品 |
| TPC-H由来 | 既存テンプレートの全22クエリ、DBGEN SF 0.01、3製品 |
| OLTP | 10処理 × 1／4／16クライアント × 3試行 × 3製品 |
| HTAP | 1クライアントが100万行とaccountsを結合集計し、残りが検索＋送金。4／16クライアント、3試行、3製品 |
| 正しさ | OLAP全行照合、反復時の結果安定性、ロード時の総和、残高総和・行数・churn空状態、HTAPの残高保存 |
| 計時 | OLAPは各条件の最初の実行と続く5回を分離。OLTPは1秒のウォームアップ＋3秒の測定を3試行 |

共通OLAPには、COUNT、SUM、1%／10%フィルタ、数式、100／10,000グループ、DISTINCT、Top 100、内部／外部結合、結合集計、EXISTS、スカラー副問い合わせ、CTE、HAVING、UNION ALL、ウィンドウ関数を含む。OLTPには通信固定費の参考になるSELECT 1、主キー／非キー／範囲検索、ランダム／集中更新、2行送金、4回検索＋送金、INSERT＋DELETE、ROLLBACKを含む。

既存のTPC-C／TPC-DSベンチマークはtinylamb内のAPIを直接使うため、今回の3製品共通の接続経路には流用していない。送金等は独自のOLTPワークロードであり、TPC-Cの業務構成やtpmCを実装したものではない。TPC-H由来の測定も、小数SF、固定パラメータ、binary64、更新関数なしという工学的比較であり、公式のQphHとは異なる。[TPC-Hの公式定義](https://www.tpc.org/tpch/)と混同しない。

## 比較条件

| 項目 | 設定 |
| --- | --- |
| ホスト | AMD Ryzen 9 9950X3D、16物理コア／32論理CPU、メモリ約121 GiB、Linux 7.0.0-38-generic |
| CPU配分 | 全サーバを同じ論理CPU 0–7に制限。負荷生成は別プロセス、CPU制限なし |
| ストレージ | tinylambは`/var/tmp`のext4、PostgreSQL／MySQLはDockerのlocal volume。接続はhost network |
| tinylambビルド | RelWithDebInfo、`-O2 -g -DNDEBUG`、LTOなし、`-march`指定なし、LLVM 21のJIT有効 |
| バッファ | 各製品512 MiB。これはプロセスの総メモリ制限ではない |
| クエリメモリ | tinylamb 64 MiB／query、PostgreSQL `work_mem=64MB`／演算子、MySQLの他のクエリメモリ設定は既定値 |
| 並列実行 | tinylamb read-workers=8、内部スケジューラは既定。PostgreSQLはGatherあたり最大4ワーカー、JIT有効 |
| 永続化 | tinylambは既定の同期commit、PostgreSQLはfsyncとsynchronous_commit有効、MySQLはinnodb_flush_log_at_trx_commit=1 |
| レプリケーション | 全製品でレプリケーションを構成しない。MySQLのbinlogは無効 |
| 分離レベル | PostgreSQL／MySQLをREAD COMMITTEDに設定。tinylambは既定のMVCC |
| プロトコル | tinylamb／PostgreSQLはSimple Query、MySQLは非prepared COM_QUERY。TLSなし、接続再利用、1文ずつ送信 |
| ドライバ | psycopg2-binary 2.9.13、mysql-connector-python 26.10.0のC拡張、Python 3.14.4 |
| スキーマ | accounts 10,000行、主キーid、残高100,000、64文字ペイロード。salesは7列。主キー以外の索引は付けない |

PostgreSQLとMySQLの同期commit設定は、それぞれ[PostgreSQLのWAL設定](https://www.postgresql.org/docs/18/runtime-config-wal.html)、[InnoDBの設定](https://dev.mysql.com/doc/refman/8.4/en/innodb-parameters.html)に基づく。tinylambの契約は[commit_durability.md](commit_durability.md)にある。設定の確認と、停電・クラッシュでの耐久性検証は別の試験である。

MySQLの既定の分離レベルはREPEATABLE READだが、この測定ではREAD COMMITTEDへ変更した。[InnoDBの分離レベル](https://dev.mysql.com/doc/refman/8.4/en/innodb-transaction-isolation-levels.html)が説明するようにロックや読み取りの動作が変わるため、MySQLの既定設定の成績とは扱わない。tinylambとの完全な分離特性の同値性は検証していない。

データ投入は3製品とも500行単位の複数VALUESによるINSERTを使い、COPYやLOAD DATA等の製品専用ローダーを使わない。測定前にANALYZEする。異なる製品で同じ設定名を持たないものは、その意味を表に明記した。CPUとバッファ設定は共通化したが、すべてのリソース消費を同一にした試験ではない。

同じSQLを繰り返す条件では、tinylambのプロセス内のテンプレート／プランキャッシュも性能に含まれる。PostgreSQL側で明示的なprepared statementを使う条件とは異なる。最初の実行の値も生データへ保存しているが、以下の表はウォーム時の中央値を比較する。Simple Queryしか提供しない現状のtinylambと接続方式を揃えた評価であり、各製品の最適なAPIを使った場合の上限性能ではない。

## データ投入

| 投入対象 | tinylamb 秒 | PostgreSQL 秒 | MySQL 秒 |
| --- | --- | --- | --- |
| 共通データ 1万行 | 0.139 | 0.093 | 0.103 |
| 共通データ 10万行 | 0.407 | 0.579 | 0.440 |
| 共通データ 100万行 | 3.093 | 5.409 | 3.655 |
| TPC-H由来 SF 0.01 | 8.718 | 1.044 | 0.788 |


共通のINSERT経路では、100万行のsalesと1万行のaccountsを投入する時間はtinylamb 3.09秒、PostgreSQL 5.41秒、MySQL 3.66秒だった。行ごとのcommitではなく500行単位なので、単発更新の性能とは傾向が異なる。DDL、バッチSQLの生成・送信・同期commitまで含み、ANALYZEは含まない。COPY等を使う各製品の最高ロード速度を表す値ではない。

## 共通OLAPの結果

100万行での値は以下のとおり。単位はms、5回の中央値で、小さいほど速い。クライアントへの全行転送とデコードも含むため、10,000グループの結果では演算以外の費用も含まれる。

| クエリ | tinylamb ms | PostgreSQL ms | MySQL ms |
| --- | --- | --- | --- |
| count | 33.327 | 5.888 | 6.992 |
| sum | 53.809 | 10.547 | 149.597 |
| filter_1pct | 10.496 | 8.328 | 133.461 |
| filter_10pct | 13.073 | 8.726 | 136.294 |
| expression | 40.525 | 11.314 | 155.232 |
| group_100 | 14.718 | 20.684 | 236.372 |
| group_10000 | 21.030 | 28.181 | 251.487 |
| distinct | 51.791 | 69.707 | 213.075 |
| top_100 | 89.922 | 14.809 | 154.194 |
| join | 12.945 | 12.162 | 594.876 |
| join_group | 980.347 | 31.108 | 731.595 |
| left_join | 581.991 | 29.648 | 156.729 |
| exists | 164.894 | 8.517 | 133.441 |
| scalar_subquery | 109.395 | 24.581 | 271.134 |
| cte | 16.619 | 19.742 | 236.608 |
| having | 17.754 | 19.708 | 260.722 |
| union_all | 8.133 | 30.236 | 256.839 |
| window | 2.311 | 7.420 | 142.086 |


![100万行のOLAP比較](../benchmark/comparison/results/2026-10-28/analysis/micro_1m.svg)

tinylambが特に良いのは、100／10,000グループの集計、HAVING、CTEによる集計、UNION ALL、選択条件付きウィンドウ関数である。ウィンドウ関数のSQLは`id < 1000`で対象を絞っており、100万行全体をウィンドウ処理した結果ではない。

単純COUNTはtinylamb 33.3 msに対してPostgreSQL 5.89 ms、SUMは53.8 msに対して10.5 msだった。tinylambのEXPLAINはどちらも16ワーカーのParallelAggregationExecutorを示している。並列演算子が選ばれていることだけでは、実際の処理速度やCPU利用効率を保証しない。集計の種類によって速さが変わるため、PAXやJITの存在だけでOLAP全般の優劣を説明することはできない。

Top 100はtinylamb 89.9 ms、PostgreSQL 14.8 ms。EXISTSも165 ms対8.52 msで差が大きい。絞り込み・ソート・副問い合わせのそれぞれで、実行経路の質を個別に確認する必要がある。

### データ量による変化

![OLAPのデータ量依存性](../benchmark/comparison/results/2026-10-28/analysis/olap_scaling.svg)

100グループの集計はtinylambが1万行で0.495 ms、10万行で2.36 ms、100万行で14.7 msとなり、PostgreSQLの0.749／7.15／20.7 msを上回った。一方、結合集計は6.07／38.5／980 msで、100万行で悪化が目立つ。PostgreSQLは同じ条件で1.83／12.1／31.1 msだった。

tinylambのrelational経路のEXPLAINは`heuristic estimate`を含む表示であり、実行時カウンタではない。その表示では、10万行時はsalesを構築側にするHashJoin（推定12.2 MiB）、100万行時は同じsalesを構築側にするHybridHashJoin（推定122.1 MiB）を選んだ。クエリ予算は64 MiBである。PostgreSQLは10,000行のaccountsを構築側とする並列Hash Joinを選んでいる。大きい側の構築とメモリ予算を超えるプランへの切り替えは、tinylambの悪化と整合する。これはEXPLAINに基づく原因候補であり、実際のspill量やI/O待機を測定した断定ではない。

外部結合も100万行でtinylamb 582 ms、PostgreSQL 29.6 ms、MySQL 157 msとなった。tinylambのプランは、こちらもsalesを構築側とするHybridHashJoinを示している。メモリを増やすだけで解決するか、構築側の入れ替え・述語の適用位置・中間行の幅が支配的なのかは、追加のA/B測定で切り分ける価値がある。

## TPC-H由来クエリの結果

正しい結果が得られた21クエリの中央値では、tinylambがPostgreSQLより速かったのは14、MySQLより速かったのは20だった。一方、共通OLAPの100万行ではPostgreSQLより速かったのは18クエリ中7であり、TPC-H由来の成績だけをOLAP全般へ一般化できない。接近した値の優劣は、5回の反復だけで統計的有意差を主張するものではない。

差が大きいのはQ17とQ20である。Q17はtinylamb 1.12 ms、PostgreSQL 166 ms、MySQL 1,217 ms。Q20は1.39 ms、9,403 ms、1,178 msだった。PostgreSQLの保存プランには、外側の行に依存するSubPlan内でlineitemを逐次走査する構造がある。tinylambのQ20はSemiHashJoinを示す。副問い合わせの処理戦略の差が大きな性能差に結び付いていると考えられる。ただし索引のない条件なので、適切な索引やSQLの書き換えを使うPostgreSQLとの比較まで示す値ではない。

Q9はtinylamb 12.5 msに対してPostgreSQL 5.55 ms、Q16は2.64 ms対1.52 msで、常にtinylambが速いわけでもない。Q21はPostgreSQLが予備実行で約156 ms、本測定で18.8 msとなり、プランの変動も見られた。[予備実行のプランと計時](../benchmark/comparison/results/2026-10-28/diagnostics/q21-preflight.json)ではNested Loop、本測定ではHash Join／Merge Joinを含む構造になっている。報告値は本測定のものを採用し、特定の遅い予備実行を比較対象に選んでいない。

| クエリ | tinylamb ms | PostgreSQL ms | MySQL ms |
| --- | --- | --- | --- |
| Q01 | 5.863 | 6.149 | 29.482 |
| Q02 | 0.930 | 2.764 | 128.627 |
| Q03 | 1.595 | 3.353 | 14.096 |
| Q04 | 1.766 | 3.494 | 15.610 |
| Q05 | 2.989 | 2.703 | 17.668 |
| Q06 | **不一致／失敗 (0.912)** | 2.133 | 11.477 |
| Q07 | 5.881 | 3.279 | 25.052 |
| Q08 | 3.081 | 2.888 | 1223.051 |
| Q09 | 12.535 | 5.554 | 22.845 |
| Q10 | 3.307 | 3.967 | 14.741 |
| Q11 | 1.282 | 0.729 | 2.720 |
| Q12 | 2.361 | 4.784 | 14.348 |
| Q13 | 5.162 | 2.969 | 8.461 |
| Q14 | 1.905 | 2.262 | 11.266 |
| Q15 | 0.949 | 2.195 | 10.684 |
| Q16 | 2.636 | 1.518 | 2.553 |
| Q17 | 1.123 | 165.689 | 1217.167 |
| Q18 | 7.508 | 7.659 | 29.668 |
| Q19 | 1.426 | 3.577 | 12.517 |
| Q20 | 1.385 | 9403.096 | 1177.886 |
| Q21 | 14.948 | 18.800 | 64.425 |
| Q22 | 1.157 | 1.280 | 3.558 |


![TPC-H由来クエリの比較](../benchmark/comparison/results/2026-10-28/analysis/tpch.svg)

全製品で同じDBGENの8ファイルを投入した。SF 0.01の行数はregion 5、nation 25、supplier 100、customer 1,500、part 2,000、partsupp 8,000、orders 15,000、lineitem 60,175。価格・割引等はbinary64に統一し、日付はDATE型を使う。浮動小数点結果は相対1e-7／絶対0.01の許容差で照合した。

### Q6の正しさ

tinylambは最初の実行とその後の5回で一貫してNULLを返し、独立した予備実行でも再現した。PostgreSQLとMySQLの結果はともに約`1787210.8440999957`で、浮動小数点の丸め差では説明できない。tinylambのプラン表示は、列を射影したParallelScanのworker-filterとスカラーSUMを含む。原因はこの比較では確定していない。

再現の短縮実行は、READMEの環境を用意した上で`run.py --output /var/tmp/new-q6-repro --sizes 100 --skip-oltp --repeats 1 --timeout 30 --tpch-scale 0.01`とする。入力は固定コミットのDBGENから再生成され、保存済みhashと照合できる。これは今回の作業ツリーのビルドでの観測であり、特定の既存変更を原因と断定していない。

性能の評価に使えるのは、正しい結果が得られたクエリの値である。不一致のあるQ6を含めた総合スコアは作らない。全件照合の記録は`query-details/`、不一致の索引は`failures.json`に保存している。

## OLTPの結果

読み取りと書き込みで挙動が分かれた。主キー読み取りはtinylambが1接続の37,000 query/sから16接続の202,000へ伸び、MySQLの222,000に近付いた。非キー検索は16接続でも3,620 query/sで、PostgreSQL 85,715、MySQL 71,095に差を付けられた。主キー検索の成績から一般的な検索性能を推定することはできない。

SELECT 1の1接続はtinylamb 46,125 query/s、PostgreSQL 105,632、MySQL 72,996だった。ここにはプロトコル、ドライバ、スケジューリング、サーバ処理の固定費を含む。小さい処理ほどこの費用が相対的に大きい。

![OLTPの同時接続数による変化](../benchmark/comparison/results/2026-10-28/analysis/oltp_scaling.svg)

| 処理 | 接続数 | tinylamb 成功TPS（失敗率） | PostgreSQL 成功TPS（失敗率） | MySQL 成功TPS（失敗率） |
| --- | --- | --- | --- | --- |
| point_read | 1 | 36,996.0（0.00%） | 66,843.0（0.00%） | 55,002.3（0.00%） |
| point_read | 16 | 201,652.7（0.00%） | 375,911.7（0.00%） | 221,735.7（0.00%） |
| nonkey_read | 1 | 1,591.3（0.00%） | 6,791.3（0.00%） | 11,636.3（0.00%） |
| nonkey_read | 16 | 3,620.0（0.00%） | 85,715.0（0.00%） | 71,094.7（0.00%） |
| range_read | 1 | 7,400.0（0.00%） | 40,115.0（0.00%） | 36,054.7（0.00%） |
| range_read | 16 | 49,056.3（0.00%） | 228,483.0（0.00%） | 162,926.3（0.00%） |
| update | 1 | 3,908.0（0.00%） | 7,625.7（0.00%） | 7,066.0（0.00%） |
| update | 16 | 3,994.0（0.00%） | 62,962.0（0.00%） | 9,065.3（0.00%） |
| transfer | 1 | 493.7（0.00%） | 985.3（0.00%） | 879.7（0.00%） |
| transfer | 16 | 494.0（0.22%） | 8,175.0（0.00%） | 8,221.7（0.00%） |
| insert_delete | 1 | 450.3（0.00%） | 920.3（0.00%） | 4,921.0（0.00%） |
| insert_delete | 16 | 0.0（80.36%） | 7,429.3（0.00%） | 7,473.0（0.00%） |


表中のTPSは各試行の成功トランザクション数／3秒を算出した後の3試行中央値。失敗率は3試行の失敗数合計／試行要求数合計である。境界を跨いだ要求は成功TPSから外して別に保存する。失敗を自動再試行しないため、アプリケーション側のリトライ込みの完了率は表さない。

### クライアント経由の書き込み経路

tinylambの[サーバ実装](../server/postgres_server.cpp)は、単独SELECTを読み取りワーカーへ渡し、書き込みと明示的トランザクションのCOMMIT／ROLLBACKを単一のイベントループで処理する。また、そのスレッドを別セッションのwrite intent待機で止めないようNoWaitポリシーを設定している。したがって、この比較で観測する並列書き込みの上限や競合中断は、ストレージ・WALに加えて現在のサーバ経路を含む性質である。組み込みAPIによる既存TPC-Cの値から、今回のSQLサーバの性能を推定してはいけない。

2行送金のtinylambは1／4／16接続で494／495／494成功txn/sだった。PostgreSQLは985／2,053／8,175、MySQLは880／2,117／8,222である。16キーへの集中更新でも、tinylambは約504 txn/sのまま頭打ちになった。

INSERT＋DELETEでは、tinylambの4接続は3.0成功txn/s、要求失敗率46.88%、16接続は0.0成功txn/s、失敗率80.36%だった。ワーカーごとに異なるキーを使うので、意図的な同一キーの衝突はない。エラー例は`statement execution failed: Conflicts`であり、索引構造等の競合が候補になるが、この情報だけで待機箇所や根本原因は特定できない。最終的にchurnが空であることは確認できた。

通常更新の16接続では、tinylambの3試行が483～4,041 txn/s、PostgreSQLは8,147～65,311、MySQLは7,922～40,898と大きく揺れた。図の縦線はこの最小・最大であり、信頼区間ではない。中央値だけを定常的な上限値として扱わず、後述の延長測定も併せて読む必要がある。

### 遅延とHTAP

16接続時の成功要求の遅延を以下に示す。値は各試行の分位点を求めた後の3試行中央値で、全試行をプールした分位点ではない。単位はms。

| 処理 | tinylamb p95 / p99 ms | PostgreSQL p95 / p99 ms | MySQL p95 / p99 ms |
| --- | --- | --- | --- |
| point_read | 0.105 / 0.114 | 0.068 / 0.089 | 0.117 / 0.147 |
| range_read | 0.438 / 0.540 | 0.101 / 0.119 | 0.162 / 0.204 |
| transfer | 43.034 / 47.015 | 2.092 / 2.813 | 2.068 / 2.642 |
| read_write | 46.764 / 51.509 | 2.268 / 3.053 | 2.258 / 4.049 |

主キー読み取りのtinylamb p99は0.114 msで、MySQLの0.147 msより小さかった。一方、送金は47.0 ms、検索＋送金は51.5 msで、PostgreSQL／MySQLとの差が大きい。INSERT＋DELETEの16接続では3試行合わせて窓内成功が1件しかなく、成功時p99を安定した指標として採用できない。付録では成功数と値が得られた試行数も明記した。

HTAPの成功時分位点は種類の異なる集計と送金を混ぜた値になるため、個別処理の遅延評価には使わない。今回のドライバは役割別のTPSを記録するが、役割別の遅延サンプルは保存していない。

成功した要求のp95／p99は、失敗した要求の待ち時間を含まない。失敗率や成功数と併せて読む必要がある。3秒の測定窓は長期の裾の遅延やcheckpoint周期を代表するには短いので、ここでのp99を運用上のSLAへ直接使わない。

| 製品 | 接続数 | 書込txn/s | 結合集計query/s | 全要求失敗率 |
| --- | --- | --- | --- | --- |
| tinylamb | 4 | 2,947.0 | 0.7 | 0.01% |
| postgresql | 4 | 9,468.7 | 31.0 | 0.00% |
| mysql | 4 | 1,886.3 | 0.7 | 0.00% |
| tinylamb | 16 | 441.7 | 0.7 | 0.09% |
| postgresql | 16 | 6,763.3 | 30.3 | 0.00% |
| mysql | 16 | 11,278.3 | 0.3 | 0.00% |

PostgreSQLは1本の集計を約30～31 query/sで維持した。tinylambは約0.7 query/s、MySQLは0.3～0.7 query/sだった。tinylambの送金側は16接続で約442 txn/s、MySQLは約11,278、PostgreSQLは約6,763である。これも短い窓での中央値で、役割ごとのmin/maxは生データから確認できる。

HTAPは送金と結合集計を別の単位として併記する。集計1回と送金1回を足した単一TPSには、仕事量が異なるため意味が乏しい。accounts全体の残高は保存され、salesの各account_idの出現回数は同じなので、集計側でも総和の保存を確認できる。

### 更新処理の追測

16接続の通常更新とINSERT＋DELETEを、新しい3製品インスタンスで、ウォームアップ3秒＋計測10秒、各3試行で追測した。accountsは同じ10,000行、salesは100万行、バイナリ・CPU・永続化設定は同じである。データベースの更新履歴も新しくなるため、3秒と10秒の違いだけを隔離したA/B試験ではない。

| 処理 | 接続数 | tinylamb 成功TPS [最小–最大]; 失敗率 | PostgreSQL 成功TPS [最小–最大]; 失敗率 | MySQL 成功TPS [最小–最大]; 失敗率 |
| --- | --- | --- | --- | --- |
| update | 16 | 467.2 [462.2–4,044.0]; 0.00% | 24,880.4 [7,599.3–64,742.9]; 0.00% | 7,709.7 [7,602.9–22,439.7]; 0.00% |
| insert_delete | 16 | 1.1 [0.7–1.2]; 88.70% | 47,997.7 [7,654.4–63,089.7]; 0.00% | 9,988.3 [7,559.4–38,210.0]; 0.00% |

通常更新のばらつきは延長後も残った。tinylambは462～4,044 txn/s、PostgreSQLは7,599～64,743、MySQLは7,603～22,440となった。短い測定窓だけが揺れの原因とは言えない。同期I/O、checkpointやバックグラウンド処理、ホスト負荷を計測しなければ原因は確定できない。

INSERT＋DELETEのtinylambは中央値1.1 txn/s、失敗率88.70%で、窓を延ばしても問題が再現した。計測中の中断は3試行で306件、窓内成功は30件だった。PostgreSQL／MySQLは中断0件である。tinylambの成功時p99の試行中央値は約2,755 msだったが、成功サンプルが少ないため、一般的な遅延分布の推定としては使わない。

追測のOLTP 18試行もすべて最終状態の不変条件を満たした。付随して再実行した共通OLAP 54条件も全件一致した。追測の[生データ](../benchmark/comparison/results/2026-10-28-long/results.jsonl)、[環境](../benchmark/comparison/results/2026-10-28-long/environment.json)、[成功時遅延](../benchmark/comparison/results/2026-10-28-long/analysis/oltp_latency.md)を別に保存し、本測定の表と混ぜていない。

## 正確性と適用範囲

本測定は2026年10月28日21:11～21:34 JSTに実行した。共通OLAP 162件は全件一致し、TPC-H由来66件のうち65件が一致、tinylambのQ6の1件が不一致だった。OLTP／HTAP 288試行はすべて最終行数・残高総和・churnの不変条件を満たした。

計測窓内に開始した要求の中断はtinylamb 265件、PostgreSQL／MySQL 0件。ウォームアップ中の中断はtinylamb 139件だった。ドライバは中断も失敗として検知するため、本測定の終了コードは1で、`failures.json`の26件は結果不一致1件と、中断を含む25試行から成る。25試行のうち2試行はウォームアップ中だけの中断だった。これは26件すべてが誤ったデータを返したという意味ではない。

結果の正しさは、PostgreSQLを参照とした全行比較と、独立した総和等の不変条件で確認した。参照DBとの一致は、すべてのSQL意味論や分離異常が正しいことの証明ではない。また、残高総和が保存されても、個々の行の更新履歴やすべての直列化異常を検証したことにはならない。

今回の比較で明らかになったSQL機能上の差は`CREATE INDEX`で、tinylambのフロントエンドは`unsupported statement CreateIndexStatement`を返す。ストレージ層に索引が存在しないという意味ではなく、公開SQL経路からの作成が使えないという意味である。共通測定に追加索引を使わなかった理由でもある。PostgreSQL／MySQLで適切な二次索引を追加した実運用の性能は、この表より良くなる可能性がある。

対象はキャッシュを使用できるデータ量である。ロード後の最初の実行を保存しているが、OSのページキャッシュは既に温まっており、コールドI/O値ではない。DB全体のメモリ超過、意図的なspill量、CPU時間／RSS／I/O量、長時間の定常負荷、レプリケーション、障害復旧、prepared protocolの最高性能は測定していない。MySQL／PostgreSQLのコンテナとtinylambネイティブ実行の梱包差、ドライバ差、ホスト上の他プロセスも残る。

PostgreSQLはANALYZE時の標本抽出などでプランが変化し得るため、再実行で同じ時間やプランになる保証はない。[PostgreSQLのプラン設定の説明](https://www.postgresql.org/docs/18/runtime-config-query.html)も、統計やプラン選択が性能に影響することを説明している。今回のEXPLAINは各測定の後に採取したもので、全試行の実行時プランや実測カウンタではない。

## 改善の優先順位

後続の実装状況は[TODO](../TODO.md)に記録する。以下の優先順位は本測定から
導いたもので、本測定の数値は修正前の記録として保存している。

1. **Q6の結果不一致を先に扱う。** 性能向上の前提は正しい結果である。保存した入力hash、SQL、全結果、プランを再現材料にし、型・フィルタ・統計・並列走査のどこで差が生じるかを切り分ける。
2. **INSERT＋DELETEの競合とサーバの書き込み経路を優先して調べる。** 異なるキーでも成功率が低下し、10秒窓でも再現する。行のwrite intent、索引の構造変更、取り消し用の予約・待機のどこで時間を使うかを計測する。
3. **サーバの書き込み実行とcommit待機を測り分ける。** 単一イベントループ上の仕事を分離し、同じワークロードで成功TPS、競合率、p99がどう変わるかを見る。永続化待機を無効化した値を同期commitの改善として扱わない。
4. **結合の構築側と中間データ量を改善する。** 100万行と1万行の結合で、大きい側を構築するプランを重点的に調査する。64／256 MiB等の予算変更と実測spill量を併せて評価する。
5. **単純COUNT／SUMとTop Nを独立して最適化する。** グループ集計の良さは確認できたが、全件集計とTop Nにも同じ改善が効くとは限らない。演算・走査・結果転送のCPU時間を分けて測る。
6. **SQL経由の索引作成と比較条件を拡張する。** 非キー検索やTPC-Hに追加索引を適用できる条件を作り、索引なしの強さと実用構成の強さを区別する。次段階は長時間OLTP、キャッシュを超えるデータ、TPC-C／TPC-DSの共通実装である。

## 再現方法と証跡

```bash
/var/tmp/tinylamb-comparison-venv/bin/python benchmark/comparison/run.py \
  --output benchmark/comparison/results/new-run \
  --sizes 10000 100000 1000000 --clients 1 4 16 \
  --seconds 3 --warmup 1 --trials 3 --repeats 5 --timeout 30 --tpch-scale 0.01

# グラフ生成のみmatplotlibが必要
/var/tmp/tinylamb-comparison-venv/bin/pip install matplotlib==3.11.2
/var/tmp/tinylamb-comparison-venv/bin/python benchmark/comparison/analyze.py \
  benchmark/comparison/results/2026-10-28
```

パッケージ、ビルド、長時間プロファイル等の説明は[ベンチマークREADME](../benchmark/comparison/README.md)にある。製品バイナリを揃える場合は、`environment.json`内のイメージdigestを指定する。tinylambは測定時点の未コミット差分も必要になる。

- [実行環境とバージョン](../benchmark/comparison/results/2026-10-28/environment.json)
- [ビルドフラグ](../benchmark/comparison/results/2026-10-28/build-flags.txt)、[コンテナのボリュームとCPU配分](../benchmark/comparison/results/2026-10-28/storage-layout.json)
- [試行単位の結果JSONL](../benchmark/comparison/results/2026-10-28/results.jsonl)、[一覧CSV](../benchmark/comparison/results/2026-10-28/summary.csv)
- [OLTP全32条件の成功TPS・失敗率](../benchmark/comparison/results/2026-10-28/analysis/oltp_tps.md)、[成功時p95・p99](../benchmark/comparison/results/2026-10-28/analysis/oltp_latency.md)
- [OLAP 1万行](../benchmark/comparison/results/2026-10-28/analysis/micro_10000.md)、[10万行](../benchmark/comparison/results/2026-10-28/analysis/micro_100000.md)、[100万行](../benchmark/comparison/results/2026-10-28/analysis/micro_1000000.md)
- [SQL結果とプラン](../benchmark/comparison/results/2026-10-28/query-details/)、[エラー・不一致一覧](../benchmark/comparison/results/2026-10-28/failures.json)
- [測定時のtrackedファイルの差分](../benchmark/comparison/results/2026-10-28/tracked-changes.patch)、[入力データのhash](../benchmark/comparison/results/2026-10-28/tpch-data-sha256.json)

検証は、ベンチマーク／集計処理の単体テスト11件、`scripts/check_layering.py`、差分の空白検査を実施して通過した。図4点はPNGでも生成し、ラベル・単位・凡例と不一致の表示を目視確認した。測定用サーバとコンテナはすべて停止し、実験用DBデータは削除した。

## 修正後の確認: Q6（2026-10-28）

Q6の不一致は修正した。ANALYZE後に選択される並列走査で、DOUBLEの
`l_quantity < 25`をraw整数peekに変換し、浮動小数点の格納バイトを整数として
比較していた。`plan/implementation_rules.cpp`の重複したpeek生成を、
列型を検査する既存の`BuildIntegerPeeks`へ統一した。

8192行の回帰テストは修正前に3回ともNULL、修正後は初回・キャッシュ再利用とも
期待値32776となり、ParallelScanを選択することも確認した。別にoptimizerの
回帰で、INT64のpeekを残し、DOUBLEのpeekを生成しないことを検証した。
ベンチマークのQ6は日付演算をリテラルへ畳んでおり、テストもこのSQL形状を使う。

SF 0.01を3製品で再実行し、Q6は`1787210.8440999957`相当の値へ一致した。
TPC-H全22クエリ×3製品の66条件と共通OLAP 54条件も全件一致し、終了コード0。
この再実行の共通OLAPは100行の検証用であり、本測定の性能表とは混ぜない。
[生データ](../benchmark/comparison/results/2026-10-28-q6-fixed/results.jsonl)と
[環境・設定](../benchmark/comparison/results/2026-10-28-q6-fixed/environment.json)
を別に保存した。

Q6修正時点の全体ctestは4061件中失敗0（3件skip）、式差分テスト16件通過。
SQL oracle fuzzerは180秒指定で181秒・270入力を実行し、不一致・クラッシュなし。
layering検査も通過した。有限の回帰・fuzz検証であり、SQL全体の正しさの証明ではない。

## 修正後の確認: 異なるキーのINSERT＋DELETE

二つの原因を分離した。第一にNoWait設定でも索引undo予約の事前drainは250ms
待ち、I/Oスレッドを止めていた。予約drainにもNoWaitを適用した。領域の予約と
各ページのadmission検査はそのままで、領域不足を無視する修正ではない。
第二に別キーの物理削除で索引をstaleと判定すると、DMLも全表走査へ退避し、
無関係な行をロックしていた。自分が既にintentを持つ完全uniqueキーがread gate下で
存在する場合だけ点検索を維持する。通常SELECTの過去版へのfallbackは維持した。
統計が空の表でキャッシュされるDML全走査にも、訪問行をロックする費用を加えた。

16セッションで全員のINSERT後にDELETEを交互実行し、commit／rollback後の空状態、
未commit削除のSELECTとabortによる復元を回帰テストで確認した。
予約drainの回帰は、即時の競合判定後にも予約128 byteが保持され、所有者のcommit後に
解放されることを確認する。

sales 1万行・accounts 1万行の新規インスタンスで、ウォームアップ3秒＋計測10秒、
各3試行を実施した。tinylambの成功TPS中央値は1接続466.7、4接続453.2、
16接続435.5。3製品×3接続条件×3試行すべて中断0で、最終状態も一致した。
これはsales 100万行の原測定と更新履歴が異なる追測であり、直接の速度比にはしない。
一律待機と異なるキーの中断は解消したが、同期commitを単一スレッドで待つ制約は残る。
[生データ](../benchmark/comparison/results/2026-10-28-churn-fixed/results.jsonl)を保存した。

## 修正後の確認: 書き込みワーカ

通常のINSERT／UPDATE／DELETEとBEGIN／COMMIT／ROLLBACKを、読み取りと共有する
ワーカプールへ移した。1接続につき1メッセージだけ実行し、TransactionContextの
所有権をタスクへ渡し、完了時にセッションへ返す。ソケットと送受信バッファは
I/Oループが所有する。切断時の未完了blockのabort、終了時のdrain、複数のQuery
メッセージをまとめて送った場合の順序を回帰テストで確認した。
同期commitの設定は維持している。

TEMP／UDFのthread-local状態を使うセッションとDDLは従来のスレッドに残る。
またbounded poolが待機者だけで埋まらないよう、write intentと索引予約はNoWaitを
維持する。catalog変更は他のtransaction blockやworkerの実行・完了・後始末が
残っている間SQLSTATE 55006を返す。CREATE INDEXのonline追従機構は実装していない。

sales／accounts各1万行、ウォームアップ2秒＋計測5秒、各3試行のtinylamb中央値:

| 負荷 | 1接続 TPS | 4接続 TPS | 16接続 TPS | 16接続の窓内要求失敗率 |
| --- | ---: | ---: | ---: | ---: |
| update | 268.8 | 566.4 | 1115.2 | 0.004% |
| transfer | 264.8 | 561.0 | 1217.8 | 0.155% |
| read_write | 248.2 | 527.2 | 15275.4 | 0.105% |
| insert_delete | 447.6 | 959.0 | 2124.4 | 0.000% |

4負荷×3接続数×3試行×3製品の108試行すべてで最終状態の不変条件を満たした。
同一キーを偶然選んだ要求の競合は残り、中断を含む13測定をドライバは失敗として
記録した。誤ったSQL結果や不変条件違反を検出したという意味ではない。
read_writeの16接続がtransferより大きい値になったことも、スレッド数だけでは
説明できない。同じ実行内でも同期I/Oのばらつきが大きく、条件の実行順と履歴が
異なるため、この表から処理内容の優劣や安定した倍率を決めない。
[全製品のTPSと中断率](../benchmark/comparison/results/2026-10-28-session-workers/analysis/oltp_tps.md)、
[生データ](../benchmark/comparison/results/2026-10-28-session-workers/results.jsonl)を参照。

## 修正後の確認: 結合・scalar集計・Top N

relational IRのinner hash joinは、構築側の選択をHybrid判定より前に行うように
変更した。probe入力がspillしたことだけを理由に再分割せず、小さい構築側が
常駐できる場合はprobeのファイルを順に読む。通常列、複合INT、文字列キーの
回帰でNULL除外と元の左右の列順を確認した。outer joinは既存のNULL拡張経路を保つ。

100万行の結合集計は64／256 MiBの両方で、実行カウンタがbuild 1万行、
hash索引の見積課金640,000 byteを示した。64 MiBではprobe 100万行をspillから読み、
入力・結合結果を含めrelation_spills=2。格納7列すべてではなく、走査時の復元は
2,020,000／7,040,000値だった。64 MiBの実行例はscan約135 ms、join約262 ms、
project／aggregate側約424 msで、結合後100万行の生成・集計にも費用が残る。
EXPLAINの推定表示だけでは構築側やspillの発生を確定できないため、追測では
EXPLAIN ANALYZEも別に保存した。

| 100万行、tinylamb単体の診断中央値 | 64 MiB | 256 MiB |
| --- | ---: | ---: |
| join_group | 869.8 ms | 775.1 ms |
| left_join | 590.3 ms | 542.2 ms |

[64 MiB](../benchmark/comparison/results/2026-10-28-join-64/results.jsonl)／
[256 MiB](../benchmark/comparison/results/2026-10-28-join-256/results.jsonl)の結果は、
保存済みのPostgreSQL参照結果と一致した。構築側を小さくしても、spill下の集計融合は
まだ働かず、大きい中間結果を作る。この部分が次の性能改善余地である。

scalar集計は直接列のprojectionをscanまで渡し、COUNTと単純INT列集計に不要な
16 worker間のbatch同期を省いた。SUMの走査は必要な2／7列だけを復元する。
DOUBLEの集計には既存の並列merge順序を残した。Top Nは直接列キーをbatchから読み、
heapに採用される行だけpayloadを復元する。NULL・空入力、同値キーの安定性、
OFFSETと行位置・payloadの保持を回帰で確認した。WITH TIESは既存の経路を保つ。

| 100万行、tinylamb単体の診断中央値 | 変更前 | 変更後 |
| --- | ---: | ---: |
| COUNT | 33.7 ms | 25.8 ms |
| SUM 2列 | 55.0 ms | 45.4 ms |
| Top 100 | 90.4 ms | 58.8 ms |

[変更前](../benchmark/comparison/results/2026-10-28-scalar-before/results.jsonl)と
[変更後](../benchmark/comparison/results/2026-10-28-scalar-final/results.jsonl)は
同じfixtureを使う。1万／10万／100万行の9条件は全件参照結果と一致した。
[照合記録](../benchmark/comparison/results/profile-correctness.json)では、PostgreSQLの
DECIMALがJSONで文字列保存される点を戻して数値比較している。
これら単体診断は他の検証やホスト負荷と重なり、時刻も異なる。
方向性を確認する値であり、統計的に隔離した速度比や3製品の順位としては扱わない。

## 修正後の確認: SQL索引と索引ありの比較

`CREATE [UNIQUE] INDEX [IF NOT EXISTS] ...`をGoogleSQL ASTから既存Catalog／Table APIへ
接続した。通常の昇順列キーとSTORINGを扱い、式索引・DESC・SEARCH／VECTOR・
OPTIONS・OR REPLACEは明示的に拒否する。索引名の存在判定は表内で行う。
作成失敗、重複・不明列、abort後のcatalog、作成後のDML、再オープンを回帰で確認した。

索引あり測定では`lineitem(l_orderkey)`の構築が失敗し、追加でB+Treeの境界不具合を
発見した。既存行の更新が、圧縮後の連続領域の確認で余分なslotを数えていた。
容量の事前検査を通ってから圧縮で旧slotを空にし、その後失敗するため、呼び出し側が
成功と扱ったのにキーが消え、次のsplitでDuplicatesになっていた。
判定を既存slot数に合わせた。増大する値リストの回帰は修正前にキー675の80 byte更新で
失敗し、修正後は全更新と再オープン後の読取が通る。元の60,175行でも構築が成功した。
途中で失敗した`2026-10-28-indexed*diagnostic*`等は完了測定には含めない。

全製品に同じ二次索引を作った新規実行では、共通OLAP 5クエリ×3製品、
TPC-H全22クエリ×3製品、非キー検索1／16接続×3製品の全件が一致し、中断0。
3製品ともSQL CREATE INDEX機能検査も成功した。

| 索引ありの非キー検索、各1試行 | tinylamb | PostgreSQL | MySQL |
| --- | ---: | ---: | ---: |
| 1接続 query/s | 4946.3 | 41195.7 | 13899.7 |
| 16接続 query/s | 32201.7 | 245974.7 | 57713.7 |
| TPC-H由来Q20 ms、反復3回中央値 | 1.331 | 0.568 | 1.167 |

索引を加えるとPostgreSQL／MySQLも大きく変わる。特に原測定のQ20のような
索引なしの高コストな問い合わせでtinylambが勝っても、索引ありの順位を意味しない。
今回の索引ありTPC-H 22条件でtinylambが速かったのはPostgreSQL比7件、MySQL比12件。
この条件には小数SF、固定パラメータ、短い反復、ホスト負荷の制約もある。
[SQLと設定](../benchmark/comparison/results/2026-10-28-indexed-fixed/environment.json)、
[全結果](../benchmark/comparison/results/2026-10-28-indexed-fixed/results.jsonl)、
[TPC-H一覧](../benchmark/comparison/results/2026-10-28-indexed-fixed/analysis/tpch.md)を保存した。

## 最終確認: 追加索引なしの共通OLAP

すべての修正後に、元の追加索引なし・512 MiB buffer／tinylamb 64 MiB予算へ戻し、
1万／10万／100万行、全18クエリ、3製品で再実行した。初回を分けた反復3回の中央値を
使い、162条件すべてで結果が一致し、実行エラーはなかった。

| 100万行、中央値 ms | tinylamb | PostgreSQL | MySQL |
| --- | ---: | ---: | ---: |
| COUNT | 25.600 | 5.890 | 6.928 |
| SUM 2列 | 45.601 | 10.632 | 147.571 |
| 100グループ集計 | 15.083 | 20.138 | 236.544 |
| Top 100 | 57.899 | 14.838 | 153.414 |
| 結合集計 | 879.522 | 30.567 | 728.736 |
| 外部結合集計 | 596.072 | 31.778 | 164.386 |

現在も100グループの集計は強く、COUNT／Top Nと結合にはPostgreSQLとの差が残る。
小さい構築側を使えるようになっても、結合集計は中間行の生成と後段集計が重い。
TODOの完了は、対象の不具合・経路変更・回帰・追測の完了を示し、性能差をすべて
解消したという意味ではない。時刻・キャッシュやメンテナンス履歴、反復回数も
原測定と異なるため、原測定からの速度比は厳密なA/B値ではない。
[全スケールの結果](../benchmark/comparison/results/2026-10-28-final-unindexed/results.jsonl)、
[100万行の全18クエリ](../benchmark/comparison/results/2026-10-28-final-unindexed/analysis/micro_1000000.md)、
[環境](../benchmark/comparison/results/2026-10-28-final-unindexed/environment.json)を保存した。

最終検証はctest 4,071項目中3 skip、残りは失敗分の再実行を含め通過した。
追加した再オープン回帰のテスト側にあったWAL replay手順の漏れも修正し、
B+Tree全71テストを通した。式差分16件、SQL oracle 180秒指定・181秒256入力、
layeringと変更C++24ファイルのformatも通過した。
[検証ログ](../benchmark/comparison/results/2026-10-28-validation/)に全体実行と
失敗分再実行を分けて保存した。利用者が元から変更していた22ファイルは
原測定時の差分と完全一致を確認した。測定用サーバとコンテナは停止済みである。
