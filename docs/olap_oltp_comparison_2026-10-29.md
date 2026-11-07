# tinylamb PostgreSQL MySQLのOLAP OLTP再測定 2026年10月29日

この測定を起点とした[A–Hの実装・追測](postgresql_gap_results_2026-10-29.md)を別途記録している。
以下の数値は改善前の測定として保存する。

PostgreSQLに10倍以上劣る全指標の比較と実施順序は[指標別改善計画](postgresql_gap_plan_2026-10-29.md)にまとめた。

前回の6項目の修正を含む現在のコードで、追加索引なしと共通二次索引ありの全ベンチマークを取り直した。OLAPは456／456条件でPostgreSQL参照結果と一致し、OLTP／HTAPは576／576試行で測定後の行数・残高・churnの不変条件が成立した。競合による中断は、結果不一致と分けて記録した。

今回明確になった性能上の問題は、索引追加後の無条件集計である。tinylambの100万行COUNTは25.8 msから4316.3 msへ167.2倍、SUMは45.0 msから8950.8 msへ199.0倍に遅くなった。両方とも索引を使う計画を選び、全結果は一致している。

書き込みでは、追加索引なし・16接続の2行送金が2,018.7成功TPS、INSERT＋DELETEが2,183.7成功TPSだった。後者の全接続数・全試行での中断数は0件。前回の不要な競合は今回の条件では再発していない。

## 測定条件

| 項目 | 条件 |
| --- | --- |
| 日時 | 2026-10-29T00:12:02+0900 ～ 2026-10-29T00:59:44+0900 |
| 対象 | tinylamb作業ツリー、PostgreSQL 18.6、MySQL 8.4.13 |
| tinylamb HEAD | `d9315a8277650d96a5f535f5b85a7a964ce86fa5`＋記録した未コミット差分 |
| サーバSHA256 | `60770620313559e14e50e5e2e391718cc02d72e5403d876aad6065c7d07e950c` |
| CPU／バッファ | 同じ論理CPU 0–7、各512 MiB。ホスト・ストレージは前回と同じ |
| クエリメモリ | tinylamb 64 MiB／query、PostgreSQL work_mem=64MB／演算子、MySQLは既定 |
| 永続化 | 同期commit有効、PostgreSQL fsync有効、MySQL binlog無効 |
| 並列実行 | tinylamb共有SQLワーカ8、PostgreSQL Gather最大4並列ワーカ、JIT有効 |
| 共通OLAP | 18クエリ × 1万／10万／100万行 × 3製品、初回を分離し、その後5回の中央値 |
| TPC-H由来 | 22クエリ × SF 0.01 × 3製品、同じDBGEN入力、binary64 |
| OLTP／HTAP | 1／4／16接続、1秒warmup＋3秒測定、各3試行。HTAPは4／16接続 |
| 追加索引 | なし／ありを別インスタンスで順に測定。ありは全製品に同じキーを作成 |
| 診断 | 通常計時後にEXPLAINを保存し、tinylambはEXPLAIN ANALYZEも保存 |

DBイメージは前回保存したdigestに固定した。両条件で同一のtinylambバイナリを使い、測定中にコードを変更していない。実行順と要求生成のseedも前回の20261028を維持した。ANALYZEの統計サンプリング乱数は固定していない。接続・データ生成・正しさ判定・索引一覧は[ベンチマーク仕様](../benchmark/comparison/README.md)に従う。各製品のプロトコル／ドライバ費用も含むクライアント側の測定で、メモリ設定は総RSSの同一上限を意味しない。

3秒窓は短期の応答性能を測る。全試行で同じインスタンスを使うため、キャッシュ・WAL・MVCC・保守処理の履歴と同期I/Oの揺らぎを含む。リンク先のOLTP全条件表には試行間の最小・最大を併記した。TPC-H由来の値は公式TPCスコアではない。

## データ投入と索引構築

500行単位の共通INSERT経路。ロードと追加索引構築を分け、単位は秒。ロード値は既存表のDROP／新規DDL、SQL生成・送信・commitを含み、ANALYZEは含まない。製品固有の高速ローダーは使っていない。

| 追加索引 | 対象 | 製品 | ロード秒 | 追加索引構築秒 |
| --- | --- | --- | --- | --- |
| なし | 共通100万行 | tinylamb | 3.296 | 0.000 |
| なし | 共通100万行 | PostgreSQL | 5.314 | 0.000 |
| なし | 共通100万行 | MySQL | 3.682 | 0.000 |
| なし | TPC-H SF 0.01 | tinylamb | 9.473 | 0.000 |
| なし | TPC-H SF 0.01 | PostgreSQL | 1.221 | 0.000 |
| なし | TPC-H SF 0.01 | MySQL | 1.670 | 0.000 |
| あり | 共通100万行 | tinylamb | 8.120 | 13.675 |
| あり | 共通100万行 | PostgreSQL | 5.339 | 0.496 |
| あり | 共通100万行 | MySQL | 3.653 | 1.469 |
| あり | TPC-H SF 0.01 | tinylamb | 9.255 | 1.554 |
| あり | TPC-H SF 0.01 | PostgreSQL | 1.072 | 0.044 |
| あり | TPC-H SF 0.01 | MySQL | 0.781 | 0.181 |

## 共通OLAPの結果

100万行、単位ms。小さいほど速い。追加索引なしでもaccounts／churnの主キーは存在する。

### 追加索引なし

| クエリ | tinylamb ms | PostgreSQL ms | MySQL ms |
| --- | --- | --- | --- |
| count | 25.816 | 5.674 | 6.587 |
| sum | 44.984 | 10.784 | 150.146 |
| filter_1pct | 10.648 | 8.438 | 131.835 |
| filter_10pct | 13.290 | 8.668 | 134.783 |
| expression | 40.535 | 11.127 | 156.356 |
| group_100 | 17.022 | 19.828 | 239.248 |
| group_10000 | 20.974 | 28.430 | 251.372 |
| distinct | 46.162 | 70.307 | 212.048 |
| top_100 | 57.212 | 15.130 | 152.016 |
| join | 11.743 | 12.223 | 594.578 |
| join_group | 888.471 | 30.228 | 730.581 |
| left_join | 593.713 | 32.374 | 158.234 |
| exists | 174.311 | 8.475 | 136.005 |
| scalar_subquery | 111.142 | 24.443 | 271.592 |
| cte | 16.971 | 19.333 | 237.089 |
| having | 19.227 | 19.410 | 262.276 |
| union_all | 9.138 | 30.019 | 254.455 |
| window | 2.975 | 6.937 | 141.282 |

tinylambはPostgreSQLより8／18クエリ、MySQLより14／18クエリで短い中央値だった。全クエリを一つの処理能力に換算せず、負荷の形ごとに評価する。

### 共通二次索引あり

| クエリ | tinylamb ms | PostgreSQL ms | MySQL ms |
| --- | --- | --- | --- |
| count | 4316.350 | 6.794 | 109.668 |
| sum | 8950.838 | 10.604 | 147.932 |
| filter_1pct | 45.406 | 0.257 | 1.567 |
| filter_10pct | 13.080 | 2.340 | 16.256 |
| expression | 40.259 | 10.977 | 153.562 |
| group_100 | 15.010 | 13.413 | 145.697 |
| group_10000 | 23.614 | 27.770 | 655.431 |
| distinct | 4844.186 | 23.526 | 4.729 |
| top_100 | 574.413 | 0.072 | 186.115 |
| join | 11.309 | 12.053 | 65.371 |
| join_group | 884.940 | 30.550 | 690.545 |
| left_join | 583.665 | 15.838 | 673.313 |
| exists | 171.802 | 3.443 | 7.643 |
| scalar_subquery | 113.441 | 20.284 | 191.863 |
| cte | 15.655 | 12.178 | 134.971 |
| having | 16.336 | 12.198 | 145.064 |
| union_all | 9.022 | 3.667 | 25.159 |
| window | 2.862 | 7.116 | 139.846 |

tinylambはPostgreSQLより3／18クエリ、MySQLより11／18クエリで短い中央値だった。全クエリを一つの処理能力に換算せず、負荷の形ごとに評価する。

![100万行のOLAP比較](../benchmark/comparison/results/2026-10-29/olap-comparison.svg)

追加索引なしでは、100／10,000グループ集計、DISTINCT、CTE、HAVING、UNION ALL、選択条件付きウィンドウ関数などでtinylambが比較的良い。単純COUNT／SUM、結合集計、外部結合、EXISTS／スカラー副問い合わせではPostgreSQLとの差が残る。ウィンドウ関数は`id < 1000`で絞ったSQLであり、100万行全体をウィンドウ処理した成績ではない。

### データ量による変化

tinylambのウォーム中央値、単位ms。

| 処理 | 追加索引 | 1万行 | 10万行 | 100万行 |
| --- | --- | --- | --- | --- |
| count | なし | 0.610 | 3.185 | 25.816 |
| count | あり | 4.360 | 74.878 | 4316.350 |
| sum | なし | 0.630 | 4.679 | 44.984 |
| sum | あり | 9.706 | 235.215 | 8950.838 |
| distinct | なし | 1.638 | 6.656 | 46.162 |
| distinct | あり | 5.367 | 79.016 | 4844.186 |
| top_100 | なし | 1.207 | 6.250 | 57.212 |
| top_100 | あり | 6.065 | 56.729 | 574.413 |

全3製品・全18クエリのサイズ別表は、[追加索引なし1万](../benchmark/comparison/results/2026-10-29/unindexed/analysis/micro_10000.md)、[10万](../benchmark/comparison/results/2026-10-29/unindexed/analysis/micro_100000.md)、[100万](../benchmark/comparison/results/2026-10-29/unindexed/analysis/micro_1000000.md)、[索引あり1万](../benchmark/comparison/results/2026-10-29/indexed/analysis/micro_10000.md)、[10万](../benchmark/comparison/results/2026-10-29/indexed/analysis/micro_100000.md)、[100万](../benchmark/comparison/results/2026-10-29/indexed/analysis/micro_1000000.md)に保存した。

### 索引追加で走査が遅くなる経路

| tinylamb 100万行 | 追加索引なし ms | 二次索引あり ms | あり／なし |
| --- | --- | --- | --- |
| count | 25.816 | 4316.350 | 167.2× |
| sum | 44.984 | 8950.838 | 199.0× |
| distinct | 46.162 | 4844.186 | 104.9× |
| top_100 | 57.212 | 574.413 | 10.0× |

COUNTは`bench_sales_0(account_id)`の`IndexOnlyScan`、SUMは同じ索引を使う`IndexSkipScan`→`ParallelIndexScan`→`ParallelAggregationExecutor`を選ぶ。EXPLAIN ANALYZEでは両者ともバッファミス0だった。COUNTで約301万、SUMで約803万のバッファhitを記録しており、この試行の遅さはストレージの読み出しミスだけでは説明できない。

実装では、非unique索引の`operator++`と`UpdateIteratorState`が同じキーの値リストを行ごとに丸ごとデコードする（[index_scan_iterator.cpp](../index/index_scan_iterator.cpp#L210)、[前進処理](../index/index_scan_iterator.cpp#L301)）。リストにd件あれば、デコードの仕事量がキーごとにO(d²)へ増え得る。今回のaccount_idは最大1万種類で、100万行では1キーあたり100件ある。これはコードから確認できる改善対象で、実行時間に占める比率はプロファイラでの切り分けが必要である。

次は、重複値リストをキーの切り替わりまで保持する実行器の改善と、選択率のない索引全走査／skip scanをPAX全走査より安く見積もるコストモデルの検証を優先する。NULL・逆順・古いsnapshot・索引変更時の整合性を保ち、COUNT／SUMを3スケールで回帰させる必要がある。今回の再測定中にはDB実装を変更していない。

計画の原本: [COUNT](../benchmark/comparison/results/2026-10-29/indexed/query-details/micro-1000000-count-tinylamb.json)、[SUM](../benchmark/comparison/results/2026-10-29/indexed/query-details/micro-1000000-sum-tinylamb.json)。

Top 100にも別の課題がある。tinylambは`IndexOnlyScan`の100万行を`TopN`へ渡し、100行を返した。PostgreSQLの計画は逆順の`Index Only Scan`→`Incremental Sort`→`Limit`である。SQLは`amount DESC, id ASC`であり、索引`(amount ASC, id ASC)`を単に逆順に読むだけでは第2キーの順序を満たさない。先頭キーの順序を使い、同値グループだけを整列してLIMITへ渡す経路の差が改善対象になる。PostgreSQL側の行数カウンタは今回の通常EXPLAINには含まれない。

Top 100の計画: [tinylambの実測カウンタ](../benchmark/comparison/results/2026-10-29/indexed/query-details/micro-1000000-top_100-tinylamb.json)、[PostgreSQL](../benchmark/comparison/results/2026-10-29/indexed/query-details/micro-1000000-top_100-postgresql.json)。

### 修正前からの変化

| tinylamb 100万行 | 10月28日修正前 ms | 10月29日追加索引なし ms | 修正前／今回 |
| --- | --- | --- | --- |
| count | 33.327 | 25.816 | 1.29× |
| sum | 53.809 | 44.984 | 1.20× |
| top_100 | 89.922 | 57.212 | 1.57× |
| join_group | 980.347 | 888.471 | 1.10× |
| left_join | 581.991 | 593.713 | 0.98× |

この比率は別時刻の実測比較で、変更ごとの因果を分離したA/B試験ではない。前回の修正後追測も[10月28日のレポート](olap_oltp_comparison_2026-10-28.md)に保存している。

## TPC-H由来の結果

SF 0.01、単位ms。Q6も含む全22クエリを検査した。索引の有無で実行計画が変わるため、条件ごとに比較する。

### 追加索引なし

| クエリ | tinylamb ms | PostgreSQL ms | MySQL ms |
| --- | --- | --- | --- |
| Q01 | 6.963 | 5.960 | 29.250 |
| Q02 | 0.999 | 2.700 | 124.712 |
| Q03 | 1.730 | 3.292 | 14.026 |
| Q04 | 1.735 | 3.363 | 15.503 |
| Q05 | 2.883 | 2.701 | 17.429 |
| Q06 | 1.395 | 2.261 | 11.298 |
| Q07 | 5.892 | 3.223 | 25.396 |
| Q08 | 3.192 | 2.819 | 1262.219 |
| Q09 | 12.167 | 6.167 | 22.837 |
| Q10 | 3.154 | 3.744 | 14.931 |
| Q11 | 1.450 | 0.625 | 2.765 |
| Q12 | 2.257 | 5.006 | 14.343 |
| Q13 | 5.400 | 3.066 | 8.379 |
| Q14 | 2.378 | 2.294 | 11.500 |
| Q15 | 5.753 | 2.193 | 10.553 |
| Q16 | 2.385 | 1.471 | 2.491 |
| Q17 | 1.196 | 168.180 | 1201.306 |
| Q18 | 8.163 | 7.614 | 29.087 |
| Q19 | 1.424 | 3.513 | 12.414 |
| Q20 | 1.425 | 9360.615 | 1166.680 |
| Q21 | 14.438 | 157.089 | 64.083 |
| Q22 | 0.993 | 1.141 | 3.489 |

tinylambの中央値が短かったのは、PostgreSQL比11／22、MySQL比22／22クエリ。

### 共通二次索引あり

| クエリ | tinylamb ms | PostgreSQL ms | MySQL ms |
| --- | --- | --- | --- |
| Q01 | 6.687 | 6.121 | 29.381 |
| Q02 | 0.956 | 0.963 | 0.531 |
| Q03 | 1.730 | 2.024 | 5.785 |
| Q04 | 1.750 | 3.030 | 1.153 |
| Q05 | 2.903 | 1.370 | 7.337 |
| Q06 | 28.968 | 0.862 | 6.031 |
| Q07 | 6.107 | 1.781 | 7.291 |
| Q08 | 3.217 | 1.568 | 2.858 |
| Q09 | 12.873 | 5.829 | 7.891 |
| Q10 | 3.219 | 3.446 | 4.756 |
| Q11 | 1.479 | 0.913 | 17.439 |
| Q12 | 2.534 | 4.899 | 13.038 |
| Q13 | 5.333 | 2.977 | 11.186 |
| Q14 | 2.272 | 0.429 | 1.340 |
| Q15 | 1.245 | 0.569 | 1.765 |
| Q16 | 2.092 | 1.439 | 1.023 |
| Q17 | 1.209 | 1.163 | 2.564 |
| Q18 | 8.429 | 7.698 | 32.951 |
| Q19 | 1.455 | 0.310 | 0.637 |
| Q20 | 1.359 | 0.582 | 1.125 |
| Q21 | 16.310 | 20.383 | 100.502 |
| Q22 | 0.996 | 1.251 | 3.087 |

tinylambの中央値が短かったのは、PostgreSQL比7／22、MySQL比13／22クエリ。

索引なしQ20のPostgreSQLは9360.6 ms、索引ありは0.582 msだった。索引なしの計画ではpartsuppのフィルタ内の相関SubPlanがlineitemのSeq Scanとなる。この条件の大きな差は、こうした具体的な計画の差として評価する必要がある。

Q20のPostgreSQL計画: [追加索引なし](../benchmark/comparison/results/2026-10-29/unindexed/query-details/tpch-0.01-Q20-postgresql.json)、[索引あり](../benchmark/comparison/results/2026-10-29/indexed/query-details/tpch-0.01-Q20-postgresql.json)。

## OLTPと混合負荷

以下は16接続、3試行の成功TPS中央値／測定窓内の失敗率。失敗要求は再試行しない。主キー検索等はquery/s、送金等はtransaction/s。HTAPの合計値には分析クエリと取引が混在するため、下に役割別の値も示す。

### 追加索引なし

| 処理 | tinylamb 成功TPS／失敗率 | PostgreSQL 成功TPS／失敗率 | MySQL 成功TPS／失敗率 |
| --- | --- | --- | --- |
| noop | 209,142.3 / 0.00% | 621,862.0 / 0.00% | 352,823.0 / 0.00% |
| point_read | 186,958.0 / 0.00% | 387,974.3 / 0.00% | 223,046.7 / 0.00% |
| nonkey_read | 3,834.0 / 0.00% | 85,471.3 / 0.00% | 71,431.0 / 0.00% |
| range_read | 48,156.7 / 0.00% | 239,674.7 / 0.00% | 163,650.7 / 0.00% |
| update | 1,943.0 / 0.01% | 21,750.0 / 0.00% | 35,397.7 / 0.00% |
| hot_update | 1,898.7 / 4.84% | 4,624.7 / 0.00% | 7,653.7 / 0.00% |
| transfer | 2,018.7 / 0.16% | 7,606.7 / 0.00% | 7,621.0 / 0.00% |
| read_write | 1,980.7 / 0.12% | 7,641.0 / 0.00% | 7,052.7 / 0.00% |
| insert_delete | 2,183.7 / 0.00% | 7,664.0 / 0.00% | 7,682.3 / 0.00% |
| rollback | 65,023.0 / 0.05% | 166,802.7 / 0.00% | 37,420.3 / 0.00% |
| htap | 1,750.7 / 0.12% | 7,037.0 / 0.00% | 6,994.7 / 0.00% |

### 共通二次索引あり

| 処理 | tinylamb 成功TPS／失敗率 | PostgreSQL 成功TPS／失敗率 | MySQL 成功TPS／失敗率 |
| --- | --- | --- | --- |
| noop | 195,430.3 / 0.00% | 630,524.0 / 0.00% | 351,746.3 / 0.00% |
| point_read | 205,931.7 / 0.00% | 369,496.7 / 0.00% | 222,260.7 / 0.00% |
| nonkey_read | 36,840.3 / 0.00% | 267,052.0 / 0.00% | 58,050.7 / 0.00% |
| range_read | 48,686.7 / 0.00% | 218,470.0 / 0.00% | 161,937.0 / 0.00% |
| update | 1,966.7 / 0.01% | 7,725.0 / 0.00% | 7,899.3 / 0.00% |
| hot_update | 1,921.3 / 5.20% | 4,629.0 / 0.00% | 7,816.7 / 0.00% |
| transfer | 2,075.3 / 0.12% | 17,161.7 / 0.00% | 13,514.0 / 0.00% |
| read_write | 15,364.3 / 0.08% | 34,271.7 / 0.00% | 7,427.7 / 0.00% |
| insert_delete | 2,179.3 / 0.00% | 20,025.7 / 0.00% | 27,990.0 / 0.00% |
| rollback | 67,016.0 / 0.06% | 163,540.7 / 0.00% | 38,476.3 / 0.00% |
| htap | 1,761.3 / 0.11% | 7,105.7 / 0.00% | 6,919.3 / 0.00% |

二次索引の効果が出たのは非キー検索である。tinylambの16接続は3,834.0から36,840.3 query/sへ9.6倍に改善した。全走査型の分析と、少数の行へ絞る検索では索引の影響が大きく異なる。

### tinylambの並列書き込みと修正前の比較

| 処理 | 今回1接続TPS | 今回4接続TPS | 今回16接続TPS | 修正前16接続TPS |
| --- | --- | --- | --- | --- |
| update | 3,801.0 | 8,822.7 | 1,943.0 | 3,994.0 |
| hot_update | 466.3 | 977.3 | 1,898.7 | 503.7 |
| transfer | 445.3 | 981.7 | 2,018.7 | 494.0 |
| read_write | 430.7 | 936.0 | 1,980.7 | 436.3 |
| insert_delete | 454.0 | 973.3 | 2,183.7 | 0.0 |

書き込みの短期測定には大きな振れがある。以下はtinylamb・16接続・3試行の最小／中央値／最大であり、接続数による性能曲線や、構成SQLの単純な費用順位をこれだけで一般化しない。長い窓と、実行順を変えた再試行で定常性能を確認する余地がある。

| 追加索引 | 処理 | 最小TPS | 中央値TPS | 最大TPS |
| --- | --- | --- | --- | --- |
| なし | update | 1,937.7 | 1,943.0 | 17,007.7 |
| なし | transfer | 2,014.0 | 2,018.7 | 2,071.3 |
| なし | read_write | 1,938.0 | 1,980.7 | 2,060.0 |
| なし | insert_delete | 2,074.0 | 2,183.7 | 2,201.0 |
| なし | htap | 1,745.7 | 1,750.7 | 11,108.7 |
| あり | update | 1,937.0 | 1,966.7 | 16,972.0 |
| あり | transfer | 2,027.7 | 2,075.3 | 2,083.3 |
| あり | read_write | 2,027.3 | 15,364.3 | 15,366.0 |
| あり | insert_delete | 2,085.7 | 2,179.3 | 16,333.0 |
| あり | htap | 1,734.7 | 1,761.3 | 13,043.0 |

INSERT＋DELETEが更新するのはchurn表で、今回の追加索引はaccounts／salesに作る。churnの索引構成は両条件で同じなので、この処理の条件間TPS差を二次索引の高速化効果とは解釈しない。インスタンスの履歴、試行直前のリセット処理、同期I/O等を含む実行時の差を分離するには、より長い測定とI/O待機の観測が必要である。

![追加索引なしのOLTPスケーリング](../benchmark/comparison/results/2026-10-29/oltp-scaling.svg)

### HTAPの役割別スループット

役割ごとに3試行の中央値を取るため、分析と取引の和が合計TPSの中央値と一致しない場合がある。

| 追加索引 | 総接続数 | 製品 | 分析query/s | 取引txn/s | 失敗率 |
| --- | --- | --- | --- | --- | --- |
| なし | 4 | tinylamb | 0.667 | 2,548.0 | 0.01% |
| なし | 4 | PostgreSQL | 32.667 | 1,439.0 | 0.00% |
| なし | 4 | MySQL | 1.000 | 1,433.3 | 0.00% |
| なし | 16 | tinylamb | 0.667 | 1,750.0 | 0.12% |
| なし | 16 | PostgreSQL | 30.333 | 7,006.3 | 0.00% |
| なし | 16 | MySQL | 0.667 | 6,994.0 | 0.00% |
| あり | 4 | tinylamb | 0.667 | 728.0 | 0.02% |
| あり | 4 | PostgreSQL | 39.000 | 1,440.3 | 0.00% |
| あり | 4 | MySQL | 4.000 | 1,405.3 | 0.00% |
| あり | 16 | tinylamb | 0.667 | 1,760.7 | 0.11% |
| あり | 16 | PostgreSQL | 37.667 | 7,068.0 | 0.00% |
| あり | 16 | MySQL | 3.667 | 6,915.7 | 0.00% |

### 成功時遅延

追加索引なし・16接続、各試行の成功サンプルから求めた分位点の中央値、単位ms。中断時の待ち時間は含まない。

| 処理 | 製品 | p50 | p95 | p99 |
| --- | --- | --- | --- | --- |
| point_read | tinylamb | 0.079 | 0.114 | 0.124 |
| point_read | PostgreSQL | 0.036 | 0.065 | 0.086 |
| point_read | MySQL | 0.064 | 0.117 | 0.146 |
| update | tinylamb | 8.142 | 8.818 | 10.071 |
| update | PostgreSQL | 0.267 | 2.239 | 2.400 |
| update | MySQL | 0.220 | 1.435 | 3.127 |
| hot_update | tinylamb | 8.085 | 9.245 | 11.091 |
| hot_update | PostgreSQL | 2.276 | 7.163 | 11.501 |
| hot_update | MySQL | 2.023 | 2.225 | 2.907 |
| transfer | tinylamb | 8.068 | 9.450 | 11.469 |
| transfer | PostgreSQL | 2.054 | 2.237 | 3.041 |
| transfer | MySQL | 2.020 | 2.209 | 3.944 |
| insert_delete | tinylamb | 7.904 | 8.888 | 11.255 |
| insert_delete | PostgreSQL | 2.051 | 2.219 | 2.829 |
| insert_delete | MySQL | 2.025 | 2.210 | 3.085 |

## 正しさと中断の内訳

| 条件 | OLAP結果一致 | OLTP／HTAP不変条件成立 | tinylamb中断数 | PostgreSQL中断数 | MySQL中断数 |
| --- | --- | --- | --- | --- | --- |
| 追加索引なし | 228／228 | 288／288 | 1473 | 0 | 0 |
| 二次索引あり | 228／228 | 288／288 | 1630 | 0 | 0 |

中断数は測定窓内開始の要求に対する件数で、warmup中の件数は別に生データへ保存する。`failures.json`は中断のあった試行も含むため、ファイルが空でないことだけで結果不一致とは判断しない。最終状態の検査は分離異常や障害復旧を網羅する検証ではない。

追加索引なしのtinylambで記録したエラー例:

- `statement execution failed: Conflicts: write intent unavailable on table accounts: conflicting writer holds the write intent`

二次索引ありのtinylambで記録したエラー例:

- `statement execution failed: Conflicts: write intent unavailable on table accounts: conflicting writer holds the write intent`

## 保存物と再現

両条件の起動引数・終了コードは[run-status.json](../benchmark/comparison/results/2026-10-29/run-status.json)、集計値は[aggregate.json](../benchmark/comparison/results/2026-10-29/aggregate.json)にある。バージョン・イメージdigest・設定・CPU・作業ツリー差分・バイナリhash、全結果行と実行計画を条件別に保存した。

条件の網羅性・重複なし・5回の反復・全結果ファイル・同一バイナリ／測定コード／TPC-H入力を検査した[audit.json](../benchmark/comparison/results/2026-10-29/audit.json)は全項目を通過した。測定用サーバとコンテナは停止・削除済み。

| 条件 | 生データ | 環境 | TPS全表 | 遅延全表 |
| --- | --- | --- | --- | --- |
| 追加索引なし | [JSONL](../benchmark/comparison/results/2026-10-29/unindexed/results.jsonl) | [設定](../benchmark/comparison/results/2026-10-29/unindexed/environment.json) | [1／4／16接続](../benchmark/comparison/results/2026-10-29/unindexed/analysis/oltp_tps.md) | [p95／p99](../benchmark/comparison/results/2026-10-29/unindexed/analysis/oltp_latency.md) |
| 二次索引あり | [JSONL](../benchmark/comparison/results/2026-10-29/indexed/results.jsonl) | [設定](../benchmark/comparison/results/2026-10-29/indexed/environment.json) | [1／4／16接続](../benchmark/comparison/results/2026-10-29/indexed/analysis/oltp_tps.md) | [p95／p99](../benchmark/comparison/results/2026-10-29/indexed/analysis/oltp_latency.md) |

再実行は`/var/tmp/tinylamb-comparison-venv/bin/python benchmark/comparison/results/2026-10-29/rerun.py`を参照し、出力先を新しい日付・名前へ変更する。既存結果は上書きしない。集計と図は同ディレクトリの`summarize.py`と`report.py`から再生成できる。
