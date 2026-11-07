# 3製品のフル再測定と性能改善（2026-10-29・第2巡）

追測: [第3巡のEXISTS改善](exists_performance_2026-10-29.md)で100万行の退行と1万行の余裕を改善した。以下は第2巡時点のフル測定値として保持する。

変更前後の各版を固定して追加索引なし／ありを測り直し、PostgreSQL・MySQLそれぞれに対する456指標から10倍以上の差を抽出した。変更前はPostgreSQL 0件、MySQL 2件、変更後はPostgreSQL **0件**、MySQL **0件**だった。

変更前後それぞれでOLAP 474測定が3製品の結果と一致し、OLTP／HTAP 594試行が最終不変条件と返却値検証を満たした。変更後の2構成は、同一バイナリを固定して全条件を測った。

[抽出した条件と両基準の判定](../benchmark/comparison/results/2026-10-29-full2-comparison/targets.md)、[全912比較のJSON](../benchmark/comparison/results/2026-10-29-full2-comparison/comparison.json)、[正しさの監査](../benchmark/comparison/results/2026-10-29-full2-comparison/correctness-audit.json)を保存した。倍率は丸める前に判定し、時間はtiny/比較先、成功TPSは比較先/tinyとする。

## 10倍差の対象と変更後

| 構成・規模 | SQL | 旧tiny ms | 今回tiny ms | 旧MySQL ms | 今回MySQL ms | 旧比→今回比 | 旧MySQL固定比 | 両MySQL基準 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 索引あり・10,000行 | exists | 2.316 | 1.335 | 0.135 | 0.241 | 17.17× → 5.54× | 9.90× | 通過 |
| 索引あり・1,000,000行 | distinct | 64.203 | 23.688 | 4.874 | 4.855 | 13.17× → 4.88× | 4.86× | 通過 |

今回の基準値は第2巡の変更前フル測定である。比較先自体が遅くなるだけで通過しないよう、変更前の比較先固定値と、変更後と同時期の比較先の両方で10倍未満を確認する。

EXISTSの旧MySQL固定比は境界に近い。局所9反復では1.097 msだったが、フル測定では1.335 msとなったため、長期に安定した余裕があるとは扱わない。比較先の期間差とサブミリ秒帯の揺れを含めて再測定できるよう全反復を保存した。

EXISTSは単一の等値相関と安全な内側比較を持ち、内側の推定結果が外側入力の1/4以下の場合に、一度だけ構築した整数／DATEキー集合で外側を照合する。NULLを含むNOT EXISTSはNOT INと異なるため、内側NULLを無視し、外側NULLはNOT EXISTSで保持する。複雑な相関、異なる型・符号、集約・LIMIT等は従来の経路を使う。

小さな関係の走査は統計上8,192行/ワーカを目安に並列度を抑え、1万行の読み込みごとに多数のスレッドを起動する費用を減らした。統計はスケジューリングにだけ使い、走査行数を打ち切る根拠にはしない。

実測のEXPLAIN ANALYZEでは、1万行EXISTSのcorrelated_index_probesが10,000から0へ減り、scan_rowsは20,000のままだった。DISTINCTは16ワーカの集約を保ち、索引走査にdistinct-keysが付くことを確認した。

COUNT(DISTINCT key)は、非uniqueの単一キーだけを読むcovering scanで、可視な代表行を見つけた後に同じキーの残りの位置を飛ばす。不可視候補は行ごとに確認し、古いsnapshotのfallback、NULL、残差述語、INCLUDE、他の集計の多重度を保つ。値リストの全デコード自体は残る。

## Top 100の追加改善

局所追測では、索引あり10万行のTop 100が0.482 ms対PG 0.047 ms、10.16倍となった。SQLにLIMIT以外のリテラルがないため、プランを保存していてもテンプレート参照の対象から外れていた。

副作用や可変ParameterSlotを持たず、実テーブルの列だけを読む単純なORDER BY／LIMITプランを、同じSQLのimmutableな成果物として共有・再利用する。LIMIT/OFFSETはfingerprintに保持される。schema epoch、table incarnation、snapshotの確認と測定された物理順序のキャッシュ除外を維持し、遅くなった共有成果物は全ワーカから退役させる。

| 追加索引あり | 旧tiny ms | 今回tiny ms | 今回PG ms | PG比 |
| --- | --- | --- | --- | --- |
| 10,000行 | 0.438 | 0.283 | 0.047 | 6.04× |
| 100,000行 | 0.455 | 0.221 | 0.056 | 3.98× |
| 1,000,000行 | 0.666 | 0.191 | 0.054 | 3.56× |

Top 100の実行計画は前後とも100行で早期停止し、診断時のexecutionは約0.050 msだった。短縮は主に通常実行のプラン再利用による。

初回を除く反復での短縮であり、初回の再計画を省く変更ではない。EXPLAINは独立に再計画するため、そのplanning_msは通常のキャッシュhit時間とは分けて読む。

## 100万行の代表的なOLAP

| 追加索引あり | 旧tiny ms | 今回tiny ms | 今回PG ms | 今回MySQL ms |
| --- | --- | --- | --- | --- |
| count | 51.714 | 53.361 | 7.710 | 109.782 |
| sum | 45.521 | 45.647 | 11.126 | 149.682 |
| filter_1pct | 1.209 | 1.202 | 2.663 | 1.557 |
| group_10000 | 26.956 | 24.159 | 29.430 | 690.006 |
| distinct | 64.203 | 23.688 | 158.167 | 4.855 |
| top_100 | 0.666 | 0.191 | 0.054 | 189.332 |
| exists | 13.593 | 17.135 | 3.517 | 7.754 |
| join_group | 259.395 | 249.895 | 30.634 | 691.650 |
| left_join | 29.188 | 31.426 | 15.498 | 698.289 |
| htap_reader | 195.524 | 191.576 | 24.482 | 232.237 |

全条件が高速化したわけではない。100万行EXISTSは13.593→17.135 ms（約26%増）だった。上記の大きな後退の判定閾値には届かないが、対象の1万行と分け、交互の対照測定でコードと期間差を切り分ける課題を残す。

全OLAPとTPC-H由来22問は、各構成のanalysis表に保存した。

![追加索引あり・100万行のOLAP中央値](../benchmark/comparison/results/2026-10-29-full2-after/indexed/analysis/micro_1m.png)

## OLTP／HTAPと残差

HTAPは同じSUM(a.balance)の結合集計をreaderとして固定し、writerを3／15接続で混在させた。全要求を混ぜたp50をreaderの遅延とは扱わない。

| 構成 | writer数 | tiny reader/s | PG reader/s | MySQL reader/s | tiny writer TPS 旧→今回 | PG writer TPS 今回 |
| --- | --- | --- | --- | --- | --- | --- |
| unindexed | 3 | 4.667 | 31.667 | 1.000 | 1,054.667 → 1,083.333 | 1,452.333 |
| unindexed | 15 | 4.667 | 29.667 | 1.000 | 3,419.667 → 3,400.667 | 7,031.333 |
| indexed | 3 | 4.667 | 38.667 | 4.000 | 1,014.667 → 987.333 | 1,447.000 |
| indexed | 15 | 4.667 | 37.000 | 3.667 | 3,378.000 → 3,335.333 | 6,995.000 |

OLAPの全158条件で、変更前比1.5倍超かつ1 ms超の後退はなかった。

OLTPとHTAP writerで、成功TPS中央値が変更前より20%以上低下した条件はなかった。これより小さい変動も生データに残している。

| 構成 | tiny SQL失敗数 旧→今回 | 今回PG / MySQL失敗数 |
| --- | --- | --- |
| unindexed | 6,961 → 6,739 | 0 / 0 |
| indexed | 7,027 → 6,671 | 0 / 0 |

上表は測定窓内に開始した要求のSQL失敗数で、成功TPSには含めない。warmupの失敗数と負荷ごとの失敗率は監査JSON・analysis表へ別に保存した。保存された例はすべてNoWaitのwrite intent競合だったが、例は各workerで打ち切るため、全エラーを個別分類したとは扱わない。

## 今回残った大きい差

主な中央値・成功TPS指標のうち、不利倍率が大きい上位5件を比較先ごとに示す。HTAPの全要求TPSは役割別との重複を避けて省いた。

| 比較先 | 構成 | 負荷 | 規模／接続数 | 指標 | tiny | 比較先 | 不利倍率 |
| --- | --- | --- | --- | --- | --- | --- | --- |
| postgresql | indexed | htap | 1 | reader_tps (query/s) | 4.667 | 39.333 | 8.43× |
| postgresql | indexed | htap | 4 | reader_tps (query/s) | 4.667 | 38.667 | 8.29× |
| postgresql | indexed | join_group | 1000000 | median_ms (ms) | 249.895 | 30.634 | 8.16× |
| postgresql | indexed | htap | 16 | reader_tps (query/s) | 4.667 | 37.000 | 7.93× |
| postgresql | indexed | htap_reader | 1000000 | median_ms (ms) | 191.576 | 24.482 | 7.83× |
| mysql | indexed | filter_1pct | 10000 | median_ms (ms) | 0.232 | 0.038 | 6.15× |
| mysql | indexed | range_read | 1 | tps (query/s) | 6,496.667 | 36,246.333 | 5.58× |
| mysql | indexed | exists | 10000 | median_ms (ms) | 1.335 | 0.241 | 5.54× |
| mysql | unindexed | nonkey_read | 1 | tps (query/s) | 1,950.000 | 10,711.333 | 5.49× |
| mysql | unindexed | range_read | 1 | tps (query/s) | 6,570.000 | 35,657.000 | 5.43× |

単一接続hot_updateの今回p50はtiny 2.111 ms、PG 1.026 ms。同期WALの物理経路は今回変更していない。前巡のさらに古いPG固定値への未達を解消した、という意味ではなく、その課題は[TODO](../TODO.md)に分けて保持する。

## 検証・再現条件

- 最終ctest 4,095件、失敗0（既存skip 3件）。SQL oracleは181秒285入力、不一致0。解析・比較器31テスト、layering（既存allowlist 65件、新規0）、formatと空白検査を通過。
- 書き込みのSQL失敗は成功TPSから除外する。保存したエラー例はNoWaitのwrite intent競合で、validとエラー0は同義ではない。返却値不一致はwarmupを含め0、HTAP readerのSQLエラーも0だった。
- 8 CPU、buffer 512 MiB、query memory 64 MiB、SQLワーカ8。各製品を逐次実行し、初回＋5反復のOLAP、1秒warmup＋3秒×3試行のOLTP／HTAPを比較。初回値もOS cacheを使い得る。
- 3規模（1万／10万／100万行）、19種類の共通OLAP（HTAPと同じreaderを含む）、TPC-H由来22問SF 0.01、11種類のOLTP／HTAP・1／4／16接続。公式TPCの監査スコアではない。
- PostgreSQL 18.6／MySQL 8.4.13はdigest固定。同期commitを維持。プロトコル、メモリ設定の意味、分離レベルには製品差がある。
- 前後とも各試行前に残高とchurnをリセットし、同じインスタンスのWAL・MVCC・cache履歴を引き継ぐ。製品順は同じseedでシャッフルする。自分の別DB負荷、C++ビルド、ctest、fuzzerを計時中に並走させていない。host I/Oやbackground checkpointの期間差までは隔離していない。
- 書き込みの長時間定常性能、クラッシュ時耐久性、分離異常、cacheを超える規模、prepared protocolの性能をこの測定だけで証明するものではない。3秒窓のHTAP reader tailはサンプルが少なく、その件数も[役割別の遅延・件数表](../benchmark/comparison/results/2026-10-29-full2-comparison/htap-role-latency.md)へ記録した。

使用版は[変更前](../benchmark/comparison/results/2026-10-29-full2-before/server-source.json)と[変更後](../benchmark/comparison/results/2026-10-29-full2-after/server-source.json)で区別できる。変更後SHA-256は`876da51b…`で両構成が一致する。変更前の索引あり測定開始時には実装作業の差分も存在したため、再ビルドには各時点のworkspace差分より、共通のserver-source.patchを使う。

[最終テストログ](../benchmark/comparison/results/2026-10-29-full2-validation/ctest-final.log)、[oracleログ](../benchmark/comparison/results/2026-10-29-full2-validation/oracle.log)、[検証一覧](../benchmark/comparison/results/2026-10-29-full2-validation/validation-manifest.json)。

全表・生データ: [変更前](../benchmark/comparison/results/2026-10-29-full2-before/)、[変更後・追加索引なし](../benchmark/comparison/results/2026-10-29-full2-after/unindexed/analysis/)、[変更後・追加索引あり](../benchmark/comparison/results/2026-10-29-full2-after/indexed/analysis/)。各results.jsonl、summary.csv、query-details、environment.jsonを保持した。再現・再集計の手順は[benchmark README](../benchmark/comparison/README.md)。
