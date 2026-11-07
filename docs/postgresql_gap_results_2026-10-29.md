# PostgreSQLとの差を縮める A–H の実施・追測レポート（2026-10-29）

A〜Hの実装・検証・再測定を実施した。旧PG値と今回PG値の**両方で不利倍率10未満**となったのは、主対象32/35件、p95／p99の補足4/6件、合計36/41件。
A〜FとHのreader目標は通過し、Gには旧PG固定基準で5指標が残った。
今回PG比だけなら全41指標が10倍未満だが、PG自身も遅くなった期間があり、それだけでGを解決済みとは扱わない。

[全41指標の旧値・今回値・両PG比](../benchmark/comparison/results/2026-10-29-gap-final-comparison/comparison.md)と
[集計JSON](../benchmark/comparison/results/2026-10-29-gap-final-comparison/comparison.json)を保存した。
元の[総合比較](olap_oltp_comparison_2026-10-29.md)と[指標別計画](postgresql_gap_plan_2026-10-29.md)の数値は保持している。

![計画別の最大不利倍率と単一接続hot_updateの試行差](../benchmark/comparison/results/2026-10-29-gap-final-comparison/comparison.png)

左は各計画内の最大不利倍率（時間はtiny/PG、throughputはPG/tiny）。破線が10倍で、各点の最大値は異なる条件の場合がある。
青は今回PG比、橙は同じ今回tinyを旧PGへ比べた値。Gの残差をPGの期間差で隠さない。
右は60秒×5試行のp50で、破線は旧PGの10倍目標。[SVG](../benchmark/comparison/results/2026-10-29-gap-final-comparison/comparison.svg)も保存した。

## 実装と判定

| 計画 | 両PG基準の通過 | 実施内容 |
| --- | --- | --- |
| A | 9/9 | 非unique value listをキー切替まで保持。guard下のB+Tree葉を所有コピーし、キーごとのpinを減らす。NDV・heapアクセス・LIMITを費用に反映し、型付き集計へ接続。 |
| B | 3/3 | 混合ASC/DESCの先頭順序を候補に残し、IncrementalSortをpeer group単位のstreamへ変更。LIMIT 100は入力101行で停止する回帰を追加。大きなpeer groupは既存Sortへspill。 |
| C | 5/5 | 内側だけで評価できる非例外の単純比較をbuild前へ押す。EXISTSの整数キー集合probeから文字列化と結果Relation生成を除く。NULL・outer domain・cache所有者を維持。 |
| D | 2/2 | resident buildとspill済みprobeを直接group accumulatorへ流し、100万行のjoin出力を作らない。ORDER BYの射影ordinal解決と、最後の結合候補での不要な頻度推定を修正。 |
| E | 2/2 | 右側だけの安全なON比較を読み込み時に適用。右側が64 MiB内に収まり既存LEFT融合へ進む。元のONも残し、WHERE・未一致NULL・COUNT(*)を区別。保持側buildの新方式は追加せず。 |
| F | 8/8 | 既存主キーの順序＋residual＋有限LIMITを候補化。短いprefixを逐次走査し、prefetchの二重走査を避ける。LIMITの必要batch行数をProjection/Selectionへ伝える。 |
| G | 3/8 | commit公開後のown／依存LSN待機をcoordinatorへ渡し、SQLワーカを解放。100µsの集約窓と証明済みLSNの境界を使う。変更のないcontextの空commitを除き、複文エラー前の部分応答にもD4を適用。旧PG固定基準に5件残る。 |
| H | 4/4 | DとGを同じSUM(a.balance)のreaderへ適用。単独OLAPと3／15 writerの混合を区別し、役割別TPS・遅延・境界外完了・MVCC履歴を保存。readerの4目標は通過。 |

NULL、逆順、MVCC snapshot、ONとWHERE、OFFSET／WITH TIES、大きなpeer group、spill、破損キーの伝播を回帰で確認した。
葉snapshotとvalue list cacheは読み取りguard下のsnapshot用であり、DMLのhead追従には使わない。

GではSQLワーカ数を増やさず、commit公開後にLSNだけを応答queueへ渡す。
必要なLSNが永続化されるまで応答全体を保持し、失敗時にCOMMIT成功を返さない。
既に耐久化済みの依存は、成功した待機から得たLSNの境界で判定し、無関係な将来commitを待たせない。
明示block内のSELECTと、複文の後半が失敗した場合の先行SELECTにも依存バリアを適用した。

## 最終OLAPの代表値

追加索引あり、microは100万行、Q06はTPC-H由来SF 0.01。いずれも初回を除く5回の中央値。

| SQL | 旧tinylamb ms | 今回tinylamb ms | 今回PG ms | 今回MySQL ms |
| --- | --- | --- | --- | --- |
| count | 4,316.350 | 51.272 | 7.607 | 109.406 |
| sum | 8,950.838 | 45.464 | 10.920 | 147.638 |
| filter_1pct | 45.406 | 1.219 | 2.664 | 1.554 |
| distinct | 4,844.186 | 63.002 | 157.225 | 4.804 |
| top_100 | 574.413 | 0.440 | 0.048 | 186.620 |
| exists | 171.802 | 15.525 | 3.481 | 7.682 |
| join_group | 884.940 | 242.283 | 30.438 | 700.284 |
| left_join | 583.665 | 28.541 | 16.181 | 722.935 |
| Q06 | 28.968 | 1.558 | 0.815 | 6.091 |

COUNTは約84倍、SUMは約197倍、Top 100は約1,305倍短縮した。
一方、今回のDISTINCTはMySQLの4.804 msが速い。tinylambも索引なし46.179 msに対して索引あり63.002 ms、COUNTは25.920 msに対して51.272 msで、無条件集計の費用較正とキー／リスト単位の集計には余地がある。
PGとの10倍差を解消したことと、最適な走査を常に選べることは区別する。

## F: 既存主キーを使う非キー検索

追加索引なし、random bucket、10秒×3試行。全12測定で値・順序・最終状態を検証し、エラー0だった。

| 接続 | tiny query/s | PG query/s | tiny p50 ms | tiny p95 ms | tiny p99 ms |
| --- | --- | --- | --- | --- | --- |
| 4 | 7,199.100 | 13,519.900 | 0.548 | 0.636 | 0.756 |
| 16 | 12,633.300 | 25,208.600 | 1.244 | 1.515 | 1.722 |

以前の経路はLIMIT 10でも並列走査とProjectionのbatch充填で1万行を読んだ。
必要行数を下流へ伝えると、通常は約900行のprefixで止まる。
固定bucketの診断ではbuffer参照が約8万回から912回へ減ったが、判定には上表のrandom bucket比較を使った。

## G: 書き込みの待機と残差

10秒warmup＋60秒×5試行。TPSとp50はそれぞれ試行値の中央値であり、相互の逆数ではない。
旧値は3秒×3試行なので、この比だけをコード変更の純粋な寄与とは解釈しない。

| 構成／負荷／接続 | 旧tiny TPS | 今回tiny TPS | 今回PG TPS | 旧tiny p50 ms | 今回tiny p50 ms | 今回PG p50 ms | tiny SQL失敗/attempt |
| --- | --- | --- | --- | --- | --- | --- | --- |
| unindexed/hot_update/1 | 466.333 | 264.833 | 558.933 | 2.120 | 3.652 | 1.733 | 0/126,571 (0.0000%) |
| unindexed/update/16 | 1,943.000 | 3,655.750 | 4,503.517 | 8.142 | 3.836 | 3.432 | 193/1,092,423 (0.0177%) |
| indexed/transfer/16 | 2,075.333 | 5,766.617 | 7,443.600 | 8.081 | 2.467 | 2.066 | 4687/1,744,086 (0.2687%) |
| indexed/insert_delete/16 | 2,179.333 | 5,828.583 | 7,483.850 | 7.938 | 2.519 | 2.058 | 0/1,757,590 (0.0000%) |

16接続UPDATEのカウンタは、旧診断でqueue約4.08 ms、WAL待ちを含むワーカ占有約4.13 ms、同期1回あたり約4 commitだった。
最終測定ではqueue 0.022 ms、SQLワーカ占有0.039 ms、耐久化応答/同期13.91。
fdatasync平均は旧診断約2.06 msから今回3.56 msへ遅くなった期間でも、成功TPSは1,943から3,656、p50は8.142から3.836 msとなった。
ワーカ占有の計測境界が変わっており、SQL計算そのものが100倍速くなったという意味ではない。

単一writerのhot_updateは改善していない。今回はqueue 0.010 ms、SQLワーカ占有0.054 msに対し、応答の耐久化待ち3.658 ms、fdatasync平均3.548 msが支配した。
今回PGのp50も1.733 msで、旧PGの約0.124 msより遅い。期間差は大きいが、tinylambのWAL物理経路やcheckpointとの重なりまで分離できたわけではない。

| 未達指標 | 旧PG固定目標 | 今回tiny | 今回PG比 | 旧PG比 |
| --- | --- | --- | --- | --- |
| update/16/p50_ms | <2.671 | 3.836 | 1.12× | 14.36× |
| hot_update/1/tps | >758.600 | 264.833 | 2.11× | 28.64× |
| hot_update/1/p50_ms | <1.243 | 3.652 | 2.11× | 29.37× |
| hot_update/1/p95_ms | <1.538 | 3.957 | 2.11× | 25.73× |
| hot_update/1/p99_ms | <1.928 | 6.957 | 2.04× | 36.08× |

単なるKEEP_SIZE予約の[同期実験](../benchmark/comparison/results/2026-10-29-gap-g-diagnostic/sync-probe.json)では安定した改善が得られず、同期保証を落とす変更は加えていない。
次は同じ長さ・履歴・I/O状態で変更前後を交互に測り、WALの物理予約・再利用と論理終端を分離して設計する必要がある。
現状のfile sizeをそのまま論理終端やDurableLSNへ流用したまま、単にファイルを拡張する実装は採用しない。
再開、torn tail、ENOSPC/EIO、segment境界、checkpoint／recycle、D4の検証を含めて[TODO](../TODO.md)へ残した。

## H: 同じreader SQLを単独／混合で比較

readerは全条件で次のSQLを使う。単独時も最終OLAPの`htap_reader`で計時した。

```sql
SELECT a.bucket, SUM(a.balance)
FROM sales s JOIN accounts a ON s.account_id = a.id
GROUP BY a.bucket ORDER BY a.bucket
```

| 構成 | writer数 | 製品 | reader/s 旧→今回 | writer TPS 旧→今回 | 今回reader p50 ms | 今回writer p50 ms |
| --- | --- | --- | --- | --- | --- | --- |
| unindexed | 3 | tinylamb | 0.667 → 5.183 | 2548.000 → 1026.050 | 192.371 | 2.450 |
| unindexed | 3 | postgresql | 32.667 → 32.567 | 1439.000 → 1426.950 | 30.449 | 2.030 |
| unindexed | 15 | tinylamb | 0.667 → 5.083 | 1750.000 → 3504.917 | 195.882 | 4.339 |
| unindexed | 15 | postgresql | 30.333 → 30.800 | 7006.333 → 6904.083 | 32.338 | 2.071 |
| indexed | 3 | tinylamb | 0.667 → 5.167 | 728.000 → 993.983 | 192.858 | 2.471 |
| indexed | 3 | postgresql | 39.000 → 39.317 | 1440.333 → 1394.350 | 25.203 | 2.054 |
| indexed | 15 | tinylamb | 0.667 → 5.083 | 1760.667 → 3455.217 | 195.690 | 4.381 |
| indexed | 15 | postgresql | 37.667 → 37.417 | 7068.000 → 6760.883 | 26.227 | 2.094 |

readerの4目標はすべて通過し、索引有無によるtinylambの差は小さかった。
readerのSQLエラー・値検証エラーはwarmupを含め0で、全試行の残高保存も成立した。
旧3秒窓の0.667 query/sの逆数を旧readerの中央値遅延とは扱わない。

writerが全条件で改善したわけではない。追加索引なし・3 writerでは2,548から1,026 TPSへ低下した。
分析側の実行頻度、測定窓、履歴、同期I/Oの状態も異なるため、コード単独の回帰かは未分離である。
Hの完了は4つのreader目標と比較・整合性検証を指す。全負荷のwriter無退行を宣言せず、I/O状態を揃える前後比較を共通ゲートに残す。

## 正しさ・回帰検証

最終OLAP 462測定は3製品で全件一致。G/Hの80試行とFの12試行は全て最終不変条件を満たした。
書き込みのSQL失敗は成功TPSから除き、保存されたエラー例は全てNoWaitのwrite intent競合だった。
`valid`とエラー0は同義ではない。FとHTAP readerは全件で`errors`／`warmup_errors`が0であることも別途監査した。
[条件別の監査結果](../benchmark/comparison/results/2026-10-29-gap-validation/correctness-audit.json)を保存した。

空commitの除去前には、小規模CTEとQ15で旧比の後退が見つかった。
1万行CTEは2.403→0.455 ms、10万行CTEは6.606→2.616 ms、索引ありQ15は2.930→0.888 msとなった。
修正後の全OLAPで、旧値に対して1.5倍超かつ1 ms超の後退はなかった（[152条件の検査](../benchmark/comparison/results/2026-10-29-gap-validation/olap-regressions.json)）。

全ctest 4,090件は失敗0、既存skip 3件。SQL oracleは181秒277入力で不一致なし。
解析・整合性判定の27テスト、layering（既存allowlist内65件、新規違反0）、format、diffの空白検査を実施した。
[最終検証の一覧と使用バイナリhash](../benchmark/comparison/results/2026-10-29-gap-validation/validation-manifest.json)、
[ctestログ](../benchmark/comparison/results/2026-10-29-gap-validation/ctest-post-final.log)、
[oracleログ](../benchmark/comparison/results/2026-10-29-gap-validation/oracle-post-final.log)、
[修正前の再現](../benchmark/comparison/results/2026-10-29-gap-validation/partial-before.log)と
[修正後](../benchmark/comparison/results/2026-10-29-gap-validation/partial-after.log)を保存した。

比較器も、値の不一致を通常のSQL競合と分けて無効試行にするよう補強した。
新しい判定器の3製品smokeは15試行で不変条件成立・値不一致0。NoWait競合1件を記録したため、通常runnerの終了コードは1だった。
この短いsmokeの値は性能判定には混ぜていない。

## 測定条件・使用版・範囲

- 8 CPU、buffer 512 MiB、query memory 64 MiB、共有SQLワーカ8。Gのcoordinatorは別に1 thread。CPU affinityは専有予約ではない。
- PostgreSQL 18.6／MySQL 8.4.13はdigest固定。同期commitを維持。メモリ設定の意味と分離レベルは製品間で同一ではない。詳細は元の総合比較を参照。
- OLAPは初回＋5反復、3規模とTPC-H由来22問。Fは2秒warmup＋10秒×3、G/Hは10秒＋60秒×5。FとG/Hの計時はtiny／PGのみで、MySQLは起動・fixture検証を行う。最終OLAPは3製品を計時。
- `fresh`は表・索引をDROP／CREATEしてfixtureを再ロードする意味。プロセス、WAL履歴、OS cacheはリセットしない。ANALYZE後に測り、VACUUM／checkpointの完了待ちはしない。他DBのbackground処理とhostのI/O変動も含む。
- 長時間G/Hは`b7af77d1…`版、最終OLAPとFは空commit・複文エラー修正版`6ce27fb4…`版。後続修正は実更新のcommit手順を変えないが、G/H値は修正版での80試行再実行ではない。各ディレクトリの`environment.json`、バイナリSHA-256、tracked差分で使用版を区別できる。
- 測定境界をまたぐ成功は窓内TPSへ含めず、開始・終了時刻を保存。成功時のp50／p95／p99と失敗件数を分ける。HTAPは役割別に比較し、全要求を混ぜたp50をreader遅延に使わない。

最終データ: [OLAP追加索引なし](../benchmark/comparison/results/2026-10-29-gap-post-unindexed-olap/summary.csv)、
[OLAP追加索引あり](../benchmark/comparison/results/2026-10-29-gap-post-indexed-olap/summary.csv)、
[G/H追加索引なし](../benchmark/comparison/results/2026-10-29-gap-final-g-unindexed/summary.csv)、
[G/H追加索引あり](../benchmark/comparison/results/2026-10-29-gap-final-g-indexed/summary.csv)、
[F](../benchmark/comparison/results/2026-10-29-gap-post-f/summary.csv)。
再現手順は[benchmark README](../benchmark/comparison/README.md)、集計は[gap_results.py](../benchmark/comparison/gap_results.py)、
カウンタは[trial_metrics.py](../benchmark/comparison/trial_metrics.py)、図は[plot_gaps.py](../benchmark/comparison/plot_gaps.py)で再生成できる。
