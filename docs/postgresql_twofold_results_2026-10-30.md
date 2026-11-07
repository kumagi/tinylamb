# PostgreSQL比2倍以内への改善結果（2026-10-30）

**全種目のフル比較と、未達条件をそろえた精密再測定の結果、元の161対象は161/161、全456指標は456/456で、今回測定したPostgreSQL比2倍以内となった。最大は索引ありQ02の1.938倍。**

ただし、最初の短時間フル測定だけでは13指標が未達だった。これを隠さず保存し、該当条件は同じTinylambバイナリで測定窓・反復数を増やした。UNION ALLはPGの可視性情報で計画が変わることも確認し、**速いIndex Only Scan側のPG**を最終比較に採用した。以下の「最終」はこの再確認を反映した値であり、短時間フル測定単独の結果とは区別する。

達成基準はユーザー指定の「Tinylambの時間が今回同条件で再測定したPGの2倍以下」。TPSは逆向き、すなわちPGの半分以上で判定する。旧PG固定値は参考として残したが、その基準では149/161であり、旧基準まで全件達成したという結論ではない。

## 1. 判定と測定範囲

| 段階 | 範囲 | 結果 |
| --- | --- | --- |
| 標準フル測定 | OLAP 474測定、OLTP/HTAP 396試行 | 全件整合性通過。PGとの456比較中443通過、13未達 |
| 精密再測定 | 未達を含むOLTP 4条件、30秒×3試行×2製品＝24試行 | 各条件の全24指標が通過。失敗した指標だけを抜き出して採用せず、条件全体を置換 |
| UNION ALL状態確認 | 100万行・索引あり、3製品各101反復、PG VACUUM前後 | 全件一致。VACUUM後の速いPGとの比較で1.613倍 |
| 最終判定 | 上記を同じ条件キーで照合した456指標 | **456/456通過、最大1.938倍** |

対象456指標の内訳はOLAPのwarm中央値158、OLTP/HTAPの成功TPS・p50/p95/p99と役割別HTAPの298。初回時間、ロード時間、最大遅延、中断率を「2倍以内」の判定に混ぜていない。それらは生データまたは後述の別評価に残す。

| 改善群 | 元の対象 | 最終通過 |
| --- | ---: | ---: |
| A: 走査・型付き集計 | 14 | 14 |
| B: 結合・副問い合わせのstream化 | 39 | 39 |
| C: 小規模SQL・索引の固定費 | 10 | 10 |
| D: SQL dispatch・transaction固定費 | 35 | 35 |
| E: 同期WAL・混合負荷 | 63 | 63 |
| 合計 | **161** | **161** |

[元161対象の変更前・変更後・旧PG比・今回PG比](../benchmark/comparison/results/2026-10-30-gap6-c34-final/target-comparison.md)、[全456比較](../benchmark/comparison/results/2026-10-30-gap6-c34-final/all-comparisons.md)、[CSV](../benchmark/comparison/results/2026-10-30-gap6-c34-final/all-comparisons.csv)、[監査JSON](../benchmark/comparison/results/2026-10-30-gap6-c34-final/audit.json)で個別に追える。

## 2. 条件と再現性

| 項目 | 条件 |
| --- | --- |
| ホスト | AMD Ryzen 9 9950X3D、Linux、ext4/NVMe |
| サーバCPU | 全製品ともCPU 0–7の8論理CPU。負荷生成CPUは固定しない |
| Tinylamb | RelWithDebInfo、SQL worker 8、page pool 512 MiB、query budget 64 MiB、同期commit |
| PostgreSQL | 18.6、shared_buffers 512 MiB、work_mem 64 MiB、Gather最大4 worker、JIT on、fsync/synchronous_commit on、fdatasync |
| MySQL | 8.4.13、buffer pool 512 MiB、innodb_flush_log_at_trx_commit=1、O_DIRECT、binlog off |
| 比較方法 | ローカルTCP、全結果取得までのクライアント時間。製品を同時に負荷実行しない。順序は固定seedでシャッフル |
| データ | sales 1万・10万・100万行、accounts最大1万行、TPC-H由来SF 0.01。追加索引なし／共通二次索引あり |
| 標準反復 | OLAPは初回を分離しwarm 9回。OLTPはwarmup 2秒＋測定5秒×3試行、1/4/16接続 |
| 精密反復 | 該当OLTPを同じ設定で30秒×3試行。UNION ALLはwarm 101回 |

バッファ設定は各製品の総メモリ制限を同一にするものではない。Tinylambの正確なキー件数cacheはcaller threadごとに最大1 MiB・1エントリを保持し、8 SQL workerなら保持分の上限は8 MiB。dimensionの列形式入力も1 MiB上限とし、処理中の予約はquery budgetへ計上する。

測定中にビルド・テスト・プロファイラーを並行させていない。C34のサーバSHA-256は全工程で次の値を維持し、測定前後の全C++／比較ハーネスのhash一致も確認した。

```
4567bacc718b1f97381bc5a46ed3dab182b74e123fbe5a01337118bf550d9ecd
```

Docker imageはdigestで固定し、実行引数、ポート、ビルド条件、driver version、同期設定、CPU条件を保存した。[再現コマンドと検証記録](../benchmark/comparison/results/2026-10-30-gap6-c34-provenance/)、[標準環境情報](../benchmark/comparison/results/2026-10-30-gap6-c34-full/unindexed/environment.json)を参照。

今回の**OLAPは3製品を再実行、OLTP/HTAPはTinylambとPGを再実行**した。MySQLの全OLTP比較は[第5巡の3製品フルレポート](benchmark_full_comparison_2026-10-29.md)を参照し、今回の新しい測定値としては扱わない。小数SF・固定パラメータ・型の調整を含むため、公式TPCスコアではない。

## 3. 実装した改善

### A: 走査と整数集計

行ごとの汎用Row構築を減らし、型付きbatchからCOUNT/SUM/MIN/MAXや整数の条件・式を集計する。morselを並列処理し、callerも参加する。後続の不要列を復号せず、固定長部分の位置をschemaからまとめて求める。

NULL、符号、型、snapshot、メモリ予算、整数の途中overflowを確認し、対応できない形は通常経路へ戻す。FLOAT条件を無理に整数経路へ入れてQ6の意味を変える処理は避けた。optimizer統計の行数を答えとして返す最適化ではない。

### B: 結合・副問い合わせ・繰り返し集計

scalar subqueryの一回評価、選択的なdimensionキーによる事前削減、整数joinと集計の融合、中間materialize/spillの削減を実装した。小さい結合集計は補助threadを1本作らずcallerで実行する。

HTAPの繰り返し集計では、fact側整数キーの**実際に読んだ正確な出現回数**を再利用する。各呼び出しでtable incarnation、完全なpage列、現在のnext linkと終端、各page latch下のMVCC chain不在とPageLSN、前後のlayout generationを検証する。証明できない場合は通常のsnapshot scanへ戻す。変更・abort・削除穴・未公開tail・drop/recreateで古い件数を使わない。

dimensionの残高などは毎回そのsnapshotから読み、SQL結果や集計結果をcacheしない。固定長整数/DATEは列形式で処理し、グループ・キー範囲・非負SUMの検査を一回にまとめる。cacheがある時はキー対応表と頻度配列を省く。正の重複キーは各dimension行を一度ずつ数え、負値やprobe側引数は順序付きの通常集計へ戻してoverflow契約を守る。

この効果はfactが変化しない反復に大きい。factへの取り込み・更新が続く場合は無効化・再構築・通常走査が発生するため、今回のreader速度をそのまま適用できない。

### C: 索引と小さいSQL

セッション内の計画・式再利用、索引上限の再バインド、Top N、singleton/小さいpeer groupの処理を改善した。IndexOnlyScanは条件を一度compileし、決定的なキー列だけの条件は同じ重複keyのvalue list内で判定を再利用する。INCLUDE値や未対応・非決定的な式はこの再利用から除外する。返す値の所有権と古いsnapshotの解決は維持する。

### D: dispatchと通信

worker間でセッション計画を引き継ぎ、短い確定応答を耐久性確認後にworkerからnonblocking送信できるようにした。部分送信はepollへ戻し、send/closeを共有mutexで排他する。既に安全性を確認したliteral応答は受信bufferから処理し、SQL文字列のコピーや一時文字列のhash検索を減らす。

pipelineとhalf-closeが交差した時に未読Queryを捨てる回帰を修正した。受信の連続処理は32 packetごとにevent loopへ戻す。200反復のpipeline検査、50 packet burst、部分・不正frame、transaction状態、idle timeoutを検証した。

### E: WALと混合負荷

SQL worker上のcommit公開と耐久化待ちを分離し、own/dependency LSNを満たすまで応答全体を保持する。既にfdatasync済みならLoggerの実際のDurableLSNを証明として使い、無関係な将来commitを待たない。group化はcommitを作れる要求だけを対象にし、要求された同期は即時、要求のないdirty WALには200 msのbackstopを使う。

混合負荷ではreaderの要求内並列数を抑えて短い要求のCPUを確保した。単純にreaderの並列数を増やす案はwriterのtailを悪化させたため取り除いた。readerを遅くして混合分位点を見かけ上改善する扱いも採用していない。

新規server DBの固定長WALはCRC付き二重の論理終端を使い、body・終端・fdatasyncの成功後だけDurableLSNを進める。WAL-before-data、own/観測依存の同期条件を維持した。[耐久性契約](commit_durability.md)と[ADR 0001](adr/0001-fixed-wal-logical-end.md)を参照。

## 4. 代表値

OLAPの時間はms。UNION ALLだけはPG状態を確認した101反復、それ以外は標準フル測定のwarm 9回である。

| 条件 | 負荷 | Tinylamb | PostgreSQL | MySQL |
| --- | --- | ---: | ---: | ---: |
| 索引なし・100万行 | SUM | 2.298 | 10.873 | 150.171 |
| 索引あり・100万行 | 10%フィルタ | 3.709 | 7.886 | 16.753 |
| 索引あり・100万行 | EXISTS | 2.280 | 3.545 | 7.798 |
| 索引あり・100万行 | HTAP reader SQL単独 | 0.673 | 24.607 | 230.607 |
| 索引あり・100万行、PG VACUUM後 | UNION ALL | 5.808 | 3.600 | 26.125 |
| 索引あり・SF 0.01 | Q14 | 0.730 | 0.438 | 1.341 |
| 索引あり・SF 0.01 | Q15 | 0.612 | 0.487 | 1.804 |

読み取り系のすべてでTinylambが勝つという結果ではない。最終的にPGが速い側の最大は索引ありQ02（1.729ms対0.892ms、1.938倍）、次がrollback/1接続のp50（1.928倍）。小さいSQLやPGの強い索引・並列計画にはまだ差がある。

| 条件 | 指標 | Tinylamb | 今回PG | 不利倍率 |
| --- | --- | ---: | ---: | ---: |
| 索引あり・SELECT 1・16接続 | query/s | 351,949 | 614,891 | 1.747 |
| 索引あり・HTAP reader・1接続 | query/s | 1,586.4 | 39.2 | 0.025 |
| 索引なし・読取＋更新・16接続、30秒 | txn/s | 7,575.3 | 7,628.1 | 1.007 |
| 索引あり・読取＋更新・4接続、30秒 | txn/s | 1,939.5 | 1,930.2 | 0.995 |
| 索引なし・HTAP・4接続、30秒 | 混合p95 ms | 2.205 | 2.297 | 0.960 |

索引なしHTAP/4接続のreader p50は長窓で2.016ms対PG 30.824ms。高速同期の局所試行では約0.8msだった。元の遅いreaderを隠さず、reader/transactionのTPSとp50/p95/p99を混合値と別に保存している。

## 5. 短時間の13残差をどう扱ったか

### 同期時間の変動

| 条件・TPS | 5秒窓 Tiny | 5秒窓 PG | 30秒窓 Tiny | 30秒窓 PG |
| --- | ---: | ---: | ---: | ---: |
| 索引なし・読取＋更新・16接続 | 7,675.4 | 41,228.8 | 7,575.3 | 7,628.1 |
| 索引あり・読取＋更新・4接続 | 1,961.0 | 11,372.6 | 1,939.5 | 1,930.2 |

ここではTinyの絶対TPSはほぼ変わっておらず、PG側の同期速度が違っていた。したがって、この比率の縮小を追加のコード高速化の効果とは呼ばない。

短窓の索引なし読取＋更新/1接続では、TinyのSQL実行は平均約6µs/command、長い待ちは約1msのWAL同期だった。別試行は約0.1ms/syncまで短く、PGのクライアント遅延にも約0.2msと約1.1msの状態があった。追加の無駄な同期が大量にあったという観測ではなく、該当Tiny試行は約1 deferred reply/syncだった。

長窓の全試行を含めて中央値を再計算した。最速試行だけの採用、旧PG値への差し替え、失敗した分位点だけの置換は行っていない。標準フル測定と[30秒再測定](../benchmark/comparison/results/2026-10-30-gap6-c34-long/)は両方残した。この結果は今回の条件での中央値であり、任意のI/O状態・単一試行・最大時間まで2倍以内を保証するものではない。

### UNION ALLのPG状態

| PG状態 | relallvisible / relpages | 実行計画 | Tiny ms | PG ms |
| --- | --- | --- | ---: | ---: |
| 明示VACUUM前、101反復 | 0 / 11,364 | 2本のBitmap Heap Scan | 6.203 | 11.164 |
| VACUUM (ANALYZE)後、101反復 | 11,364 / 11,364 | 2本のIndex Only Scan、Heap Fetches各0 | 5.808 | 3.600 |

両方ともPGは2並列workerを実際に起動していた。標準フル測定の推定計画もIndex Only Scanだった。新しいDBで単に反復を増やした結果の遅いPG値（約11.5ms）だけを採用せず、明示VACUUM後の速いPGで判定した。

Tiny自体も標準9反復の8.051msから状態間で5.8–6.2msへ変動している。PGのVACUUMがTinyを高速化したという解釈はしない。反復・warm状態を明記して両製品を同じ段階で比較した。[計画・実測・メンテナンス情報](../benchmark/comparison/results/2026-10-30-gap6-c34-union-state/pg-state-summary.json)で検証できる。

## 6. 正しさ・中断・適用範囲

- 標準フル測定はOLAP 474件すべて結果一致、OLTP/HTAP 396試行すべて口座件数・残高変化・churn・読み取り結果の検査を通過した。型の違いを含む結果同値性を確認しており、製品間の生JSON文字列hashが同じという意味ではない。
- 競合によるTinyのNoWait中断は標準測定で索引なし28,480件、索引あり24,530件。PGは0件。成功時TPS・分位点は中断を含まない。
- 全要求をまとめた中断率はそれぞれ約0.074%／0.060%だが、SELECT 1などの大量の成功要求が分母に入る。集中更新/16接続だけでは**約13.4%／13.7%**だった。PGの待機とTinyの中断という仕様差を成功時latencyだけで評価しない。
- 初回やcache再構築、fact取り込み併用、別の分布・大きなSF、異なるCPU数は別条件。今回の全456指標通過を未測定条件への保証に広げない。
- serverはSimple Queryのみ。extended protocol/prepared statementを含む製品の最高性能や全機能の比較ではない。

全ctestは**4,178件・失敗0、既存skip 3件**（290.47秒）。SQL oracleは64入力から変異を生成し、**602秒771入力、新規追加655件、不一致0**。比較ハーネス34テスト、整形109ファイル、layering（既存allowlist 66、新規違反0）、diff checkを通過した。

削除穴の新回帰は誤ったRowCount終端の旧実装で失敗することを確認し、RowMaxまで走査する修正後に通した。NULL、負key、重複、DATE、古いsnapshot、commit/abort、未公開tail、memory上限、overflow、drop/recreate、pipeline/half-closeを検証した。[検証ログ](../benchmark/comparison/results/2026-10-30-gap6-c34-provenance/validation.json)を参照。

## 7. 互換性と残る課題

新規server DBは固定長WAL metadata v2を使う。**旧バイナリはv2を開けない**。既存metadata v1を自動変換せず、埋込みDatabase/Loggerは明示指定なしなら従来方式を維持する。これは新規DBの互換性に関わる変更であり、速度のために同期を省略する変更ではない。[ADR](adr/0001-fixed-wal-logical-end.md)に方式・破損時の扱い・検証をまとめた。

今後は、短い窓の判定がI/O状態に左右される条件と、PGの可視性情報・自動メンテナンスを測定手順に明示することが重要になる。fact取り込みを伴うHTAP、cold/rebuild時、より大きいSFも別枠で増やす。今回の基準では未達0だが、これらを解決済みとは扱わない。

## 8. 全データ

- [標準フル測定・索引なし](../benchmark/comparison/results/2026-10-30-gap6-c34-full/unindexed/)／[索引あり](../benchmark/comparison/results/2026-10-30-gap6-c34-full/indexed/)
- [標準フル監査と13残差](../benchmark/comparison/results/2026-10-30-gap6-c34-comparison/)
- [30秒再測定](../benchmark/comparison/results/2026-10-30-gap6-c34-long/)
- [UNION ALLの状態確認](../benchmark/comparison/results/2026-10-30-gap6-c34-union-state/)
- [最終161対象・456比較・採用元](../benchmark/comparison/results/2026-10-30-gap6-c34-final/)
- [ソースhash・パッチ・コマンド・検証ログ](../benchmark/comparison/results/2026-10-30-gap6-c34-provenance/)
- [実装計画と中間候補・棄却案](postgresql_twofold_plan_2026-10-29.md)／[TODO](../TODO.md)

## 9. TPC-C／TPC-H単体測定の追記

2026-10-30に現行TinylambのC++ベンチマークを追加実行した。3試行の中央値は
TPC-C W=1・10端末が **1,087.700 TPS**（New-Order 30,016件/分）、
TPC-H SF=1・全22問の時間合計が **8.24156秒**。全問の時間と結果行数、
各試行の値・実行条件は従来の[BenchmarkHistory](../BenchmarkHistory.md)へ追記した。

TPC-Cの競合中断率は全体で3.128–3.285%、Delivery単独で79.86–82.54%だった。
TPC-Hは初回を含む4巡・88実行で成功し、結果行数が一致した。SF=1での別製品との
結果値照合は行っていない。今回の単体測定はSQLエンジンへの直接呼び出しで、
上記のTCP経由・SF=0.01のPG比較456指標には含めない。
[生データと測定記録](../benchmark/comparison/results/2026-10-30-tpc-native/)を参照。
