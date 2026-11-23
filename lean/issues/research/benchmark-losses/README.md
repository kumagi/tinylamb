# 敗北クエリ全 66 件の最適化命題（第5波・未証明課題 27 件）

**更新: 27 件中 26 件を `theorem sound : Goal` で証明、1 件（functional-dependent-group-key-elim）は反例により `¬ Goal` を登録済み**（[../RESULTS.md](../RESULTS.md) の第五波の項）。

`after-lean-opts-round2`（`8 threads`, CPUs `0–7`, `TZ=UTC`, `1 warmup + 3 timed` 中央値）における **TPC-H SF=10（22q 中 7 敗）**、**TPC-DS SF=10（99q 中 41 敗）**、**ClickBench 100M（43q 中 18 敗）** の **全 66 敗北クエリ（vs DuckDB 2.0.0 / ClickHouse 26.10）** を個別プロファイル・SQL 構造解析し、勝利・逆転に直結する 27 件の最適化命題を Lean 4 の未証明 `Goal : Prop` として定式化した。

各課題は [`TinylambOpt.ResearchGoals.<Name>.Goal`](../../../TinylambOpt/ResearchGoals/) を提出型として定義している。証明は別モジュールで該当ファイルを import し、`theorem sound : TinylambOpt.ResearchGoals.<Name>.Goal` として提出する。

---

## 命題一覧（27 件）

### 1. TPC-H SF=10・ブロック集約・ソート・結合（7 件）

- [ブロック単位全件肯定での無条件走査＋`(SUM, COUNT)` 及び中間乗算共有](unconditional-block-sum-avg-reuse.md) (`UnconditionalBlockSumAvgReuse`)
- [キー単調チャンクのワーカー局所 Top-K ストリーム集約＋境界シーム結合](monotonic-key-stream-topk.md) (`MonotonicKeyStreamTopK`)
- [3 表以上等値結合の双方向セミジョイン伝播と対称 OR 対の因数分解](bidirectional-equijoin-semijoin.md) (`BidirectionalEquiJoinSemijoin`)
- [Fubini 総和保存＋誤差帯分類によるスケール閾値 HAVING の厳密再集計絞り込み](float-fubini-having-band.md) (`FloatFubiniHavingBand`)
- [`is_identity_1_to_n` 左表に対する片側ヒストグラム直計算](identity-fk-left-join-histogram.md) (`IdentityFkLeftJoinHistogram`)
- [パック済み `(gid, val)` ソート重複排除＆値ハッシュ分割による無マージ `COUNT(DISTINCT)`](packed-gid-distinct-sort-dedup.md) (`PackedGidDistinctSortDedup`)
- [不変 TLCS v3 ゾーンマップ行数上界による `exact_integer_total` 復帰](immutable-tlcs-zonemap-row-bound.md) (`ImmutableTlcsZoneMapRowBound`)

### 2. TPC-DS SF=10・結合除去・ピボット・相関・統計（13 件）

- [右辺一意キー `LEFT JOIN` 除去・1-bit マーク化・非等値上界プッシュダウン](unique-key-left-join-elision-and-mark.md) (`UniqueKeyLeftJoinElisionAndMark`)
- [排他的 `CASE` バケット分類の 1 パス単一スロット更新（`ELSE 0`/`ELSE NULL`）](disjoint-case-pivot-bucket-dispatch.md) (`DisjointCasePivotBucketDispatch`)
- [属性正規 ID 空間での最小チャネル順 `INTERSECT` / `EXCEPT` 集合演算](canonical-equiv-id-set-algebra.md) (`CanonicalEquivIdSetAlgebra`)
- [単一テーブルの異種複数コンシューマへの和述語 1 パス並列供給](single-pass-multi-consumer-fact-scan.md) (`SinglePassMultiConsumerFactScan`)
- [`LEFT JOIN` 保存側結合キー条件の NULL 側転送と `UNION DISTINCT` 分配則](left-join-preserved-to-nullable-filter-transfer.md) (`LeftJoinPreservedToNullableFilterTransfer`)
- [相関パーティション平均のパーティションキー押込み vs 半結合パーティション枝刈り](correlated-agg-partition-key-filter-pushdown.md) (`CorrelatedAggPartitionKeyFilterPushdown`)
- [単射代理キー整数空間でのチャネルピボット＋遅延ワイド属性参照](surrogate-key-channel-pivot-late-dim.md) (`SurrogateKeyChannelPivotLateDim`)
- [選言 `EXISTS` 連鎖の短絡ビット集合段階縮小](disjunctive-exists-cascading-bitset.md) (`DisjunctiveExistsCascadingBitset`)
- [非等値自己 `EXISTS` のグループ `(min, max)` Witness O(1) 判定](nonequi-self-exists-minmax-witness.md) (`NonEquiSelfExistsMinMaxWitness`)
- [整数 Chan/Welford 公式による `(COUNT, AVG, STDDEV_SAMP)` 状態共有と並列統合](chan-welford-shared-variance-state.md) (`ChanWelfordSharedVarianceState`)
- [次元・ファクト跨ぎ選言結合の 3 値ビットマスク事前枝刈り＋1 命令プローブ](cross-table-disjunctive-branch-mask.md) (`CrossTableDisjunctiveBranchMask`)
- [`RANK()` 自己結合の隣接 `LAG`/`LEAD` 還元と無順序パーティション窓のハッシュ集約](rank-self-join-to-lag-lead-and-hash-window.md) (`RankSelfJoinToLagLeadAndHashWindow`)
- [重複除去済み 2 ストリーム `FULL OUTER JOIN` 指標の単一表 2-bit タグ計数](full-outer-indicator-bitset-complement.md) (`FullOuterIndicatorBitsetComplement`)

### 3. ClickBench 100M・列指向集約・文字列・Top-N/Offset（7 件）

- [無順序 `GROUP BY ... LIMIT K` の固定グループ数凍結・既存キー限定更新](capped-group-early-freeze.md) (`CappedGroupEarlyFreeze`)
- [空文字・NULL 頻出列の整数キー分離・低コスト述語先行・整数 `AVG` 遅延除算](empty-string-key-partition-split.md) (`EmptyStringKeyPartitionSplit`)
- [`VarChar` オフセット終端配列 `O(1)` 総バイト長算出と payload 非参照非空カウント](varchar-offset-only-length-sum.md) (`VarcharOffsetOnlyLengthSum`)
- [関数従属グループキー `f_j(k)` 除去＋`LIMIT K` 後遅延評価（オーバーフロー安全境界）](functional-dependent-group-key-elim.md) (`FunctionalDependentGroupKeyElim`)
- [ビット演算 AND/OR ブロック除外とソート境界 Fence＋遅延ワイド行フェッチ](late-wide-row-topn-block-fence.md) (`LateWideRowTopNBlockFence`)
- [高選択性整数プレフィルタ後の遅延スライス集約と `CASE` ガード付き二次列デコード](selective-filter-late-slice-group.md) (`SelectiveFilterLateSliceGroup`)
- [`OFFSET ≫ LIMIT` の逆順最小 Top 抽出双対性と `TIMESTAMP_TRUNC` 単調バケット走査](offset-heavy-reverse-topn-and-quotient-bucket.md) (`OffsetHeavyReverseTopNAndQuotientBucket`)

---

## 全 66 敗北クエリ完全網羅マトリクス

数値は `after-lean-opts-round2` の実行結果（ms, 小数第2位）。

### A. TPC-H SF=10（全 7 敗：対 DuckDB 7 敗 / 対 ClickHouse 0 敗）

| Query | TL (ms) | DD (ms) | CH (ms) | 差 (vs 最速) | 主ボトルネック | 対応 Issue |
|---|---:|---:|---:|---:|---|---|
| **Q01** | 74.03 | 73.50 | 464.37 | +0.53 (1.01x) | 60M 行ブロック毎 `l_shipdate` 評価、`SUM`/`AVG` と割引計算の重複 | [unconditional-block-sum-avg-reuse.md](unconditional-block-sum-avg-reuse.md) |
| **Q03** | 64.35 | 54.45 | 245.02 | +9.90 (1.18x) | `lineitem` 融合プローブで 147 万グループ展開後に Top-10 抽出 | [monotonic-key-stream-topk.md](monotonic-key-stream-topk.md) |
| **Q07** | 59.43 | 54.54 | 336.65 | +4.88 (1.09x) | `nation×2 ↔ supplier/customer → lineitem/orders` 双方向セミジョイン未伝播 | [bidirectional-equijoin-semijoin.md](bidirectional-equijoin-semijoin.md) |
| **Q08** | 55.11 | 52.31 | 356.53 | +2.80 (1.05x) | 8 表結合で `part/region` 選択率が中間の `lineitem/orders` に戻らない | [bidirectional-equijoin-semijoin.md](bidirectional-equijoin-semijoin.md) |
| **Q11** | 9.50 | 9.12 | 80.67 | +0.38 (1.04x) | `(partsupp ⋈ supplier ⋈ nation)` のサブクエリ側と主側で 2 重走査 | [float-fubini-having-band.md](float-fubini-having-band.md) |
| **Q13** | 162.01 | 157.61 | 287.05 | +4.40 (1.03x) | 150 万顧客左外部結合で 1500 万 `orders` 全走査＋2 重 `GROUP BY` | [identity-fk-left-join-histogram.md](identity-fk-left-join-histogram.md) |
| **Q16** | 46.61 | 46.30 | 60.20 | +0.31 (1.01x) | 27,840 グループ × 8 ワーカーの `COUNT(DISTINCT ps_suppkey)` マージ | [packed-gid-distinct-sort-dedup.md](packed-gid-distinct-sort-dedup.md) |

### B. TPC-DS SF=10（全 41 敗：対 DuckDB 41 敗 / 対 ClickHouse 3 敗）

| Query | TL (ms) | DD (ms) | CH (ms) | 差 (vs 最速) | 主ボトルネック | 対応 Issue |
|---|---:|---:|---:|---:|---|---|
| **Q01** | 13.82 | 12.35 | 52.53 | +1.47 (1.12x) | `s_state='TN'` のパーティションキー押込み不足＋相関 `AVG` 2 重走査 | [correlated-agg-partition-key-filter-pushdown.md](correlated-agg-partition-key-filter-pushdown.md) |
| **Q02** | 63.73 | 35.54 | 58.18 | +28.19 (1.79x) | `web_sales ∪ catalog_sales` の 7 曜日 `SUM(CASE)` が直交分岐未統合 | [disjoint-case-pivot-bucket-dispatch.md](disjoint-case-pivot-bucket-dispatch.md) |
| **Q04** | 288.00 | 234.11 | 889.25 | +53.90 (1.23x) | 3 チャネル×6 列のワイド文字列グループ化と `y1>0` 先行セミジョイン欠如 | [surrogate-key-channel-pivot-late-dim.md](surrogate-key-channel-pivot-late-dim.md) |
| **Q10** | 25.93 | 20.27 | 104.96 | +5.66 (1.28x) | `EXISTS(store) AND (EXISTS(web) OR EXISTS(catalog))` で既確定行も全チャネル探索 | [disjunctive-exists-cascading-bitset.md](disjunctive-exists-cascading-bitset.md) |
| **Q11** | 190.91 | 145.08 | 471.99 | +45.83 (1.32x) | `store/web` の 3 文字列列早すぎ結合と非選択チャネル全走査 | [surrogate-key-channel-pivot-late-dim.md](surrogate-key-channel-pivot-late-dim.md) |
| **Q14** | 439.90 | 231.14 | 1582.65 | +208.76 (1.90x) | `cross_items` / `avg_sales` / 3 分岐で同一 3 ファクト表を多重走査＋3 列値 `INTERSECT` | [canonical-equiv-id-set-algebra.md](canonical-equiv-id-set-algebra.md), [single-pass-multi-consumer-fact-scan.md](single-pass-multi-consumer-fact-scan.md) |
| **Q15** | 17.76 | 14.19 | 32.48 | +3.57 (1.25x) | クロス表 `OR` 条件をプローブ後 `residual` 評価して次元枝刈りできず | [cross-table-disjunctive-branch-mask.md](cross-table-disjunctive-branch-mask.md) |
| **Q16** | 7.99 | 6.62 | 70.12 | +1.37 (1.21x) | `cs1.wh <> cs2.wh` 自己非等値 `EXISTS` でハッシュ再結合 | [nonequi-self-exists-minmax-witness.md](nonequi-self-exists-minmax-witness.md) |
| **Q17** | 52.69 | 39.39 | 173.08 | +13.30 (1.34x) | 3 段事実結合の中間枝刈り不足と `COUNT/AVG/STDDEV_SAMP` State 重複 | [bidirectional-equijoin-semijoin.md](bidirectional-equijoin-semijoin.md), [chan-welford-shared-variance-state.md](chan-welford-shared-variance-state.md), [unconditional-block-sum-avg-reuse.md](unconditional-block-sum-avg-reuse.md) |
| **Q21** | 24.66 | 8.21 | 432.34 | +16.45 (3.00x) | 1.33 億行 `inventory` に対する直交 `CASE WHEN` 前後分岐の重複計算 | [disjoint-case-pivot-bucket-dispatch.md](disjoint-case-pivot-bucket-dispatch.md) |
| **Q22** | 185.67 | 144.45 | 441.27 | +41.23 (1.29x) | block-lazy 時のゾーンマップ行数上界無効化で 1-pass `ROLLUP` 融合停止 | [immutable-tlcs-zonemap-row-bound.md](immutable-tlcs-zonemap-row-bound.md) |
| **Q25** | 27.27 | 13.29 | 163.68 | +13.98 (2.05x) | `store_sales ⋈ store_returns ⋈ catalog_sales` 3 表鎖の双方向セミジョイン欠如 | [bidirectional-equijoin-semijoin.md](bidirectional-equijoin-semijoin.md) |
| **Q29** | 42.65 | 41.26 | 192.47 | +1.39 (1.03x) | 同上（`ss ⋈ sr ⋈ cs` の範囲を狭める `d2/d3` 選択率が逆流せず） | [bidirectional-equijoin-semijoin.md](bidirectional-equijoin-semijoin.md) |
| **Q30** | 19.15 | 16.25 | 51.24 | +2.90 (1.18x) | `ca_state='GA'` が顧客住所側のため状態別 `AVG` 前への半結合枝刈り未実施 | [correlated-agg-partition-key-filter-pushdown.md](correlated-agg-partition-key-filter-pushdown.md) |
| **Q35** | 95.04 | 50.83 | 106.61 | +44.22 (1.87x) | 選言 `EXISTS` 短絡と `COUNT/MIN/MAX/AVG(cd_dep*)` 状態共有の両方が未適用 | [disjunctive-exists-cascading-bitset.md](disjunctive-exists-cascading-bitset.md), [unconditional-block-sum-avg-reuse.md](unconditional-block-sum-avg-reuse.md) |
| **Q38** | 201.54 | 72.41 | 182.95 | +129.14 (2.78x) | 3 チャネルで `(c_last_name, c_first_name, d_date)` 文字列組を全生成し `INTERSECT` | [canonical-equiv-id-set-algebra.md](canonical-equiv-id-set-algebra.md) |
| **Q39** | 94.60 | 57.48 | 162.11 | +37.12 (1.65x) | 1.33 億行 `inventory` で `STDDEV_SAMP` と `AVG` が別状態＋102 万グループマージ重負荷 | [chan-welford-shared-variance-state.md](chan-welford-shared-variance-state.md) |
| **Q40** | 6.70 | 5.40 | 69.78 | +1.30 (1.24x) | 1 対多でない `catalog_returns` 右一意キーの 1-bit/配列直接参照未適用 | [unique-key-left-join-elision-and-mark.md](unique-key-left-join-elision-and-mark.md) |
| **Q44** | 45.28 | 31.93 | 199.26 | +13.35 (1.42x) | `store_sales` 2880 万行を `V1` / `V2` / 閾値計算の 3 パス独立走査 | [single-pass-multi-consumer-fact-scan.md](single-pass-multi-consumer-fact-scan.md) |
| **Q48** | 40.44 | 37.93 | 176.30 | +2.51 (1.07x) | `cd_* ∧ ca_* ∧ ss_sales_price/net_profit` の 3×3 直交 OR 条件の `residual` 評価 | [cross-table-disjunctive-branch-mask.md](cross-table-disjunctive-branch-mask.md) |
| **Q57** | 50.97 | 45.02 | 315.70 | +5.95 (1.13x) | `v1` の `RANK()` 自己 3 結合による前後月比較と `AVG() OVER` ソート負担 | [rank-self-join-to-lag-lead-and-hash-window.md](rank-self-join-to-lag-lead-and-hash-window.md) |
| **Q58** | 35.87 | 23.74 | 123.88 | +12.13 (1.51x) | 3 チャネルの `±10%` 範囲比率フィルタを事後結合し相互上界絞り込みせず | [bidirectional-equijoin-semijoin.md](bidirectional-equijoin-semijoin.md) |
| **Q59** | 147.34 | 90.95 | 178.57 | +56.39 (1.62x) | 2880 万行の 7 曜日 `SUM(CASE WHEN d_day_name = ...)` が 7 分岐個別評価 | [disjoint-case-pivot-bucket-dispatch.md](disjoint-case-pivot-bucket-dispatch.md) |
| **Q62** | 27.99 | 12.71 | 43.87 | +15.28 (2.20x) | `ws_ship_date_sk - ws_sold_date_sk` の 5 バケット判定の毎回再評価＋5 スロット走査 | [disjoint-case-pivot-bucket-dispatch.md](disjoint-case-pivot-bucket-dispatch.md) |
| **Q63** | 25.62 | 20.24 | 145.45 | +5.38 (1.27x) | `AVG(SUM(ss_sales_price)) OVER (PARTITION BY i_manager_id)` での不要ソート窓実行 | [rank-self-join-to-lag-lead-and-hash-window.md](rank-self-join-to-lag-lead-and-hash-window.md) |
| **Q65** | 162.21 | 55.44 | 282.98 | +106.77 (2.93x) | `sb` と `sc` が同一 `store_sales` 2880 万行集約を 2 回独立実行 | [single-pass-multi-consumer-fact-scan.md](single-pass-multi-consumer-fact-scan.md), [correlated-agg-partition-key-filter-pushdown.md](correlated-agg-partition-key-filter-pushdown.md) |
| **Q66** | 57.35 | 22.24 | 104.58 | +35.11 (2.58x) | `web_sales`＋`catalog_sales` の 12 ヶ月 ×2 指標＝24 列 `SUM(CASE WHEN d_moy=m)` | [disjoint-case-pivot-bucket-dispatch.md](disjoint-case-pivot-bucket-dispatch.md) |
| **Q72** | 277.28 | 87.35 | 15468.28 | +189.93 (3.17x) | `LEFT JOIN catalog_returns`（未使用列）＋`inv < cs` 上界による 1.33 億行枝刈り欠如 | [unique-key-left-join-elision-and-mark.md](unique-key-left-join-elision-and-mark.md) |
| **Q74** | 202.22 | 124.73 | 361.07 | +77.48 (1.62x) | `year_total` CTE で 3 文字列列を含む広域 `GROUP BY`＋4 者自己結合 | [surrogate-key-channel-pivot-late-dim.md](surrogate-key-channel-pivot-late-dim.md) |
| **Q75** | 181.71 | 103.69 | 658.93 | +78.02 (1.75x) | `UNION DISTINCT` への `d_year IN (2001,2002)` 未押込み＋3 返品 `LEFT JOIN` 負荷 | [left-join-preserved-to-nullable-filter-transfer.md](left-join-preserved-to-nullable-filter-transfer.md) |
| **Q78** | 580.22 | 417.52 | 644.27 | +162.70 (1.39x) | `ss_sold_year=2000` を `LEFT JOIN ws / cs` の右側年条件へ伝播せず全年度集約＋境界揺らぎ | [left-join-preserved-to-nullable-filter-transfer.md](left-join-preserved-to-nullable-filter-transfer.md) |
| **Q79** | 51.77 | 40.12 | 149.44 | +11.65 (1.29x) | `GROUP BY (..., s_city)` の文字列集約前持ち上げと OR 条件結合後の評価 | [cross-table-disjunctive-branch-mask.md](cross-table-disjunctive-branch-mask.md), [surrogate-key-channel-pivot-late-dim.md](surrogate-key-channel-pivot-late-dim.md) |
| **Q80** | 40.19 | 40.09 | 241.06 | +0.10 (1.00x) | 3 チャネル `LEFT OUTER JOIN *_returns` が各々フル結合後に 1 ヶ月 `ROLLUP` | [unique-key-left-join-elision-and-mark.md](unique-key-left-join-elision-and-mark.md) |
| **Q81** | 48.91 | 32.43 | 62.07 | +16.48 (1.51x) | `ca_state='GA'` を状態 `ctr_state` の半結合枝刈りに使えず全状態 `AVG` 算出 | [correlated-agg-partition-key-filter-pushdown.md](correlated-agg-partition-key-filter-pushdown.md) |
| **Q83** | 10.91 | 8.21 | 41.38 | +2.70 (1.33x) | 3 返品 CTE を個別集計後結合しており、最狭 `item_id` 集合の他分岐逆流欠如 | [bidirectional-equijoin-semijoin.md](bidirectional-equijoin-semijoin.md) |
| **Q86** | 11.58 | 10.43 | 31.34 | +1.16 (1.11x) | `ROLLUP` 後段の `RANK() OVER (PARTITION BY lochierarchy, ...)` での不要ソート負荷 | [rank-self-join-to-lag-lead-and-hash-window.md](rank-self-join-to-lag-lead-and-hash-window.md) |
| **Q87** | 214.44 | 85.69 | 205.68 | +128.75 (2.50x) | 3 チャネルで `(c_last_name, c_first_name, d_date)` を文字列展開し `EXCEPT` 実行 | [canonical-equiv-id-set-algebra.md](canonical-equiv-id-set-algebra.md) |
| **Q89** | 27.60 | 22.72 | 161.73 | +4.88 (1.21x) | 2880 万行後段の無順序 `AVG() OVER (PARTITION BY i_category, ...)` 窓ソート | [rank-self-join-to-lag-lead-and-hash-window.md](rank-self-join-to-lag-lead-and-hash-window.md) |
| **Q90** | 7.81 | 5.17 | 29.02 | +2.63 (1.51x) | `__merged_scalar_agg` 融合内で `t_hour ∈ [8,9] / [19,20]` を毎回 `CASE` 分岐 | [disjoint-case-pivot-bucket-dispatch.md](disjoint-case-pivot-bucket-dispatch.md) |
| **Q97** | 82.39 | 75.04 | 216.54 | +7.35 (1.10x) | 2 つの巨大 `(customer_sk, item_sk)` 去重テーブルを個別構築後 `FULL OUTER JOIN` | [full-outer-indicator-bitset-complement.md](full-outer-indicator-bitset-complement.md) |
| **Q99** | 67.14 | 15.89 | 70.78 | +51.25 (4.23x) | 5 バケット `SUM(CASE WHEN cs_ship - cs_sold ...)` で差算・判定が 5 系統重複 | [disjoint-case-pivot-bucket-dispatch.md](disjoint-case-pivot-bucket-dispatch.md) |

### C. ClickBench 100M（全 18 敗：対 DuckDB 13 敗 / 対 ClickHouse 8 敗）

| Query | TL (ms) | DD (ms) | CH (ms) | 差 (vs 最速) | 主ボトルネック | 対応 Issue |
|---|---:|---:|---:|---:|---|---|
| **Q09** | 195.85 | 194.28 | 312.06 | +1.57 (1.01x) | 高カーディナリティ `COUNT(DISTINCT UserID)` のワーカー間ハッシュセット統合 | [packed-gid-distinct-sort-dedup.md](packed-gid-distinct-sort-dedup.md) |
| **Q12** | 55.31 | 45.94 | 66.67 | +9.37 (1.20x) | 2 キー `(MobilePhone, MobilePhoneModel)` 辞書上での `COUNT(DISTINCT)` 統合 | [packed-gid-distinct-sort-dedup.md](packed-gid-distinct-sort-dedup.md) |
| **Q14** | 320.21 | 378.02 | 310.31 | +9.90 (1.03x) | 超高カーディナリティ文字列グループ別 `COUNT(DISTINCT UserID)` マージコスト | [packed-gid-distinct-sort-dedup.md](packed-gid-distinct-sort-dedup.md) |
| **Q17** | 458.07 | 454.70 | 507.45 | +3.37 (1.01x) | 全行の 86.8% を占める `SearchPhrase=''` 行に対して毎回複合 `(UserID, Str)` ハッシュ化 | [empty-string-key-partition-split.md](empty-string-key-partition-split.md) |
| **Q18** | 27.07 | 311.13 | 8.66 | +18.41 (3.13x) | `ORDER BY` なし `LIMIT 10` なのに大規模グループ表を走査し続け ClickHouse の早期停止に劣後 | [capped-group-early-freeze.md](capped-group-early-freeze.md) |
| **Q20** | 12.12 | 4.42 | 8.32 | +7.69 (2.74x) | `UserID = 435090932899640449` 点検索でブロック bitwise AND/OR マスクによるスキップなし | [late-wide-row-topn-block-fence.md](late-wide-row-topn-block-fence.md) |
| **Q22** | 384.20 | 340.61 | 579.91 | +43.59 (1.13x) | 安価な `SearchPhrase <> ''`（86.8% 除外）より先に長大 `URL LIKE '%google%'` を評価 | [empty-string-key-partition-split.md](empty-string-key-partition-split.md) |
| **Q24** | 35.93 | 26.02 | 70.05 | +9.91 (1.38x) | `SELECT * ... ORDER BY EventTime LIMIT 10` で上位確定後も 105 列ロード/フィルタ継続 | [late-wide-row-topn-block-fence.md](late-wide-row-topn-block-fence.md) |
| **Q28** | 181.75 | 237.71 | 96.78 | +84.97 (1.88x) | `AVG(BYTE_LENGTH(URL))` がブロック先頭・末尾オフセット `ends.back()` の `O(1)` 差算未活用 | [varchar-offset-only-length-sum.md](varchar-offset-only-length-sum.md) |
| **Q31** | 161.71 | 183.84 | 142.07 | +19.64 (1.14x) | 多数グループで `(SearchEngineID, ClientIP)` と `AVG(CAST(Width AS FLOAT))` の表現変換コスト | [empty-string-key-partition-split.md](empty-string-key-partition-split.md) |
| **Q32** | 244.37 | 223.57 | 166.24 | +78.13 (1.47x) | ほぼ全行ユニークな `(WatchID, ClientIP)` に加え `AVG` の毎行 `double` 変換オーバーヘッド | [empty-string-key-partition-split.md](empty-string-key-partition-split.md) |
| **Q36** | 144.43 | 191.92 | 128.11 | +16.31 (1.13x) | `GROUP BY ClientIP, ClientIP-1, ClientIP-2, ClientIP-3` の冗長 4 キー計算・ハッシュ化 | [functional-dependent-group-key-elim.md](functional-dependent-group-key-elim.md) |
| **Q37** | 25.72 | 15.48 | 18.78 | +10.24 (1.66x) | 6 整数フィルタ後の疎行に対して `URL <> ''` / `GROUP BY URL` の全ブロック一括デコード | [selective-filter-late-slice-group.md](selective-filter-late-slice-group.md) |
| **Q38** | 7.06 | 2.52 | 10.23 | +4.55 (2.81x) | 整数条件後わずかな生存行に対し `Title` 列オフセット／スライス直利用ではなく全展開 | [selective-filter-late-slice-group.md](selective-filter-late-slice-group.md) |
| **Q40** | 35.40 | 24.24 | 44.55 | +11.16 (1.46x) | 98.7% が空文字へ落ちる `CASE ... THEN Referer ELSE ''` の不要参照＋`OFFSET 1000` | [selective-filter-late-slice-group.md](selective-filter-late-slice-group.md), [offset-heavy-reverse-topn-and-quotient-bucket.md](offset-heavy-reverse-topn-and-quotient-bucket.md) |
| **Q41** | 2.77 | 1.72 | 4.77 | +1.05 (1.61x) | `ORDER BY PageViews DESC LIMIT 10 OFFSET 100` がヒープ過大管理・配列末尾逆順未活用 | [offset-heavy-reverse-topn-and-quotient-bucket.md](offset-heavy-reverse-topn-and-quotient-bucket.md) |
| **Q42** | 1.66 | 1.55 | 3.76 | +0.11 (1.07x) | 総グループ `10,948` に対し `OFFSET 10000 LIMIT 10` を DESC 10,010 件ヒープ保持（ASC 948 件で済む） | [offset-heavy-reverse-topn-and-quotient-bucket.md](offset-heavy-reverse-topn-and-quotient-bucket.md) |
| **Q43** | 1.79 | 1.78 | 3.02 | +0.02 (1.01x) | `TIMESTAMP_TRUNC(EventTime, MINUTE)` 2 日間分のハッシュ＋ソートを密配列バケット `O(1)` 化未実装 | [offset-heavy-reverse-topn-and-quotient-bucket.md](offset-heavy-reverse-topn-and-quotient-bucket.md) |
