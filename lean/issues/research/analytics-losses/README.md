# 派生分析ベンチマーク 5 種（JOB / STATS-CEB / H2O / LDBC-SNB / BigBench）全 15 敗北クエリの未証明研究課題（第六波）

**更新: 以下 18 件はすべて `theorem sound : Goal` を証明済み**（登録は [../RESULTS.md](../RESULTS.md) の第六波の項）。

`benchmark/comparison/analytics_bench.py`（SF=1、全 5 ファミリ 26 クエリ、総行数約 182 万行）について、当初ご提示の Debug ビルド（`build-verify/tinylamb_server`, `-O0 -g`, `ANALYZE`/TLCS 未実行）での大敗（全 26 問合計 `5,869.6 ms` vs DuckDB `253.9 ms` / ClickHouse `532.6 ms`）からビルド条件とストレージ条件を切り分けた上で、**正当な Release ビルド（`build-tpcds-release/tinylamb_server`, `-O3 -DNDEBUG`, `mimalloc`, `-march=native`, LTO）＋ `TINYLAMB_ANALYZE_AUTO_TLCS=1`＋ロード後 `ANALYZE`（全 36 テーブルを TLCS 化・統計収集）** で再計測した結果をベースラインとして起票した。

## 計測条件とファミリ別サマリ（Release + `ANALYZE` + TLCS, SF=1, 3DB ローテーション計測）

- Tinylamb: `build-tpcds-release/tinylamb_server --read-workers 2`（`TINYLAMB_ANALYZE_AUTO_TLCS=1`、`ANALYZE` 後にウォームアップ 1 回＋本計測 3 回の中央値、`psycopg2` 経由クライアント実測、併せて `EXPLAIN ANALYZE Execution Time` を記録）
- DuckDB: `duckdb 2.0.0`（組込み、`SET threads=2`）
- ClickHouse: `26.10.2.103`（HTTP 経由、`max_threads=2`）
- 結果検証: 全 26 クエリで 3DB 間の結果多重集合が完全一致。
- 勝敗内訳: **11 勝 15 敗**（vs 最速 DB `min(DD, CH)`。うち 4 クエリ `H5`, `C4`, `L1`, `B2` だけで総遅延の大部分を占有、さらに `H2`, `H4`, `B1`, `L2` に PostgreSQL テキストワイヤ＋`Row` 実体化コストが集中）。

| ファミリ | 問題数 | 敗北数 | TL (ms) | DD (ms) | CH (ms) | TL / DD | TL / CH | 主な課題群 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| `job` (Join Order) | 5 | 2 (`J4, J5`) | 18.4 | 17.9 | 48.8 | `1.03x` | **`0.38x`** | FK-FK 多重度展開・推移等価キー・小次元直接索引 |
| `stats_ceb` (Cardinality) | 5 | 4 (`C1, C2, C4, C5`) | 123.5 | 23.8 | 34.5 | `5.18x` | `3.57x` | **C4 (`103.5ms`)** 左外部結合多重度縮約・文字列 residual 除去・ファンアウト重み並列化 |
| `h2o` (GROUP BY / JOIN) | 6 | 3 (`H2, H4, H5`) | 212.9 | 29.5 | 131.7 | `7.22x` | `1.62x` | **H5 (`162.6ms`)** 両側事前集約・**H2/H4** 2 キー密配列集約＋ワイヤ直列化 |
| `ldbc_snb` (Graph BI) | 5 | 3 (`L1, L2, L5`) | 130.0 | 67.8 | 81.5 | `1.92x` | `1.60x` | **L1 (`81.9ms`)** 因子化結合木重み伝播・ブッシー再結合・自己結合共有走査 |
| `bigbench` (Web / Retail) | 5 | 3 (`B1, B2, B3`) | 80.3 | 29.5 | 52.7 | `2.72x` | `1.52x` | **B2 (`44.7ms`)** 右列非参照重み・文字列 residual・**B1** 局所辞書 2 段集約＋ワイヤ |

*(対比参考: `build-verify` Debug `-O0` ＋ `ANALYZE`/TLCS 無しでは `job=131.3`, `stats_ceb=1615.3`, `h2o=2419.4`, `ldbc_snb=991.9`, `bigbench=711.6 ms`。Release + `ANALYZE` + TLCS により約 **10.4 倍**短縮した上で残った本質ボトルネックを以下に分解している。)*

---

## 起票した 18 件の未証明命題と提出型

すべての提出型は [`lean/TinylambOpt.lean`](../../../TinylambOpt.lean) で import され、[`CheckProofs.lean`](../../../CheckProofs.lean) と共存可能な `def Goal : Prop := ...`（`sorry` なし）として型検査済み。

### 1. 両側／左外部／未参照側 事前集約・結合残差・並列分割（8 件：`H5, C4, B2, J4, J5, C1, C5, L2, L5`）

- [M:N 内部結合越し両側事前集約（Bilateral Eager Agg）](bilateral-eager-agg-join-pushdown.md) (`BilateralEagerAggJoinPushdown`)
- [セミジョイン枝刈り先行型・検査付き事前集約 `SUM`](semijoin-guarded-checked-eager-sum.md) (`SemijoinGuardedCheckedEagerSum`)
- [右表列非参照 `LEFT OUTER JOIN` の多重度重み縮約](left-join-right-multiplicity-collapse.md) (`LeftJoinRightMultiplicityCollapse`)
- [バイナリ照合文字列等値キーの結合残差述語除去](binary-collation-string-equi-join-elision.md) (`BinaryCollationStringEquiJoinElision`)
- [ファンアウト重み付きプローブ／バケット並列分割と外部結合未マッチ集約](fanout-weighted-bucket-work-split.md) (`FanoutWeightedBucketWorkSplit`)
- [`(col IS NULL OR LIKE ...)` の辞書ビットマップ一括先行判定](null-or-like-dict-bitmap-prefilter.md) (`NullOrLikeDictBitmapPrefilter`)
- [右表列非参照内部結合のキー多重度重みプッシュダウン](inner-join-unreferenced-side-weight-pushdown.md) (`InnerJoinUnreferencedSideWeightPushdown`)
- [短固定長文字列の 64-bit Prefix+長ビットパック等価表現](compressed-string-prefix-hash-equiv.md) (`CompressedStringPrefixHashEquiv`)

### 2. 多表結合木・ブッシー再結合・推移閉包・自己結合・小次元直接索引（6 件：`L1, C1, C2, C5, J4, J5, L2, L5, B3`）

- [非巡回多段結合木のボトムアップ重み伝播（Factorized Join Tree）](factorized-join-tree-count-propagation.md) (`FactorizedJoinTreeCountPropagation`)
- [鎖結合のブッシー再結合と死キー列即時枝刈り](bushy-join-associativity-with-dead-key-prune.md) (`BushyJoinAssociativityWithDeadKeyPrune`)
- [スター型ハブ FK-FK 3 表結合の独立枝積因子化](star-hub-multiway-weighted-product.md) (`StarHubMultiwayWeightedProduct`)
- [等値結合推移閉包の先行セミジョイン枝刈りと重複結合キー除去](transitive-equiv-predicate-pushdown-and-dup-key-drop.md) (`TransitiveEquivPredicatePushdownAndDupKeyDrop`)
- [自己結合表の 1 パス共有走査＋2 ロールビットマスク分割](self-join-shared-scan-dual-filter-split.md) (`SelfJoinSharedScanDualFilterSplit`)
- [小整数次元キーのハッシュレス密配列直接索引結合](small-dim-dense-array-direct-lookup.md) (`SmallDimDenseArrayDirectLookup`)

### 3. 複数キー配列集約・局所辞書・基数ソート・PostgreSQL ワイヤ直列化（4 件：`H2, H4, B1, L2, C2`）

- [境界既知・複数キー集約の番兵スロット付き並列平坦配列直集計](bounded-multi-key-direct-array-aggregate.md) (`BoundedMultiKeyDirectArrayAggregate`)
- [複数ブロック局所辞書のコード添字 2 段階集約](multi-block-local-dict-two-stage-aggregate.md) (`MultiBlockLocalDictTwoStageAggregate`)
- [符号反転ビット射影＋安定 LSB 基数ソートによる大規模降順整列](bounded-count-radix-desc-sort-equiv.md) (`BoundedCountRadixDescSortEquiv`)
- [列指向結果から PostgreSQL `DataRow` チャンク直列化](columnar-to-pg-wire-chunked-framing-equiv.md) (`ColumnarToPgWireChunkedFramingEquiv`)

---

## 全 15 敗北クエリ完全網羅マトリクス（＋11 勝クエリ参考一覧）

単位は ms（`TL`=Tinylamb クライアント、`srv`=サーバ内 `EXPLAIN ANALYZE Execution Time`、`DD`=DuckDB、`CH`=ClickHouse、`vs 最速`=`TL / min(DD, CH)`）。

| Family | Query | 判定 | TL (ms) | srv (ms) | DD (ms) | CH (ms) | 差 / 倍率 (vs 最速) | rows | 主ボトルネック | 対応 Issue |
| --- | --- | --- | ---: | ---: | ---: | ---: | --- | ---: | --- | --- |
| `job` | **J4** | LOSS | 8.8 | 11.0 | 5.8 | 13.2 | +3.0 (1.52x) | 8 | `movie_keyword ⋈ movie_info` FK-FK 展開 (`747,046` 比較)＋文字列/LIKE | [inner-join-unreferenced-side-weight-pushdown.md](inner-join-unreferenced-side-weight-pushdown.md), [factorized-join-tree-count-propagation.md](factorized-join-tree-count-propagation.md), [null-or-like-dict-bitmap-prefilter.md](null-or-like-dict-bitmap-prefilter.md), [transitive-equiv-predicate-pushdown-and-dup-key-drop.md](transitive-equiv-predicate-pushdown-and-dup-key-drop.md), [small-dim-dense-array-direct-lookup.md](small-dim-dense-array-direct-lookup.md) |
| `job` | **J5** | LOSS | 2.8 | 3.3 | 2.7 | 5.4 | +0.1 (1.03x) | 1 | `HashJoin keys=2` (`a2.movie_id = mi.movie_id`)・3 表ハブ直積展開 | [inner-join-unreferenced-side-weight-pushdown.md](inner-join-unreferenced-side-weight-pushdown.md), [transitive-equiv-predicate-pushdown-and-dup-key-drop.md](transitive-equiv-predicate-pushdown-and-dup-key-drop.md), [star-hub-multiway-weighted-product.md](star-hub-multiway-weighted-product.md) |
| `stats_ceb` | **C1** | LOSS | 8.6 | 10.0 | 7.3 | 10.3 | +1.4 (1.19x) | 2 | `posts ⋈ comments ⋈ votes` FK-FK (`879,203` 比較)・`keys=2` | [star-hub-multiway-weighted-product.md](star-hub-multiway-weighted-product.md), [bilateral-eager-agg-join-pushdown.md](bilateral-eager-agg-join-pushdown.md), [semijoin-guarded-checked-eager-sum.md](semijoin-guarded-checked-eager-sum.md), [transitive-equiv-predicate-pushdown-and-dup-key-drop.md](transitive-equiv-predicate-pushdown-and-dup-key-drop.md) |
| `stats_ceb` | **C2** | LOSS | 3.9 | 4.1 | 3.8 | 8.3 | +0.1 (1.03x) | 3514 | `users` 自己結合 2 回走査＋4 表鎖左深木＋3,514 行ソート・ワイヤ | [bushy-join-associativity-with-dead-key-prune.md](bushy-join-associativity-with-dead-key-prune.md), [self-join-shared-scan-dual-filter-split.md](self-join-shared-scan-dual-filter-split.md), [small-dim-dense-array-direct-lookup.md](small-dim-dense-array-direct-lookup.md), [bounded-count-radix-desc-sort-equiv.md](bounded-count-radix-desc-sort-equiv.md), [columnar-to-pg-wire-chunked-framing-equiv.md](columnar-to-pg-wire-chunked-framing-equiv.md) |
| `stats_ceb` | **C4** | LOSS | **103.5** | **109.5** | 5.5 | 4.4 | **+99.1 (23.52x)** | 1 | `residual=1` (`p.tags = t.tag_name`) × `1,144,500` ペアを **`workers=1`** で評価＋非参照左外部結合展開 | [left-join-right-multiplicity-collapse.md](left-join-right-multiplicity-collapse.md), [binary-collation-string-equi-join-elision.md](binary-collation-string-equi-join-elision.md), [fanout-weighted-bucket-work-split.md](fanout-weighted-bucket-work-split.md), [null-or-like-dict-bitmap-prefilter.md](null-or-like-dict-bitmap-prefilter.md) |
| `stats_ceb` | **C5** | LOSS | 5.3 | 5.9 | 4.8 | 5.5 | +0.4 (1.09x) | 36 | `users ⋈ posts ⋈ badges` 3 表ハブ展開 (`277,573` 比較)・`keys=2` | [star-hub-multiway-weighted-product.md](star-hub-multiway-weighted-product.md), [inner-join-unreferenced-side-weight-pushdown.md](inner-join-unreferenced-side-weight-pushdown.md), [transitive-equiv-predicate-pushdown-and-dup-key-drop.md](transitive-equiv-predicate-pushdown-and-dup-key-drop.md), [bounded-multi-key-direct-array-aggregate.md](bounded-multi-key-direct-array-aggregate.md) |
| `h2o` | **H2** | LOSS | 29.0 | 11.5 | 10.9 | 31.9 | +18.1 (2.66x) | 37709 | 2 キー `(id1, id2)` 汎用ハッシュ集約＋3.77 万行ソート＋**PG ワイヤ (+17.5ms)** | [bounded-multi-key-direct-array-aggregate.md](bounded-multi-key-direct-array-aggregate.md), [bounded-count-radix-desc-sort-equiv.md](bounded-count-radix-desc-sort-equiv.md), [columnar-to-pg-wire-chunked-framing-equiv.md](columnar-to-pg-wire-chunked-framing-equiv.md) |
| `h2o` | **H4** | LOSS | 18.2 | **5.4** | 5.9 | 31.1 | +12.3 (3.08x) | 29057 | サーバ内は既に DuckDB 超え（5.4 vs 5.9 ms）、**2.9 万行 PG ワイヤ (+12.8ms)** と 2 キー配列 | [columnar-to-pg-wire-chunked-framing-equiv.md](columnar-to-pg-wire-chunked-framing-equiv.md), [bounded-multi-key-direct-array-aggregate.md](bounded-multi-key-direct-array-aggregate.md) |
| `h2o` | **H5** | LOSS | **162.6** | **117.1** | 7.7 | 58.5 | **+154.9 (21.09x)** | 400 | `x ⋈ y ON id4` で **`23,996,360`** ペア展開（両側事前集約欠如＋検査付き SUM ガード） | [bilateral-eager-agg-join-pushdown.md](bilateral-eager-agg-join-pushdown.md), [semijoin-guarded-checked-eager-sum.md](semijoin-guarded-checked-eager-sum.md), [fanout-weighted-bucket-work-split.md](fanout-weighted-bucket-work-split.md) |
| `ldbc_snb` | **L1** | LOSS | **81.9** | **49.2** | 16.0 | 29.4 | **+65.9 (5.12x)** | 10 | 左深木で `person_knows ⋈ person_likes` **243 万行・12 列実体化**・自己結合 2 回走査＋ワイヤ | [factorized-join-tree-count-propagation.md](factorized-join-tree-count-propagation.md), [bushy-join-associativity-with-dead-key-prune.md](bushy-join-associativity-with-dead-key-prune.md), [self-join-shared-scan-dual-filter-split.md](self-join-shared-scan-dual-filter-split.md), [transitive-equiv-predicate-pushdown-and-dup-key-drop.md](transitive-equiv-predicate-pushdown-and-dup-key-drop.md) |
| `ldbc_snb` | **L2** | LOSS | 41.7 | 30.0 | 44.7 | 38.5 | +3.2 (1.08x) | 25779 | `forum ⋈ fm ⋈ m` で **`2,721,858`** 比較 (`keys=2`)＋2.58 万行ソート＋ワイヤ (+11.7ms) | [bilateral-eager-agg-join-pushdown.md](bilateral-eager-agg-join-pushdown.md), [semijoin-guarded-checked-eager-sum.md](semijoin-guarded-checked-eager-sum.md), [star-hub-multiway-weighted-product.md](star-hub-multiway-weighted-product.md), [bounded-multi-key-direct-array-aggregate.md](bounded-multi-key-direct-array-aggregate.md), [bounded-count-radix-desc-sort-equiv.md](bounded-count-radix-desc-sort-equiv.md), [columnar-to-pg-wire-chunked-framing-equiv.md](columnar-to-pg-wire-chunked-framing-equiv.md) |
| `ldbc_snb` | **L5** | LOSS | 3.2 | 3.2 | 2.7 | 3.5 | +0.5 (1.19x) | 10 | 小次元直接索引＆右非参照 `person_knows` 重み縮約未適用 | [small-dim-dense-array-direct-lookup.md](small-dim-dense-array-direct-lookup.md), [inner-join-unreferenced-side-weight-pushdown.md](inner-join-unreferenced-side-weight-pushdown.md), [factorized-join-tree-count-propagation.md](factorized-join-tree-count-propagation.md) |
| `bigbench` | **B1** | LOSS | 29.0 | 15.2 | 11.5 | 15.5 | +17.4 (2.51x) | 19646 | `GROUP BY dest_url` が 2 段階局所辞書集約 (`dictionary_aggregations=0`) に乗らず＋**PG ワイヤ (+13.8ms)** | [multi-block-local-dict-two-stage-aggregate.md](multi-block-local-dict-two-stage-aggregate.md), [compressed-string-prefix-hash-equiv.md](compressed-string-prefix-hash-equiv.md), [bounded-count-radix-desc-sort-equiv.md](bounded-count-radix-desc-sort-equiv.md), [columnar-to-pg-wire-chunked-framing-equiv.md](columnar-to-pg-wire-chunked-framing-equiv.md) |
| `bigbench` | **B2** | LOSS | **44.7** | **40.2** | 10.6 | 17.4 | **+34.1 (4.21x)** | 5 | `u.dest_url = k.page_url` 文字列 `residual=1` × **`1,476,707`** ペア全展開 | [binary-collation-string-equi-join-elision.md](binary-collation-string-equi-join-elision.md), [inner-join-unreferenced-side-weight-pushdown.md](inner-join-unreferenced-side-weight-pushdown.md), [compressed-string-prefix-hash-equiv.md](compressed-string-prefix-hash-equiv.md), [fanout-weighted-bucket-work-split.md](fanout-weighted-bucket-work-split.md), [semijoin-guarded-checked-eager-sum.md](semijoin-guarded-checked-eager-sum.md) |
| `bigbench` | **B3** | LOSS | 3.7 | 3.6 | 3.2 | 9.5 | +0.4 (1.14x) | 120 | 3 小次元表（`store/item/customer`）との汎用ハッシュプローブ (`167,706` 比較) | [small-dim-dense-array-direct-lookup.md](small-dim-dense-array-direct-lookup.md), [bounded-multi-key-direct-array-aggregate.md](bounded-multi-key-direct-array-aggregate.md), [star-hub-multiway-weighted-product.md](star-hub-multiway-weighted-product.md) |

### 参考：勝利した 11 クエリ（Tinylamb 最速）

`job:J1` (TL `2.1` vs DD `2.1` / CH `10.1`, `0.99x`), `job:J2` (`2.3` vs `4.0` / `6.7`, `0.59x`), `job:J3` (`2.4` vs `3.3` / `13.3`, `0.72x`), `stats_ceb:C3` (`2.2` vs `2.4` / `6.1`, `0.89x`), `h2o:H1` (`0.4` vs `1.3` / `2.4`, **`0.27x`**), `h2o:H3` (`1.3` vs `2.2` / `4.6`, `0.61x`), `h2o:H6` (`1.3` vs `1.4` / `3.2`, `0.91x`), `ldbc_snb:L3` (`2.2` vs `2.9` / `5.0`, `0.74x`), `ldbc_snb:L4` (`1.0` vs `1.5` / `5.2`, `0.69x`), `bigbench:B4` (`1.3` vs `1.9` / `5.3`, `0.70x`), `bigbench:B5` (`1.6` vs `2.2` / `5.1`, `0.74x`)。
