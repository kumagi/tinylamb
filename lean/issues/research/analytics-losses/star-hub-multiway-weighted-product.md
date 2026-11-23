# スター型ハブ FK-FK 3 表結合の独立枝積因子化（Star-Hub Multiway Weighted Product Factorization）

**未証明。** ハブ表 `H` の同一結合キー `k` に 2 つの枝表 `B1`（集約値列 `v` を持つ）と `B2`（重み・フィルタのみ）が同時に FK-FK 多対多結合する `(H ⋈_k B1) ⋈_k B2` 上の `GROUP BY H.g`（`COUNT(*), COUNT(B1.v), SUM(B1.v)`）では、3 表の巨大直積ペアを展開する代わりに、`B1` をキー `k` ごとに `(c1, nn1, s1)`、`B2` をキー `k` ごとに多重度 `c2` へ独立事前集約し、`H` の 1 パス走査で `c1 * c2`・`nn1 * c2`・`s1 * c2` を加算するだけで完全同値になることを検証する。

## 対象敗北クエリとボトルネック

- **STATS-CEB C1** (`8.6 ms` [srv `10.0 ms`] vs DuckDB `7.3 ms`, `1.19x` 差): `posts p ⋈ comments c ON p.id = c.post_id ⋈ votes v ON p.id = v.post_id` で `c` と `v` の FK-FK 直積 `879,203` 件を展開している（`v` は `vote_type = 1` のフィルタ後の多重度のみ必要、`c` は `SUM(c.score)` のみ必要）。
- **STATS-CEB C5** (`5.3 ms` [srv `5.9 ms`] vs DuckDB `4.8 ms`, `1.09x` 差): `users u ⋈ posts p ⋈ badges b`（`u.id` ハブへの `posts`・`badges` FK-FK 結合、`277,573` 比較）。
- **JOB J5** (`2.8 ms` vs DuckDB `2.7 ms`, `1.03x` 差): `title t ⋈ aka_title a2 ⋈ movie_info_idx mi`（`t.id` ハブへの 2 枝 FK-FK 結合）。
- **LDBC-SNB L2** (`41.7 ms` vs ClickHouse `38.5 ms`, `1.08x` 差): `forum f ⋈ forum_member fm ⋈ message m`（`f.id` ハブへの `fm × m` FK-FK 直積、`2,721,858` 比較）。
- **BigBench B3** (`3.7 ms` vs DuckDB `3.2 ms`, `1.14x` 差): `store_sales` ハブへの次元枝乗数。

## 提出型

[`StarHubMultiwayWeightedProduct.Goal`](../../../TinylambOpt/ResearchGoals/StarHubMultiwayWeightedProduct.lean) を import し、`theorem sound : TinylambOpt.ResearchGoals.StarHubMultiwayWeightedProduct.Goal` を提出する。

`∀ hs b1s b2s g, optStarHub hs b1s b2s g = refStarHub hs b1s b2s g` を示す。

## 境界例と適用範囲

- ハブ `H` 自体にも同じ `k` が重複して登場するケース（非 PK の一般 FK-FK ハブ）や、`k = none` の行、`B1.v` が全 NULL で `nn1 = 0`（`SUM` は `none` だが `COUNT(*)` には `c1 * c2` が加算される）でも厳密一致する。
- 一方の枝 `B2` がフィルタ後に空になった場合、`c2 = 0` によりハブ上の積が 0 となり `(0, 0, none)` が保たれる。

## 実装との照合先

- `executor/detail/fused_grouped_join.cpp` (multi-join greedy hook & FK-FK pair expansion)
- `plan/cascades.cpp` (`eager_aggregation_over_join`)
