# M:N 内部結合越し両側事前集約（Bilateral Eager Aggregation Pushdown）

**未証明。** グループキーが左表のみに存在し、集約式が両側表を跨ぐ M:N 等値内部結合 `(L ⋈_{L.k = R.k} R)` 上の `(COUNT(*), SUM(L.v), SUM(R.v))` 集約では、`M × N` ペアを展開せずに `L` を `(g, k)` 単位、`R` を `k` 単位でそれぞれ `(COUNT(*), COUNT(v), SUM(v))` へ独立事前集約し、`cL * cR` / `sL * cR` / `cL * sR` の直積和で合算する双方向プッシュダウンが、全 NULL・ゼロ相殺・空グループ・NULL 結合キーを含め完全に同値であることを検証する。

## 対象敗北クエリとボトルネック

- **H2O H5** (`162.6 ms` [srv `117.1 ms`] vs DuckDB `7.7 ms` / ClickHouse `58.5 ms`, **`21.09x` 差**): `x(20万行) ⋈ y(12万行) ON x.id4 = y.id4` で `23,996,360` 回の結合比較を実行（DuckDB は結合前に `y` を 1,000 行、`x` を 127,416 行へ事前集約）。
- **STATS-CEB C1** (`8.6 ms` [srv `10.0 ms`] vs DuckDB `7.3 ms`, `1.19x` 差): `posts ⋈ comments ⋈ votes` で `879,203` 回の結合比較を実行。
- **LDBC-SNB L2** (`41.7 ms` [srv `30.0 ms`] vs ClickHouse `38.5 ms` / DuckDB `44.7 ms`, `1.08x` 差): `forum ⋈ forum_member ⋈ person ⋈ message` で `m.score` と多重度を掛け合わせる結合に `2,721,858` 回の比較。

## 提出型

[`BilateralEagerAggJoinPushdown.Goal`](../../../TinylambOpt/ResearchGoals/BilateralEagerAggJoinPushdown.lean) を import し、`theorem sound : TinylambOpt.ResearchGoals.BilateralEagerAggJoinPushdown.Goal` を提出する。

`∀ (ls : List LRow) (rs : List RRow) (g : Int), optGroup ls rs g = refGroup ls rs g` を要求し、`COUNT(*)`・`SUM(L.v)`・`SUM(R.v)` の 3 指標すべてが参照実装と一致することを示す。

## 境界例と適用範囲

- ある結合キー `k` 上で `L.v` が全 `none`（`nnL = 0`）であっても、`R.v` に非 NULL があれば `SUM(R.v)` は `some ...`・`SUM(L.v)` は `none` となる（グループ内での独立 NULL 判定が必須）。
- 結合後に正負の和が相殺して `0` になった場合は `some 0`、そもそも非 NULL 値が 0 件なら `none` を返す必要がある（`nn == 0` 判定）。
- 結合キーが `none`（NULL）の行は事前集約・結合のいずれからも除外される。

## 実装との照合先

- `plan/cascades.cpp` (`eager_aggregation_over_join`)
- `executor/detail/fused_grouped_join.cpp` (2-table bilateral partial agg hook)
