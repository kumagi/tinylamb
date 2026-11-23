# `RANK()` 自己結合の隣接 `LAG`/`LEAD` 還元と無順序パーティション窓のハッシュ集約

**未証明。** (1) 各パーティション内の順序キー `(part, ord)` が一意（`UniquePartOrd`）なテーブル上で `RANK()` 順位の `rn = rn_lag + 1 ∧ rn = rn_lead - 1` による 3 者自己結合は、単一のソート済みパーティション線形走査で隣接要素 `(a.val, c.val)` を読む `LAG`/`LEAD` 1 パスと一致すること（重複同順位がある場合は `RANK()` の順位飛びにより不一致となる反例付き）、および (2) `ORDER BY` なしの `AVG(x) OVER (PARTITION BY part)` はソート無しハッシュ集約と一致することを検証する。

## 対象敗北クエリとボトルネック

- **TPC-DS Q57** (`50.97 ms` vs DuckDB `45.02 ms`, `+5.95 ms`): `v1` 上で `AVG() OVER` と `RANK() OVER` を評価した結果を `v1, v1_lag, v1_lead` に 3 重自己結合している。
- **TPC-DS Q63** (`25.62 ms` vs DuckDB `20.24 ms`)・**Q89** (`27.60 ms` vs `22.72 ms`)・**Q86** (`11.58 ms` vs `10.43 ms`): 順序フレームを持たないパーティション集約窓や小グループ `ROLLUP` 後窓で不要なソート付きウィンドウ演算子が走っている。

## 提出型

[`RankSelfJoinToLagLeadAndHashWindow.Goal`](../../../TinylambOpt/ResearchGoals/RankSelfJoinToLagLeadAndHashWindow.lean) を import し、`theorem sound : TinylambOpt.ResearchGoals.RankSelfJoinToLagLeadAndHashWindow.Goal` を提出する。

`UniquePartOrd` 下の `optLagLeadSweep rows = refRankSelfJoin rows`、無順序パーティション窓のハッシュ vs ソート同値性、および同率タイ (`tiedRows`) で `RANK()` が壊れる存在反例を要求する。

## 境界例と適用範囲

- 同率 `ord = 10` が 2 件ある `[10, 10, 20]` では `RANK()` が `[1, 1, 3]` になり `rn = 2` が存在しないため、自己結合は 0 行になるが隣接走査は 1 行出してしまう。よって `UniquePartOrd`（`GROUP BY d_year, d_moy` 等）確認時のみ還元する。

## 実装との照合先

- `executor/detail/window_eval.cpp` / `executor/detail/window_eval_columnar.cpp`
- `plan/window_plan.cpp` / `query/select_rewrites.cpp`
