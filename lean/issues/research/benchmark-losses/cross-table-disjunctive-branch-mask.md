# 次元・ファクト跨ぎ選言結合の 3 値ビットマスク事前枝刈り＋1 命令プローブ

**未証明。** 次元側述語 `pD_j` とファクト側述語 `pF_j` を含む選言結合条件 `(pD_0 ∧ pF_0) ∨ ... ∨ (pD_{k-1} ∧ pF_{k-1})` について、結合前に各次元行にビットマスク `maskD`（`pD_j(d) = some true` ならビット `j` を立てる）を事前計算して `maskD = 0` の次元行を即座にドロップし、ファクト走査時は `(maskD &&& maskF) ≠ 0` の 1 命令ビット演算だけで判定しても 3 値論理 `NULL` を含め完全同値であることを検証する。

## 対象敗北クエリとボトルネック

- **TPC-DS Q48** (`40.44 ms` vs DuckDB `37.93 ms`)・**TPC-DS Q15** (`17.76 ms` vs DuckDB `14.19 ms`)・**TPC-DS Q79** (`51.77 ms` vs DuckDB `40.12 ms`): 次元表（`customer_demographics`, `customer_address`, `household_demographics`）とファクト表（`store_sales`, `catalog_sales`）に跨がる OR 条件が結合後の `residual` 評価として残存し、無関係な次元行の枝刈りもプローブの高速ビット比較も行えていない。

## 提出型

[`CrossTableDisjunctiveBranchMask.Goal`](../../../TinylambOpt/ResearchGoals/CrossTableDisjunctiveBranchMask.lean) を import し、`theorem sound : TinylambOpt.ResearchGoals.CrossTableDisjunctiveBranchMask.Goal` を提出する。

任意の 3 値述語リスト `branches : List ((D → Option Bool) × (F → Option Bool))` について `optMaskedJoin ds fs branches = refJoin ds fs branches` が成立することを要求する。

## 境界例と適用範囲

- ある分岐 `j` で `pD_j(d) = none`（SQL UNKNOWN）または `some false` のとき、他の結合側が `some true` であっても `sqlAnd (pD_j d) (pF_j f)` は決して `some true` にならないため、ビットを立てず `maskD = 0` を事前破棄しても絶対に取りこぼしが起きない。

## 実装との照合先

- `executor/detail/fused_grouped_join.cpp` (`residual_predicate`)
- `query/select_rewrites.cpp`
