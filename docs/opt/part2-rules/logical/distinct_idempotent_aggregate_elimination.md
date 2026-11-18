# distinct_idempotent_aggregate_elimination

- 状態: draft   /   執筆基準リビジョン: `9ce7837` + Lean拡張 (2026-11-18)
- 定義位置: `plan/cascades.cpp` の `RuleSet::Default()`（登録名 `"distinct_idempotent_aggregate_elimination"`）、および `query/select_rewrites.cpp` の `StripIdempotentDistinctAggregates` / `executor/detail/aggregate_accumulator.cpp` の `AggregateAccumulator::AnalyzeShape`
- Lean 形式証明: `lean/TinylambOpt/AvgMinMaxHavingDistinct.lean`（定理 `min_max_distinct_elim_sound`）

## 概要

べき等（idempotent）かつ可換・結合的な集約演算子（`MIN`, `MAX`, `LOGICAL_AND`, `LOGICAL_OR`, `BIT_AND`, `BIT_OR`）に付与された冗長な `DISTINCT` 修飾子を除去します。

$$\text{MIN}(\text{DISTINCT } x) \equiv \text{MIN}(x), \qquad \text{MAX}(\text{DISTINCT } x) \equiv \text{MAX}(x)$$

ハッシュ集合による重複排除を省略できるだけでなく、並列 `HashAggregate` の部分マージ、OOC 部分状態のスピル直列化、および `aggregate_union_transpose` / `eager_aggregation_over_join` の後続発火を連鎖的に解禁します。

## 適用条件と安全性制約

1. 対象集約型が `kMin`, `kMax`, `kLogicalAnd`, `kLogicalOr`, `kBitAnd`, `kBitOr` のいずれかであること（`BIT_XOR` は $x \oplus x = 0$ でありべき等ではないため除外）。
2. `!agg.NeedsGroupContext()` であること（集約内 `HAVING MAX/MIN`・`ORDER BY`・`LIMIT` 等を持たないこと）。
3. GoogleSQL の `DOUBLE` `NaN` 汚染規則においても、`saw_nan_` は論理和（OR）として伝播するため重複の有無で結果は変化しません。

## 検証テスト

- `CascadesTest.DistinctIdempotentAggregateEliminationStripsMinMaxDistinct`
- `SelectRewritesTest.StripIdempotentDistinctEliminatesMinMaxOnly`
