# 共有 CTE 多分岐 3 値フィルタの 1 パス並列集約還元

**未証明。** TPC-DS Q90（`at_` と `pt` の時間帯別 `COUNT`）や Q9/Q28/Q88 等で有効な `__merged_scalar_agg` 書き換えにおいて、共通述語 `base` と複数分岐述語 `p_1..p_k` の 3 値選言フィルタを通した 1 パス並列集約が独立 `k` 回走査と完全一致することを検証する。

## 提出型

[`CteSinglePassSplitFilter.Goal`](../../../TinylambOpt/ResearchGoals/CteSinglePassSplitFilter.lean) を import し、`theorem sound : TinylambOpt.ResearchGoals.CteSinglePassSplitFilter.Goal` を提出する。

Kleene 3 値論理 `sqlAnd` / `sqlOr` を用い、`p_i` 同士が重複したり `NULL`（`none`）を返したりしても、`singlePassSplit` が返す分岐ごとの `(COUNT, SUM)` ベクタが独立多パス参照 `multiPassRef` と一致することを要求する。

## 境界例と適用範囲

分岐の `SUM` 初期値を SQL の `NULL`（`none`）ではなく `some 0` にすると、一致行 0 件の分岐で `0` と `NULL` が食い違う反例（`badSinglePassZeroSum`）を固定した。

## 実装との照合先

- `query/tpch_select_rewrites.cpp` (`__merged_scalar_agg`)
- `executor/detail/fused_grouped_join.cpp`
