# `prior_offset` を伴う `Limit`/`TopN` 前置スキップ合成と射影可換性

**未証明。** `window_halo_topn` や `topn_push_through_projection` のように内側演算子がすでに先頭 `p ≤ O` 行を読み飛ばしつつ `m ≥ (O - p) + L` 行以上を残している場合、外側 `Limit` のオフセットを `O - p` へ補正すべき契約を検証する。

## 提出型

[`LimitOffsetPrefixComposition.Goal`](../../../TinylambOpt/ResearchGoals/LimitOffsetPrefixComposition.lean) を import し、`theorem sound : TinylambOpt.ResearchGoals.LimitOffsetPrefixComposition.Goal` を提出する。

`p ≤ O` かつ `(O - p) + L ≤ m` のもとで `limitOp L (O - p) (limitOp m p xs) = limitOp L O xs` が成り立ち、行単位の射影 `f` を挟んでも可換であることを要求する。

## 境界例と適用範囲

内側 `TopN(limit=220, offset=100)`（`prior_offset = 100`）の結果に正しい外側 `Limit(20, 0)` を載せた上から、`prior_offset` 非考慮で重複 `Limit(20, 100)` を積むと出力が空列 `[]` に消える反例を固定した。

## 実装との照合先

- `plan/limit_plan.hpp` (`prior_offset_`, `EnforcesLimit`)
- `plan/topn_plan.hpp` (`PriorOffset`, `EnforcesLimit`)
- `plan/cascades.cpp` (`window_halo_topn`, `topn_push_through_projection`)
