# キー単調圧縮ファクトの二分探索区間切り出し

**未証明。** TPC-DS Q65 等で有効な単調連続ファクトテーブル（キー昇順ソート・非 NULL 保証）に対し、ランタイムフィルタの最小・最大境界 `[lo, hi]` から `lower_bound`/`upper_bound` で切り出したスライスだけを集約走査しても結果が完全一致することを検証する。

## 提出型

[`MonoContiguousFactSlice.Goal`](../../../TinylambOpt/ResearchGoals/MonoContiguousFactSlice.lean) を import し、`theorem sound : TinylambOpt.ResearchGoals.MonoContiguousFactSlice.Goal` を提出する。

`SortedByKey rows` かつ `pred` の真値が `[lo, hi]` 内に限られるとき、切り出し範囲のフィルタ結果が全行フィルタと等しく、区間 `[lb, ub)` の外側には一致行が 1 件も存在しないことを要求する（`lo > hi` の空範囲を含む）。

## 境界例と適用範囲

列途中に `NULL`（`none`）が混在すると単調性が崩れ、二分探索が `some 5` を通り過ぎて右側の正答を取りこぼす反例（`nullableBsLower`）を固定した。

## 実装との照合先

- `executor/detail/fused_grouped_join.cpp` (`mono_contiguous_rf`)
