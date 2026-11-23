# 適応 4 状態 `COUNT(DISTINCT)`（empty/single/small/large）の状態不変量・NULL 除外

**未証明。** ClickBench（Q04/05/06/09/10/34 等）向けの適応的 `COUNT(DISTINCT)` 状態機械（`empty -> single -> small(≤ T) -> large`）において、ワーカー間マージとサイズ閾値遷移が重複排除・容量上限・SQL NULL 除外を保ち厳密一致することを検証する。

## 提出型

[`CountDistinctAdaptiveSwitch.Goal`](../../../TinylambOpt/ResearchGoals/CountDistinctAdaptiveSwitch.lean) を import し、`theorem sound : TinylambOpt.ResearchGoals.CountDistinctAdaptiveSwitch.Goal` を提出する。

閾値 `thresh ≥ 1` のもとで全ワーカー状態およびマージ後状態が `StateInvariant`（`small` は要素数 `≤ thresh` かつ重複なし、`large` は重複なし）を保ち、最終カーディナリティが全体の非 NULL 重複排除長と一致することを要求する。

## 境界例と適用範囲

`none`（SQL NULL）をセンチネル値 `0` として `insertNonNull` へ渡す誤り（`badInsertNullAsZero`）は、NULL を 1 種類として数えてカーディナリティを 1 増やす反例を固定している。

## 実装との照合先

- `executor/detail/typed_stream_aggregate_compact.cpp`
- `executor/detail/aggregate_accumulator.cpp`
