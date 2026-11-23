# 整数 Chan/Welford 公式による `(COUNT, AVG, STDDEV_SAMP)` 状態共有と並列統合

**未証明。** 同一グループ内で `COUNT(x)`, `AVG(x)`, `STDDEV_SAMP(x)` を計算する際、共通状態 `(n, sum, sumSq)` を共有し、整数スケールの二次モーメント分子 `chanM2Num = n * ∑ x^2 - (∑ x)^2` を用いれば、(1) 中心化二乗偏差和との恒等式 `n * chanM2Num = ∑ (n*x - S)^2`、(2) 並列ワーカー間の無丸め Chan 結合則、(3) `n=0 / n=1` での `NULL` 返却規約がすべて成立することを検証する。

## 対象敗北クエリとボトルネック

- **TPC-DS Q39** (`94.60 ms` vs DuckDB `57.48 ms`, `+37.12 ms / 1.65x` 差): 1.33 億行 `inventory` に対し `STDDEV_SAMP(inv_quantity_on_hand)` と `AVG(inv_quantity_on_hand)` を別アキュムレータとして更新したうえ、102 万グループを並列マージしているためメモリアクセス・浮動小数点演算が倍増している。
- **TPC-DS Q17** (`52.69 ms` vs DuckDB `39.39 ms`): `COUNT`, `AVG`, `STDDEV_SAMP` を 3 チャネル（`ss`, `sr`, `cs`）× 4 本ずつ計 12 集約実行。

## 提出型

[`ChanWelfordSharedVarianceState.Goal`](../../../TinylambOpt/ResearchGoals/ChanWelfordSharedVarianceState.lean) を import し、`theorem sound : TinylambOpt.ResearchGoals.ChanWelfordSharedVarianceState.Goal` を提出する。

並列状態の結合一致・中心化偏差和恒等式・Chan クロス項の厳密整数恒等式・`n ≤ 1 → VAR_SAMP = none` の 4 点を要求する。

## 境界例と適用範囲

- `n = 0`（全て `none` または空リスト）のとき `AVG = none` かつ `VAR_SAMP = none`。
- `n = 1` のとき `AVG = some (x, 1)` かつ `VAR_SAMP = none`（SQL `STDDEV_SAMP` は標本数 1 で `NULL`）。`xs = [some 2, some 4]` では `m2Num = 4`, 分母 `n*(n-1) = 2` で厳密に `VAR_SAMP = 2` を返す。

## 実装との照合先

- `executor/detail/aggregate_accumulator.cpp`
- `executor/detail/fused_grouped_join.cpp` / `executor/detail/typed_stream_aggregate.cpp`
