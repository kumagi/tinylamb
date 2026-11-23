# Fubini 総和保存＋誤差帯分類によるスケール閾値 HAVING の厳密再集計絞り込み

**未証明。** 1 回のパスで並列・近似集約したグループ和 `gHat` と全体和 `tHat`（`∑_{g} groupSum(g) = totalSum` による 1 パス計算）に対し、丸め誤差上界 `epsG, epsT` の下で `den * gHat - num * tHat` を誤差帯 `margin = den * epsG + num * epsT` と比較し、帯外（確実通過／確実棄却）のグループについては厳密再計算や全グループソートを省けることを検証する。

## 対象敗北クエリとボトルネック

- **TPC-H Q11** (`9.50 ms` vs DuckDB `9.12 ms`, `+0.38 ms / 1.04x` 差): `HAVING SUM(...) > 1e-5 * (SELECT SUM(...))` で同一結合 `(partsupp ⋈ supplier ⋈ nation)` のサブクエリ側とメイン側を重複評価する無駄と、Fubini 和＋浮動小数点比較境界の両立が課題。

## 提出型

[`FloatFubiniHavingBand.Goal`](../../../TinylambOpt/ResearchGoals/FloatFubiniHavingBand.lean) を import し、`theorem sound : TinylambOpt.ResearchGoals.FloatFubiniHavingBand.Goal` を提出する。

`(1) (keysOf rows |>.map (groupSum rows)).sum = totalSum rows`（Fubini 恒等式）と、`(2) classifyBand = .certainPass / .certainFail` のそれぞれで厳密判定 `num * tExact < den * gExact` が確定することを要求する。

## 境界例と適用範囲

- 境界ギリギリ（`margin ≥ |den * gHat - num * tHat|`）は `.uncertain` となり、安全に厳密整数・補償和（Kahan/Shewchuk）へフォールバックする。
- 空入力や `none` グループキー混在でも Fubini 総和保存は成立する。

## 実装との照合先

- `query/tpch_select_rewrites.cpp` (`Q11` Fubini HAVING rewrite)
- `executor/detail/fused_grouped_join.cpp`
