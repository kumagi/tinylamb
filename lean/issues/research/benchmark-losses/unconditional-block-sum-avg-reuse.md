# ブロック単位全件肯定での無条件走査＋`(SUM, COUNT)` 及び中間乗算共有

**未証明。** ブロックヘッダの `(minDate, maxDate, hasNullDate)` が述語範囲を完全に包含する（`hasNullDate = false ∧ maxDate ≤ cutoff`）ブロックでは行毎の日付比較をゼロにし、`SUM(qty)` と `AVG(qty)` で非 NULL 件数と総和を共有しつつ、`disc_price = price * (100 - disc)` を `SUM(disc_price)` と `SUM(disc_price * (100 + tax))` で一回計算にまとめる融合の健全性を検証する。

## 対象敗北クエリとボトルネック

- **TPC-H Q01** (`74.03 ms` vs DuckDB `73.50 ms`, `+0.53 ms / 1.01x` 差): 60M 行 `lineitem` のほとんど全行が `l_shipdate <= '1998-09-18'` を通過するにもかかわらず、ブロック全通過判定後の無条件走査と `AVG`/`SUM`/割引式の状態・乗算共有が限定的。
- **TPC-DS Q17** (`52.69 ms` vs DuckDB `39.39 ms`), **TPC-DS Q35** (`95.04 ms` vs DuckDB `50.83 ms`): 同一列の `COUNT` / `MIN` / `MAX` / `AVG` 状態が独立アキュムレータとして重複更新される。

## 提出型

[`UnconditionalBlockSumAvgReuse.Goal`](../../../TinylambOpt/ResearchGoals/UnconditionalBlockSumAvgReuse.lean) を import し、`theorem sound : TinylambOpt.ResearchGoals.UnconditionalBlockSumAvgReuse.Goal` を提出する。

`ValidBlockHeader` の下で `optScan` が独立走査 `refScan` と完全一致すること、および全ブロックが `hasNullDate = false ∧ maxDate ≤ cutoff` のとき行単位の日付比較回数 `dateChecks = 0` となることを要求する。

## 境界例と適用範囲

- `shipdate` に `none` を含むブロック（`hasNullDate = true`）では無条件走査へ入らないが、`cutoff < minDate` のブロックは即座にスキップできる。
- `qty = none` の行が通過する場合、`COUNT(qty)`（`cntQty = 1`）と `COUNT(*)`（`cntStar = 2`）を混同すると `AVG(qty)` が壊れる（`badAvgUsesCountStar` により反例検証済み）。

## 実装との照合先

- `executor/detail/typed_stream_aggregate.cpp` / `executor/detail/typed_column_eval.cpp`
- `executor/detail/fused_grouped_join.cpp`
