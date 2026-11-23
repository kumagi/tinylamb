# 不変 TLCS v3 ゾーンマップ行数上界による `exact_integer_total` 復帰

**未証明。** 不変な TLCS v3 ブロック（`ValidBlockHeader` を満たす）に対して実行時フィルタ `[rfLo, rfHi]` を適用する際、ゾーンマップ一致ブロック（`!b.allNull ∧ b.minKey ≤ rfHi ∧ rfLo ≤ b.maxKey`）の行数和 `zmRowUpperBound` が実生存行数 `survivingRowCount` の健全な上界になり、`zmRowUpperBound * buildFanout * maxAbs ≤ exactLimit` から無検査 1 パス並列和の安全条件を導けることを検証する。

## 対象敗北クエリとボトルネック

- **TPC-DS Q22** (`185.67 ms` vs Round 1 `75.71 ms`, DuckDB `144.45 ms`, `+110 ms` 後退 / `+41.23 ms` 差): 第 1 弾後の健全性修正で `executor/detail/fused_grouped_join.cpp` の block-lazy ゾーンマップ上界利用を外した結果、`inventory`（1.33 億行）× `item`（10.2 万）の上界が `2^53` を超過して 1-pass `ROLLUP` 融合が発動しなくなった。ただし `is_persistent_tlcs` で不変性が保証された TLCS v3 ブロックに限ればゾーンマップは厳密に健全である。

## 提出型

[`ImmutableTlcsZoneMapRowBound.Goal`](../../../TinylambOpt/ResearchGoals/ImmutableTlcsZoneMapRowBound.lean) を import し、`theorem sound : TinylambOpt.ResearchGoals.ImmutableTlcsZoneMapRowBound.Goal` を提出する。

不変ブロック列での `survivingRowCount ≤ zmRowUpperBound` およびオーバーフロー上界の含意に加え、古い可変ヘッダ `staleHeaderExample` では実際に過小評価が起きる反例の成立も同時に要求する。

## 境界例と適用範囲

- `lo > hi` の空区間は `mayMatchHeader = false` で 0 行上界、`allNull = true` ブロックも `mayMatchHeader = false` で安全にスキップする。
- MVCC 版チェーンや in-place 書換えがありうる可変ストアには適用せず、不変かつ有効チェック済みの TLCS ブロックストアに限定して適用する。

## 実装との照合先

- `executor/detail/fused_grouped_join.cpp` (`exact_left_rows_bound`, `exact_integer_total`)
- `table/tlcs_file.hpp` (`TlcsFile::BlockDirEntry`, `ColumnMeta`)
