# 2D ペアビットマップの密外側キーリマップ＋軸反転の完全一致

**未証明。** 2 キー結合のストリーミングプローブ前段で用いる `pair_bits` + `pair_outer_dense` リマップ（およびカーディナリティ比較による外側・内側軸の入れ替え `swap`）が、偽陰性だけでなく偽陽性も出さない完全フィルタであることを検証する。

## 提出型

[`PairBitmapDenseRemap.Goal`](../../../TinylambOpt/ResearchGoals/PairBitmapDenseRemap.lean) を import し、`theorem sound : TinylambOpt.ResearchGoals.PairBitmapDenseRemap.Goal` を提出する。

任意の `pairs`, `swap`, クエリペア `q` について、`probePairBitmap pairs swap q = pairs.contains q` を要求する。

## 境界例と適用範囲

密インデックス写像が単射でない（`badModuloProbe` のように `outer % 2` で衝突する）場合は、ビルド側に存在しないペア `(3, 10)` に偽陽性を出す反例を固定している。

## 実装との照合先

- `executor/detail/build_input.cpp` (`pair_bits`, `pair_outer_dense`)
