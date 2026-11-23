# `packed_2x32` ワーカー局所集約＋NULL・範囲外フォールバック統合

**未証明。** 2 キー結合集約において、範囲内に収まる組を `packed_2x32` 高速マップでワーカー単位集計し、NULL または 32-bit 超過の組だけを汎用フォールバックマップへ逃がして最終的にマージする分割集約の正当性を検証する。

## 提出型

[`PackedRadix2x32BatchAgg.Goal`](../../../TinylambOpt/ResearchGoals/PackedRadix2x32BatchAgg.lean) を import し、`theorem sound : TinylambOpt.ResearchGoals.PackedRadix2x32BatchAgg.Goal` を提出する。

`unpack` が `tryPack` の左逆写像であること、高速マップ由来のキー集合とフォールバック由来キー集合が互いに素であること、および全ワーカー統合後のグループ別和が単一走査と一致することを要求する。

## 境界例と適用範囲

範囲検査なしに 32 ビットへ丸める `uncheckedPack` は `lo1 + 2^32` を `lo1` に衝突させる反例を持つ。負値を含むキーでも `lo1, lo2` が非負差を保証すれば正しく復元される。

## 実装との照合先

- `executor/detail/fused_grouped_join.cpp` (`packed_2x32` radix batch aggregation path)
