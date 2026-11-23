# オフセット加算済み 2x32 ビットキー＋2 プローブ Bloom 等値結合の無偽陰性

**未証明。** 2 つの INT64 キーから最小値 `lo1, lo2` を引いて `[0, 2^32)` に収まる場合に 64-bit ワードへパックし、ビット幅 `2^m` の 2 プローブ Bloom フィルタ（`kh & mask`, `(kh >> 24) & mask`）でプローブ行を事前刈り込みする結合の完全性を検証する。

## 提出型

[`TwoProbePackedBloom.Goal`](../../../TinylambOpt/ResearchGoals/TwoProbePackedBloom.lean) を import し、`theorem sound : TinylambOpt.ResearchGoals.TwoProbePackedBloom.Goal` を提出する。

全ビルド行で `packKey lo1 lo2` が成功するとき、任意のハッシュ関数 `h`・ビット幅 `1 ≤ m ≤ 20`・NULL を含むプローブ列に対し、Bloom 前置結合と SQL 等値結合が一致することを要求する。

## 境界例と適用範囲

ベース減算と非負・`2^32` 未満チェックを怠り符号付きのまま上位シフト・OR する `badPack` は、下位キー `-1` の符号拡張ビットで上位キーを破壊し `(1, -1)` と `(0, -1)` が衝突する。

## 実装との照合先

- `executor/detail/build_input.cpp` (`packed_bloom`, `b0`/`b1` 2-probe bitset)
