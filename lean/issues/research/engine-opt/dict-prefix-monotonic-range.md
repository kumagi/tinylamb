# ソート済み辞書の `LIKE 'prefix%'` 単調区間・末尾 `0xFF` 繰り上がり変換

**未証明。** 辞書式ソート済み辞書に対する前方一致検索 `LIKE 'prefix%'` を、下限 `lowerBound pref` と繰り上がり上限 `nextPrefix pref` による連続 ID 区間 `[cLo, cHi)` の比較へ還元する正当性を検証する。

## 提出型

[`DictPrefixMonotonicRange.Goal`](../../../TinylambOpt/ResearchGoals/DictPrefixMonotonicRange.lean) を import し、`theorem sound : TinylambOpt.ResearchGoals.DictPrefixMonotonicRange.Goal` を提出する。

狭義単調増加ソートされた任意のバイト列辞書 `dict` と任意プレフィックス `pref` について、`startsWith pref dict[i] ↔ (cLo ≤ i ∧ i < cHi)` が成り立つことを要求する。

## 境界例と適用範囲

末尾バイトだけを単純に `+1` する `badLastByteInc` は `[0x41, 0xFF]` が `[0x41, 0x00]` へ折り返し全行を取りこぼす。末尾連続 `0xFF` を削って手前を繰り上げる `nextPrefix`（全 `0xFF` 時は上限 `dict.length`）により空・全一致・`0xFF` 境界がすべて解決する。

## 実装との照合先

- `executor/detail/columnar_tlcs_fast_path.cpp`
- `table/pax_chunk/pax_chunk_codecs.cpp`
