# 負開始・0 開始・境界外を含む ASCII `SUBSTR` O(1) バイトスライス等価性

**未証明。** ClickBench Q19/28/29 等で頻出する負開始・0 開始・長大オフセット・負長エラーを含む 1-based `SUBSTR` に対し、入力が全 ASCII（各バイト `< 128`）であることを検査したうえで O(1) のバイト配列スライスへ切り替える最適化の等価性を検証する。

## 提出型

[`SubstrAsciiByteSliceEquiv.Goal`](../../../TinylambOpt/ResearchGoals/SubstrAsciiByteSliceEquiv.lean) を import し、`theorem sound : TinylambOpt.ResearchGoals.SubstrAsciiByteSliceEquiv.Goal` を提出する。

`isAsciiStr bs = true` の下で、任意の符号付き開始位置 `start : Int`（正・0・負・文字列長超過の負方向クランプ）および `optLen : Option Int`（負長時のエラーを含む）について O(1) スライスが UTF-8 コードポイント走査と一致することを要求する。

## 境界例と適用範囲

2 バイト以上の UTF-8 文字 `[0xC3, 0xA9, 0x61]` では 1 バイト切り出しが文字境界を壊すため `IsAllAsciiFast` 判定を必須とする。負の長さは開始位置に関わらず先にエラーとなる点もモデルに含めた。

## 実装との照合先

- `expression/bytecode.cpp` (`IsAllAsciiFast`, `TryEvalSubstrView`)
- `executor/detail/columnar_tlcs_fast_path.cpp`
