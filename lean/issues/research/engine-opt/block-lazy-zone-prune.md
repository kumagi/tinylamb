# ZoneMap ヘッダ判定＋キーデコード＋一致ブロック限定 payload 遅延デコード

**未証明。** TLCS の列単位遅延ブロック読込みにおいて、ZoneMap ヘッダで偽と確定したブロックはキー列すら開かず、キーデコード後に一致行が 1 件も無かったブロックでは payload 列を開かない 2 段階刈り込みの等価性と I/O 最小性を検証する。

## 提出型

[`BlockLazyZonePrune.Goal`](../../../TinylambOpt/ResearchGoals/BlockLazyZonePrune.lean) を import し、`theorem sound : TinylambOpt.ResearchGoals.BlockLazyZonePrune.Goal` を提出する。

`ValidHeader` の下で 2 段階走査の結果が全走査と一致し、`keyOpened` が ZoneMap 一致条件、`payOpened` が実一致行存在とそれぞれ同値で `payOpened ⊆ keyOpened` となることを要求する。

## 境界例と適用範囲

最小キー 1・最大キー 10 のブロックに対し `[5, 6]` を問い合わせ、実データが `[some 1, some 10]` しかない場合は `keyOpened` には入るが `payOpened` に入らない。`lo > hi` の逆転区間ではどちらにも入らない。

## 実装との照合先

- `table/tlcs_file.cpp` (`TlcsFile::Open` / `TlcsFile::OpenInternal`, lazy block decode path)
- `executor/relational_regression_test_part4.cpp` (`LazyDecodedBlockCount` 検証)
