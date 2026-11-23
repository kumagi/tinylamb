# 世代番号付き列単位ブロックキャッシュの遅延無効化と再読込回避

**未証明。** テーブル変更のたびに列ブロックキャッシュ全体を即時クリアせず、`LayoutGeneration` を 1 つ進めるだけで古い世代のエントリを自然に無効化し、有効世代の一致時のみデコードを省略する契約を検証する。

## 提出型

[`LazyBlockCacheReuse.Goal`](../../../TinylambOpt/ResearchGoals/LazyBlockCacheReuse.lean) を import し、`theorem sound : TinylambOpt.ResearchGoals.LazyBlockCacheReuse.Goal` を提出する。

読み出し結果が常にキャッシュ無し参照実装と一致すること、および直近の `mutate` または対象 `(col, blk)` の `evict` 以降に未読出しのアクセスでのみデコードが発生することを要求する。

## 境界例と適用範囲

`gen == curGen` 検査を省略する素朴実装（`badOptRun`）は、`mutate` 後に旧値 `10` を返す反例を `example` に固定している。

## 実装との照合先

- `table/tlcs_file.cpp` (`ReuseLazyDecodedBlock`)
- `table/lstable_bridge.cpp` (`LayoutGeneration`, `is_block_lazy`)
