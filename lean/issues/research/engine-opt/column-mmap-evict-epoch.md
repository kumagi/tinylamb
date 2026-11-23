# pin 保護付き LRU 退避による列キャッシュ解放の上界・順序保証

**未証明。** メモリ圧迫時に列デコード済みブロックキャッシュから空き領域を回収する際、現在参照（pin）中のブロックを一切退避せず、`pinned == 0` かつ `resident` なブロックのみを `(lruTick, id)` の古い順に目標 `target` バイトへ達するまで退避する契約を検証する。

## 提出型

[`ColumnMmapEvictEpoch.Goal`](../../../TinylambOpt/ResearchGoals/ColumnMmapEvictEpoch.lean) を import し、`theorem sound : TinylambOpt.ResearchGoals.ColumnMmapEvictEpoch.Goal` を提出する。

ID 重複なし仮定の下で、pin 保護保存・解放バイト数保存・停止条件（目標達成または未 pin 残余ゼロ）・退避ブロックと残存未 pin ブロック間の全順序 LRU 性を要求する。

## 境界例と適用範囲

pin 済みブロックしか残っていない場合に `target` が大きくても、無限ループせず可能な分だけ回収して終了し、pin 済みブロックの `resident` を変更しない。

## 実装との照合先

- `table/tlcs_file.cpp` (`EvictDecodedCacheForReclaim`) / `table/table.cpp` (`EvictIdleColumnarHeadroom`)
- `table/tlcs_file.hpp` (`EvictDecodedCacheForTest`)
