# WAL チェックポイント・TLCS 境界と MVCC 書込み意図順序での復旧同値性

**未証明。** 永続 TLCS スナップショットの基準 `cpLsn` より前に開始・書込み済みだがコミットが `cpLsn` 以降にあるトランザクションを、WAL リプレイ開始点 `minActiveWriteLsn` を遡らせることで取りこぼさず復元できる条件を検証する。

## 提出型

[`WalCheckpointTlcsBoundary.Goal`](../../../TinylambOpt/ResearchGoals/WalCheckpointTlcsBoundary.lean) を import し、`theorem sound : TinylambOpt.ResearchGoals.WalCheckpointTlcsBoundary.Goal` を提出する。

`WritesBeforeCommit`（コミット後書込み無し）および MVCC 書込み意図ロックが保証する `SameKeyIntentOrder` の下で、TLCS スナップショット＋`minActiveWriteLsn` からの部分リプレイが LSN 0 全文リプレイと全キーで一致することを要求する。

## 境界例と適用範囲

`replayFrom = cpLsn` とする素朴復旧は `cpLsn` 跨ぎの未コミット書込みを失い、`SameKeyIntentOrder` が破れると後コミット側への誤上書きが起きる反例を `example` に収録した。

## 実装との照合先

- `recovery/checkpoint_manager.cpp` (`WriteCheckpoint`)
- `recovery/recovery_manager.cpp` / `table/tlcs_file.cpp`
