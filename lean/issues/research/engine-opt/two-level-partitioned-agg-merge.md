# 2 段基数パーティション並列集約 `(count, sum)` 統合の重複排除・同値性

**未証明。** ClickBench 高基数 `GROUP BY`（Q14/17/19/29/32/34/35 等）で用いる 2 段基数パーティション並列集約において、ワーカー局所マップを `hash k % P` でパーティションへ振り分けてマージする際の重複排除と結果一致を検証する。

## 提出型

[`TwoLevelPartitionedAggMerge.Goal`](../../../TinylambOpt/ResearchGoals/TwoLevelPartitionedAggMerge.lean) を import し、`theorem sound : TinylambOpt.ResearchGoals.TwoLevelPartitionedAggMerge.Goal` を提出する。

`numParts > 0` のもとで、全パーティションの出力キー連結に重複がないこと（`Nodup`）、および各キー `k` の `(count, sum)` が単一スレッド走査の結果と一致することを要求する。

## 境界例と適用範囲

`AVG` をワーカーごとにスカラー平均へ潰してから単純平均する `badAvgOfAvgs` は件数重みが失われて誤答する。`count` と `sum` の組（可換モノイド）を保持すれば任意チャンク分割で厳密一致する。

## 実装との照合先

- `executor/detail/typed_stream_aggregate_partitioned.cpp`
