# ワーカー局所 Top-(offset+limit) 2 段並列マージの正当性

**未証明。** 大規模テーブルの並列 Top-N（`ORDER BY ... LIMIT L OFFSET O`）において、各ワーカーが独立に上位 `O + L` 件だけを保持して 2 段目で統合ソートし先頭 `O` 件を捨てて `L` 件を取る方式の完全一致を検証する。

## 提出型

[`BoundedHeapTopNMerge.Goal`](../../../TinylambOpt/ResearchGoals/BoundedHeapTopNMerge.lean) を import し、`theorem sound : TinylambOpt.ResearchGoals.BoundedHeapTopNMerge.Goal` を提出する。

比較関数 `rowLt` に対して、任意の `offset`, `limit`, チャンク分割 `chunks` で 2 段並列 Top-N 出力が全体ソート＋スライスと一致することを要求する。

## 境界例と適用範囲

ワーカー側の上限を `offset + limit` ではなく `limit` のみに削る `badWorkerLimitOnly` は、`offset > 0` のとき他チャンクに追い出されるべき上位行だけでなく本来の生存行まで捨ててしまう。

## 実装との照合先

- `executor/topn.cpp`
- `executor/detail/relational_sort_limit.cpp`
