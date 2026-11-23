# DISTINCT 集約のマージ契約

## 提案する命題

`COUNT(DISTINCT x)` / `SUM(DISTINCT x)` を分割並列で計算するとき、部分状態が「見た値の集合」なら和集合を取って再度冪等化すれば全体結果と一致する。部分状態が件数だけなら不一致になり得る。

[ProposedGoals/DistinctAggMergeContract.Goal](../../TinylambOpt/ProposedGoals/DistinctAggMergeContract.lean) は任意のパーティション列について `mergePartial` の distinct 値集合と `parts.flatten` のそれが一致することを要求する。NULL は一つの値として扱う。

## モデルと適用条件

値は `Option Int` で、等値は通常の SQL 等号（NULL 同士はグループ化の文脈で同一視）。各部分状態は重複を除いた値集合を保持する。Sketch による近似は対象外（偽陽性は I/O だけに効く別命題）。

## 回帰例

`[1,2]` と `[2,3]` の部分件数は 2+2=4 だが全体の distinct 件数は 3。`naiveCountSum` が 4 になることを fixture で固定する。

## 実装との照合先

TODO.md の「DISTINCT / 順序依存 / FILTER 付き集約は merge 契約が無い限り並列拒否を維持」。
