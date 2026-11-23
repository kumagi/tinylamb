# DISTINCT ON を group ごと top-1 状態に落とす

## 提案する命題

`DISTINCT ON (keys) ORDER BY ...` は、全行をソートしてから key ごとの先頭を残す代わりに、走査中に key ごとの最良行だけを保持しても同じになる。

[ProposedGoals/DistinctOnTop1.Goal](../../TinylambOpt/ProposedGoals/DistinctOnTop1.lean) は `top1Fold rows k = sortDedup rows k` を全 key（NULL を含む）について要求する。順序キー同値は入力位置で安定に解決する。

## モデルと適用条件

`ORDER BY` キーは完全順序、同率は入力順で tie-break。ペイロードを含む行全体を観測する。集約や後続の `OFFSET` はこの命題の外。

## 回帰例

`(key=1,order=1,payload=10)` と `(key=1,order=1,payload=11)` は同一順位でも先頭の payload=10 が残る。key が NULL の行同士も一つの group になる。

## 実装との照合先

`executor/topn.cpp` の同率保持と `plan/cascades.cpp` の `eliminate_sort_under_unordered_consumer`。
