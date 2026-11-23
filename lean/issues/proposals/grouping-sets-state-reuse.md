# GROUPING SETS を最細粒度の状態から合成する

## 提案する命題

`ROLLUP` / `CUBE` / `GROUPING SETS` を groupingset ごとに再スキャンせず、最細粒度の grouping set を一度だけ計算し、その部分状態を上位 set へ combine しても同じ group 列が得られる。

[ProposedGoals/GroupingSetsReuse.Goal](../../TinylambOpt/ProposedGoals/GroupingSetsReuse.lean) は 4 通りの集合 `(a,b), (a), (b), ()` について再利用経路と直接再走査の `Key → State` が一致することと、再利用経路が入力を 2 回以上読まないことを要求する。NULL は一つの group に入り、件数とペイロード和を観測する。

## モデルと適用条件

状態は可換モノイド（件数と和）で、`combine` が結合法則を持つこと、最細 key から粗 key への写像が各 set で一意に決まることが前提。順序依存・`DISTINCT` 付き集約は対象外。

## 回帰例

`(a=1,b=2,payload=5)` と `(a=1,b=2,payload=3)` は最細 group では 1 行にまとまり、`(a)` では 2 行分の和 8 になる。`(none)` を混ぜても NULL group として別に数える。

## 実装との照合先

TODO.md の「ROLLUP / CUBE / GROUPING SETS の状態再利用」（Q67 の `aggregate_input_rows` 削減）。
