# 順序集合集約 PERCENTILE_DISC の二つの定義

## 提案する命題

`PERCENTILE_DISC(p) WITHIN GROUP (ORDER BY x)` を、ソートして第 rank 要素を取る経路と、「`x < v` の件数 < rank ≤ `x ≤ v` の件数」を満たす v を探す参照経路で比べる。

[ProposedGoals/OrderedSetPercentile.Goal](../../TinylambOpt/ProposedGoals/OrderedSetPercentile.lean) は `den > 0 ∧ num ≤ den` のもとで両者が一致することを要求する。rank は `ceil(num/den * n)` の 1-始まり。NULL と空集合は `none` を返す。

## モデルと適用条件

入力は `Option Int`、`p` は分子・分母の自然数対（実数を使わない）。値の重複は許可し、同値なら同じ v が選ばれる。近似パーセンタイル Sketch は対象外。

## 回帰例

`[3,1,2]` の中央値（1/2）は 2。`[1,1,5]` の 3/4 分位は rank 3 で 5。`num=0` は rank 0 となり `none`。

## 実装との照合先

`expression/rewrite.cpp` の順序集合集約追加時と `executor/detail/window_eval.cpp` の frame 集約。
