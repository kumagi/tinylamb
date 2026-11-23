# QUALIFY と WHERE の交換条件

## 提案する命題

ウィンドウ結果に対する `QUALIFY` と、パーティションキーのみに依存する `WHERE` は順序を入れ替えても同じ行の bagを返す。パーティションキー以外に依存する述語は押し下げてはならない。

[ProposedGoals/QualifyPushdown.Goal](../../TinylambOpt/ProposedGoals/QualifyPushdown.lean) は `PartitionOnly w` のもとで `where(w, qualify(q, rows)) = qualify(q, where(w, rows))` を要求する。

## モデルと適用条件

ウィンドウ値はパーティション内の measure の和。順序は入力順を保存する。`w` が同一キーの行間で一定であることが十分条件。`q` はウィンドウ値のみに依存する。

## 回帰例

`(key=1,value=1)` と `(key=1,value=2)` に `w = (value == 1)` を適用すると、押し下げ後はパーティション和が 1 に変わり `q(s>2)` が落ちる。押し下げ前は両行とも残る。

## 実装との照合先

`plan/cascades.cpp` の window 規則と `executor/detail/window_eval.cpp`。
