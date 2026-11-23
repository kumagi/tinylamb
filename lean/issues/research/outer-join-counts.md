# FULL OUTER JOIN と GROUP BY を二つの頻度表に変換する

左右を `l.k = r.k` で完全外部結合し、`COALESCE(l.k,r.k)` ごとの `COUNT(*)` を求める。参照側は未マッチ行を補ってバッグを作る。候補側は入力ごとの頻度表だけを構築する。両側にある非 NULL キーの出現数は積、片側だけのキーと NULL グループの出現数は和になる。

## 証明対象

[OuterJoinCounts.Goal](../../TinylambOpt/ResearchGoals/OuterJoinCounts.lean) の定義を以下に示す。証明は `ResearchProofs/OuterJoinCounts.lean` の `theorem sound` にあります。

```lean
def Goal : Prop := ∀ l r key,
  groupCount (histogram l) (histogram r) key = (projected l r).count key
```

結合時の `NULL = NULL` はマッチしない一方、集約時の NULL は一つのグループに入る。全キーについて件数を比較することで、結果に出現しないグループも検査対象になる。

## 境界例と適用範囲

NULL が左に 2 行、右に 3 行あれば結果は 5 行。同じ多重度の非 NULL キーなら 6 行。カウントは `Nat` の正確な件数で、固定幅カウンタのオーバーフロー方針は実装への接続条件。
