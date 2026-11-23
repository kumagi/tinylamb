# 相関 ANY・ALL をグループごとの最小・最大と NULL 情報に変換する

**証明済み。** 証明は `ResearchProofs/QuantifiedRangeSummary.lean` の `theorem sound` にあります。

`x < ANY (SELECT v FROM t WHERE t.k = outer.k)` と同形の ALL を射影に使う。結果を SQL の三値で観測するので UNKNOWN と FALSE の混同も検出できる。

## 証明対象

[QuantifiedRangeSummary.Goal](../../TinylambOpt/ResearchGoals/QuantifiedRangeSummary.lean) の定義を示します。

内表を一度だけ読み、各非 NULL キーに最小・最大・NULL の有無を格納する。空集合と全 NULL の集合を区別し、任意の外部キーと x で逐次 OR/AND と一致すること。NULL キーで空になったときは x が NULL でも ANY は FALSE、ALL は TRUE。非空の混合群では確定した TRUE/FALSE が UNKNOWN に優先する。

## 適用範囲

純粋な nullable 整数比較と SQL 等号による相関が対象。順序が全順序にならない型、照合順序、揮発性の式への適用は別途検討する。 [実装との照合先](../../../query/googlesql_ast_visitor_expr.cpp)
