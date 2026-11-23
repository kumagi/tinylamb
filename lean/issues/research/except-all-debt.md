# EXCEPT ALL の差を spill 部分木間で相殺する

**証明済み。** `ResearchProofs/ExceptAllDebt.lean` の `theorem sound` が、左右の袋を任意に分割してから結合する性質を示す。参照器は右の各行につき左の等しい一行を消す。行の全列で比較し、対応する NULL 同士も等しい。

## 提出型

[ExceptAllDebt.Goal](../../TinylambOpt/ResearchGoals/ExceptAllDebt.lean) を別モジュールから import し、`theorem sound : TinylambOpt.ResearchGoals.ExceptAllDebt.Goal` を提出する。

各葉が行ごとに符号付きの出現数を作り、内部ノードで加算する。出力時だけ負数をゼロに丸めると、どの木でも参照器と同じ bag になること。右に2件、同じ部分木の左に1件、別部分木の左に2件ある例では最終出力が1件になる。途中で負数をゼロに丸める候補はこの例で落ちる。

## 適用範囲

入力は nullable 整数列からなる行で、出力順は任意。件数は数学整数のため、実装での幅選択や spill 書込みの原子性は別の条件。 [実装との照合先](../../../executor/detail/pipeline/ooc_setop_incsort_sink.cpp)
