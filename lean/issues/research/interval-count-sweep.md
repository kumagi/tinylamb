# 重なり区間数を端点の走査で求める

**証明済み。** `ResearchProofs/IntervalCountSweep.lean` に `theorem sound` があり、公開 Goal を証明しています。 各観測時刻 t に対する相関集約 `COUNT(*) WHERE lo <= t AND t < hi` をまとめて処理する。区間の重複はそれぞれ数え、同一時刻の問い合わせも元の出現順で返す。

## 証明対象

[IntervalCountSweep.Goal](../../TinylambOpt/ResearchGoals/IntervalCountSweep.lean) の証明は `ResearchProofs/IntervalCountSweep.lean` の `theorem sound` にあります。

区間の終了・開始・問い合わせをこの順に整列し、開いている区間数を更新する。任意の入力で逐次フィルタと件数列が一致すること。右端と次の左端が重なる例、空区間、NULL 時刻を固定した。

## 適用範囲

時刻は整数、件数は自然数。LEFT JOIN 後に NULL 補完行も数える `COUNT(*)` とは集約の位置が異なる。 [実装との照合先](../../../executor/band_join.cpp)
