# 三値述語の「3個中2個」を論理回路にする

**未証明。** `(A AND B) OR (A AND C) OR (B AND C)` を、確実に真のビットと真の可能性があるビットに対する多数決演算へ落とす。

## 提出型

[`ThresholdBitCounter.Goal`](../../../TinylambOpt/ResearchGoals/ThresholdBitCounter.lean) を含むモジュールを import し、別ファイルで `theorem sound : TinylambOpt.ResearchGoals.ThresholdBitCounter.Goal` を提出する。

活性レーンごとの TRUE と UNKNOWN のマスクを、SQL 三値の式木の評価と別々に一致させる。これにより WHERE 以外の投影でも使える。

## 境界例と適用範囲

同一バッチに TRUE・UNKNOWN・FALSE のレーンを混在させた。入力は評価済みの述語で、短絡の途中で発生する副作用やエラーの並べ替えは含まない。
