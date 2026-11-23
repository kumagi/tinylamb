# 符号付きビットスライス列を復号せず SUM する

**未証明。** 8ビットの符号付き値を8枚のビットプレーンで保持する。選択・validity マスクとの積の人口数を重み付きで足し、レーンごとの復号を省く。

## 提出型

[`BitPlaneSum.Goal`](../../../TinylambOpt/ResearchGoals/BitPlaneSum.lean) を含むモジュールを import し、別ファイルで `theorem sound : TinylambOpt.ResearchGoals.BitPlaneSum.Goal` を提出する。

任意の nullable 値64個で逐次 SUM と一致する。符号面の重みは −128、選択された非 NULL がゼロなら結果は NULL。

## 境界例と適用範囲

−128 と 127 が同時に生きていると結果は −1。選択と validity が交わらない例も含む。加算は数学整数で、`count` は popcount の意味を定義する。
