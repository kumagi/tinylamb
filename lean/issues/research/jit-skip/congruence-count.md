# 剰余述語をブロック内の差の GCD で判定する

**未証明。** 非負の整数列で `x % m = r % m` を評価する。先頭値とそこからの差の GCD が分かれば、min/max が通すページも除外できる。

## 提出型

[`CongruenceCount.Goal`](../../../TinylambOpt/ResearchGoals/CongruenceCount.lean) を含むモジュールを import し、別ファイルで `theorem sound : TinylambOpt.ResearchGoals.CongruenceCount.Goal` を提出する。

全走査との件数一致に加え、開いたページの要約が表す等差数列には合格値・不合格値の両方が存在すること。一定値のページでは stride が0になる。

## 境界例と適用範囲

法4で全不一致・全一致・混在・空のページを連続して処理する。正の法を仮定。負の SQL 剰余や古いヘッダへの適用は別契約。
