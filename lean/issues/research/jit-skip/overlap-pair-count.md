# 区間 JOIN のページ対を全不一致・全一致・残余に分ける

**未証明。** 二つのページの開始・終了の最小値と最大値を使い、半開区間の重なりを数える。全組が一致するときは左右件数を乗算して両ペイロードの組列挙を省く。

## 提出型

[`OverlapPairCount.Goal`](../../../TinylambOpt/ResearchGoals/OverlapPairCount.lean) を含むモジュールを import し、別ファイルで `theorem sound : TinylambOpt.ResearchGoals.OverlapPairCount.Goal` を提出する。

完全な二重ループと件数が一致し、残余評価を行ったページ対には少なくとも一組の不一致があること。すべての区間は非空とする。

## 境界例と適用範囲

端点が触れるだけなら不一致。重複区間は件数に加算する。大きな空隙があり、統計上は残余評価が必要でも実際は0組という場合も固定した。
