# 最終出現位置の永続索引で任意区間の COUNT DISTINCT を返す

**未証明。** 同じ列に対して任意の半開区間の `COUNT(DISTINCT x)` を何度も求める。各 prefix に対応する木を残しておくと、要求が前後してもその時点の状態を使える。

## 提出型

[RangeDistinctIndex.Goal](../../TinylambOpt/ResearchGoals/RangeDistinctIndex.lean) を別モジュールから import し、`theorem sound : TinylambOpt.ResearchGoals.RangeDistinctIndex.Goal` を提出する。

非 NULL 値の再出現では前の位置の印を消し、新位置に印を付ける。区間の右端の木で左端以降の印を数えれば、区間を取り出して重複を除いた件数と一致すること。過去の木、範囲外の右端、逆転した端、NULL を検査する。

## 適用範囲

単一整数列での意味保存が提出型。木ノードの共有率やバイト単位のメモリ上限まではこの定理からは導けない。 [実装との照合先](../../../executor/detail/window_eval.cpp)
