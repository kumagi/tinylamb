# 分散 Top-N に同率を失わない停止水位を付ける

## 証明済みの命題

証明: `lean/TinylambOpt/Research/TieWatermark.lean` の主定理。

各入力の未読キーの境界と局所上位列から、全体の OFFSET と FETCH WITH TIES を確定できる最初の停止条件を導く。

## モデルと適用条件

複合 ASC/DESC と NULL 順序を共有する。水位の等号と履歴に現れた出現 ID を区別する。

## 回帰例

最後の未読ブロックに境界と同じキーが一万件ある。局所 take K では回復不能。

## 完了条件

ペイロード付き全出力と確定可能な接頭辞を証明し、境界を厳密に越えた入力を読まないことを示す。

実装との照合先は [executor/topn.cpp](../../../executor/topn.cpp)。
