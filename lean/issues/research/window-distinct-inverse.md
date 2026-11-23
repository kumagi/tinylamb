# 移動窓の DISTINCT 集約を差分更新する

## 証明済みの命題

証明: `lean/TinylambOpt/Research/WindowDistinctInverse.lean` の主定理。

窓の出入りと EXCLUDE CURRENT ROW をキー別出現回数で処理し、各窓を再集計した COUNT DISTINCT と nullable SUM DISTINCT を再現する。

## モデルと適用条件

SQL NULL と同値類、部分和の範囲、同順位の複数行と端の空窓を扱う。

## 回帰例

値 7 が二度入り片方だけ退出する。集合から直ちに削ると次の窓で 7 が消える。

## 完了条件

全行の結果を列で比較し、更新回数をフレーム移動量で評価する。

実装との照合先は [executor/detail/window_eval.cpp](../../../executor/detail/window_eval.cpp)。
