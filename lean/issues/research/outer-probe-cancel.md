# 外部結合の未マッチ確定を並列化する

## 証明済みの命題

右入力を複数の morsel に分けても、各左行の NULL 補完は全右入力の完了後に一度だけ発生する。モデル上の等価性と補完回数は `TinylambOpt.Research.OuterProbeCancel.outer_probe_cancel_sound`、到着順の不変性は `arrival_order_invariance` で証明済み。

## モデルと適用条件

左行の出現 ID ごとのマッチ、完了、水位、打切りを区別する。ON は三値で評価する。

## 回帰例

最初の worker が UNKNOWN だけを見て補完した後、最後の worker が TRUE を返す。

## 完了条件

任意の到着順・同値な左行・空パーティションでの bag 等価は証明済み。実装における不要な補完の生成数の評価は残る。

実装との照合先は [executor/detail/pipeline/ooc_hash_join_sink.cpp](../../../executor/detail/pipeline/ooc_hash_join_sink.cpp)。
