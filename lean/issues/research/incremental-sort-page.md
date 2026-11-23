# 部分整列キーを使う spill 対応 FETCH

## 証明済みの命題

整列済みの先頭キーの run だけを局所ソートし、要求件数で早期停止しても全体安定ソートと同じページを返す。

## モデルと適用条件

順序保証の有効範囲、同率、OFFSET、run が spill ファイルをまたぐ場合を扱う。

## 回帰例

同じ先頭キーが二つのファイルにまたがり、二つ目の末尾キーが最小。ファイル境界で run を閉じると誤る。

## 完了条件

安定なペイロード列と WITH TIES 境界を証明し、保持行数を最大未確定 run 長で評価する。

実装との照合先は [executor/detail/pipeline/ooc_setop_incsort_sink.cpp](../../../executor/detail/pipeline/ooc_setop_incsort_sink.cpp)。
