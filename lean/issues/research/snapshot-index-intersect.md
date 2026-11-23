# MVCC 索引の積集合から遅延して列を読む

## 証明済みの命題

証明: `lean/TinylambOpt/Research/SnapshotIndexIntersect.lean` の主定理。

異なる時点の索引に保守的な候補を許し、版の再検証と更新差分の和集合で、二述語の snapshot スキャンと同じ行列を得る。

## モデルと適用条件

RID の再利用、削除、キー更新、索引 watermark を扱い、候補消去に必要な被覆条件を定める。

## 回帰例

古い二索引には個別に値があるが同じ版に同時には存在しない。RID だけの積集合は偽陽性を出す。

## 完了条件

版ごとの健全性と完全性を別々に証明し、再読が必要な候補数を評価する。

実装との照合先は [table/full_scan_iterator.cpp](../../../table/full_scan_iterator.cpp)。
