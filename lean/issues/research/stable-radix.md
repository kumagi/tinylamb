# 符号付き nullable INT64 の安定降順ソートをビット分配に落とす

`ORDER BY key DESC NULLS LAST` を、キーの各ビットに対する安定分配で処理する。参照側は符号付き数値を比較する安定マージソート。候補側は下位から順に非符号ビットを反転して分配し、符号ビットと NULL 専用の最上位桁を個別に扱う。

## 証明済みの命題

[StableRadix.Goal](../../TinylambOpt/ResearchGoals/StableRadix.lean) は [`ResearchProofs/StableRadix.sound`](../../TinylambOpt/ResearchProofs/StableRadix.lean) で証明済み。命題の定義は以下のとおり。

```lean
def Goal : Prop := ∀ rows, radix rows = rows.mergeSort before
```

i 桁の処理後に下位 i 桁で安定整列していることと、最終的なビット順が SQL キー順に対応することを示す。比較対象は payload を含む行列全体。入力順の安定性を必要とする物理演算子への契約として使える。

## 境界例と適用範囲

`-1` の 2 行と NULL の 2 行に異なる payload を付け、両方の同順位群で入力順を検査する。65 回の二分配は基準カーネル。実機で複数ビットをまとめる場合はそのカーネルとの対応が別途必要。
