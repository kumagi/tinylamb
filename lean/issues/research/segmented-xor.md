# リセットと NULL を含む XOR 差分列を並列 prefix scan で復号する

XOR 差分符号化を採用する列スキャン向けの課題。逐次デコーダは直前の値を差分と XOR し、絶対値トークンで履歴をリセットする。候補側は任意の分割木を上向きに一度たどり、各区間を「定数でリセット」または「入力と XOR」に要約する。下向きの走査で各区間の初期値を配れる。

## 証明対象

[SegmentedXor.Goal](../../TinylambOpt/ResearchGoals/SegmentedXor.lean) の定義を以下に示す。証明は `ResearchProofs/SegmentedXor.lean` の `theorem sound` にあります。

```lean
def Goal : Prop := ∀ carry t, parallel carry t = stream carry (flatten t)
```

リセットをまたぐと左側の履歴が最終値に影響しなくなる。要約に対する作用の合成を任意の初期値について示し、個々の NULL を含む出力列と次ブロックに渡す最終値の両方を一致させる。

## 境界例と適用範囲

初期値 4 から差分 3、NULL、絶対値 8、差分 1 を読むと `[7, NULL, 8, 9]`、最終値は 9。空部分木や不均衡な木も量化する。実際のストレージ形式や並列実行時間はこの命題の対象外。
