# 定数の符号付き INT64 商と余りを補正付き逆数乗算で計算する

両方の演算が要求されるとき、定数除数の絶対値から逆数を一度準備し、各行では積の上位 64 ビットから商を推定する。128 ビットの積で過大推定を検出して商を 1 減らし、符号を復元して余りを求める。参照側はゼロ方向に丸める整数除算と剰余。

## 証明済みの命題

[ReciprocalDivMod.Goal](../../TinylambOpt/ResearchGoals/ReciprocalDivMod.lean) の定義を以下に示す。証明は [ResearchProofs/ReciprocalDivMod.lean](../../TinylambOpt/ResearchProofs/ReciprocalDivMod.lean) の `TinylambOpt.ResearchProofs.ReciprocalDivMod.sound` にある。

```lean
def Goal : Prop := ∀ x d, run (prepare d) x = reference x d
```

非単位の除数に対して真の商と推定との差が 0 または 1 であること、補正判定の積が収まることが核心。NULL、ゼロ除算、`INT64_MIN / -1` を結果型で区別する。商と余りの組またはエラー種別までが観測対象。

## 境界例と適用範囲

`9223372036854775804 / 5` は補正を省くと商が 1 大きい。`-7 / 3` の余りは `-1`。NULL 入力とゼロ除数の組も検査する。符号付き経路が対象で、LLVM の命令選択やエラーメッセージは別の検証対象。
