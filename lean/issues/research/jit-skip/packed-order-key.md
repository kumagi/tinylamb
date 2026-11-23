# 混合方向・NULL 順の複合キーを単一の符号なし比較にする

**未証明。** 第1キーは nullable な符号付き16ビット降順・NULLS LAST、第2キーは符号なし16ビット昇順。JIT がタグと正規化済みの値を33ビットにパックする。

## 提出型

[`PackedOrderKey.Goal`](../../../TinylambOpt/ResearchGoals/PackedOrderKey.lean) を含むモジュールを import し、別ファイルで `theorem sound : TinylambOpt.ResearchGoals.PackedOrderKey.Goal` を提出する。

全ペアでパック値の符号なし比較が SQL の成分比較と一致し、パック値の等しさが両キーの等しさと同値になること。後者は同率境界や重複判定の利用条件になる。

## 境界例と適用範囲

最小負値と NULL、0 と −1 に逆向きの第2キーを組み合わせた。第1キーが NULL 同士なら第2キーを比較する。
