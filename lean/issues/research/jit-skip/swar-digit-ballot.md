# 8バイトの ASCII 数字判定を1語の演算にする

**未証明。** 数値文字列の高速経路やパーサで、各バイトが `0`〜`9` かを64ビット語の加算・論理演算でまとめて分類する。

## 提出型

[`SwarDigitBallot.Goal`](../../../TinylambOpt/ResearchGoals/SwarDigitBallot.lean) を含むモジュールを import し、別ファイルで `theorem sound : TinylambOpt.ResearchGoals.SwarDigitBallot.Goal` を提出する。

任意の入力語について、出力は数字バイトの最上位ビットだけが立つ。ゼロバイト検出用の加算と下位ニブルの加算が隣のレーンを汚さないことが鍵になる。

## 境界例と適用範囲

`/`、`0`、`9`、`:`、上位ビット付きバイトを同じ語に混在させた。対象は ASCII の分類で、Unicode の数字や SQL の CAST 全体は別の契約。
