# 既出キーだけのページを DISTINCT の先頭行選択で飛ばす

**未証明。** 決定済みの入力順からキー別の先頭 payload を出す。辞書キー63種と NULL 1種をページ要約に載せ、未出キーがあるページだけを開く。

## 提出型

[`FirstKeyBlock.Goal`](../../../TinylambOpt/ResearchGoals/FirstKeyBlock.lean) を含むモジュールを import し、別ファイルで `theorem sound : TinylambOpt.ResearchGoals.FirstKeyBlock.Goal` を提出する。

参照側が先頭出現から作った payload 付き行列に一致し、開いたページと『以前のページに存在しないキーを含むページ』が双方向に対応すること。

## 境界例と適用範囲

NULL の先頭 payload を保持し、NULL が再登場するだけのページは開かない。実際に DISTINCT ON に使う際は、この入力順をプランが保証する必要がある。
