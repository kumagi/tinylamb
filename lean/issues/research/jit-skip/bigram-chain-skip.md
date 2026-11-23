# 必須リテラルの隣接バイト対で LIKE ページを除外する

**未証明。** `%片1%片2%` の各片から必須の隣接バイト対を取り、64ビットのハッシュ要約に存在しない対があれば、ページ全体の残余 LIKE 評価を省く。

## 提出型

[`BigramChainSkip.Goal`](../../../TinylambOpt/ResearchGoals/BigramChainSkip.lean) を含むモジュールを import し、別ファイルで `theorem sound : TinylambOpt.ResearchGoals.BigramChainSkip.Goal` を提出する。

NULL・複数行・複数の片について件数が一致し、開かなかったページには欠落した必須対が存在すること。同じ語の中だけで対を作り、別行にまたがる対を作らない。

## 境界例と適用範囲

異なる対のハッシュ衝突は余分な読取りだけを起こす例を固定した。前提はバイナリ照合と抽出済みのリテラル片で、エスケープの字句解析は含まない。
