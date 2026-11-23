# ビット列の AND/OR 要約で COUNT のページを三分類する

**未証明。** `(x & mask) = (want & mask)` を、ブロック内全値の AND と OR だけで全不一致・全一致・要検査に分ける。全一致ではヘッダ件数を直接足す。

## 提出型

[`BitSignatureCount.Goal`](../../../TinylambOpt/ResearchGoals/BitSignatureCount.lean) を含むモジュールを import し、別ファイルで `theorem sound : TinylambOpt.ResearchGoals.BitSignatureCount.Goal` を提出する。

結果件数が全走査と一致し、開いた全ページに、実データで0と1の両方を取る照合対象ビットが存在すること。正確な要約・件数を前提にする。

## 境界例と適用範囲

全一致・全不一致・混在ページの3枚から混在ページだけを開き、件数は3。行は非 NULL の64ビット値。
