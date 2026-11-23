# OFFSET で捨てるページから payload を読まない

**未証明。** ページ別の選択ビットマップが先にあるとき、OFFSET を有効件数から減算し、LIMIT 分の出力だけを gather する。

## 提出型

[`OffsetBitmapGather.Goal`](../../../TinylambOpt/ResearchGoals/OffsetBitmapGather.lean) を含むモジュールを import し、別ファイルで `theorem sound : TinylambOpt.ResearchGoals.OffsetBitmapGather.Goal` を提出する。

全走査の選択列を drop/take した値、実際の payload 読取りのページ番号・レーン番号・順序が一致すること。開いたページ列も出力に寄与したページ集合の初出順と一致する。

## 境界例と適用範囲

OFFSET が最初のページの選択数と一致し、間に空ページがある例と LIMIT 0 を検査。対象は payload 読取りで、ビットマップ取得コストは別。
