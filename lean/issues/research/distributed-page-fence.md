# 分散 FETCH WITH TIES で開くブロックを確定する

[分散 Top-N](tie-watermark.md) の差し戻し。空でないブロックごとに、データを開かず取得できる正確な最小キーを持つディレクトリを仮定する。最小は SQL の指定順での先頭を意味し、数値の最小に限らない。任意の入力順で担当 worker からブロックが集まる。

## 提出型

[`DistributedPageFence.Goal`](../../TinylambOpt/ResearchGoals/DistributedPageFence.lean) を import し、別モジュールで `theorem sound : TinylambOpt.ResearchGoals.DistributedPageFence.Goal` を提出する。

最小キー順にディレクトリを歩き、既読行から OFFSET 後の境界を作る。全量ソートと安定な出力（同率内は一意な出現 ID 順）が一致し、開いたブロック列が全入力から計算した必要ブロック列に一致すること。先頭の同率が二つのブロックにまたがれば両方を開き、後続の 9 は開かない。

## 適用範囲

正確な最小キーを外から取得できる設定でのブロックアクセスを数える。保守的な下限しか持てない場合や、既読行の再ソートを効率化するヒープは別課題。
