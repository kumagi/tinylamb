# 二次元の相関 NOT EXISTS を一度のソートと走査に変換する

**モデル上で証明済み。** [`ResearchProofs/SkylineAntiJoin.sound`](../../TinylambOpt/ResearchProofs/SkylineAntiJoin.lean) が Goal を証明する。`WHERE NOT EXISTS (SELECT 1 FROM t a WHERE a.x < b.x AND a.y < b.y)` を考える。両軸とも狭義不等号。自己比較、NULL、同一座標の複数出現が結果の多重度に影響する。

## 提出型

[`SkylineAntiJoin.Goal`](../../TinylambOpt/ResearchGoals/SkylineAntiJoin.lean) を import し、別モジュールで `theorem sound : TinylambOpt.ResearchGoals.SkylineAntiJoin.Goal` を提出する。

外側の行ごとの全表照会と、x 順にソートし「前の x 群までの最小 y」と「現在群の最小 y」を分けて更新する走査で、出現 ID 込みの bag が等しい。同じ x 群内で y が小さくても他の行を消せず、同じ y も他の行を消せない。NULL を持つ行は残る。

## 適用範囲

ID 一意性も DISTINCT も前提にしない。ORDER BY のない問い合わせとして順序は観測しない。
