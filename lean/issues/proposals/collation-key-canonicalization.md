# 照合キー正規化による group/order 書き換え

## 提案する命題

照合順序つき比較を正規化キーへ写し、`GROUP BY` と `ORDER BY` をキー比較に置き換えても、異なる生値を誤って同じ group にしない限り結果が保存される。

[ProposedGoals/CollationKeyCanonicalization.Goal](../../TinylambOpt/ProposedGoals/CollationKeyCanonicalization.lean) は `KeyInject key`（キーが等しいなら生値も等しい）と生順序・キー順序の一致を仮定し、`groupByKey = groupRaw` を要求する。

## モデルと適用条件

キー関数は決定的で、等値と順序の両方について整合的であること。大小文字・アクセント折りなどで異なる生値が同じキーになる collation は `KeyInject` を満たさないため、その場合はキー比較の group 化を拒否する。NULL は対象外（別途 NULL 順序を扱う）。

## 回帰例

定数キー `fun _ => []` は `[1]` と `[2]` を同じ group にして measure を 3 にしてしまう。生値なら 1 と 2 である。fixture で両者を固定する。

## 実装との照合先

TODO.md の不変条件「collation を全て保つ」と「collation つき比較は Tier 0 に逃がす」。
