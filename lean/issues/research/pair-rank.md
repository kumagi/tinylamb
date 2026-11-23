# 直積を展開せずに加算キーの第 k 位を求める

`CROSS JOIN` の後で両列の非 NULL 行を選び、`ORDER BY l.x + r.y LIMIT 1 OFFSET k` の値だけを返す。入力値域が 0〜255 と分かる場合を扱う。参照側は全組の和を広い整数で生成してソートする。候補側は左右を逆向きに整列し、閾値以下の組数を階段状に走査する。その個数で 0〜510 の候補域を二分する。

## 証明対象

[PairRank.Goal](../../TinylambOpt/ResearchGoals/PairRank.lean) の定義を以下に示す。証明は `ResearchProofs/PairRank.lean` の `theorem sound` にあります。

```lean
def Goal : Prop := ∀ left right k, selectSum left right k = reference left right k
```

`countLE` が捨てる列の全要素は、以降の左値でも閾値を超える。逆に行を進めるときは、残りの右列の幅すべてを加える。この不変条件を重複込みで示し、`count > k` となる最小値に接続する。

## 境界例と適用範囲

左右に 0 が二つずつなら第 4 要素（`k = 3`）も 0。255 同士の和は 510。範囲外は `none`。同率の組の行 ID は観測せず、射影は和に限る。
