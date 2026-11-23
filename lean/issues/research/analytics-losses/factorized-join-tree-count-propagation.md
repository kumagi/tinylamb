# 非巡回多段結合木のボトムアップ重み伝播（Factorized Join-Tree Count Propagation）

**未証明。** 6 表結合 `place ⋈ person ⋈ person_knows ⋈ person2 ⋈ person_likes ⋈ message` のような非巡回結合木（chain＋star）上で `COUNT(*)` のみを求める場合、中間テーブル `person_knows ⋈ person_likes`（243 万行）のような多対多の直積ペアを一切実体化せず、葉テーブルから根に向かって各行の「下流結合多重度（NULL キー・空サブツリーで 0 吸収、複数子木は積）」をスカラー整数としてボトムアップ伝播して根のグループへ加算する因子化（Factorized / Yannakakis-like count pass）が、全タプル展開後の `COUNT(*)` と任意の深さ・分岐数の木構造に対して完全同値であることを検証する。

## 対象敗北クエリとボトルネック

- **LDBC-SNB L1** (`81.9 ms` [srv `49.2 ms`] vs DuckDB `16.0 ms` / ClickHouse `29.4 ms`, **`5.12x` 差**): `[fusion] greedy hook: next=message left_rows=2428656 right_rows=11067` — 左深木で `person_knows ⋈ person_likes` を展開した結果、**2,428,656 行・幅 12 列**の中間リレーションを実体化し、`join_comparisons = 4,686,709` を費やしている。
- 同じく木構造上の `COUNT(*)` または加法測度のみを含む全多表結合：**JOB J4** (`8.8 ms`), **JOB J5** (`2.8 ms`), **STATS-CEB C1** (`8.6 ms`), **STATS-CEB C2** (`3.9 ms`), **STATS-CEB C5** (`5.3 ms`), **LDBC L2** (`41.7 ms`), **LDBC L5** (`3.2 ms`), **BigBench B3** (`3.7 ms`)。

## 提出型

[`FactorizedJoinTreeCountPropagation.Goal`](../../../TinylambOpt/ResearchGoals/FactorizedJoinTreeCountPropagation.lean) を import し、`theorem sound : TinylambOpt.ResearchGoals.FactorizedJoinTreeCountPropagation.Goal` を提出する。

任意の非巡回結合木 `t : JoinTree` と任意のグループ `g : Int` に対し、`optRootCount t g = refCount t g` が成立することを示す。

## 境界例と適用範囲

- 結合木のある枝で結合キーが `none`（NULL）のときや、子木側にマッチキーが 1 件も無いとき、その枝の重みが `0` になるため他枝が何件マッチしても積 `optChildrenProd` 全体が `0` になり、消滅挙動が厳密に再現される。
- `JoinTree` にサイクル（自己閉路や三角結合）がある場合は木分解または 1 辺を残余チェックに回す必要があり、本定理は非巡回結合木に適用する。

## 実装との照合先

- `executor/detail/fused_grouped_join.cpp` (Lines 23055–23087 `greedy hook` 連鎖・最終結合前の行配列展開)
- `executor/detail/build_input.cpp` (greedy 結合計画構築部)
