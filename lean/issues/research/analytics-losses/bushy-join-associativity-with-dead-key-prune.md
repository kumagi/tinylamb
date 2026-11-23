# 鎖結合のブッシー再結合と死キー列即時枝刈り（Bushy Join Associativity with Dead-Key Pruning）

**未証明。** 4 表鎖結合 `((A ⋈_{k1} B) ⋈_{k2} C) ⋈_{k3} D` を現在の貪欲法のように単一の左深木で進めると、`A ⋈ B`（例: `person ⋈ person_knows`）と無関係な側 `C ⋈ D`（例: `person_likes ⋈ message`）の結合結果で先に相手を絞れず、中間行幅も全結合キーを運んだまま肥大する。結合順を `(A ⋈_{k1} B) ⋈_{k2} (C ⋈_{k3} D)` のブッシー木（bushy tree）へ組み替え、各段で消費済みの結合キー `k1`, `k3` を即時射影除去しても、重複キーや NULL 結合キーを含む全入力で出力多重集合が完全同値であることを検証する。

## 対象敗北クエリとボトルネック

- **LDBC-SNB L1** (`81.9 ms` vs DuckDB `16.0 ms`, **`5.12x` 差**): DuckDB の `EXPLAIN ANALYZE` では `(person_likes ⋈ message ⋈ p2)` を先に **15,208 行**へ絞ってから `person_knows` へ動的 PRF を送り、さらに `(person ⋈ place)` と結合して幅 1 列を保つ（`33.0 ms`）。Tinylamb は左深木 `place ⋈ person ⋈ person_knows ⋈ p2 ⋈ person_likes ⋈ message` を選び **243 万行・幅 12 列**へ膨張した。
- **STATS-CEB C2** (`3.9 ms` [srv `4.1 ms`] vs DuckDB `3.8 ms`, `1.03x` 差): `users u ⋈ posts p ⋈ comments c ⋈ users u2`（4 表鎖）。
- **JOB J4** (`8.8 ms`) / **BigBench B3** (`3.7 ms`): 多表結合の中間射影と結合トポロジ最適化。

## 提出型

[`BushyJoinAssociativityWithDeadKeyPrune.Goal`](../../../TinylambOpt/ResearchGoals/BushyJoinAssociativityWithDeadKeyPrune.lean) を import し、`theorem sound : TinylambOpt.ResearchGoals.BushyJoinAssociativityWithDeadKeyPrune.Goal` を提出する。

任意の `aRows, bRows, cRows, dRows` と任意のペア `p : Int × Int` に対し、`(optBushyPruned ...).count p = (refLeftDeep ...).count p`（多重集合の完全一致）を示す。

## 境界例と適用範囲

- `D` が空のときや中間結合キー `k2` が `none` のときも、不要な射影除去（`k1` を `AB` から、`k3` を `CD` から落とす）によって結果タプルの一致回数が変化しない。
- 等価結合の残余条件や上段フィルタが `k1` や `k3` を参照しない場合のみ、それぞれ `A ⋈ B` / `C ⋈ D` の直後で即座にカラムを落とせる。

## 実装との照合先

- `executor/detail/build_input.cpp` (`greedy_table_rows` / `ordered_column_prune`)
- `executor/detail/explain_format.cpp` (Line 1275 `greedy_table_rows`)
- `plan/cascades.cpp` (Line 1284 `GreedyConnectedSplit`)
