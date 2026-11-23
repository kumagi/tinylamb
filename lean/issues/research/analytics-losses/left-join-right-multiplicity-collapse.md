# 右表列非参照 `LEFT OUTER JOIN` の多重度重み縮約（Right Multiplicity Collapse）

**未証明。** `SELECT COUNT(*), COUNT(L.v), SUM(L.v) FROM L LEFT JOIN R ON L.k = R.k WHERE pred(L)` のように右表 `R` の列を出力・WHERE 述語で一切参照しない左外部結合では、`R` との全ペアを生成せずに、`R` の非 NULL キーごとの多重度 `mult(k)` のみを集計し、各左行に重み `w = if L.k IS NULL then 1 else max 1 mult(k)` を掛けて直接集約する手法が完全に同値になることを検証する。

## 対象敗北クエリとボトルネック

- **STATS-CEB C4** (`103.5 ms` [srv `109.5 ms`] vs ClickHouse `4.4 ms` / DuckDB `5.5 ms`, **`23.52x` 差**): `posts LEFT JOIN tags ON p.tags = t.tag_name WHERE (p.tags IS NULL OR p.tags LIKE '%sql%') AND ...` で `'sql'` の 4,578 行 × 250 行＝`1,144,500` ペアを結合し、`t` の列は 1 つも出力しないにもかかわらず全ペア評価を実行。

## 提出型

[`LeftJoinRightMultiplicityCollapse.Goal`](../../../TinylambOpt/ResearchGoals/LeftJoinRightMultiplicityCollapse.lean) を import し、`theorem sound : TinylambOpt.ResearchGoals.LeftJoinRightMultiplicityCollapse.Goal` を提出する。

`∀ (ls : List LRow) (rs : List RRow), optMultiplicityAgg ls rs = refLeftJoinAgg ls rs`（`(COUNT(*), COUNT(L.v), SUM(L.v))` の一致）を示す。

## 境界例と適用範囲

- `L.k = none` かつ `rs` にも `R.k = none` が複数存在する行では、SQL の `NULL = NULL` 非マッチ規約により重みは **必ず `w = 1`** になる（`buggyNullMatchAgg` のように NULL 同士を数えると壊れる）。
- `L.v = none` でも `L` 行が生き残る限り `COUNT(*)` には `+w` され、`COUNT(L.v)` には `+0`・`SUM(L.v)` への非 NULL 寄与もなし。
- `R` に追加の結合残余条件（非等値や左表列を含む `ON` 述語）がある場合はキー多重度のみへの縮約は不可。

## 実装との照合先

- `executor/detail/fused_grouped_join.cpp` (Line 1304付近 `JoinType::kLeft` / Line 1459 `ResidualJoinPredicates`)
- `executor/detail/planning_heuristics.cpp` (`ResidualJoinPredicates`)
