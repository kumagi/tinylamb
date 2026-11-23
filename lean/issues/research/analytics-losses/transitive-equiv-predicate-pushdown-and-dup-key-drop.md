# 等値結合推移閉包の先行セミジョイン枝刈りと重複結合キー除去（Transitive Equivalence Semijoin & Duplicate Key Drop）

**未証明。** 実行時の `EXPLAIN ANALYZE` で観測された `HashJoin keys=2`（`C1`: `c.post_id = v.post_id`, `L1`: `k.person2 = l.person_id`, `L2`: `fm.forum_id = m.forum`, `J5`: `a2.movie_id = mi.movie_id`, `C5`: `p.owner_user_id = b.user_id`）の多くは推移律による同値キー重複であり、`probe_rows_rejected = 0` のままハッシュ・比較・プローブコストを増やしている。内部結合鎖 `A ⋈_{a.k = b.k} B ⋈_{b.k = c.k ∧ a.k = c.k} C` では `A` のキー集合を `C` へ推移セミジョイン押込みした上で第 2 結合の冗長キー `a.k = c.k` を安全に落とせること、および `LEFT JOIN` を挟む場合は冗長キー除去が結果を破壊しうることを検証する。

## 対象敗北クエリとボトルネック

- **JOB J5** (`2.8 ms` [srv `3.3 ms`] vs DuckDB `2.7 ms`, `1.03x` 差): `EXPLAIN ANALYZE` 上 `HashJoin keys=2` ＋ `Filter ... AND (a2.movie_id = mi.movie_id)` が残存。
- **STATS-CEB C1** (`8.6 ms` vs DuckDB `7.3 ms`, `1.19x` 差): `HashJoin keys=2` かつ `Filter ... AND (c.post_id = v.post_id)`。
- **STATS-CEB C5** (`5.3 ms` vs DuckDB `4.8 ms`, `1.09x` 差): `HashJoin keys=2` かつ `p.owner_user_id = b.user_id`。
- **LDBC-SNB L1** (`81.9 ms` vs DuckDB `16.0 ms`, `5.12x` 差): `HashJoin keys=2` かつ `m.creation_yr` の高い選択性・下流キー集合が `person_likes` / `person_knows` へ逆流しきらない。
- **LDBC-SNB L2** (`41.7 ms` vs ClickHouse `38.5 ms`, `1.08x` 差): `HashJoin keys=2` かつ `fm.forum_id = m.forum`。
- **JOB J4** (`8.8 ms`): `movie_keyword` と `movie_info` 間の `movie_id` 推移閉包セミジョイン不足。

## 提出型

[`TransitiveEquivPredicatePushdownAndDupKeyDrop.Goal`](../../../TinylambOpt/ResearchGoals/TransitiveEquivPredicatePushdownAndDupKeyDrop.lean) を import し、`theorem sound : TinylambOpt.ResearchGoals.TransitiveEquivPredicatePushdownAndDupKeyDrop.Goal` を提出する。

1. 3 表内部結合での `optInner3 as bs cs = refInner3 as bs cs`（`Option Int`・NULL 含む全入力）
2. `A ⟕ B`（左外部結合）を挟むと重複キー除去（`buggyDropAcrossLeftJoin`）が不 soundness になる反例存在

## 境界例と適用範囲

- `(A ⟕_{a.k = b.k} B)` で `b` が未マッチ `NULL` の場合、`b.k = c.k` は NULL により偽になるが `a.k = c.k` だけは真になりうる（`a.k=10, b=[], c.k=10` の反例）。したがって冗長キー除去は**非 NULL 確定の内部結合等価クラス上でのみ許可**する。

## 実装との照合先

- `executor/detail/fused_grouped_join.cpp` (Lines 2790–2803 `[fusion] join_keys=2`)
- `executor/detail/build_input.cpp` (`TINYLAMB_DISABLE_TRANSITIVE_KEY_FILTER`, Lines 10955–11055)
