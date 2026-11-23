# 単一テーブルの異種複数コンシューマへの和述語 1 パス並列供給

**未証明。** 同一の巨大ファクト表（または同一結合サブツリー）を参照し、それぞれ異なるフィルタ `p1, p2, p3` と異なる演算構造（例：重複排除集合・大域平均・グループ集約）を持つ複数のコンシューマ／CTE 分岐に対し、和述語 `p1 ∨ p2 ∨ p3` で 1 回だけ走査して一致する状態だけを更新すれば、3 回個別走査するのと同一出力を保ちつつスキャン数を `1x`（`reads ≤ rows.length`）へ削減できることを検証する。

## 対象敗北クエリとボトルネック

- **TPC-DS Q14** (`439.90 ms` vs DuckDB `231.14 ms`, `+208.76 ms`): `store_sales` / `catalog_sales` / `web_sales` を `cross_items`・`avg_sales`・外側本体クエリの 3 箇所で個別スキャンしている。
- **TPC-DS Q65** (`162.21 ms` vs DuckDB `55.44 ms`, `2.93x` 差): 同一 `(store_sales ⋈ date_dim)` 上の `GROUP BY ss_store_sk, ss_item_sk` 集計をサブクエリ `sb` と `sc` で 2 度走査している。
- **TPC-DS Q44** (`45.28 ms` vs DuckDB `31.93 ms`): `store_sales (ss_store_sk = 4)` を昇順順位 `V1`・降順順位 `V2`・`HAVING` スカラ閾値の 3 回個別走査。

## 提出型

[`SinglePassMultiConsumerFactScan.Goal`](../../../TinylambOpt/ResearchGoals/SinglePassMultiConsumerFactScan.lean) を import し、`theorem sound : TinylambOpt.ResearchGoals.SinglePassMultiConsumerFactScan.Goal` を提出する。

3 種の異種コンシューマ出力（`distinctKeys`, `globalSumCnt`, `groupedSumCnt`）が 3 パス参照と一致し、`reads ≤ rows.length` かつ非空時は `opt.reads < ref.reads` となることを要求する。

## 境界例と適用範囲

- 1 行が `p1` と `p2` の両方にヒットする場合でも、各コンシューマの内部状態だけを独立更新するため重複や欠落は起きない。
- どの述語にも合致しない行は 3 コンシューマすべてをスキップする。空表でも 3 パスと一致する。

## 実装との照合先

- `query/tpch_select_rewrites.cpp` (`__merged_scalar_agg`)
- `executor/detail/fused_grouped_join.cpp` / `query/sql_engine.cpp`
