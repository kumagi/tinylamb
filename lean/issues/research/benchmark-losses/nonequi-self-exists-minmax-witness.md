# 非等値自己 `EXISTS` のグループ `(min, max)` Witness O(1) 判定

**未証明。** 相関自己非等値結合 `EXISTS (SELECT * FROM T r2 WHERE r2.oid = r1.oid AND r2.wh <> r1.wh)` について、ハッシュ自己結合を行う代わりに `oid` 毎の非 NULL `(minWh, maxWh)` 要約だけを 1 回構築し、`r1.wh = some a` に対し `minWh < maxWh ∨ a ≠ minWh` を検査する `O(1)` 判定（`r1.wh = none` 時は常に `false`）が全入力で厳密同値であることを検証する。

## 対象敗北クエリとボトルネック

- **TPC-DS Q16** (`7.99 ms` vs DuckDB `6.62 ms`, `+1.37 ms / 1.21x` 差): `catalog_sales cs1` と `cs2` の `cs1.cs_order_number = cs2.cs_order_number AND cs1.cs_warehouse_sk <> cs2.cs_warehouse_sk` で非等値自己ハッシュ結合を行っている。

## 提出型

[`NonEquiSelfExistsMinMaxWitness.Goal`](../../../TinylambOpt/ResearchGoals/NonEquiSelfExistsMinMaxWitness.lean) を import し、`theorem sound : TinylambOpt.ResearchGoals.NonEquiSelfExistsMinMaxWitness.Goal` を提出する。

すべての `rows : List Row` と `r : Row` について `optMinMaxWitness rows r = refSelfExists rows r` が無条件に成り立つことを要求する。

## 境界例と適用範囲

- 同一 `oid` 内に異なる倉庫 `(some 3, some 8)` が存在して `minWh < maxWh` であっても、調べる行自身の `r.wh` が `none`（NULL）であれば SQL 三値論理 `some 3 <> NULL` は UNKNOWN のため必ず `false` を返す。
- グループ内すべての非 NULL 値が `minWh = maxWh = 5` で揃っているとき、`a = 5` なら `false`、`a ≠ 5`（例：`r` が外れ値のとき）なら `true` を返す。

## 実装との照合先

- `query/select_rewrites.cpp` / `plan/cascades.cpp`
- `executor/parallel_hash_join.cpp`
