# 属性正規 ID 空間での最小チャネル順 `INTERSECT` / `EXCEPT` 集合演算

**未証明。** 複数チャネル間で共通属性・日付の `INTERSECT DISTINCT` や `EXCEPT DISTINCT` 件数を求めるとき、単射な属性正規 ID（`canon a = canon b ↔ a = b`）へ先に写像した上で最小チャネルから順に整数ペア集合のプローブやビット削除を行えば、巨大な複合文字列タプルのハッシュ化やソートを避けつつ厳密な結果件数を得られることを検証する。

## 対象敗北クエリとボトルネック

- **TPC-DS Q38** (`201.54 ms` vs DuckDB `72.41 ms`, ClickHouse `182.95 ms`, `2.78x` 差): `store_sales` / `catalog_sales` / `web_sales` 3 チャネルで `(c_last_name, c_first_name, d_date)` の複合文字列タプルを全展開してから `INTERSECT DISTINCT` している。
- **TPC-DS Q87** (`214.44 ms` vs DuckDB `85.69 ms`, ClickHouse `205.68 ms`, `2.50x` 差): 同じく 3 チャネルの `(c_last_name, c_first_name, d_date)` 文字列組に対する `EXCEPT DISTINCT`。
- **TPC-DS Q14** (`439.90 ms` vs DuckDB `231.14 ms`): `cross_items` 算出での 3 チャネル `(i_brand_id, i_class_id, i_category_id)` `INTERSECT DISTINCT`。

## 提出型

[`CanonicalEquivIdSetAlgebra.Goal`](../../../TinylambOpt/ResearchGoals/CanonicalEquivIdSetAlgebra.lean) を import し、`theorem sound : TinylambOpt.ResearchGoals.CanonicalEquivIdSetAlgebra.Goal` を提出する。

単射正規化関数 `canon` 下での `optCanonIntersectCount = refIntersectCount` 及び `optCanonExceptCount = refExceptCount` と、正規化なしの代理キー直利用（同一姓名の別 `c_customer_sk` 存在時）が誤答となる存在反例を要求する。

## 境界例と適用範囲

- 顧客 `sk=1` と `sk=2` が同一属性 `"Smith"` を持つとき、生の代理キー `(1, 100)` と `(2, 100)` で `INTERSECT` すると `0` を返してしまうため、次元テーブルから構築した属性同値クラス ID への正規化が必須である。
- NULL 属性同士も SQL `DISTINCT` / `INTERSECT` / `EXCEPT` では同一扱いとなる（`canon none = 0` に統一）。

## 実装との照合先

- `executor/set_operation.cpp` / `executor/detail/fused_grouped_join.cpp`
- `table/tlcs_file.hpp` (`LSTableColumnDictionary`)
