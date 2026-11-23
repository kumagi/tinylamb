# `LEFT JOIN` 保存側結合キー条件の NULL 側転送と `UNION DISTINCT` 分配則

**未証明。** (1) `L LEFT OUTER JOIN R ON L.k = R.k` において保存側 `L` が結合キー上の述語 `P(L.k)` を満たす（`none` は偽）とき、NULL 側 `R` にも結合前フィルタ `P(R.k)` を転送して `R` の集約・スキャン規模を劇的に削っても結果と NULL パディングが完全一致すること（非キー列条件の転送は誤り）、(2) 射影後の値に対するフィルタが `UNION DISTINCT` の内側に分配できることを検証する。

## 対象敗北クエリとボトルネック

- **TPC-DS Q78** (`580.22 ms` vs DuckDB `417.52 ms`, `+162.70 ms`): 主側 `ss` の `ss_sold_year = 2000` が `LEFT JOIN ws / cs` の右側結合キー（`ws_sold_year`, `cs_sold_year`）へ伝播せず、`ws` / `cs` が全年度を集約している。
- **TPC-DS Q75** (`181.71 ms` vs DuckDB `103.69 ms`, `+78.02 ms`): `UNION DISTINCT` の外側の `curr_yr.d_year = 2002 AND prev_yr.d_year = 2001` が 3 チャネル CTE 内側の `d_year IN (2001, 2002)` へ押込まれていない。

## 提出型

[`LeftJoinPreservedToNullableFilterTransfer.Goal`](../../../TinylambOpt/ResearchGoals/LeftJoinPreservedToNullableFilterTransfer.lean) を import し、`theorem sound : TinylambOpt.ResearchGoals.LeftJoinPreservedToNullableFilterTransfer.Goal` を提出する。

`LEFT JOIN` への結合キー条件転送同値、`[LawfulBEq α]` 下での `(unionDistinct a b).filter p = unionDistinct (a.filter p) (b.filter p)`、および非結合キー列フィルタ `R.payload` を `R` へ転送すると結果が壊れる反例を要求する。

## 境界例と適用範囲

- `L` と `R` の結合キーがともに `none` でも SQL `=` はマッチしないため、左側 `L` が `P(L.k)` を通る非 NULL 値しか残らないことで `R` の全非一致行と `none` キーを事前除去しても NULL パディング行を含め変化しない。
- 非結合列（例：`ss_sales_price > 100`）を右表 `R` に直接送るとマッチが消えて NULL 埋め行へ化ける（`badFilterRPayload`）。

## 実装との照合先

- `plan/optimizer.cpp` (`PredicatePushdown` / `EquivalenceClass`)
- `query/select_rewrites.cpp`
