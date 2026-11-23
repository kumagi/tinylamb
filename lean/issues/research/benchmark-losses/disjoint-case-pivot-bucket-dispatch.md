# 排他的 `CASE` バケット分類の 1 パス単一スロット更新（`ELSE 0`/`ELSE NULL`）

**未証明。** 同一キー／日付／期間列に対する `K` 個の相互排他的な `SUM(CASE WHEN bucket = j THEN val ELSE 0/NULL END)` を、行毎に `K` 回独立評価する代わりに `bucket(r)` を 1 回だけ評価して高々 1 スロットを更新し、空入力や `val = NULL` ヒットを含む `ELSE 0` vs `ELSE NULL` の極限境界まで厳密一致させることを検証する。

## 対象敗北クエリとボトルネック

- **TPC-DS Q99** (`67.14 ms` vs DuckDB `15.89 ms`, `4.23x` 差)・**Q62** (`27.99 ms` vs `12.71 ms`, `2.20x` 差): `ship_date_sk - sold_date_sk` の 5 区間 (`<=30`, `31..60`, `61..90`, `91..120`, `>120`) を 5 本の独立アキュムレータとして評価している。
- **TPC-DS Q66** (`57.35 ms` vs `22.24 ms`, `2.58x` 差): 12 ヶ月×2 指標＝24 列の `SUM(CASE WHEN d_moy=m ...)` を融合で 24 本独立更新。
- **TPC-DS Q59** (`147.34 ms` vs `90.95 ms`)・**Q02** (`63.73 ms` vs `35.54 ms`): 7 曜日 `SUM(CASE WHEN d_day_name=... END)` の 7 分岐重複評価。
- **TPC-DS Q21** (`24.66 ms` vs `8.21 ms`, `3.00x` 差)・**Q90** (`7.81 ms` vs `5.17 ms`): 日付境界 2 バケット / 時間帯 2 バケットの分岐未一本化。

## 提出型

[`DisjointCasePivotBucketDispatch.Goal`](../../../TinylambOpt/ResearchGoals/DisjointCasePivotBucketDispatch.lean) を import し、`theorem sound : TinylambOpt.ResearchGoals.DisjointCasePivotBucketDispatch.Goal` を提出する。

`optPivotSums` が `.elseZero` / `.elseNull` 両モードで `refPivotSums` と一致し、かつ 1 行あたりスロット更新数 `maxTouches ≤ 1` であることを要求する。

## 境界例と適用範囲

- 入力空行 (`totalRows = 0`) では `ELSE 0` でも SQL `SUM` は `NULL` (`none`) を返す。
- 1 行しかなく、それがバケット `0` に当たったのに `val = none` の場合、`ELSE 0` モードでもスロット `0` は `none`、別スロット `1` は `some 0` になる（`bucketHits < totalRows` 判定で正確に区別）。

## 実装との照合先

- `executor/detail/fused_grouped_join.cpp` / `executor/detail/typed_stream_aggregate.cpp`
- `query/tpch_select_rewrites.cpp` (`__merged_scalar_agg`)
