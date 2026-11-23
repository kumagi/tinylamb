# 右表列非参照内部結合のキー多重度重みプッシュダウン（Inner Join Unreferenced-Side Weight Pushdown）

**未証明。** `L INNER JOIN (σ_pred R) ON L.k = R.k` において右表 `R` の列が結合後の `GROUP BY`・集約引数・出力に一切参照されない場合、`R` との全結合ペアを展開する代わりに `σ_pred R` をキー別多重度マップ `w(k) = COUNT(*)` へ事前縮約し、`L` を 1 回走査して `w(k) = 0` の行は即座にスキップし、`w(k) > 0` の行では加法指標（`COUNT(*)`, `COUNT(v)`, `SUM(v)`）へ `w(k)` 倍寄与、冪等指標（`MIN(v)`, `MAX(v)`）へは `w(k)` に依存せず 1 回だけ寄与させる変換が、全グループで完全同値であることを検証する。

## 対象敗北クエリとボトルネック

- **BigBench B2** (`44.7 ms` [srv `40.2 ms`] vs DuckDB `10.6 ms` / ClickHouse `17.4 ms`, **`4.21x` 差**): `uservisits u JOIN rankings k ON u.dest_url = k.page_url WHERE k.page_rank > 40` で `k` の列を一切出力せず、偏りのある重複 `page_url` のせいで `1,476,707` 件の結合比較を発生させている（DuckDB 側実測でも `1,476,707` 行展開が発生）。
- **JOB J4** (`8.8 ms` [srv `11.0 ms`] vs DuckDB `5.8 ms`, `1.52x` 差): `movie_info mi` の列が未参照にもかかわらず `747,046` 回の結合比較を実行。
- **JOB J5** (`2.8 ms` [srv `3.3 ms`] vs DuckDB `2.7 ms`, `1.03x` 差): `aka_title`, `movie_info_idx`, `title` のいずれも結果列に出ない `COUNT(*)` 結合で `165,532` 回の結合比較。
- **STATS-CEB C5** (`5.3 ms` [srv `5.9 ms`] vs DuckDB `4.8 ms`, `1.09x` 差) / **LDBC L5** (`3.2 ms` vs DuckDB `2.7 ms`, `1.19x` 差): `badges`（`b.user_id` 重複）、`person_knows` の右列非参照カウント。

## 提出型

[`InnerJoinUnreferencedSideWeightPushdown.Goal`](../../../TinylambOpt/ResearchGoals/InnerJoinUnreferencedSideWeightPushdown.lean) を import し、`theorem sound : TinylambOpt.ResearchGoals.InnerJoinUnreferencedSideWeightPushdown.Goal` を提出する。

`∀ (ls : List LRow) (rs : List RRow) (g : Int), optAgg5 ls rs g = refAgg5 ls rs g`（5 集約指標同時一致）を示す。

## 境界例と適用範囲

- **`w = 0`（右表未マッチまたは `pred = false`）の行は絶対に `MIN`/`MAX` や `COUNT` に渡さず必ずスキップ**しなければならない（`optAgg5` で `MIN`/`MAX` に `-100` 等が混入しない境界を例示）。
- `l.v = none` でも結合が成功（`w > 0`）した行は `COUNT(*)` には `+w` 寄与し、`COUNT(v)`・`SUM(v)` には寄与しない。

## 実装との照合先

- `executor/detail/fused_grouped_join.cpp` (Line 1017 `EqualityKeys`, Line 23055 `count_star_only` / `raw_sum_fusion`)
- `plan/cascades.cpp` (`eager_aggregation_over_join`)
