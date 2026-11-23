# セミジョイン枝刈り先行型・検査付き事前集約 `SUM`（Semijoin-Guarded Checked Eager SUM）

**未証明。** 従来の片側／両側事前集約では、相手表と一度も結合しない死キー（unmatched keys）のグループ上で `[INT64_MAX, 1]` が足されると逐次検査付き `SUM` が偽のオーバーフロー失敗を起こすため、未結合キーが存在しうる一般の結合へ `SUM` プッシュダウンを適用できなかった。`L` を先に `R` のキー集合へセミジョイン枝刈りした上で、生存行上のファンアウト重み付き正・負寄与和が `i64` に収まるならば、任意精度（i128）中間和＋最終 1 回範囲検査の事前集約が、展開後逐次検査付き `SUM` と完全同値（かつ偽オーバーフローを起こさない）であることを検証する。

## 対象敗北クエリとボトルネック

- **H2O H5** (`162.6 ms` [srv `117.1 ms`] vs DuckDB `7.7 ms`, `21.09x` 差): 符号付き INT64 列 `x.v1`, `y.v2` の `SUM` オーバーフロー保守制約により、事前集約プッシュダウンを適用できず 2,400 万行を展開。
- **STATS-CEB C1** (`8.6 ms` vs DuckDB `7.3 ms`, `1.19x` 差): `SUM(c.score)` を結合前に集約する際の死キー境界保証が未整理。
- **LDBC-SNB L2** (`41.7 ms` vs ClickHouse `38.5 ms`, `1.08x` 差): `SUM(m.score)` の事前集約ガード。
- **BigBench B2** (`44.7 ms` vs DuckDB `10.6 ms`, `4.21x` 差): `uservisits` と `rankings (page_rank > 40)` で未マッチ `dest_url` がある場合の `SUM(u.ad_revenue)` プッシュダウン安全性。

## 提出型

[`SemijoinGuardedCheckedEagerSum.Goal`](../../../TinylambOpt/ResearchGoals/SemijoinGuardedCheckedEagerSum.lean) を import し、`theorem sound : TinylambOpt.ResearchGoals.SemijoinGuardedCheckedEagerSum.Goal` を提出する。

生存行上の重み付き正・負合計が共に `[i64Min, i64Max]` 内なら `optSemijoinEagerSum ls rs = refFlatJoinSum ls rs ∧ optSemijoinEagerSum ls rs ≠ none` であること、および死キー `k=99` 上で無ガード事前集約だけが `none` に失敗する具体反例を示す。

## 境界例と適用範囲

- `deadOverflowExampleL` の未マッチキー `k = 99` に `[i64Max, 1]` があっても、参照実行はマッチせず `some 14` を返す。セミジョイン枝刈り無しの事前集約（`unguardedPreAgg`）はここで誤ってオーバーフローエラー（`none`）になる。
- 生存行の重み付き和であっても、入力値自身が `inI64` 外の場合や正寄与和＋負寄与和の中間順序によっては前提が守るため、正寄与総和・負寄与総和を**別々に** `[i64Min, i64Max]` で抑えることが必須。

## 実装との照合先

- `lean/TinylambOpt/CheckedEagerSum.lean`（既存の無セミジョイン全行範囲版定理）
- `plan/cascades.cpp` (`eager_aggregation_over_join`)
- `executor/detail/fused_grouped_join.cpp` (`RuntimeFilter` 付きビルド→プローブ事前集約)
