# 境界既知・複数キー集約の番兵スロット付き並列平坦配列直集計（Bounded Multi-Key Flat Array Parallel Aggregate）

**未証明。** 2 列整数キー `GROUP BY (k1, k2)` において `k1 ∈ [min1, min1 + span1)`・`k2 ∈ [min2, min2 + span2)` のような狭い直積域（例: `(id1[1..400], id2[0..99])` や `(f.id, yr)`）が TLCS ゾーンマップ等から判明している場合、`NULL` キーを末尾番兵座標 `span1` / `span2` へ割り当てた `(span1 + 1) × (span2 + 1)` 要素の平坦配列をワーカーごとに確保してハッシュ計算ゼロで集計し、ワーカー間で配列添字対応の要素加算マージを行う方式が、空入力・ゼロ相殺和・NULL キーを含め汎用ハッシュ集約と完全同値であることを検証する。

## 対象敗北クエリとボトルネック

- **H2O H2** (`29.0 ms` [srv `11.5 ms`] vs DuckDB `10.9 ms`, `2.66x` 差): `GROUP BY id1, id2`（200,000 行 → 37,709 グループ）。ゾーンマップ上 `400 × 100 = 40,000` スロットの完全密 2D 配列に収まるのに `HashAggregate`（`aggregate_ms = 8.37 ms`）を通っている。
- **H2O H4** (`18.2 ms` [srv `5.4 ms`] vs DuckDB `5.9 ms`, `3.08x` 差): `WHERE id3 < 4 GROUP BY id1, id2`（80,157 行 → 29,057 グループ、`aggregate_ms = 3.05 ms`）。
- **LDBC-SNB L2** (`41.7 ms` [srv `30.0 ms`] vs ClickHouse `38.5 ms`, `1.08x` 差): `GROUP BY f.id, m.creation_yr`（3,000 × 20 域、`25,779` グループ）。
- **BigBench B3** (`3.7 ms`) / **STATS-CEB C5** (`5.3 ms`): 小直積複数キー（`region × category × yr`, `creation_yr × post_type`）のハッシュ集約。

## 提出型

[`BoundedMultiKeyDirectArrayAggregate.Goal`](../../../TinylambOpt/ResearchGoals/BoundedMultiKeyDirectArrayAggregate.lean) を import し、`theorem sound : TinylambOpt.ResearchGoals.BoundedMultiKeyDirectArrayAggregate.Goal` を提出する。

入力分割 `rowsA ++ rowsB` に対する 2 ワーカー並列構築＋要素マージ `optParallelFlatAgg` が、NULL 番兵キー `(none, some 0)`・非出現グループ（`none`）・負数相殺 0 グループ（`some 0`）を含めて `refGroupAgg (rowsA ++ rowsB)` と一致することを示す。

## 境界例と適用範囲

- `seen : Bool` ビットを `cntStar` / `sumVal` と独立に持つことで、未出現スロット（`none`）と `v = NULL` のみ出現したグループ（`some (cnt, none)`）や和 0 グループ（`some (cnt, some 0)`）を完全に区別する。
- `(span1 + 1) * (span2 + 1)` が L2/L3 キャッシュ予算または出力閾値を超える場合（疎な高カーディナリティ域）は通常のハッシュ集約へフォールバックする。

## 実装との照合先

- `executor/detail/typed_stream_aggregate.cpp` (Lines 4369–4530 `HashAggregate` パス / dense 1-key vs 2-key)
- `executor/detail/fused_grouped_join.cpp` (`split_key_unpack_only` / dense direct array)
