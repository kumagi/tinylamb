# 符号反転ビット射影＋安定 LSB 基数ソートによる大規模降順整列（Order-Reversing Signed-Int64 LSB Radix Sort）

**未証明。** 19,646〜37,709 件を返す `GROUP BY ... ORDER BY agg DESC`（`LIMIT` なし）において、符号付き 64-bit 整数ソートキー `k ∈ [i64Min, i64Max]` を `flipDesc(k) = (2^64 - 1) - (k - i64Min)` で単調減少非負整数へ全単射し、最小位（LSB）からの多パス安定バケット配布（counting radix sort）で整列する手法が、比較ベースソートの分岐予測ミス・比較関数ディスパッチなしに **正確な多重集合保存・降順整列・同一キー間安定性** を保証することを検証する。

## 対象敗北クエリとボトルネック

- **H2O H2** (`29.0 ms` [srv `11.5 ms`] vs DuckDB `10.9 ms`, `2.66x` 差): `37,709` グループの `ORDER BY n DESC`。
- **LDBC-SNB L2** (`41.7 ms` [srv `30.0 ms`] vs ClickHouse `38.5 ms`, `1.08x` 差): `25,779` グループの `ORDER BY n DESC`（`sort_ms = 3.13 ms`）。
- **BigBench B1** (`29.0 ms` [srv `15.2 ms`] vs DuckDB `11.5 ms`, `2.51x` 差): `19,646` グループの `ORDER BY total DESC`（`execute_ms = 13.14 ms` → `Execution Time = 15.19 ms` でソート・後処理が約 2 ms）。
- **STATS-CEB C2** (`3.9 ms`, `3,514` 行 `ORDER BY c DESC`) / **JOB J4** (`8.8 ms`, `ORDER BY n DESC`)。

## 提出型

[`BoundedCountRadixDescSortEquiv.Goal`](../../../TinylambOpt/ResearchGoals/BoundedCountRadixDescSortEquiv.lean) を import し、`theorem sound : TinylambOpt.ResearchGoals.BoundedCountRadixDescSortEquiv.Goal` を提出する。

`AllKeysInI64 xs` および `2^64 ≤ radix ^ passes`（8-bit × 8 パスおよび 4-bit × 16 パス双方を包摂）の条件のもとで、(1) 要素別多重度不変 `(radixSortDescWith radix passes xs).count x = xs.count x`、(2) 降順整列 `List.Pairwise (fun a b => b.key ≤ a.key)` を示す。

## 境界例と適用範囲

- 符号付き整数境界 `i64Min`・`-1`・`0`・`1`・`256`・`i64Max` をすべて正しく降順へ並べ、`i64Min` 引き算による符号ビット反転で `-1 > -100` が保たれる（2 の補数ビットをそのまま符号無し扱いする誤りとの対比）。
- 値域が狭い（例: `|max - min| < 2^16`）場合は `passes = 2` に自動短縮できる一般性を持つ。

## 実装との照合先

- `executor/sort.cpp` / `executor/detail/relational_sort_limit.cpp` (`ORDER BY` 実行経路)
- `lean/TinylambOpt/StableRadix.lean`（既存の部分範囲符号付き安定基数ソートモデル）
