# パック済み `(gid, val)` ソート重複排除＆値ハッシュ分割による無マージ `COUNT(DISTINCT)`

**未証明。** 大量グループや高カーディナリティ列に対する `COUNT(DISTINCT val)` において、(1) 非 NULL 行を `(gid, val)` のペアでパックしソート＋隣接重複除去して `gid` 毎に数える手法と、(2) 値ハッシュ `hash(val) % W` でワーカーへ分配して各ワーカーが独立にローカル重複除去・カウントし、マージ段階ではグループ間セット統合を一切行わず件数だけ加算する手法の双方の正当性を検証する。

## 対象敗北クエリとボトルネック

- **TPC-H Q16** (`46.61 ms` vs DuckDB `46.30 ms`, `+0.31 ms`): 27,840 グループ × 8 ワーカー分のハッシュセット構築・統合。
- **ClickBench Q09** (`195.85 ms` vs DuckDB `194.28 ms`)・**Q12** (`55.31 ms` vs `45.94 ms`)・**Q14** (`320.21 ms` vs ClickHouse `310.31 ms`): 1 億行に対する高カーディナリティ `COUNT(DISTINCT UserID)` でワーカー間のハッシュセット統合（merge）がボトルネックになっている。

## 提出型

[`PackedGidDistinctSortDedup.Goal`](../../../TinylambOpt/ResearchGoals/PackedGidDistinctSortDedup.lean) を import し、`theorem sound : TinylambOpt.ResearchGoals.PackedGidDistinctSortDedup.Goal` を提出する。

`0 < w` の下で `sortDedupCount rows gid = refDistinctCount rows gid` および `hashPartitionCount w h rows gid = refDistinctCount rows gid` の両方を要求する。

## 境界例と適用範囲

- `val = none` はどちらの経路でも重複除去前に完全に除外され、全て `none` のグループは `0` を返す。
- 値ハッシュ `h v % w` によるパーティション分割では、異なるワーカーのローカル集合が互いに素（交わり空）になるため、`(gid, cnt)` の和だけで厳密な `COUNT(DISTINCT)` に一致する。

## 実装との照合先

- `executor/detail/typed_stream_aggregate.cpp` / `typed_stream_aggregate_partitioned.cpp`
- `executor/detail/fused_grouped_join.cpp`
