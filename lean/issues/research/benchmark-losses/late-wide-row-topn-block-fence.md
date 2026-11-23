# ビット演算 AND/OR ブロック除外とソート境界 Fence＋遅延ワイド行フェッチ

**未証明。** (1) ブロック単位に保持した 64-bit `andMask` / `orMask` サマリにより点検索 `col = target` で不要ブロックを `O(1)` ビットテスト `(target &&& ~~~orMask) = 0 ∧ (~~~target &&& andMask) = 0` で弾けること（ハッシュ／非ソート列でも効く）、(2) ワイド行 `SELECT * ... ORDER BY sortKey NULLS LAST LIMIT K` でソート列ヘッダ `minNonNullSort` とヒープ第 K 位上界を用いたブロック全体スキップおよび最終勝者 `≤ K` 件へのワイド列遅延フェッチの正当性を検証する。

## 対象敗北クエリとボトルネック

- **ClickBench Q20** (`12.12 ms` vs DuckDB `4.42 ms`, ClickHouse `8.32 ms`, `2.74x` 差): ほぼ空振り／超少数一致の `UserID = 435090932899640449` 点検索でブロック読み飛ばし粒度が粗く全ブロックデコード／比較が走る。
- **ClickBench Q24** (`35.93 ms` vs DuckDB `26.02 ms`, `+9.91 ms / 1.38x` 差): `SELECT * FROM hits WHERE URL LIKE '%google%' ORDER BY EventTime LIMIT 10` で 105 列すべての物質化と `LIKE` 判定がソート上界で十分に早期打ち切りされていない。

## 提出型

[`LateWideRowTopNBlockFence.Goal`](../../../TinylambOpt/ResearchGoals/LateWideRowTopNBlockFence.lean) を import し、`theorem sound : TinylambOpt.ResearchGoals.LateWideRowTopNBlockFence.Goal` を提出する。

非空ブロック上の `t ∈ xs → mayMatchMask (foldAndMask xs) (foldOrMask xs) t = true`、および `ValidSortBlock` 下での `res.payloads = refWideTopN k bs ∧ res.payloadDecodes ≤ k` を要求する。

## 境界例と適用範囲

- `EventTime ASC NULLS LAST` において、ヒープにすでに `K` 件の **非 NULL** 値が入っていれば、末尾最大値 `τ` より大きい `minNonNullSort` を持つブロックおよび `allNullSort = true` のブロックは、たとえブロック内に NULL や大きな時刻があっても絶対に Top-K に入らない。
- ヒープ内の第 `K` 位に `none`（NULL）が残っている段階ではブロック枝刈りを行わない（`blockPrunedByHeap` が `false`）。

## 実装との照合先

- `table/tlcs_file.cpp` (`DecodeSingleRowByBlockIndex` / late column fetch / block metadata)
- `executor/sort.cpp` / `executor/detail/relational_sort_limit.cpp`
