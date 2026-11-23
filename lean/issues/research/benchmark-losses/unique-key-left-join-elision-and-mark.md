# 右辺一意キー `LEFT JOIN` 除去・1-bit マーク化・非等値上界プッシュダウン

**未証明。** 右側の非 NULL 結合キー集合に重複が無いとき (`NonNullKeysNodup R`)、(1) 右側列を参照しない `LEFT OUTER JOIN` は左表そのものに完全除去でき、(2) 右キーの非 NULL 判定 (`r.key IS NOT NULL`) は右ペイロードを読まない 1-bit マーク集合帰属検査に落ち、(3) `l.val < r.val` 型非等値結合は右側最大値 `maxR` による左側事前フィルタ `l.val < maxR` を許すことを検証する。

## 対象敗北クエリとボトルネック

- **TPC-DS Q72** (`277.28 ms` vs DuckDB `87.35 ms`, `+189.93 ms / 3.17x` 差): 未使用の `LEFT OUTER JOIN catalog_returns`（複合主キー上）と、`inv_quantity_on_hand < cs_quantity` 非等値条件の上界事前枝刈り欠如により、`inventory`（1.33 億行）× `catalog_sales`（1440 万行）が全展開されている。
- **TPC-DS Q40** (`6.70 ms` vs DuckDB `5.40 ms`)・**TPC-DS Q80** (`40.19 ms` vs DuckDB `40.09 ms`): 返品表への `LEFT OUTER JOIN` で一意キー構造の直接配列／マーク化が未活用。

## 提出型

[`UniqueKeyLeftJoinElisionAndMark.Goal`](../../../TinylambOpt/ResearchGoals/UniqueKeyLeftJoinElisionAndMark.lean) を import し、`theorem sound : TinylambOpt.ResearchGoals.UniqueKeyLeftJoinElisionAndMark.Goal` を提出する。

一意キー前提での左射影同値・1-bit マーク同値、`∀ r ∈ rs, r.2 ≤ maxR` での `innerJoinLt` 事前フィルタ同値、および右キー重複 (`dupKeyR`) で除去が破綻する反例を要求する。

## 境界例と適用範囲

- 左キーが `none`（NULL）の行は、右側に `none` があっても決してマッチせず `ljoin` でもマーク `false` でも 1 回だけ出力される。
- 右側に重複非 NULL キー `[(some 1, 10), (some 1, 20)]` があると左行が 2 倍になるため、複合一意キー制約または動的重複無し検査が必須である。

## 実装との照合先

- `plan/optimizer.cpp` / `query/select_rewrites.cpp` (redundant LEFT JOIN elimination)
- `executor/detail/fused_grouped_join.cpp` / `executor/parallel_hash_join.cpp`
