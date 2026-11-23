# 選言 `EXISTS` 連鎖の短絡ビット集合段階縮小

**未証明。** `EXISTS(S1) AND (EXISTS(S2) OR EXISTS(S3))` 型の相関セミジョイン列を評価する際、`S1` を通った顧客キー集合 `B1` に対してまず `S2` を調べ、即座に確定したヒット集合 `B_hit = B1 ∩ S2` を除外した **残余キー集合 `B_rem = B1 \ B_hit` だけ** で第 3 ファクト表 `S3` をプローブしても、NULL キーを含め結果が完全一致し、`B_rem = ∅` なら `S3` スキャン／プローブが 0 回で済むことを検証する。

## 対象敗北クエリとボトルネック

- **TPC-DS Q35** (`95.04 ms` vs DuckDB `50.83 ms`, `+44.22 ms / 1.87x` 差)・**TPC-DS Q10** (`25.93 ms` vs DuckDB `20.27 ms`, `+5.66 ms`): `store_sales` の存在を確認した後、`web_sales` で既に真と確定した顧客キーまで含めて `catalog_sales` のハッシュ構築・プローブを重複実行している。

## 提出型

[`DisjunctiveExistsCascadingBitset.Goal`](../../../TinylambOpt/ResearchGoals/DisjunctiveExistsCascadingBitset.lean) を import し、`theorem sound : TinylambOpt.ResearchGoals.DisjunctiveExistsCascadingBitset.Goal` を提出する。

段階縮小結果 `res.rows = refFilter c s1 s2 s3`、`s3Probes ≤ |B1|`、および `B1 ⊆ S2 → s3Probes = 0` を要求する。

## 境界例と適用範囲

- 候補キーまたは `S1/S2/S3` に `none` (NULL) が含まれても SQL 等値では一致しないため、`optExistsIn none _ = false` で安全に落とせる。
- 元の出現順は `zipIdx` の添字順 `sortByIdx` で厳密に保存される。

## 実装との照合先

- `executor/detail/fused_grouped_join.cpp` / `executor/parallel_hash_join.cpp`
- `query/select_rewrites.cpp` (`EXISTS` OR rewrite)
