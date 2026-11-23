# 自己結合表の 1 パス共有走査＋2 ロールビットマスク分割（Shared Scan & Dual Role-Bitmask Split）

**未証明。** 同一ベーステーブル `T` を別名・異なるフィルタ条件で 2 回参照する自己結合（例: `L1` の `person p JOIN person_knows k ON p.id = k.person1 JOIN person p2 ON k.person2 = p2.id WHERE p2.creation_yr < 2013`、`C2` の `users u ... JOIN users u2 ON c.user_id = u2.id WHERE u.creation_yr >= 2012 AND u2.reputation > 1000`）では、`T` を 2 度別個に走査・ハッシュ構築する代わりに `T` を 1 パス走査して 2-bit ロールマスク `(cond1, cond2)` 付きノード配列へ射影し両端プローブへ共有供給する方が、出力多重集合は完全一致のまま表走査タッチ回数を厳密に半減できることを検証する。

## 対象敗北クエリとボトルネック

- **LDBC-SNB L1** (`81.9 ms` [srv `49.2 ms`] vs DuckDB `16.0 ms`, **`5.12x` 差**): `person` を `p`（フィルタなし）と `p2`（`creation_yr < 2013`）として別々に走査・マテリアライズ・ハッシュ構築している。
- **STATS-CEB C2** (`3.9 ms` [srv `4.1 ms`] vs DuckDB `3.8 ms`, `1.03x` 差): `users` を `u`（`creation_yr >= 2012`）と `u2`（`reputation > 1000`）として独立に 2 回走査・ハッシュ構築。

## 提出型

[`SelfJoinSharedScanDualFilterSplit.Goal`](../../../TinylambOpt/ResearchGoals/SelfJoinSharedScanDualFilterSplit.lean) を import し、`theorem sound : TinylambOpt.ResearchGoals.SelfJoinSharedScanDualFilterSplit.Goal` を提出する。

任意の `rows, edges` で出力ペア多重集合が完全一致し、かつ `rows ≠ []` のとき `costOpt < costRef`（走査タッチ数が `T.length < 2 * T.length`）となることを示す。

## 境界例と適用範囲

- `cond1` と `cond2` の両方を同時に満たす行（`overlap`）が自己結合で `src` にも `dst` にも使われるケースでも、2-bit ビットマスク `(mask1, mask2)` により双方の役割を独立に正しく判定できる。
- 空表 `rows = []` では両側 0 となり厳密不等号は `rows ≠ []` 前提のもとで成立する。

## 実装との照合先

- `executor/detail/build_input.cpp` (`base_scan_cache_hits` — `EXPLAIN ANALYZE L1/C2` では片側しか共有されていない)
- `executor/detail/parallel_table_scan.cpp` (ベーススキャン共有)
