# キー単調チャンクのワーカー局所 Top-K ストリーム集約＋境界シーム結合

**未証明。** グループ化キーが全ワーカーチャンクを連結したとき単調非減少（`lineitem.l_orderkey` 結合 `orders.o_orderkey` など）であるとき、各ワーカーは完了済みグループをサイズ `≤ K` の Top-K リストだけに即時畳み込み、隣接ワーカーを跨ぎうる先頭・末尾シーム（高々 2 グループ）だけを外に出すことで、巨大ハッシュ表なしに厳密な `Top-K` が得られることを検証する。

## 対象敗北クエリとボトルネック

- **TPC-H Q03** (`64.35 ms` vs DuckDB `54.45 ms`, `+9.90 ms / 1.18x` 差): `orders`（147 万件）をプローブ側に 60M 行 `lineitem` を融合集約する際、147 万グループ分の全状態バッファを確保・マージしてから最後に Top-10 へ絞っている。

## 提出型

[`MonotonicKeyStreamTopK.Goal`](../../../TinylambOpt/ResearchGoals/MonotonicKeyStreamTopK.lean) を import し、`theorem sound : TinylambOpt.ResearchGoals.MonotonicKeyStreamTopK.Goal` を提出する。

`(chunks.flatten.map Prod.fst).Pairwise (fun a b => a ≤ b)` の下で、`optStreamTopK k chunks = refTopK k chunks` と各ワーカーの保持閉区間長 `(streamWorker k chunk).interiorTop.length ≤ k` を要求する。

## 境界例と適用範囲

- 同一キー `3` がワーカー `0`, `1`, `2` の 3 チャンクに連続して跨がるケースでも、`seams` の逐次合成 `combineTwo` によって合算値が保たれる。
- `k = 0` または空チャンク `[[]]` では出力は空リスト `[]` となる。キー単調性が崩れると同一キーが後続で再出現するため本最適化は適用不可（TLCS v3 `is_monotonic` メタデータで判定）。

## 実装との照合先

- `executor/detail/fused_grouped_join.cpp` (`TryFusedGroupedJoin` monotonic key path)
- `table/tlcs_file.hpp` (`ColumnSynopsis::is_monotonic`)
