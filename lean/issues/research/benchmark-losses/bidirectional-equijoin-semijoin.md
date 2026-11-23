# 3 表以上等値結合の双方向セミジョイン伝播と対称 OR 対の因数分解

**未証明。** 結合鎖 `A ⋈ B ⋈ C` において `A, C` の非 NULL キー集合で中間表 `B` を絞ったのち、`B'` の生存キー集合 `keysB1, keysB2` を再び `A, C` へ逆伝播（双方向セミジョイン）しても結合結果と多重度が完全に保たれること、および `(a = x ∧ b = y) ∨ (a = y ∧ b = x)` 型の対称ペア選言フィルタを単一表プッシュ可能な `(a = x ∨ a = y) ∧ (b = x ∨ b = y) ∧ a ≠ b` へ書き換えられる十分条件 `x ≠ y` を検証する。

## 対象敗北クエリとボトルネック

- **TPC-H Q07** (`59.43 ms` vs DuckDB `54.54 ms`, `+4.88 ms`)・**TPC-H Q08** (`55.11 ms` vs DuckDB `52.31 ms`, `+2.80 ms`): `nation`/`part`/`region` の高選択率が中間ファクト（`lineitem`/`orders`）や対向次元へ双方向に流れず、また Q07 の 2 国間対称ペア OR が個別列フィルタへ因数分解されていない。
- **TPC-DS Q17** (`52.69 ms` vs DuckDB `39.39 ms`)・**Q25** (`27.27 ms` vs `13.29 ms`)・**Q29** (`42.65 ms` vs `41.26 ms`)・**Q58** (`35.87 ms` vs `23.74 ms`)・**Q83** (`10.91 ms` vs `8.21 ms`): `store_sales ↔ store_returns ↔ catalog_sales` 等の多段鎖で最狭チャネルからのキー逆流（RuntimeFilter / ビット集合）が 1 方向に留まっている。

## 提出型

[`BidirectionalEquiJoinSemijoin.Goal`](../../../TinylambOpt/ResearchGoals/BidirectionalEquiJoinSemijoin.lean) を import し、`theorem sound : TinylambOpt.ResearchGoals.BidirectionalEquiJoinSemijoin.Goal` を提出する。

`Option Int` の NULL 不一致・重複行を含むリスト同値性 `optBidirectionalSemijoinChain as bs cs = refJoinChain as bs cs` と、`x ≠ y → symPairFactored x y a b = symPairRef x y a b` の両方を要求する。

## 境界例と適用範囲

- `x = y = 5` のとき、元の式は `(5, 5)` で `true` だが因数分解形の `a ≠ b` は `false` となるため、`x ≠ y` が必須条件である（ファイル内 `decide` で検証済み）。
- NULL キーは左・中間・右のいずれにあっても結合およびセミジョイン通過しない。

## 実装との照合先

- `common/runtime_filter.hpp` / `executor/detail/fused_grouped_join.cpp`
- `plan/optimizer.cpp` / `query/select_rewrites.cpp`
