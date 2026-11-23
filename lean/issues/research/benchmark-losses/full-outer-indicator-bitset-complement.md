# 重複除去済み 2 ストリーム `FULL OUTER JOIN` 指標の単一表 2-bit タグ計数

**未証明。** 2 本の `(key1, key2)` 重複除去ストリームを結合して `(store_only, catalog_only, store_and_catalog)` の 3 件数だけを求める際、2 つの独立ハッシュ表を構築して `FULL OUTER JOIN` する代わりに、非 NULL ペア `(k1, k2)` を 1 つの 2-bit タグ付き集合へ統合登録（`bit 1` = `A`, `bit 2` = `B`, `bit 3` = 両方）し、部分 NULL `(some k1, none)` だけを別カウントして加算すれば、完全 NULL 結合不成立や `key1 IS NULL` の 3 値分岐まで厳密一致することを検証する。

## 対象敗北クエリとボトルネック

- **TPC-DS Q97** (`82.39 ms` vs DuckDB `75.04 ms`, `+7.35 ms`): `store_sales` と `catalog_sales` から作成した数十万組の `(customer_sk, item_sk)` 去重 CTE を別々に物質化したうえで `FULL OUTER JOIN` しており、プローブ・NULL 埋めコストが重複している。

## 提出型

[`FullOuterIndicatorBitsetComplement.Goal`](../../../TinylambOpt/ResearchGoals/FullOuterIndicatorBitsetComplement.lean) を import し、`theorem sound : TinylambOpt.ResearchGoals.FullOuterIndicatorBitsetComplement.Goal` を提出する。

任意の `a b : List (Option Int × Option Int)`（重複・部分 NULL・全 NULL を含む）に対して `optBitsetCounts a b = refFullOuterCounts a b` を要求する。

## 境界例と適用範囲

- `(some 2, none)` が `A` と `B` の双方にあっても SQL `ON a.1=b.1 AND a.2=b.2` は不一致となるため、互いに相手側で NULL 埋めされ `storeOnly` と `catalogOnly` の両方に `+1` される（`nullItemA` / `nullItemB` として加算）。
- 一方 `(none, some 9)` は Q97 外側 `CASE` が `customer_sk IS NULL` を検査するため、どのカウンタにも寄与しない。

## 実装との照合先

- `executor/parallel_hash_join.cpp` / `executor/detail/fused_grouped_join.cpp`
- `query/select_rewrites.cpp`
