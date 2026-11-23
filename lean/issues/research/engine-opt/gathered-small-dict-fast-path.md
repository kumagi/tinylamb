# `left_pack` 小辞書圧縮 ID リマップでの NULL・未参照センチネル分離

**未証明。** 4096 行以下の小表・高選択結合などで参照行だけから稠密サブ辞書 `compactDict` を構築しグループ集約する際、SQL の `NULL` バケットと未参照エントリ用センチネルが衝突せず各グループの `SUM` を正確に保つ契約を検証する。

## 提出型

[`GatheredSmallDictFastPath.Goal`](../../../TinylambOpt/ResearchGoals/GatheredSmallDictFastPath.lean) を import し、`theorem sound : TinylambOpt.ResearchGoals.GatheredSmallDictFastPath.Goal` を提出する。

元の辞書 `dict` に重複がなく `sel` の添字とコードが妥当であるとき、再採番後のグループ集約を `compactDict` で引き直した値が全行デコード集約と全キー `k : Option Int`（`none` を含む）で一致することを要求する。

## 境界例と適用範囲

NULL をコード `0` 等に潰す `badSentinelMap` は `NULL` グループの和を `dict[0]` に誤加算する反例を固定した。選択ベクタから外れた未参照コードは `compactDict` に現れず `none` を返す。

## 実装との照合先

- `executor/detail/fused_grouped_join.cpp` (`left_pack` `kGathered` `total_rows <= 4096` fast path)
