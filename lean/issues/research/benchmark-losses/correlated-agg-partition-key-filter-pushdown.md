# 相関パーティション平均のパーティションキー押込み vs 半結合パーティション枝刈り

**未証明。** 相関閾値比較 `r.val > (num/den) * AVG(r.val) OVER (PARTITION BY partKey)` において、(1) 外側フィルタがパーティションキーだけを参照する場合（`s_state='TN'` on `ctr_store_sk`）は CTE 内部へ直接押込んで無害なこと、(2) 外側フィルタがエンティティキー等別列を参照する場合（`ca_state='GA'` on `c_customer_sk` vs `ctr_state`）は直接押込むと平均値が壊れるため、「該当行が存在する `activeParts` で先にパーティションだけを半結合枝刈りし、生存パーティション内の全行を使って平均を出す」2 段階変換が厳密に健全であることを検証する。

## 対象敗北クエリとボトルネック

- **TPC-DS Q01** (`13.82 ms` vs DuckDB `12.35 ms`, `+1.47 ms`): `s_state = 'TN'` から絞られる `ctr_store_sk` が `store_returns` 集計前に十分押込まれず全店舗分を集約。
- **TPC-DS Q30** (`19.15 ms` vs DuckDB `16.25 ms`)・**TPC-DS Q81** (`48.91 ms` vs DuckDB `32.43 ms`, `+16.48 ms`): `ca_state = 'GA'` が現住所テーブル条件のため、対象顧客が属さない州パーティションの枝刈りや半結合最適化が効いていない。
- **TPC-DS Q65** (`162.21 ms` vs DuckDB `55.44 ms`): 店舗別平均 `sb` と明細 `sc` の結合前枝刈り・共有不足。

## 提出型

[`CorrelatedAggPartitionKeyFilterPushdown.Goal`](../../../TinylambOpt/ResearchGoals/CorrelatedAggPartitionKeyFilterPushdown.lean) を import し、`theorem sound : TinylambOpt.ResearchGoals.CorrelatedAggPartitionKeyFilterPushdown.Goal` を提出する。

`optPushPartFilter` の直接押込み同値性、`optSemiJoinActiveParts` の半結合パーティション枝刈り同値性、および `pEntity` の直接誤押込みが平均を変えて誤答を生む存在反例をすべて要求する。

## 境界例と適用範囲

- 同一パーティション `pk = 1` に顧客 `10`（値 `100`）と顧客 `20`（値 `200`）がいるとき、外側で `ek = 10` に絞る条件を `AVG` 計算前に押込むと平均が `150` から `100` へ縮み、`> 0.8 * AVG` の合否が逆転する。
- 一方、パーティション集合 `activeParts` に絞る半結合は非アクティブなパーティション丸ごとだけを削るため各生存パーティションの平均値に影響しない。

## 実装との照合先

- `query/select_rewrites.cpp` (`RewriteTpcdsDecorrelate` / correlated scalar subquery)
- `plan/optimizer.cpp`
