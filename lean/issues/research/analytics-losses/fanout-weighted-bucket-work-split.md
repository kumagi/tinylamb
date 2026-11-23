# ファンアウト重み付きプローブ／バケット並列分割と外部結合未マッチ集約（Fanout-Weighted Parallel Bucket Split）

**未証明。** 現在の融合結合では `total_left < 16384` の場合にプローブ側行数だけでワーカー数を決めるため、右表バケットとの一致ファンアウトが巨大でもシングルスレッド実行（`workers=1`）に陥り、逆に巨大バケットを単純にビルドシャード分割すると `LEFT JOIN` で片方シャード未マッチ時に誤って NULL 拡張行を重複出力する。ビルドバケットを複数シャード `bShards` に分割しつつプローブをファンアウト重み境界で並列チャンク化し、全シャード横断の `anyMatched` 論理和を取ってから単一の未マッチ `(l, none)` を出力する方式が、多重集合として単一スレッドの参照結合と完全同値であることを検証する。

## 対象敗北クエリとボトルネック

- **STATS-CEB C4** (`103.5 ms` [srv `109.5 ms`] vs ClickHouse `4.4 ms`, `23.52x` 差): `posts` のフィルタ後行数が `left_rows = 6,069` 行（`< 16,384`）であるため `[fusion] workers=1` に落ち、1,144,500 件の `'sql'` バケットマッチを 1 スレッドで逐次処理。
- **BigBench B2** (`44.7 ms` vs DuckDB `10.6 ms`, `4.21x` 差) / **H2O H5** (`162.6 ms`) / **JOB J4** (`8.8 ms`): ホットキー（power=2.6 のべき乗偏り）に大部分のファンアウトペアが集中し、単純行数均等分割ではコア間負荷が極端に偏る。

## 提出型

[`FanoutWeightedBucketWorkSplit.Goal`](../../../TinylambOpt/ResearchGoals/FanoutWeightedBucketWorkSplit.lean) を import し、`theorem sound : TinylambOpt.ResearchGoals.FanoutWeightedBucketWorkSplit.Goal` を提出する。

任意の `pChunks`（`pChunks.flatten = ls`）とビルドシャード分割 `bShards` に対して、並列分割プローブ結果の多重集合（要素別 `count`）が `refLeftJoin ls bShards.flatten` と完全一致することを示す。

## 境界例と適用範囲

- `buggyPerShardLeftJoin`（各シャードごとに独立に `LEFT JOIN` を評価する誤実装）では、一致行を含むバケットと無関係な空シャードが `(l, none)` を余分に出力してしまう。必ず全ビルドシャードの結果フラグ `anyMatched` を論理 OR してから一度だけ未マッチ出力を行う必要がある。
- NULL 結合キー（`l.k = none`）を持つ左行はシャード数にかかわらず正確に 1 回だけ `(l, none)` を出す。

## 実装との照合先

- `executor/detail/fused_grouped_join.cpp` (Lines 7823–7827 `workers = ... ParallelWorkerCount((total_left / 16384) + 1)`)
- `executor/detail/fused_grouped_join.cpp` (Lines 16916–17180 `JoinType::kLeft` probe loop)
