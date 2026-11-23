# 関数従属グループキー `f_j(k)` 除去＋`LIMIT K` 後遅延評価（オーバーフロー安全境界）

**未証明。** `GROUP BY k, k-1, k-2, k-3 ORDER BY COUNT(*) DESC LIMIT K` のように後続キーがすべて先頭キー `k` の決定的純粋関数 `f_j(k)` であるとき、全入力行が符号付き 64-bit 安全域（`int64Min + 3 ≤ k ≤ int64Max`）を満たすなら、`k` だけで集約・Top-K 抽出した後に最終 `≤ K` 件へ `(k-1, k-2, k-3)` を付与しても完全同値であることを検証する（範囲外データを含む場合に無検査遅延するとエラー隠蔽バグが起きる反例付き）。

## 対象敗北クエリとボトルネック

- **ClickBench Q36** (`144.43 ms` vs ClickHouse `128.11 ms`, `+16.31 ms / 1.13x` 差): `GROUP BY ClientIP, ClientIP-1, ClientIP-2, ClientIP-3` において、1 億行すべての `ClientIP-1..3` を事前計算して 4 倍幅のハッシュキー比較を行っている。

## 提出型

[`FunctionalDependentGroupKeyElim.Goal`](../../../TinylambOpt/ResearchGoals/FunctionalDependentGroupKeyElim.lean) を import し、`theorem sound : TinylambOpt.ResearchGoals.FunctionalDependentGroupKeyElim.Goal` を提出する。

`optKeyOnlyTopK lim rows = refGroupedTopK lim rows` の完全一致（オーバーフロー発生時はどちらも `none`）、および安全域検査を外した `badUncheckedDeferTopK` が `int64Min` でオーバーフローを握り潰す存在反例を要求する。

## 境界例と適用範囲

- TLCS v3 のブロック統計またはグループ別代表値走査で `minKey ≥ INT64_MIN + 3` が確定していれば、安全検査自体もブロックメタデータの `O(1)` チェックで済む。
- 範囲外行が Top-K 外に落ちるデータでは、無検査遅延すると参照実行の算術オーバーフロー例外と乖離する。

## 実装との照合先

- `query/select_rewrites.cpp` / `query/grouped_pipeline_lowering.cpp`
- `executor/detail/typed_stream_aggregate.cpp` / `table/tlcs_file.hpp` (`ColumnSynopsis::min_int`)
