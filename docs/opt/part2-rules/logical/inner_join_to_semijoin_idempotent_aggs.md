# inner_join_to_semijoin_idempotent_aggs

- 状態: draft   /   執筆基準リビジョン: `9ce7837` + Lean拡張 (2026-11-18)
- 定義位置: `plan/cascades.cpp` の `RuleSet::Default()`（登録名 `"inner_join_to_semijoin_idempotent_aggs"`）
- Lean 形式証明: `lean/TinylambOpt/AvgMinMaxHavingDistinct.lean`（定理 `inner_join_to_semijoin_idempotent_aggs_sound`）

## 概要

内部結合 $\text{Join}(L, R, p)$ の直上に集約 $\gamma$ があり、`GROUP BY` キーおよび出力式がすべて片側（例: $L$）のみを参照し、かつすべての集約が**重複度不変（multiplicity-insensitive）**である場合、内部結合を準結合 $\text{SemiJoin}(L, R, p)$ へ縮退させます。

$$\gamma_{L.k,\, \text{MAX}(L.v)}(L \bowtie_p R) \equiv \gamma_{L.k,\, \text{MAX}(L.v)}(L \ltimes_p R)$$

M:N 結合であっても、$R$ 側に 1 行以上マッチするか否かだけが集約結果に影響し、2 行目以降の重複マッチはべき等集約や `DISTINCT` 集約の値を変えません。

## 適用条件と反例ガード

1. `grouping_sets` / `partition_by` / `target_list` / `predicate` が参照する全列が、結合の片側 $S \in \{L, R\}$ に厳密に属すること。
2. すべての集約が `!NeedsGroupContext()` かつ `InnerGroupBy().empty()` であり、以下のいずれかであること:
   - べき等集約: `MIN`, `MAX`, `LOGICAL_AND`, `LOGICAL_OR`, `BIT_AND`, `BIT_OR`
   - 重複排除集約: `COUNT(DISTINCT e)`, `SUM(DISTINCT e)`, `AVG(DISTINCT e)`（`Child() != nullptr`）
3. **反例ガード**: 非 `DISTINCT` の `SUM` / `COUNT` / `AVG` が 1 つでも含まれる場合、$R$ 側の複数マッチによる重複度が結果を変えるため発火を禁止します。

## 検証テスト

- `CascadesTest.InnerJoinToSemiJoinForIdempotentAggsAndRefusesSum`
