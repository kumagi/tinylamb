# eager_aggregation_over_join

- 状態: draft   /   執筆基準リビジョン: `9ce7837` + Lean拡張 (2026-11-18)
- 定義位置: `plan/cascades.cpp` の `RuleSet::Default()`（登録名 `"eager_aggregation_over_join"`）
- Lean 形式証明: `lean/TinylambOpt/JoinPreaggregation.lean`（定理 `eager_scalar_agg_over_mn_join_sound`, `eager_group_agg_over_mn_join_sound`, `star_cascade_two_hop_agg_sound`）および `lean/TinylambOpt/AvgMinMaxHavingDistinct.lean`（定理 `eager_min_over_mn_innerJoin`, `eager_max_over_mn_innerJoin`）

## 概要

内部等価結合 $\text{Join}(L, R, p_{\text{equi}})$ の直上にあるグループ集約について、全集約演算（`COUNT`, `MIN`, `MAX`）が片側 $S \in \{L, R\}$ のみに依存する場合、結合前に $S$ を `(S 内の外部 GROUP BY キー ∪ S 内の結合キー)` で事前集約（2 段階 Eager Aggregation）します。

$$\gamma_{G,\, \text{AGG}(S.x)}(L \bowtie_{L.jk = R.jk} R) \equiv \gamma_{G,\, \text{FINAL\_AGG}(\text{partial})}\bigl(\gamma_{G_S \cup \{S.jk\},\, \text{PARTIAL\_AGG}(S.x)}(\sigma_{S.jk \text{ IS NOT NULL}}(S)) \bowtie R\bigr)$$

結合順序探索（`join_associativity_*`, `join_commutativity`）と組み合わせることで、スタースキーマやスノーフレークスキーマの多段結合（`Fact ⋈ Dim1 ⋈ Dim2`）に対しても段階的に事前集約が連鎖適用されます。

## 適用条件と安全性制約

1. `GROUP BY` キーが空でないこと（空グループ時のスカラー集約 1 行生成境界を回避）。
2. 全集約が `DISTINCT` / `FILTER` / `NeedsGroupContext()` を持たない `COUNT`, `MIN`, `MAX` であり、参照列が $S$ 側のみに収まること（`COUNT` の最終段は `SUM($pre_agg)` で合算）。
3. 入力に `SUM` がある場合は適用しません。結合で消えるキーを先に集約すると、元のクエリでは発生しないオーバーフローが起こります。反対側のキーが一意でも起こります。
4. **無益な事前集約の抑止**: 事前集約キーが既に $S$ の候補キー（主キー・UNIQUE）を被覆している場合は行数が減らないため発火をスキップします。
5. 結合キーが `NOT NULL` と証明されていない場合は、事前集約の前に `IS NOT NULL` 選択を挿入して結合不能な `NULL` バケットを早期除去します。
6. **不動点ガード**: 現在の Group のタグが `eager_pre_agg:` で始まる、または入力 Group のタグが `eager_agg_join:` で始まる場合は再適用しません（`plan/cascades.cpp` の `memo.Get(group).tag.starts_with("eager_pre_agg:") || memo.Get(input_id).tag.starts_with("eager_agg_join:")`）。既に事前集約済みの Group に本 Rule を再適用すると集約のネストが無限に生成されるためです。

## 検証テスト

- `CascadesTest.EagerAggregationOverJoinPushesTwoStagePreAgg`
- `CascadesTest.EagerAggregationSkipsSumEvenWithUniqueJoinPartner`
- `SqlBugfixRegressionTest.JoinDiscardsOverflowingSumGroup`
