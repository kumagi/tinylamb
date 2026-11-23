# outer_to_inner_join_on_null_rejecting_having

- 状態: draft   /   執筆基準リビジョン: `9ce7837` + Lean拡張 (2026-11-18)
- 定義位置: `plan/cascades.cpp` の `RuleSet::Default()`（登録名 `"outer_to_inner_join_on_null_rejecting_having"`）、および `query/join_reduction.cpp` の `ReduceOuterJoins`
- Lean 形式証明: `lean/TinylambOpt/AvgMinMaxHavingDistinct.lean`（定理 `having_null_rejecting_demotes_left_join_sound`、反例 `having_demote_breaks_count_star_on_coarse_group`）

## 概要

外部結合の直上に `GROUP BY` と `HAVING` がある場合、`WHERE` 句が無くても `HAVING` 条件が「全行未マッチのグループ」を棄却し、かつ混在グループ内の未マッチ NULL パディング行が全集約で無視されるならば、外部結合を内部結合へ降格します。

## 適用条件と反例ガード

1. `GROUP BY` キーおよび集約外の列が NULL 補完側リレーション $R$ を一切参照しないこと。
2. クエリ内の全集約が `!NeedsGroupContext()`・`!HasWhereFilter()`・`Child() != nullptr` であり、`SUM` / `AVG` / `MIN` / `MAX` / `COUNT(expr)` のいずれかであって、引数式 `expr` が $R$ に対して NULL 厳格（`IsStrictOnRelations`）かつ例外非送出（`ExpressionCannotThrowForPlanner`）であること。
3. **`COUNT(*)` 禁止ガード（Lean 反例）**: 粗いグループ（例: `GROUP BY l.dept`）内に「$R$ に一致した行」と「不一致の行」が混在するとき、`LEFT JOIN` の `COUNT(*)` は不一致行も数えるのに対し、`INNER JOIN` は不一致行を落とすため `COUNT(*)` が減少します。したがって `COUNT(*)` を含む場合は降格を禁止します。
4. 全未マッチグループ（`SUM`/`AVG`/`MIN`/`MAX` が `NULL`、`COUNT(r_expr)` が `0`）を代入したときに `HAVING` 連言が偽または `NULL` となること。
5. **不動点ガード（再適用防止）**: `cascades.cpp` の本 Rule は降格済みの Group を再探索しません。`memo.Get(agg_id).tag.starts_with("agg-over-demoted-outer:")` と `memo.Get(join_group_id).tag.starts_with("demoted_outer_having:")` がそれを検査します。生成される派生グループのタグは `"demoted_outer_having:" + GroupIdsTag({left_id, right_id}) + PredicateTag(join_expr.predicate)` という複合タグで、子 ID と結合述語の指紋を含みます。これにより派生グループの隔離と冪等性（同じグループへの再適用防止）が保たれます(D1 契約)。

## 検証テスト

- `CascadesTest.OuterToInnerOnHavingDemotesStrictAggAndRefusesCountStar`
- `JoinReductionTest.NullRejectingHavingOnStrictAggReducesLeftJoin`
- `JoinReductionTest.HavingPositiveCountOnRightColumnReducesLeftJoin`
- `JoinReductionTest.HavingDemotionRefusesWhenCountStarPresent`
