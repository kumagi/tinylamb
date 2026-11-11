# Cascades optimizer

tinylamb's optimizer is split into three independent rule layers:

1. `ExpressionRuleSet` normalizes and folds scalar expression trees.
2. `cascades::RuleSet` inserts equivalent logical expressions and scan-filter
   annotations into a memo.
3. `cascades::ImplementationRuleSet` turns logical expressions into physical
   plans, which are costed and cached by required physical properties.

The layers depend on their small pattern and binding APIs, not on one another.
Adding or removing a scalar rule does not require editing memo exploration,
and adding a logical equivalence does not require changing the physical
implementations.


The current audit and complete registry are in [optimizer_rule_audit.md](optimizer_rule_audit.md).
There are 86 logical and 46 physical registrations. Normal single-table queries
and simple column-key GROUP BY use the same memo as joins; only exact MIN/MAX
index endpoint retrieval retains its specialized shortcut. Advanced grouped SQL
keeps the existing relational finish where the narrow memo contract declines.

Exploration seeds reachable children before parents. A changed child group
requeues previously processed dependents from their first expression, while
budget and size caps bound the fixed point. Physical search detects cycles.
Ordering penalties select a preferred child but are not accumulated as if an
unbuilt sort had executed. EmptyPlan's child contributes schema, not execution
cost. Aggregation and sort also compare children requested in key order.

Join costing uses ColumnStats::EstimateJoinMatches (MCV frequencies, NULL mass,
NDV and disjoint histogram domains). Group/distinct estimates share
TableStatistics::EstimateDistinctRows; this is a monotone NDV backoff heuristic,
not measured multicolumn correlation. Large-join seeding uses filtered base rows.
Range-index I/O uses direction-independent `ClusteringStrength`; ascending
order certification still uses `Correlation` and its layout-generation gate.
The persisted statistics format is unchanged.

## Search flow

`Optimizer::Optimize` rewrites expressions, decomposes conjuncts while building
the memo, builds one memo group for every equivalent relation set, explores
logical rules with a child-first, dependency-aware worklist, and performs a top-down cost search
from the root group. A memo group can contain multiple join trees. The best
physical plan is cached with a `(group, PhysicalProperties)` key.

Every predicate conjunct is attached to the deepest covering join or to a
single-relation scan filter. A residual `Selection` remains where the chosen
physical access path cannot prove it consumed a conjunct. For MULTI-relation
groups the residual re-evaluates every conjunct, so scan-filter bounds are a
pure optimization there. Single-table predicates are also exposed as Selection expressions for logical
rewrites. A pushed scan is added to the Selection result group; it must not
mutate the shared input group. Scan bounds must remain semantically exact.

The built-in logical rules include (see `RuleSet::Default` in
`plan/cascades.cpp` for the authoritative list, 86 registrations):

- `join_commutativity`
- `join_enumeration`
- `join_associativity_left`
- `join_associativity_right`
- `merge_selections`
- `push_selection_into_scan`
- `push_selection_through_join`
- `split_selection_over_join`
- `merge_projections`
- `push_selection_through_projection`
- `push_limit_through_projection`
- `push_selection_through_aggregation`
- `infer_join_predicates`
- `merge_limits`
- `eliminate_true_selection`
- `push_filter_past_setop` (only when the predicate resolves in every branch)
- `push_filter_through_sort` / `limit_push_through_sort` (offset-free only)
- `push_projection_through_join` / `push_projection_through_union`
- `self_join_elimination`, `unique_semi_to_inner` (key-equality gated),
  `outer_to_anti_join`, `union_all_push_limit`, `eliminate_double_sort`,
  `join_identity_dummy`, `count_star_rewrite_on_not_null`,
  `split_window`, `values_fold_into_union`, `cast_pushdown_on_comparison`,
  `extract_year_sargable`, `comparison_self_predicates`,
  and subquery/aggregate lowering rules

The built-in physical rules include (see `plan/implementation_rules.cpp`):

- `full_scan`
- `index_scan` (forward and reverse twins for ORDER BY DESC)
- `selection`
- `projection`
- `aggregation`
- `limit`
- `hash_join` (inner/outer/semi/anti, bitmap and TopN/Sort/Distinct/SetOp/Window variants)
- `band_join` (inner; one signed INT64/DATE ordered comparison, with child SortPlans)
- `outer_nested_loop` (LEFT non-equi; RIGHT normalizes to LEFT)
- `index_join`
- `late_materialization_join` (inner equi; right index key prefix supplies
  row identities, then the payload is read above the identity join)
- `zigzag_join` (inner; two ordered INT64 index inputs, equality or one range)
- `nested_loop_join`
- `aggregation` (its third `PlanAlternative` is the scalar stream aggregate beside hash/sort)
- `sort` / `sort_external_merge` (the two memory shapes of one ORDER BY: an
  in-memory `BufferedSortPlan` at or below the policy's row capacity, a
  `ExternalMergeSortPlan` above it; exactly one of the two rules offers an
  alternative, so the choice is a deterministic function of the estimate — see
  `plan/sort_plan.hpp`)
- `sort_incremental` reuses an ordered prefix and costs only the suffix runs.

`index_scan` may produce an `IndexOnlyScanPlan` when the chosen index covers
the requested columns. Covering candidates are priced as matched rows times
`kCoveringIndexCostFactor` (0.5) and skip the range clustering penalty (their
I/O is index-only and key-ordered), so a covering index wins over a
non-covering index of the same range and stays competitive with the
sequential full scan on an unclustered column. Multi-range shapes (constant
IN-list point unions and OR-of-prefix disjunctions) build sorted, disjoint
ranges and advertise the full key order, so an ORDER BY on the key needs no
Sort. A large ascending single-range
`IndexScanPlan` emits `ParallelIndexScan` (leaf-aligned key morsels,
`kParallelIndexScanMinRows`) with the residual predicate on a Selection above
it; the morsel-ordered handoff keeps the key order. `hash_join` offers both
in-memory and hybrid variants;
`nested_loop_join` is a cross product followed by evaluation of the complete
join predicate. `late_materialization_join` is limited to inner signed
INT64/DATE equijoins whose right child is a filter-free table scan with a
payload column beyond the index key prefix; its alternative declines
row-position-required shapes, so the existing DML property boundary is
unchanged.

Scalar defaults include constant folding for binary, unary, and `IN`
expressions, comparison canonicalization, boolean identities, singleton `IN`
lowering, double-negation, De Morgan, and constant `CASE` pruning. Further
normalizations cover `NOT` push-down over comparisons, `LIKE`, and null
checks; XOR boolean identities; idempotence (`x AND x`) and absorption
(`x AND (x OR y)`); arithmetic identities (`x + 0`, `x * 1`, `-(-x)`);
constant reassociation (`(x + 1) + 2` to `x + 3`); duplicate elimination in
`IN` lists; and uniform-result `CASE` flattening. Wildcard-free `LIKE`
constants become equality comparisons, stacked null checks collapse to
constants, `(x - c1) + c2` style mixed reassociation merges the constants.
Nested identical `CAST`s
collapse, and a common conjunct factored from both sides of `OR` becomes
`x AND (y OR z)`. Rewriting is bottom-up and continues to a fixed point.

## Selecting rules per optimization

Rules are values held by `OptimizerOptions`; no global registry is mutated.

```cpp
tinylamb::OptimizerOptions options = tinylamb::OptimizerOptions::Default();
options.relational_rules.Remove("join_commutativity");
options.expression_rules.Remove("de_morgan");
options.disabled_implementation_rules.insert("index_join");

auto plan_or = tinylamb::Optimizer::Optimize(query, context, options);
if (!plan_or) {
  return plan_or.GetStatus();
}
auto plan = plan_or.Value();
```

`Add` replaces a rule with the same name, which makes a local override
explicit and avoids ordering two implementations with the same identity.

A `Rule` may declare one target operator, a set of them, or none. The target
set is an applicability prefilter for the dependency-aware worklist: a rule whose
body gates on several root operators (INTERSECT/EXCEPT shapes, the
window/projection pair of `no_op_window_elimination`, …) must declare all of
them, so scan/join expressions skip the rule without running its pattern.
Setting `TINYLAMB_RULE_STATS=1` logs per-rule attempt counts during `Explore`
(planning-cost attribution; zero cost when unset).

## Phase 0 regression contracts

`plan/phase0_contract_test.cpp` is the executable contract for the optimizer
boundary. It compares the SQL facade, a plan emitted by `Optimizer::Optimize`,
and a directly emitted physical executor on the same ordered row multiset. Its
manifest repeats the success, NULL, empty-input, duplicate, boundary, and
error cases for Phase 0 items 55–59. The rule-contract case snapshots the
output schema, child ordering properties, evaluated row multiset (including
duplicates), and `TableStatistics::Filter` estimate before and after
`push_selection_into_scan`.

Every implementation alternative considered by `SearchEngine` carries its
implementation rule name, physical-operator name, required properties, and
provided properties.
SQL `EXPLAIN` renders these as `Rule:`, `Physical operator:`,
`Required properties:`, and `Provided properties:` lines, followed by the
sorted logical `Applied rules:` trace when exploration fired any rule. Ordinary single-table statements report their selected implementation rule; a missing or degraded
route remains visible through the best plan found so far.

Memo expression fingerprints are checked before insertion, and duplicate
rewrites do not count as rule applications or memo growth. `SetStepBudget`
terminates the dependency-aware worklist at the requested rule-application bound;
the built-in worklist and rule-application ceilings remain the final safety
nets. `plan/phase0_contract_test.cpp` exercises both the fingerprint-idempotent
and unique-growth budget paths.

## Adding a scalar rule with the C++ DSL

```cpp
using namespace tinylamb;
using namespace tinylamb::expression_dsl;

options.expression_rules.Add(ExpressionRule(
    "my_double_not",
    Unary(UnaryOperation::kNot,
          Unary(UnaryOperation::kNot, Any("input"))),
    [](const Expression&, const ExpressionBindings& bindings) {
      return bindings.at("input");
    }));
```

Patterns may capture any expression or constrain its `TypeTag`, binary
operation, unary operation, and children. `ExpressionChildren` and
`WithExpressionChildren` provide generic traversal/reconstruction for binary,
unary, aggregate, `CASE`, `IN`, function-call, `CAST`, array, and subquery
expressions.

## Adding a logical transformation

```cpp
using namespace tinylamb::cascades;
using namespace tinylamb::cascades::dsl;

options.relational_rules.Add(Rule(
    "my_join_swap", Join(Any("left"), Any("right")),
    [](const Bindings&, Memo& memo, GroupId group,
       const LogicalExpression& expression) {
      memo.AddExpression(group, memo.NewJoin(expression.children[1],
                                             expression.children[0]));
    }));
```

Logical patterns know only memo groups and logical operators. They do not
include catalog, executor, plan-cost, or storage types.

## Adding a physical implementation

Construct a `cascades::ImplementationRule` and append it to
`OptimizerOptions::extra_implementation_rules`. The callback receives captured
groups, the matched logical expression, already optimized child plans, and the
required `PhysicalProperties`; it returns zero or more `PlanAlternative`
values. The rule can later be replaced by adding another rule with the same
name, or disabled by placing its name in
`disabled_implementation_rules` when it is a built-in rule.

Tests for rule isolation and memo enumeration are in
`expression/rewrite_test.cpp`, `plan/cascades_test.cpp`, and
`plan/optimizer_test.cpp`.

## Implementation notes (reviewed 2026-08-24)

- `Optimizer::Optimize` returns `StatusOr<Plan>` and uses
  `Status::kNotImplemented` when no enabled implementation-rule combination
  can produce the root.
- Total cost is child cost plus each rule's `local_cost`. Scan rules use
  statistics/selectivity and Top-K hints; joins use cardinality estimates and
  spill penalties. Range index scans multiply matched rows by the column
  clustering penalty; covering (index-only) scans apply the covering factor
  instead and carry no clustering penalty. A single equijoin key still
  estimates `|L|*|R|/max(NDV)`.
  Additional keys combine those per-key selectivities with exponential
  backoff (`EstimateEquijoinSelectivity`): the catalog has no cross-column
  NDV, so the estimate sits between max-NDV and full independence. A plan
  that misses required ordering also pays an estimated `N log N` engine-side
  sort cost.
- `PhysicalProperties` contains `require_row_position`, `ordering`,
  `limit_hint`, `access_method`, and the reserved `distribution`. Selection,
  projection, and limit forward requirements; joins and aggregation drop them.
- Logical exploration drives a dependency-aware worklist, bounded by
  `kMaxExploreSteps` (20000 worklist pops/group visits) and
  `kMaxExploreRuleSteps` (50000 rule applications). The rule-application cap
  and worklist-pop caps set `SearchEngine::ExploreLimitReached` when work
  remains. `OptimizerOptions::search_step_budget` can impose a smaller
  rule-application budget; that separate limit sets `BudgetExhausted`, which
  the optimizer reports in `dump_memo`. The optimizer keeps the best available plan when exploration
  stops. Costing prunes
  branch-and-bound style: expressions whose children alone reach the incumbent
  are skipped. Groups have an expression cap and set
  `Memo::Degraded()` when alternatives are dropped. Join enumeration partitions
  each group once. A connected group emits only csg-cmp pairs (both sides
  induced-connected and the cut crossed by a conjunct); a disconnected group
  keeps exhaustive bipartitions. Enumeration stops above 16 relations and keeps
  the initial order. Left-deep LEFT chains, including an inner prefix, lower
  to nested `kOuterJoin` nodes so the outer hash and nested-loop executors
  run; WHERE is rechecked above the chain, and only preserved-prefix conjuncts
  become scan filters. GROUPING SETS / ROLLUP / CUBE branches are UNION ALL
  of one FROM+WHERE core; `SqlEngine` keeps that Cascades plan for the
  statement and reuses it for a later branch whose required columns are
  already in the plan schema. Scalar rewriting treats the pass cap as a safety net:
  when a rule set oscillates it returns the last stable (still
  semantics-preserving) form instead of rejecting the query (D6).
- Logical and implementation rules all get a chance to apply in registration
  order. `Add` replaces an existing rule of the same name. Scalar rewriting is
  first-success per node and also supports literal function-call folding.


## Rule precondition gates and D5 audit (docs/design.md D5)

Every logical equivalence rule must carry a precondition gate that proves the
rewrite preserves the result multiset. A rule whose precondition cannot be
proved does not fire; a rule found to violate the semantics is removed or
disabled and its basis recorded here. Counterexample tests live in
`plan/cascades_test.cpp`; the dual-path row-set audit lives in
`executor_test.cpp` (`OptimizerAndRelationalPathsAgree`).

| Rule | Required precondition | State | Counterexample test |
|------|---------------------|-------|---------------------|
| `push_down_limit_through_join` | every left row matches a right row (referential integrity) — right-key uniqueness only bounds matches at ≤1 | **Disabled**: uniqueness alone let LIMIT truncate the join output whenever an unmatched left row sat inside the limit prefix | `PushDownLimitThroughJoinOnUniqueKey`, `PushDownLimitThroughJoinSkipsNonUniqueJoin` |
| `topn_push_through_inner_join` | every left row matches exactly one right row; right uniqueness + left NOT NULL proves only ≤1 | **Disabled**: an unmatched left row inside the top-(limit+offset) prefix dropped rows the original plan keeps | `TopNPushThroughInnerJoinOnForeignKey` |
| `topn_push_through_proven_one_to_one_join` | inner join, single col=col edge, TopN keys local to the pushed side, other-side keys unique + NOT NULL, **and** ≥1-match proof via declared FOREIGN KEY (only with a pushed-side NOT NULL proof: a NULL key matches nothing, so the FK certificate alone bounds matches at ≤1) or optimizer snapshot containment (`Memo::IsProvenOneToOne`); no WITH TIES | Gated: the ≥1-match half re-enables the TopN push family only under 1:1 proof (TODO.md item 1) | `TopNPushThroughProvenOneToOneJoin{WithForeignKey,WithSnapshotProof,SkipsWithoutProof,SkipsNonLocalKeys}`, `TopNPush{ThroughSnapshotProvenOneToOneJoin,SkippedWhenReferentialIntegrityFails}` |
| `or_to_union` | single-relation Selection, exactly 2 stable disjuncts (no subquery/agg/window/volatile), not same-column equality (or_to_in territory) | Gated: UNION DISTINCT keeps the OR multiset under overlap and three-valued logic; branch tags content-addressed (TODO.md item 4a) | `OrToUnion{SplitsTwoBranchDisjunction,SkipsSameColumnEqualities,SkipsThreeDisjuncts}` |
| `distinct_eager_over_join` | full-row DISTINCT directly over inner join (either side) or LEFT join (preserved side only), other-side equi keys unique | Gated: ≤1 match per row makes pre-join dedup multiplicity-preserving; no containment needed; pushed-join tags carry the predicate (TODO.md item 4b) | `DistinctEagerOverJoin{PushesDistinctBelowJoin,SkipsNonUniqueSide}` |
| `topn_push_through_union_all` | TopN (no WITH TIES, finite) over UnionAll; per-branch same-keys TopN cap not already present | Gated: branch top-(limit+offset) provably contains global candidates; parent TopN re-establishes output; branch tags carry the branch GroupId (TODO.md item 4c) | `TopNPushThroughUnionAll{CapsBranches,SkipsWithTies}` |
| `dynamic_filter_pushdown_join` | derived probe group keeps the probe group's residual filter; no filter loss; the annotated keys are pinned by an `=` conjunct of this join and live in the single-relation probe group | Gated: probe filter carried onto the derived group, tag fingerprints the key set. The rule records ONLY which columns carry the join key (`Group::dynamic_filter_keys`) -- never a filter shape; the hash-join implementation rule turns that annotation into `ProductPlan::PreferDynamicFilter(build_left)` for an inner, non-NULL-safe, single INT64/DATE equi-join whose build side is materially smaller than the probe side, and the executor then publishes a sound `RuntimeFilterGroup` (exact set / min-max / Bloom) to the probe scan | `DynamicFilterPushdownJoin`, `DynamicFilterPushdownJoinAnnotatesTheProbeKeys`, `DynamicFilterPushdownJoinSkipsNonEqualityJoins`, `DynamicFilterPushdownJoinSkipsMultiRelationProbe`, `OptimizerTest.DynamicFilterMarkerFollowsTheAnnotatedProfitableShape`, `OptimizerTest.DynamicFilterMarkerEngagesOnAProbeHeavyShape` |
| `semi_reduction_pushdown_semi_join` / `semi_reduction_pushdown_anti_join` | derived probe group keeps the probe group's residual filter; no filter loss; the annotated keys are pinned by an `=` conjunct of this semi/anti join and live in the probe group | Gated: probe filter carried onto the derived group, tag fingerprints the key set. The rule records ONLY which columns carry the probe-side join key (`Group::semi_reduction_probe_keys`) -- never how the build is shrunk. The hash-join implementation rule turns that annotation into `ProductPlan::PreferSemiReduction()` for a semi/anti equi-join (no residual, non-NULL-safe key) whose build side `SemiReductionIsProfitable` says the reduction would shrink; the executor then materializes the probe key domain first and cuts the build down to one row per distinct key inside it, falling back to the unrestricted intake when the domain outgrows its cap | `SemiReductionPushdownAnnotatesTheProbeKeys`, `SemiReductionPushdownSkipsNonEqualityJoins`, `SemiReductionPushdownAnnotatesEveryCompositeKey`, `HashSemiReductionSqlTest.*`, `HashSemiReductionExecutorTest.*`, `SemiReductionGateTest.*` |
| `late_materialization_join` | inner `kJoin` only; no row-position requirement; right child is a filter-free base scan; at least one same-typed equi column on a signed INT64/DATE index prefix; payload columns survive the right projection; no NULL-safe key, ARRAY, retained-versioned index, subquery, or volatile expression | Gated (2026-09): unsupported shapes return no alternative and the existing hash/index/outer implementations keep ownership; a stale index read set falls back in the relational factory to a predicate join | `LateMaterializationPlanTest.RulePublishesLogicalPlanForWideSmallBuild`, `LateMaterializationPlanTest.RuleRejectsRowPositionOuterAndArrayShapes`, `LateMaterializationQueryTest.OuterJoinFallsBackAndNullKeysNeverMatch`, `LateMaterializationJoinTest.StaleIndexFallsBackToPredicateJoin` |
| `outer_hash_join` | LEFT/RIGHT/FULL with an equality key; a non-equality ON residual is evaluated per candidate before match marking and NULL padding; a residual is never placed in a post-join Selection for an outer kind | Gated: residual-aware hash execution; NULL/UNKNOWN failures remain unmatched and preserve the outer side | `OptimizerTest.OuterHashResidualPadsWhenAllKeyMatchesFail`, `HashJoinOuterTest.NonRejectingResidualPadsEveryOuterKind` |
| `outer_to_anti_join` | inner join, IS-NULL on a right column, no other right-column conjunct | Gated | `OuterToAntiJoinRequiresNullCheckOnAllRightColumns` |
| `in_list_to_semi_join` | every OR branch is a col=const equality on one column (no branch dropped); group tag fingerprints all constants | Gated | `InListToSemiJoinRejectsMixedColumnOr`, `InListToSemiJoinRejectsUncollectableBranch` |
| `eliminate_double_sort` | key expressions, direction and NULLS order all equal (collation is not modelled in the logical expression, so it cannot diverge) | Gated | `EliminateDoubleSortRequiresSameKeyExpressions`, `EliminateDoubleSortRequiresSameNullsFirst` |
| `no_op_window_elimination` bypasses | kWindow targets contain no window calls (branch 1); outer targets reference no `$win` outputs of the bypassed node and contain no raw calls (branch 2) | Gated: the comment promised "when outer expressions do not reference results" but the code never checked — bypassing a live `$win` silently dropped its computation (`compatible_windows_share_single_sort` regressed to `column $win0 not found` with ANALYZE) | `NoOpWindowElimination` (plain-target bypass preserved), `SqlEngineWindowRankingPlansThroughCascades` |
| opaque recursive-CTE leaf (`LiftRecursiveCtes`) | single top-level FROM site; body references no mapped CTE except itself (worktable); unique output names; single-use counting excludes the body's own self-references | Gated: multiple sites would re-run the fixpoint per site instead of sharing it; sibling-scoped references keep map-scoped execution; a renamed alias with surviving CTE-qualified refs would orphan them | `SqlEngineRecursiveCtePlansOuterThroughCascades`, `SqlEngineMultiUseAndRecursiveCteStayCorrect` |
| `unused_left_join_elimination` (statement-level, `TryEliminateUnusedOuterJoin`) | single LEFT/RIGHT edge (FULL excluded) with a single col=col equi-ON whose null-supplying-side column is declared UNIQUE/PK (each preserved row matches ≤1, so the join is the identity); nothing above the join (SELECT/ORDER/WHERE/QUALIFY, window partition/order keys included) names the dropped side | Gated: without the uniqueness proof duplicate matches would collapse (`OuterJoinSliceKeepsJoinWithoutUniquenessProof` pins 3 rows vs 2); an orphaned right reference has no sound placement so it blocks elimination | `OuterJoinSliceEliminatesUnusedUniqueRightSide`, `OuterJoinSliceEliminatesUnusedUniqueLeftSideForRight`, `OuterJoinSliceKeepsJoinWithoutUniquenessProof` |
| derived group creation | tag must fingerprint the expression *meaning*, never a bare count | `TargetListFingerprint` used by all projection/aggregate/values derived groups | `DerivedGroupFingerprintSeparatesDifferentInLists` |
| `setop_empty_simplification` | INTERSECT empties on ANY empty branch; EXCEPT only when its LEFT branch is empty (an earlier gate accepted any empty branch, collapsing non-empty `A EXCEPT empty-B` to `Empty`) | Gated (2026-09) | `CascadesTest.SetopEmptySimplification*` family |
| `filter_aggregate_pushdown` | scalar aggregation only (with GROUP BY, groups whose every row fails the predicate would survive as COUNT=0 phantoms); the predicate must be evaluation-count-safe (`SafeToReduceEvaluationCount` — no volatile functions or subqueries, which would be re-evaluated once per aggregate FILTER); a FILTER already present on an aggregate is conjoined, never overwritten | Gated (2026-09); volatile/subquery gate (2026-09-16) | `FilterAggregatePushdown`, `FilterAggregatePushdownRefusedForGroupedAggregation`, `FilterAggregatePushdownRefusedForVolatilePredicate` |
| `eliminate_sort_under_unordered_consumer` | the lowered aggregation copies the full payload (grouping sets / partition / output schema — dropping them turned GROUP BY aggregates into scalar ones) and only fires when every aggregate is order-insensitive (count/sum/avg/min/max/logical/count_if/statistical family/bit family/approx_count_distinct; WITHIN GROUP, string_agg, array_agg, ANY_VALUE, percentile and sketch aggregates are excluded) | Gated: payload + order-sensitivity allowlist (2026-09-16) | `EliminateSortUnderAggregationKeepsGroupingSets`, `EliminateSortSkipsOrderSensitiveAggregate` |
| `push_limit_through_left_join` | LIMIT without ORDER BY (any subset of the join output is acceptable); re-introduces the limit-through-join family ONLY under unordered semantics — must not be enabled once a required ordering can attach to a kLimit expression | Gated (recorded 2026-09) | — |
| `Memo::AddExpression` | schema/arity/child-group/relation-set contract | Violations are `CHECK` aborts (fast-fail, no exceptions); rule bodies must probe with `Memo::ContainsRelation` / uniqueness checks instead of relying on throws. `ExploreGroup` skips a rule whose alternative is declined, and keeps a legacy `std::invalid_argument` containment for the remaining planning-path throws | memo contract tests in `cascades_test.cpp` |

Re-adding a disabled rule requires re-establishing its precondition gate and a
counterexample test; a rule may not be re-enabled just to recover an
optimization opportunity.
