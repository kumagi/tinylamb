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

## Search flow

`Optimizer::Optimize` rewrites expressions, decomposes conjuncts while building
the memo, builds one memo group for every equivalent relation set, explores
logical rules with an append-only worklist, and performs a top-down cost search
from the root group. A memo group can contain multiple join trees. The best
physical plan is cached with a `(group, PhysicalProperties)` key.

Every predicate conjunct is attached to the deepest covering join or to a
single-relation scan filter. A residual `Selection` remains where the chosen
physical access path cannot prove it consumed a conjunct, so bounds are an
optimization and never the sole correctness check.

The built-in logical rules include (see `RuleSet::Default` in
`plan/cascades.cpp` for the authoritative list, which has grown past 30):

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
  `outer_to_anti_join`, `union_all_push_limit`, `eliminate_double_sort`, and
  subquery/aggregate lowering rules

The built-in physical rules include (see `plan/implementation_rules.cpp`):

- `full_scan`
- `index_scan` (forward and reverse twins for ORDER BY DESC)
- `selection`
- `projection`
- `aggregation`
- `limit`
- `hash_join` (inner/outer/semi/anti, bitmap and TopN/Sort/Distinct/SetOp/Window variants)
- `index_join`
- `nested_loop_join`

`index_scan` may produce an `IndexOnlyScanPlan` when the chosen index covers
the requested columns. `hash_join` offers both in-memory and hybrid variants;
`nested_loop_join` is a cross product followed by evaluation of the complete
join predicate.

Scalar defaults include constant folding for binary, unary, and `IN`
expressions, comparison canonicalization, boolean identities, singleton `IN`
lowering, double-negation, De Morgan, and constant `CASE` pruning. Further
normalizations cover `NOT` push-down over comparisons, `LIKE`, and null
checks; XOR boolean identities; idempotence (`x AND x`) and absorption
(`x AND (x OR y)`); arithmetic identities (`x + 0`, `x * 1`, `-(-x)`);
constant reassociation (`(x + 1) + 2` to `x + 3`); duplicate elimination in
`IN` lists; and uniform-result `CASE` flattening. Wildcard-free `LIKE`
constants become equality comparisons, stacked null checks collapse to
constants, `(x - c1) + c2` style mixed reassociation merges the constants,
and `x AND (NOT x OR y)` shrinks to `x AND y`. Nested identical `CAST`s
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
  spill penalties. A plan that misses required ordering also pays an estimated
  `N log N` engine-side sort cost.
- `PhysicalProperties` contains `require_row_position`, `ordering`,
  `limit_hint`, `access_method`, and the reserved `distribution`. Selection,
  projection, and limit forward requirements; joins and aggregation drop them.
- Logical exploration has no pass cap: an append-only worklist and expression
  fingerprints drive it to completion. Groups have an expression cap and set
  `Memo::Degraded()` when alternatives are dropped. Join enumeration skips
  exponential bipartitions above 16 relations and keeps the initial order.
  Scalar rewriting treats the pass cap as a safety net: when a rule set oscillates it returns the last stable (still semantics-preserving) form instead of rejecting the query (D6).
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
| `push_down_limit_through_join` | join key unique on the pushed side | **Disabled** (Memo has no uniqueness constraints) | `PushDownLimitThroughJoinSkipsNonUniqueJoin` |
| `dynamic_filter_pushdown_join` | derived probe group keeps the probe group's residual filter; no filter loss | Gated: probe filter carried onto the derived group, tag fingerprints the bloom key set | `DynamicFilterPushdownJoin` |
| `outer_to_anti_join` | inner join, IS-NULL on a right column, no other right-column conjunct | Gated | `OuterToAntiJoinRequiresNullCheckOnAllRightColumns` |
| `eager_aggregation_over_join` | join provably 1:N (non-multiplying) on the aggregated side | **Disabled** (no constraint metadata) | `EagerAggregationOverJoin` (asserts no fire) |
| `aggregate_join_transpose` | join provably 1:N on a foreign key | **Disabled** (precondition unchecked) | `AggregateJoinTranspose` (asserts no fire) |
| `in_list_to_semi_join` | every OR branch is a col=const equality on one column (no branch dropped); group tag fingerprints all constants | Gated | `InListToSemiJoinRejectsMixedColumnOr`, `InListToSemiJoinRejectsUncollectableBranch` |
| `eliminate_double_sort` | key expressions, direction and NULLS order all equal (collation is not modelled in the logical expression, so it cannot diverge) | Gated | `EliminateDoubleSortRequiresSameKeyExpressions`, `EliminateDoubleSortRequiresSameNullsFirst` |
| `rank_row_number_to_topn` | window has no PARTITION BY and the Selection filters the window's own output column | Gated | `RankRowNumberToTopNSkipsPartitionedWindow`, `RankRowNumberToTopNSkipsNonWindowColumn` |
| derived group creation | tag must fingerprint the expression *meaning*, never a bare count | `TargetListFingerprint` used by all projection/aggregate/values derived groups | `DerivedGroupFingerprintSeparatesDifferentInLists` |
| `Memo::AddExpression` | schema/arity/child-group/relation-set contract | Throws `std::invalid_argument`; `ExploreGroup` catch-and-skips a bad rule alternative | memo contract tests in `cascades_test.cpp` |

Re-adding a disabled rule requires re-establishing its precondition gate and a
counterexample test; a rule may not be re-enabled just to recover an
optimization opportunity.
