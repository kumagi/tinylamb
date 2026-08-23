# Cascades optimizer

tinylamb's optimizer is split into four independent rule layers:

1. `ExpressionRuleSet` normalizes and folds scalar expression trees.
2. `AlgebraRuleSet` distributes predicates over a small Filter/Join/Scan tree
   before memo exploration (pushdown and absorption).
3. `cascades::RuleSet` inserts equivalent logical expressions into a memo.
4. `cascades::ImplementationRuleSet` turns logical expressions into physical
   plans, which are costed and cached by required physical properties.

The layers depend on their small pattern and binding APIs, not on one another.
Adding or removing a scalar rule therefore does not require editing memo
exploration, adding a pushdown transformation does not require touching join
enumeration, and adding a logical equivalence does not require changing the
physical implementations.

## Search flow

`Optimizer::Optimize` rewrites the predicate, normalizes the query through the
algebra layer, builds one memo group for every equivalent relation set,
explores logical rules to a fixed point, and performs a top-down cost search
from the root group. A memo group can contain multiple join trees. The best
physical plan is cached with a `(group, PhysicalProperties)` key.

After algebra normalization every predicate conjunct lives either inside a
scan payload (applied as index bounds plus a local selection) or in the
residual selection applied once above the join tree; nothing is applied twice.

The built-in algebra rules are:

- `filter_conjoin`
- `filter_true_elimination`
- `filter_pushdown_join`
- `filter_absorb_scan`
- `join_condition_side_extraction`

Conjuncts that reference more than one relation become join conditions.
Conjuncts containing subqueries never move because their evaluation context
is the full joined row.

The built-in logical rules are:

- `join_commutativity`
- `join_enumeration`

The built-in physical rules are:

- `full_scan`
- `index_scan`
- `index_only_scan`
- `hash_join`
- `index_join`
- `nested_loop_join`

Plan construction adds the `empty_result_shortcut` (a constant-FALSE
predicate collapses into an `EmptyPlan`, while aggregations on top still emit
their summary row over the empty input) and elides projections whose select
list already matches the child schema one-to-one.

Scalar defaults include constant folding for binary, unary, and `IN`
expressions, comparison canonicalization, boolean identities, singleton `IN`
lowering, double negation, De Morgan, constant `CASE` pruning, `NOT`
comparison unwrapping, negated NULL checks, arithmetic identities (`x + 0`,
`x * 1`), AND/OR idempotence, OR-of-equalities to `IN`, `IN` list dedupe,
equality transitivity derivation (`a = b AND b = 5` derives `a = 5`),
wildcard-free and prefix `LIKE` lowering to equality/range bounds, redundant
`DISTINCT` removal on `MIN`/`MAX`, and boolean `XOR` identities. Rewriting is
bottom-up and continues to a fixed point.

## Selecting rules per optimization

Rules are values held by `OptimizerOptions`; no global registry is mutated.

```cpp
tinylamb::OptimizerOptions options = tinylamb::OptimizerOptions::Default();
options.algebra_rules.Remove("filter_pushdown_join");
options.relational_rules.Remove("join_commutativity");
options.expression_rules.Remove("de_morgan");
options.disabled_implementation_rules.insert("index_join");

auto plan = tinylamb::Optimizer::Optimize(query, context, options);
```

`Add` replaces a rule with the same name, which makes a local override
explicit and avoids ordering two implementations with the same identity.

## Adding an algebra rule

Algebra rules receive one normalized subtree and return the replacement, or
`nullptr` when they do not apply. Rules must be convergent: each application
has to reduce a monotone measure (nesting depth, conjunct count, or predicate
height), otherwise the rewriter throws after exhausting its pass budget.

```cpp
using namespace tinylamb;

options.algebra_rules.Add(AlgebraRule(
    "my_filter_merge", [](const AlgebraTree& node) -> AlgebraTree {
      if (node->kind != AlgebraNode::Kind::kFilter ||
          node->children.front()->kind != AlgebraNode::Kind::kFilter) {
        return nullptr;
      }
      return AlgebraNode::Filter(node->children.front()->children.front(),
                                 node->children.front()->predicate);
    }));
```

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
unary, aggregate, `CASE`, `IN`, function-call, and subquery expressions.

## Adding a logical transformation

```cpp
using namespace tinylamb::cascades;
using namespace tinylamb::cascades::dsl;

options.relational_rules.Add(Rule(
    "my_join_swap", Join(Any("left"), Any("right")),
    [](const Bindings&, Memo& memo, GroupId group,
       const LogicalExpression& expression) {
      memo.AddExpression(
          group, LogicalExpression{LogicalOperator::kJoin,
                                   {expression.children[1],
                                    expression.children[0]}, {}});
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
`expression/rewrite_test.cpp`, `plan/algebra_test.cpp`,
`plan/cascades_test.cpp`, and `plan/optimizer_test.cpp`.
