# `plan/` — Layer 9 Cascades optimizer (logical info only)

Memo + transformation/implementation rules. `plan/` holds logical plan info;
concrete `EmitExecutor` bodies live in `executor/relational_factory.cpp`
(V4 split — see `executor/AGENTS.md`).

## Key files

- `plan.hpp` (`PlanBase`: `EmitExecutor/ScanSource/GetStats/GetSchema/
  AccessRowCount/EmitRowCount/IsOrderedBy/EnforcesLimit/Dump` only) + one
  `*_plan.{hpp,cpp}` per operator (full/index/index-only/bitmap/minmax scan,
  projection, selection, product, merge-join, sort, topn, limit, aggregation,
  distinct, set-op, values, unnest, apply, recursive-cte, …) +
  `relational_plan.hpp` (`RelationalPlan{SelectStatement + output_schema}` —
  the un-lowered physical shape chosen through the memo).
- `cascades.{hpp,cpp}` — `Memo/Group/LogicalExpression/Pattern/Rule/RuleSet/
  SearchEngine/PhysicalProperties/ImplementationRule/BestPlan`. Groups are
  keyed by relation-set + tag; each `Group` owns its scan filter so
  alternatives stay semantically identical; expression cap 4096 degrades
  gracefully (`Degraded`).
- Rule families: `ExpressionRuleSet::Default` (folds, De Morgan, comparison
  canonicalization…) → `cascades::RuleSet::Default` (80+ logical equivalences:
  join commutativity/associativity/enumeration, selection merge/pushdown,
  set-op transparency…) → `implementation_rules.{hpp,cpp}`
  (`DefaultImplementationRules`: range-sliced Index/Bitmap/MinMax, Hash/Merge/
  NL/IndexJoin; `OptimizeSingleRelation` fast path).
- `optimizer.{hpp,cpp}` (`Optimizer/OptimizerOptions`) — entry point; reads
  `query/query_data` and (read-only, allowlisted V3') `query/statement.hpp`
  for decorrelation analysis. Execution stays in `subquery_runtime`.

## Normative doc

`docs/cascades_optimizer.md` (+ `docs/optimizer_todo.md`,
`docs/optimizer_improvements.md`).

Remaining allowlisted edges: `plan/* -> query/query_data.hpp` (same CMake
target), `plan/{product_plan,implementation_rules}.cpp ->
executor/hash_join_mode.hpp` (cost-estimation helper location debt, V4).
`JoinKind` itself lives in `common/join_kind.hpp`; `executor/join_kind.hpp`
is a compat include.

Test: `./build/plan_test`, `optimizer_test`, `cascades_test`, `plan_extra_test`.
