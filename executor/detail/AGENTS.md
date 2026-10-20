# `executor/detail/` — relational pseudo-layer (Layer 8)

Not a storage layer. Shared relational-IR execution helpers used by both
`plan/` and `executor/` — hence its own rank in `check_layering.py`
(longest-prefix match puts `executor/detail/` before `executor/`).
Namespace `relational_detail`. Promotion to a real layer is an open A5 item.

## Key files

- `relation.{hpp,cpp}` — `Relation{schema, rows, spill, using_columns,
  charged_bytes, join stats}` + `MaterializeRelation/CopyExecutionStats/
  NoteRelationSpill`: materialization, spilling, memory accounting.
- `expression_eval.{hpp,cpp}` — row-at-a-time fallback evaluation
  (`Evaluate/Lookup/Binary/Like/Truthy`) + aggregation accumulators
  (SUM/MIN/MAX, stats, HLL/KLL, APPROX_TOP…). Former three-valued-logic and
  numeric-edge divergences are fixed by delegating to the canonical evaluator.
  It also owns the *grouped* key contract: `CanonicalDistinctValue` plus
  `GroupKeyEqual` / `GroupKeyTransparentHash` / `GroupKeyMap` (NULL is one
  group, NaN one peer, -0 folds, a case-insensitive key folds), shared by the
  relational grouping loop and the physical grouped aggregation shapes
  (`executor/grouped_aggregate.hpp`) so both agree on "the same group".
- `scan_filter.{hpp,cpp}` — `CompiledScanFilter` (+ residual bytecode):
  `Match/LoadSource/FilterRelation`. `LoadSource` takes an optional
  `ScanRuntimeFilter` (group + projected column + comparison domain), the
  relational-path form of the dynamic join filter published in
  `common/runtime_filter.hpp`; it is applied as a pre-decode peek when the
  guarded column maps into the scan, and post-decode otherwise.
- `planning_heuristics.{hpp,cpp}` — predicate analysis, join order/shape
  choices (`AnalyzePredicates/Join/InnerJoin/HybridHashJoin/BuildInput`),
  outer→inner reduction.
- `subquery_runtime.{hpp,cpp}` — `Scope/CteMap/ExecutionRuntime`,
  `TableKeyFilter` (the owner-scoped stash of a published dynamic filter; the
  filter is a `RuntimeFilterGroup`, not a bare key set),
  `ExecuteQuery/FinishQuery/ExecuteRecursiveCte`, correlated-index and
  uncorrelated-fingerprint caches, `OptimizeDerivedBoundaries`.
  It also owns the WITH scope contract both this layer and the SQL frontend
  call: `CollectStatementTableRefs` (every table a query tree reads, deep
  through derived tables / scalar subqueries / set-operation branches / nested
  WITH bodies, with inner WITH names shadowing outer ones),
  `CollectCteBodyOuterRefs` (the entries one WITH body depends on; the
  entry's own name is erased because a non-recursive entry is invisible
  inside its own definition and resolves outward, while a WITH RECURSIVE one
  reads the work table), `CteCycleStatus` and `ValidateCteDependencies`
  (dependency-order resolvability of one WITH list; a mutual reference is
  "cyclic CTE reference among a, b"). `ExecuteQuery` resolves the list in
  dependency order and reports the same cycle status as the backstop for
  hand-built statements — `SqlEngine::PrepareStatement` already validated
  SQL input.
- `window_eval.*`, `explain_format.*` — window evaluation, EXPLAIN rendering.
- `work_table_index.{hpp,cpp}` — `WorkTableIndex`, the hash index over a
  recursive CTE's working table. `ExecuteRecursiveCte` used to answer "did an
  earlier round already produce this row?" (UNION DISTINCT recursion) with a
  linear scan over every seen row, i.e. quadratic in the result (a 320k-row
  recursion spent minutes there; 0.27s once indexed). The peer rule is the
  DISTINCT rule (`EqualUnderCollation` + `CanonicalDistinctValue`, so NaN is
  one peer, -0.0 == +0.0, a case-insensitive pair folds, NULL == NULL) — the
  same pair `DistinctOf` uses. It is *not* a join access path: the CTE name is
  bound to the previous round's delta (BigQuery working-table contract), so
  each round's join hashes that delta itself and the term must never read the
  accumulated table. `WorkTableIndexStats()` counts `inserts`/`duplicates`
  so tests can prove the loop consults the index.

## Layering notes

- `executor/detail/* -> executor/*` (detail → executor body) is allowlisted
  (A5) — do not add new edges; put shared code here, not in `executor/`
  headers.
- `executor/detail/* -> query/statement.hpp` is V3' debt: the statement IR
  lives above its readers. Read-only use only.

Test via: `./build/relational_regression_test`, `relational_extra_test`.
