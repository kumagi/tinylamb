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
- `window_eval.*`, `explain_format.*` — window evaluation, EXPLAIN rendering.

## Layering notes

- `executor/detail/* -> executor/*` (detail → executor body) is allowlisted
  (A5) — do not add new edges; put shared code here, not in `executor/`
  headers.
- `executor/detail/* -> query/statement.hpp` is V3' debt: the statement IR
  lives above its readers. Read-only use only.

Test via: `./build/relational_regression_test`, `relational_extra_test`.
