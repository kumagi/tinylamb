# `executor/detail/` — relational pseudo-layer (Layer 8)

Not a storage layer. Shared relational-IR execution helpers used by both
`plan/` and `executor/` — hence its own rank in `check_layering.py`
(longest-prefix match puts `executor/detail/` before `executor/`).
Namespace `relational_detail`. Promotion to a real layer is an open item.

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
  "cyclic CTE reference among a, b"). `ValidateCteDependencies` checks
  dependency order and cycles during SQL preparation; `ExecuteQuery` resolves
  the list at runtime and repeats the cycle check as a backstop for hand-built
  statements.
- `window_eval.*`, `window_eval_columnar.cpp`, `window_eval_internal.hpp`,
  `explain_format.*` — window evaluation (including the columnar path),
  EXPLAIN rendering. Run `window_spill_test` for window spill changes.
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
- `ooc/` (`chunk_codec`, `spillable_store`, `external_sorter`,
  `recursive_partitioner`) — P2 OOC spillable building blocks built over
  `common/spill/spill_store.hpp` and `common/query_grant.hpp`.
- `pipeline/` (`pipeline_ir`, `pipeline_ops`, `ooc_sinks`, `ooc_*_sink.cpp`,
  `ooc_join_cte_sources.cpp`) — P3a/P3b push-breaker pipeline IR, scheduler,
  and OOC spillable sinks/sources (`OocSortSink`, `OocDistinctSink`,
  `OocHashBuildSink`, `OocGroupAggSink`, `OocWindowSink`, `OocSetOpInputSink`,
  `IncrementalSortSource`, `OocMergeJoinSource`, `OocBandJoinSource`,
  `OocRecursiveCteSource`, `OocApplySource`, etc.).
- `columnar_table.{hpp,cpp}`, `columnar_tlcs_fast_path.{hpp,cpp}`,
  `fused_grouped_join*`, `typed_stream_aggregate*` — columnar intermediate
  representation, TLCS fast path, and specialized grouped aggregation/join
  kernels.

## Layering notes

- `executor/detail/* -> executor/*` (detail → executor body) is allowlisted
  — do not add new edges; put shared code here, not in `executor/`
  headers.
- `executor/detail/* -> query/statement.hpp` is V3' debt: the statement IR
  lives above its readers. Read-only use only.

Test via: `./build/relational_regression_test`, `columnar_table_test`, `relational_extra_test`.
