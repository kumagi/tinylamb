# `plan/` — Layer 9 Cascades optimizer (logical info only)

Memo + transformation/implementation rules. `plan/` holds logical plan info;
concrete `EmitExecutor` bodies live in `executor/relational_factory.cpp`
(V4 split — see `executor/AGENTS.md`).

## Key files

- `plan.hpp` (`PlanBase`: `EmitExecutor/ScanSource/GetStats/GetSchema/
  AccessRowCount/EmitRowCount/IsOrderedBy/OrderIsMeasured/EnforcesLimit/Dump`
  only) + one
  `*_plan.{hpp,cpp}` per operator (full/index/index-only/bitmap/minmax scan,
  projection, selection, product, merge-join, sort, topn, limit, aggregation,
  distinct, set-op, values, unnest, apply, recursive-cte, …) +
  `relational_plan.hpp` (`RelationalPlan{SelectStatement + output_schema}` —
  the un-lowered physical shape chosen through the memo).
- `full_scan_plan.{hpp,cpp}` — `FullScanPlan` plus the **clustered index
  order** claim: `ClusteredOrderColumn(table, stats)` returns the single column
  whose stored ascending order a plain scan can *prove* (unique single-column
  key + NOT NULL + ANALYZE correlation 1.0 + an unchanged
  `Table::LayoutGeneration()` + no in-place rewrite in this process), and
  `AnyResolvedTableHasClusteredOrder(ctx)` lets the query engine keep such a
  statement out of the compiled-plan cache (the claim is revocable, so a plan
  that folded a LIMIT or an incremental sort on it must not outlive the
  statement). `IsOrderedBy` answers ascending/first key/bare column only.
  `PlanBase::OrderIsMeasured()` keeps that claim out of costing: an order
  measured from the layout still paid for a whole chain, so it neither wins
  the ordered-candidate preference nor the `ApplyLimitHint` early-stop
  discount (projection/selection/limit forward it).
- `collation_property` contract: `SortKey`/`TopNKey` carry the collation
  their expression resolves to (`expression/collation.hpp`) and override
  `PlanBase::OrderCollation()`; `PhysicalProperties::collation` (empty = the
  engine default, i.e. binary) travels in `Key()` and EXPLAIN, and
  `SatisfiesOrderingCollation(plan, required)` decides whether a delivered
  order may serve a requirement — a case-insensitive order is *not* a binary
  order, so `ORDER BY x COLLATE 'und:ci'` never folds into a scan/index order.
  The row-shaping wrappers (projection/selection/limit) forward the claim.
  Test: `./build/collation_property_test`.
- `sort_plan.{hpp,cpp}` — one logical ORDER BY, two memory shapes:
  `BufferedSortPlan` (in memory) and `ExternalMergeSortPlan` (sorted runs +
  merge), both deriving from `SortPlan`, which keeps the whole ordering
  contract (`IsOrderedBy` / `OrderCollation` / `EnforcesLimit` / row counts /
  parameter slots) and its historical "Sort" rendering, so existing
  `dynamic_pointer_cast<SortPlan>` callers keep working. The only per-shape
  logical information is `SortMemoryPolicy{memory_budget_bytes,
  merge_window_rows}`; `SortRowCapacity(policy)` turns the budget into a row
  count and `MakeSortPlan(child, keys, estimated_rows, policy)` is the single
  place the choice is encoded. The two implementation rules are exclusive
  halves of it — `sort` fires at or below the capacity, `sort_external_merge`
  above it — sharing key building and the collation-aware elision check
  (`PrepareSortShape`); an already-ordered child is elided by the buffered rule
  only, so the memo never holds one plan under two rule names. Each shape has
  its own `EmitExecutor` in `executor/relational_factory.cpp`.
  Test: `./build/sort_shape_test`.
- `aggregation_plan.{hpp,cpp}` — one GROUP BY payload, three physical
  algorithms. `AggregatePayload{group_keys, aggregates, having,
  estimated_groups, memory}` is what every shape receives, and the base
  `AggregationPlan` keeps the whole contract (payload accessors, output schema
  = keys then aggregates, row counts, parameter slots, `EnforcesDistinct` —
  one row per group, so the keys are unique by construction) plus its
  historical per-strategy rendering, so `dynamic_pointer_cast<AggregationPlan>`
  and the keyless scalar callers keep working. `AggregateMemoryPolicy` is the
  only per-shape logical information: `AggregateGroupCapacity` turns the budget
  into groups and `AggregateSpillPartitionCount` into partitions, which is what
  makes the memory decision decidable from an estimate. The three shapes differ
  in the *order they promise*: `HashAggregatePlan` promises none,
  `SortAggregatePlan` promises the group-key order (ascending, NULLS first) as
  a side effect of its sort, and `StreamAggregatePlan` promises it under its
  input-order contract (`RequiresOrderedInput()`, false for a keyless payload —
  one group is order independent). `OrderCollation` resolves over the group
  keys, so a collated key folds the delivered order like a `SortPlan` does.
  The `aggregation` implementation rule builds the payload from the node's
  `grouping_sets`/`partition_by` and offers hash + sort always, and the stream
  shape only when the child already delivers the group-key order
  (`ChildDeliversGroupKeyOrder`). The choice between the two grouped shapes is
  that memory contract priced as a cost: the hash alternative is one pass plus
  the spill its own group table would need (`EstimateMemorySpillCost` over
  `AggregateMemoryBudget()`), so it wins while the estimated groups fit
  `AggregateGroupCapacity()` and the sort runs win once they do not. Each plan
  type owns one half of the policy (`HashAggregatePlan::SpillPartitionCount`,
  `SortAggregatePlan::SpillThresholdRows` / `MergeWindowRows`) and the factory
  hands each half to its own operator.
  Test: `./build/aggregate_shape_test`.
- `cascades.{hpp,cpp}` — `Memo/Group/LogicalExpression/Pattern/Rule/RuleSet/
  SearchEngine/PhysicalProperties/ImplementationRule/BestPlan`. Groups are
  keyed by relation-set + tag; each `Group` owns its scan filter so
  alternatives stay semantically identical; expression cap 4096 degrades
  gracefully (`Degraded`).
- `window_plan.{hpp,cpp}` — one kWindow node, two memory shapes.
  `WindowMemoryPolicy{memory_budget_bytes, spill_partition_count}` is the only
  per-shape logical information: `WindowRowCapacity` turns the budget into a row
  count and `WindowPolicyForRows(rows)` picks buffered-only (budget 0) while the
  child estimate fits the default 64MB, the spill-capable shape above it (a NaN
  estimate stays buffered; infinity spills). The buffered shape appends a column
  per call to the rows the child produced, so it keeps the child's ordering
  claim; the spill-capable shape drains the input into PARTITION BY hash spill
  files and emits one file at a time, so `IsOrderedBy` returns false whenever
  `SpillCapable()` — decided at plan time, before the runtime row count is
  known, so a mis-estimate can only be conservative. `CallsShareOnePartitionKey`
  is the safe-split condition: one row's file is chosen from that key, so calls
  with different PARTITION BY lists would cut each other's partitions and stay
  buffered. `Dump` names the shape only for the spill form. Test:
  `./build/window_shape_test`; the operator half is
  `./build/window_spill_test` (in `executor/`).
- `multi_index_covering_scan_plan.{hpp,cpp}` — the covering case that no
  SINGLE index can serve: two or more indexes whose stored columns (key +
  INCLUDE, `Index::CoveredColumns()`) together cover every column the
  predicate and the projection touch. `ScanAlternatives` offers it only when
  no single index covers the set alone (that case stays `IndexOnlyScanPlan`),
  every participant carries a leading-key range (an index without one would
  force a full walk), a greedy minimal cover picks the set, an entry-retaining
  (`kVersionedUnique`) index is never a participant, and redundancy pruning
  leaves at least two. The node's schema is the union of the participants'
  stored columns in table slot order, so the executor can assemble rows
  positionally. Costed like a covering read (`kCoveringIndexCostFactor`).
  Its `EmitExecutor` takes `IndexOnlyScanPlan`'s stale-key gate around a read
  guard for EVERY participant, acquired in one index-root order.
- Rule families: `ExpressionRuleSet::Default` (folds, De Morgan, comparison
  canonicalization…) → `cascades::RuleSet::Default` (83 logical equivalences:
  join commutativity/associativity/enumeration, selection merge/pushdown,
  set-op transparency…) → `implementation_rules.{hpp,cpp}`
  (`DefaultImplementationRules`: range-sliced Index/Bitmap/MinMax, Hash/Merge/
  NL/LookupJoin/LateMaterializationJoin, outer NL, StreamAgg,
  `sort` / `sort_external_merge` (the buffered vs external-merge halves of one
  ORDER BY, see `sort_plan.hpp`),
  `sort_incremental`,
  multi-index covering scan candidates (see
  `multi_index_covering_scan_plan.hpp`); normal single-table queries use
  Memo, with only `OptimizeMinMaxIndex` retaining exact endpoint lookup).
  Index nested-loop costing discounts the **probe half** of its per-left-row
  cost (`kOrderedLookupProbeFactor`) when the outer plan already delivers the
  probe key order (`DeliversProbeOrder` over `probe->left_columns`): sorted
  probes stay on the leaf the previous one used (or its neighbour), so the
  descent path and the leaf page stay resident. Measured (clustered) orders
  count — the order really is delivered; `OrderIsMeasured` only keeps them out
  of the LIMIT early-stop discount, and such statements are already excluded
  from the compiled-plan cache. It is a tie-breaker, not a requirement: the
  join does not yet ask its child for the probe order.
  Dynamic join filters: `dynamic_filter_pushdown_join` records the probe-side
  join keys on a derived probe group (`Group::dynamic_filter_keys`), and the
  hash-join rule turns that annotation into `ProductPlan::PreferDynamicFilter`
  for a profitable shape. Hash semi reduction is the same two-step shape for an
  existence-only build: `semi_reduction_pushdown_semi_join` /
  `..._anti_join` record `Group::semi_reduction_probe_keys`, and the rule turns
  them into `ProductPlan::PreferSemiReduction` when
  `SemiReductionIsProfitable` (shared with the decorrelation path in
  `product_plan.cpp`, so both never disagree). Neither layer names a filter or
  reduction shape — that is an executor decision.
- `optimizer.{hpp,cpp}` (`Optimizer/OptimizerOptions`) — entry point; reads
  `query/query_data` and (read-only, allowlisted V3') `query/statement.hpp`
  for decorrelation analysis. Execution stays in `subquery_runtime`.

## Normative doc

`docs/cascades_optimizer.md` (+ `docs/optimizer_todo.md`,
`docs/optimizer_improvements.md`).

Remaining allowlisted edges: `plan/* -> query/query_data.hpp` (same CMake
target), `plan/{product_plan,implementation_rules}.cpp ->
executor/hash_join_mode.hpp` (cost-estimation helper location debt, V4),
`plan/implementation_rules.cpp -> executor/detail/scan_filter.hpp`
(CompiledScanFilter helpers used by implementation rules; same V4 debt).
`JoinKind` itself lives in `common/join_kind.hpp`; `executor/join_kind.hpp`
is a compat include.

Test: `./build/plan_test`, `optimizer_test`, `cascades_test`,
`late_materialization_join_plan_test`, `plan_extra_test`,
`full_scan_plan_test`, `collation_property_test`, `sort_shape_test`,
`aggregate_shape_test`, `window_shape_test`.

Calc policy: there is deliberately no `kCalc`/`CalcPlan`.  Filters merge
scan-ward (`push_selection_into_scan` leaves a `SelectionPlan` directly above
the scan, where the zone-map/SIMD/JIT batch filter lives) and projections merge
with each other (`merge_projections`), while identity projections disappear in
the scan's `fallback_select`.  Splitting is the transpose
`push_selection_through_projection`.  The "selection" implementation rule also
collapses a selection whose child plan (through `RelationRenamePlan`) already
applies the same predicate, comparing with `DropQualifiers` because the pushed
group filter speaks physical table names and the logical node speaks the query
alias; without that merge the filter runs twice per row.  Revisit (a fused
`CalcPlan`) only if profiling shows the Selection→Projection batch copy
dominating.

Rule reachability audit: `docs/optimizer_rule_audit.md`. There are 45 physical
registrations. Exploration retries dependent parents after a child changes.
`OptimizeGrouping` lowers the supported single-table grouped SQL slice into the
same scan/aggregate memo; `QueryData::grouping_keys_` carries resolved keys.
Do not treat a rule tested only with an invented payload as SQL integration.

`IndexScanPlan::LimitHint()` carries downstream prefix demand as scheduling
metadata. It never limits visited rows (residuals may reject them). The factory
keeps such scans serial and omits the synchronous look-ahead cursor; the
`LimitExecutor` passes a bounded NextBatch request through projections/filters.
