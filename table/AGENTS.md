# `table/` — Layer 5 heap + secondary-index sync + statistics

`Table{Schema, first/last pid, vector<Index>}` over `RowPage` chains. Every
DML keeps all secondary indexes in sync; `hint_leaf` threads through index
ops to accelerate consecutive ops inside one UPDATE.

## Key files

- `table.{hpp,cpp}` — `Insert/Update/Delete/Read`, `BeginFullScan`,
  `BeginMorselScan` (`BuildScanMorsels`, 8 pages/morsel for parallel scans),
  `BeginIndexScan`, `CreateIndex`. New tail links are owned by the creating
  transaction through `PageManager` until commit/abort; other transactions
  cannot traverse an unpublished tail page.
- `iterator_base.hpp`/`iterator.hpp`/`full_scan_iterator.{hpp,cpp}` +
  `scan_options.hpp` (`TableScanOptions`: projection, key filter,
  peek compares) — the narrow cross-layer scan interface
  (`IndexScanIterator : IteratorBase`, see `index/AGENTS.md`).
- `full_scan_iterator` visibility goes through `Transaction::ReadVersion`,
  with a fast path: pages with `PageLSN <= snapshot` are read physically
  without version resolution (boundary caveat: mutations stamped before the
  snapshot began but still uncommitted can be served physically — see
  `PhysicalReadEligible`). Returned rows are owned copies — valid after
  the latch is released.
- `full_scan_iterator` also applies a **dynamic join filter**
  (`common/runtime_filter.hpp`): `TableScanOptions::runtime_filter` +
  `runtime_filter_type` are checked against the scanned column's declared type
  once, and the filter then answers a single `Reject(int64)` per row through
  the pre-decode `Row::TryPeekInteger` peek — a probe row with no counterpart on
  a join's build side never materializes a `Row`. `IteratorBase::SetRuntimeFilter`
  installs one after construction (a full scan has already seeked, so the
  iterator re-seeks) and is a no-op for iterators that cannot prune.
- `table_statistics.{hpp,cpp}`, `hyper_log_log.{hpp,cpp}` — persistent stats:
  row count + per-column NULL/non-NULL/distinct counts, 16 equi-depth
  histogram, top/bottom values, 5 MCVs. `EstimateEqual/Range/LessThan`,
  `ReductionFactor` with explicit AND/OR/XOR/IN/IS-NULL probability rules.
  `Update()` also stamps `LayoutGeneration()` (the table's row-layout
  generation) and drops that stamp when the chain moved while it scanned; the
  stamp is the only freshness handle a *physical* order claim gets
  (`plan/full_scan_plan.hpp::ClusteredOrderColumn`), it persists in the
  per-table statistics meta record, and every derived estimate
  (Filter/TransformBy/ScaleToRows/ScaleCardinality/Concat/`operator*`) clears
  it. Payload format is TLSTATS v3; v2 still decodes as "no proof".
- **row-layout generation** — `TableIncarnation` (`common/incarnation.hpp`,
  the per-catalog-entry state every `Table` copy shares) carries
  `LayoutGeneration()` (bumped on entry to *and* exit from every row mutation,
  so a writer in flight while ANALYZE scans cannot leave a usable proof) and
  the sticky `RowsRewrittenInPlace()` latch set by UPDATE/DELETE (an in-place
  rewrite leaves a pre-image an older snapshot can resolve, and MVCC versions
  are in-memory only, so no measurement of current images speaks for it).
  Generations are seeded from a process-global counter with a $2^{32}$ stride:
  a reopened database never reproduces a stamp a persisted record was written
  with.

## Layering notes (allowlisted edges)

- `index/index_scan_iterator.* -> table/*`: the V2 cycle-avoidance seam —
  do not widen it.
- `table/* -> expression/*`: zone-map/statistics predicate evaluation. New
  predicate logic belongs in `expression/`, not here.
- Batch-first: prefer `NextBatch(DataChunk)` paths for new scan work.

Test: `./build/table_test`, `index_test`, `full_scan_iterator_test`,
`full_scan_mvcc_fastpath_test`, `table_statistics_test`,
`hyper_log_log_test` (the layout-generation cases are the
`*LayoutGeneration*` / `DerivedStatisticsDropTheLayoutGeneration` ones).

Optimizer statistics: `ColumnStats::EstimateJoinMatches` consumes MCV, NULL mass,
NDV and histogram domains; `TableStatistics::EstimateDistinctRows` is the shared
GROUP BY / DISTINCT / incremental-sort NDV backoff. Neither is a semantic proof
of uniqueness or emptiness. Multicolumn correlation is still a heuristic, not a
collected statistic. These estimators do not change the serialized format.

`ClusteringStrength()` derives direction-independent range-I/O locality from the
stored adjacency fraction. Keep it separate from `Correlation() == 1`, the
ascending-order proof used by FullScanPlan with the layout-generation gate.
