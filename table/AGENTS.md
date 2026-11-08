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
  with a fast path (`PhysicalReadEligible`): read-only transactions scanning a
  page that currently has no MVCC version chains
  (`txn.IsReadOnly() && !txn.HasPageVersionChains(pid)`) read the physical row
  image directly without per-slot version resolution (every INSERT/UPDATE/DELETE
  installs a version chain entry, so any page with uncommitted or historical
  versions takes the MVCC path). Returned rows are owned copies — valid after
  the latch is released.
- `full_scan_iterator` also applies a **dynamic join filter**
  (`common/runtime_filter.hpp`): `TableScanOptions::runtime_filter` +
  `runtime_filter_type` are checked against the scanned column's declared type
  once, and the filter then answers a single `Reject(int64)` per row through
  the pre-decode `Row::TryPeekInteger` peek — a probe row with no counterpart on
  a join's build side never materializes a `Row`. `IteratorBase::SetRuntimeFilter`
  installs one after construction (a full scan has already seeked, so the
  iterator re-seeks) and is a no-op for iterators that cannot prune.
- `table_statistics.{hpp,cpp}` — persistent stats:
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
- `lstable_bridge.{hpp,cpp}`, `memtable/`, `sorted_run/`, `pax_chunk/`,
  `fragment/`, `workload/`, `fuzz/` — native `tinylamb` LSM/PAX columnar
  storage components (`LSTableStore`, `MemTable`, `SkipList`, `SortedRun`,
  `PaxChunk`, `Fragment`, `WorkloadTracker`) and their 11 libFuzzer / GTest
  storage harnesses (`row_codec_fuzzer`, `memcmp_fuzzer`, `pax_mutate_fuzzer`,
  `lsm_merge_fuzzer`, `memtable_model_fuzzer`, `schema_fuzzer`,
  `chunk_footer_fuzzer`, `region_scheme_fuzzer`, `nullmap_varchar_fuzzer`,
  `footer_sections_fuzzer`, `encode_agree_fuzzer`, replayed in
  `pax_storage_fuzzer_test`). All reside in `namespace tinylamb` and use
  `common/` and `type/` directly.
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
`full_scan_mvcc_fastpath_test`, `table_statistics_test` (the layout-generation
cases are the `*LayoutGeneration*` / `DerivedStatisticsDropTheLayoutGeneration`
ones).

Optimizer statistics: `ColumnStats::EstimateJoinMatches` consumes MCV, NULL mass,
NDV and histogram domains; `TableStatistics::EstimateDistinctRows` is the shared
GROUP BY / DISTINCT / incremental-sort NDV backoff. Neither is a semantic proof
of uniqueness or emptiness. Multicolumn correlation is still a heuristic, not a
collected statistic. These estimators do not change the serialized format.

`ClusteringStrength()` derives direction-independent range-I/O locality from the
stored adjacency fraction. Keep it separate from `Correlation() == 1`, the
ascending-order proof used by FullScanPlan with the layout-generation gate.

`TryReadIntegerKeyCounts` is a bounded exact physical projection for unfiltered
integer join keys, independent of SQL text and dimension values. One entry
(up to 1 MiB) per caller thread stores a dense-domain histogram and page LSNs.
Every call holds a table lease, checks the complete row-page chain, and under
each page latch requires no MVCC chains and unchanged LSNs; a stable layout
generation brackets the pass. Old snapshots, pending writes, runtime undo,
unpublished tails, different domains and incarnations must never reuse a
stale histogram. Failure to prove visibility/freshness returns null so the
caller uses the ordinary snapshot scan. This is not optimizer statistics.

`FindCachedIntegerKeyCounts` uses the saved page list for that same validation
instead of first building another morsel list. It follows/checks every current
next-page link, requires the chain to finish at zero, checks PageLSN and MVCC
under each latch, and checks layout generation before/after. It returns null
on a miss and cannot build a histogram. This avoids a duplicate page-pool walk
without treating the saved page list alone as a freshness proof.
