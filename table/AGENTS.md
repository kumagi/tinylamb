# `table/` — Layer 5 heap + secondary-index sync + statistics

`Table{Schema, first/last pid, vector<Index>}` over `RowPage` chains. Every
DML keeps all secondary indexes in sync; `hint_leaf` threads through index
ops to accelerate consecutive ops inside one UPDATE.

## Key files

- `table.{hpp,cpp}` — `Insert/Update/Delete/Read`, `BeginFullScan`,
  `BeginMorselScan` (`BuildScanMorsels`, 8 pages/morsel for parallel scans),
  `BeginIndexScan`, `CreateIndex`.
- `iterator_base.hpp`/`iterator.hpp`/`full_scan_iterator.{hpp,cpp}` +
  `scan_options.hpp` (`TableScanOptions`: projection, key filter,
  peek compares) — the narrow cross-layer scan interface
  (`IndexScanIterator : IteratorBase`, see `index/AGENTS.md`).
- `full_scan_iterator` visibility goes through `Transaction::ReadVersion`,
  with a fast path: pages with `PageLSN <= snapshot` are read physically
  without version resolution. Returned rows are owned copies — valid after
  the latch is released.
- `table_statistics.{hpp,cpp}`, `hyper_log_log.{hpp,cpp}` — persistent stats:
  row count + per-column NULL/non-NULL/distinct counts, 16 equi-depth
  histogram, top/bottom values, 5 MCVs. `EstimateEqual/Range/LessThan`,
  `ReductionFactor` with explicit AND/OR/XOR/IN/IS-NULL probability rules.

## Layering notes (allowlisted edges)

- `index/index_scan_iterator.* -> table/*`: the V2 cycle-avoidance seam —
  do not widen it.
- `table/* -> expression/*`: zone-map/statistics predicate evaluation. New
  predicate logic belongs in `expression/`, not here.
- Batch-first: prefer `NextBatch(DataChunk)` paths for new scan work.

Test: `./build/table_test`, `index_test`, `full_scan_iterator_test`,
`full_scan_mvcc_fastpath_test`, `table_statistics_test`,
`hyper_log_log_test`.
