/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_PLAN_PARALLEL_THRESHOLDS_HPP
#define TINYLAMB_PLAN_PARALLEL_THRESHOLDS_HPP

#include <cstddef>

namespace tinylamb {

// A full scan is emitted as a morsel-driven ParallelScan only when the
// optimizer believes the table holds at least this many rows.  Below the
// threshold thread startup and queue handoff cost more than the scan itself,
// and single-morsel tables would serialize on one worker anyway.
inline constexpr size_t kParallelScanMinRows = 8192;

// Aggregation runs as ParallelAggregationExecutor only when its child is
// estimated to emit at least this many rows; smaller inputs keep the
// sequential executor (which additionally has the JIT sum fast path).
inline constexpr size_t kParallelAggregationMinRows = 8192;

// An inner hash join switches to the shared-build parallel executor when the
// combined build+probe input reaches this size (TODO.md item 5). Semi/anti,
// outer, null-aware, residual-carrying, and index joins stay sequential:
// their executors have no parallel counterpart (or untested parallel paths).
inline constexpr size_t kParallelHashJoinMinRows = 8192;

// An inner equi merge join without residuals switches to ParallelMergeJoin
// at the same combined-input scale. Sorted-input steering pays off only past
// thread-startup and partition-computation overhead, hence the shared
// threshold with scans and aggregations.
inline constexpr size_t kParallelMergeJoinMinRows = 8192;

}  // namespace tinylamb

#endif  // TINYLAMB_PLAN_PARALLEL_THRESHOLDS_HPP
