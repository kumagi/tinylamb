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

}  // namespace tinylamb

#endif  // TINYLAMB_PLAN_PARALLEL_THRESHOLDS_HPP
