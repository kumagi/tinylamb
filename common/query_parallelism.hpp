/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_COMMON_QUERY_PARALLELISM_HPP
#define TINYLAMB_COMMON_QUERY_PARALLELISM_HPP

#include <algorithm>
#include <atomic>
#include <cstddef>

namespace tinylamb {
// A query's coordinator supplies the shared admission count. Operators read
// it only when choosing their team size; already-running teams stay stable.
struct QueryParallelismBudget {
  const std::atomic<size_t>* outstanding{nullptr};
  size_t cpu_slots{1};
  const std::atomic<size_t>* snapshot_reads{nullptr};
};
inline thread_local QueryParallelismBudget query_parallelism_budget;

class ScopedQueryParallelismBudget {
 public:
  ScopedQueryParallelismBudget(
      const std::atomic<size_t>* outstanding, size_t cpu_slots,
      const std::atomic<size_t>* snapshot_reads = nullptr)
      : previous_(query_parallelism_budget) {
    query_parallelism_budget = {outstanding, std::max<size_t>(1, cpu_slots),
                                snapshot_reads};
  }
  ScopedQueryParallelismBudget(const ScopedQueryParallelismBudget&) = delete;
  ScopedQueryParallelismBudget& operator=(const ScopedQueryParallelismBudget&) =
      delete;
  ~ScopedQueryParallelismBudget() { query_parallelism_budget = previous_; }

 private:
  QueryParallelismBudget previous_;
};

inline size_t QueryParallelWorkerLimit(size_t requested) {
  requested = std::max<size_t>(1, requested);
  const auto budget = query_parallelism_budget;
  if (!budget.outstanding) return requested;
  const size_t active =
      std::max<size_t>(1, budget.outstanding->load(std::memory_order_relaxed));
  if (budget.snapshot_reads != nullptr) {
    const size_t reads = std::max<size_t>(
        1, budget.snapshot_reads->load(std::memory_order_relaxed));
    // Prefer parallelism between requests while interactive work is in
    // flight. Snapshot scans still run on their caller, leaving the other
    // SQL workers and the I/O coordinator available for short requests.
    if (active > reads) return 1;
    return std::min(requested, std::max<size_t>(1, budget.cpu_slots / reads));
  }
  return std::min(requested, std::max<size_t>(1, budget.cpu_slots / active));
}
}  // namespace tinylamb
#endif
