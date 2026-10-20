/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_EXECUTOR_DETAIL_WORK_TABLE_INDEX_HPP
#define TINYLAMB_EXECUTOR_DETAIL_WORK_TABLE_INDEX_HPP

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <unordered_set>

#include "type/row.hpp"

namespace tinylamb::relational_detail {

// Peer rule for a working-table row: the same DISTINCT/GROUP BY rule as
// `CanonicalDistinctValue` (every NaN is one peer, -0.0 equals +0.0, a
// case-insensitive VARCHAR pair matches once folded, NULL equals NULL), so a
// recursive UNION DISTINCT and a top-level DISTINCT agree on which rows are
// duplicates.
struct WorkTableRowEqual {
  bool operator()(const Row& left, const Row& right) const;
};

struct WorkTableRowHash {
  size_t operator()(const Row& row) const;
};

// Observability: tests read deltas to prove the recursive loop consults the
// index (a linear scan would leave both counters still).
struct WorkTableIndexCounters {
  std::atomic<uint64_t> inserts{0};
  std::atomic<uint64_t> duplicates{0};
};
WorkTableIndexCounters& WorkTableIndexStats();

// Hash index over a recursive CTE's working table.
//
// UNION (DISTINCT) recursion drops a produced row when an earlier round
// already produced it.  The loop used to answer that with a linear scan over
// every row seen so far, i.e. quadratic in the working table (a 320k-row
// recursion spent minutes there).  The index answers it with one lookup and
// keeps the same peer rule as DISTINCT.
//
// This is deliberately not a join access path.  The CTE name is bound to the
// previous round's *delta* (BigQuery's working-table contract), so each round
// hashes that delta inside the join operator anyway, and the recursive term
// must never read the accumulated table.  What the loop itself needs to index
// is "which rows are new", and that is this structure.
class WorkTableIndex {
 public:
  // `count_stats` publishes every Insert to `WorkTableIndexStats()`.  The
  // recursive loop turns it on so tests can prove the loop consults the index;
  // a top-level DISTINCT (same class, hot path) leaves it off.
  explicit WorkTableIndex(bool count_stats = false)
      : count_stats_(count_stats) {}

  // Records `row`; returns true when the index did not already hold it.
  bool Insert(const Row& row);
  [[nodiscard]] bool Contains(const Row& row) const;
  [[nodiscard]] size_t size() const { return rows_.size(); }

 private:
  bool count_stats_{false};
  std::unordered_set<Row, WorkTableRowHash, WorkTableRowEqual> rows_;
};

}  // namespace tinylamb::relational_detail

#endif  // TINYLAMB_EXECUTOR_DETAIL_WORK_TABLE_INDEX_HPP
