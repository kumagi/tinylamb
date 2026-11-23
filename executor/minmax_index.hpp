/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_MINMAX_INDEX_EXECUTOR_HPP
#define TINYLAMB_MINMAX_INDEX_EXECUTOR_HPP

#include <cstddef>

#include "executor/executor_base.hpp"

namespace tinylamb {

class MinMaxIndexExecutor final : public ExecutorBase {
 public:
  // `source` must deliver rows already ordered on `value_slot`: ascending for
  // MIN, descending for MAX. OptimizeMinMaxIndex supplies an IndexOnlyScanPlan
  // with exactly that direction (and its leading key on the aggregate
  // column), so the first non-NULL value pulled by NextBatch is the extremum.
  // A generic/unordered child would only yield an arbitrary non-NULL row, so
  // do not wire this executor to one. `descending` is the child's scan
  // direction (true for MAX): NextBatch verifies ordering only for the
  // scalar types whose Value order agrees with the index key order
  // (signed INT64/DATE); VARCHAR/FLOAT64/unsigned children are not checked
  // because their canonical orders can legitimately differ.
  MinMaxIndexExecutor(Executor source, size_t value_slot,
                      bool descending = false)
      : source_(std::move(source)),
        value_slot_(value_slot),
        descending_(descending) {}

  size_t NextBatch(DataChunk* destination,
                   size_t max_rows = kDefaultVectorSize) override;
  void Dump(std::ostream& output, int indent) const override;

 private:
  Executor source_;
  size_t value_slot_;
  bool descending_{false};
  bool emitted_{false};
};

}  // namespace tinylamb

#endif  // TINYLAMB_MINMAX_INDEX_EXECUTOR_HPP
