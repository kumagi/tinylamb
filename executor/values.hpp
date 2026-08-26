/** Copyright 2026 KUMAZAKI Hiroki. Licensed under the Apache-2.0 license. */
#ifndef TINYLAMB_VALUES_EXECUTOR_HPP
#define TINYLAMB_VALUES_EXECUTOR_HPP

#include <utility>
#include <vector>

#include "executor/executor_base.hpp"
#include "type/row.hpp"

namespace tinylamb {

// A replayable constant row source used by ValuesPlan and DummyScanPlan.
// Unlike ConstantExecutor, the executor has an explicit name in EXPLAIN so
// VALUES is distinguishable from diagnostic/status result rows.
class ValuesExecutor final : public ExecutorBase {
 public:
  explicit ValuesExecutor(std::vector<Row> rows) : rows_(std::move(rows)) {}

  bool Next(Row* row, RowPosition* rp) override;
  size_t NextBatch(DataChunk* destination,
                   size_t max_rows = kDefaultVectorSize) override;
  void Dump(std::ostream& output, int indent) const override;

 private:
  std::vector<Row> rows_;
  size_t offset_{0};
};

}  // namespace tinylamb

#endif  // TINYLAMB_VALUES_EXECUTOR_HPP
