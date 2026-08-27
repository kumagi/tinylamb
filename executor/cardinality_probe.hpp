/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_CARDINALITY_PROBE_HPP
#define TINYLAMB_CARDINALITY_PROBE_HPP

#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <ostream>
#include <string>
#include <utility>

#include "executor/executor_base.hpp"
#include "page/row_position.hpp"
#include "type/row.hpp"

namespace tinylamb {

class CardinalityProbe : public ExecutorBase {
 public:
  CardinalityProbe(Executor child, std::string operator_name = {},
                   double estimated_cardinality = 0.0)
      : child_(std::move(child)),
        operator_name_(std::move(operator_name)),
        estimated_cardinality_(estimated_cardinality) {}
  ~CardinalityProbe() override = default;

  bool Next(Row* dst, RowPosition* rp) override;
  size_t NextBatch(DataChunk* destination,
                   size_t max_rows = kDefaultVectorSize) override;
  void Dump(std::ostream& o, int indent) const override;

  [[nodiscard]] size_t ActualRowCount() const { return actual_rows_; }
  [[nodiscard]] double EstimatedRowCount() const {
    return estimated_cardinality_;
  }
  [[nodiscard]] double CardinalityError() const {
    if (estimated_cardinality_ <= 0.0) {
      return static_cast<double>(actual_rows_);
    }
    const double act = static_cast<double>(actual_rows_);
    const double est = estimated_cardinality_;
    return std::max(act, est) / std::max(std::min(act, est), 1.0);
  }
  [[nodiscard]] const std::string& OperatorName() const {
    return operator_name_;
  }
  [[nodiscard]] uint64_t ExecutionTimeNanos() const { return elapsed_nanos_; }

 private:
  Executor child_;
  std::string operator_name_;
  double estimated_cardinality_{0.0};
  size_t actual_rows_{0};
  uint64_t elapsed_nanos_{0};
};

}  // namespace tinylamb

#endif  // TINYLAMB_CARDINALITY_PROBE_HPP
