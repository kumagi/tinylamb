/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_GENERATE_SERIES_HPP
#define TINYLAMB_GENERATE_SERIES_HPP

#include <cstdint>
#include <ostream>

#include "executor/executor_base.hpp"
#include "page/row_position.hpp"
#include "type/row.hpp"

namespace tinylamb {

class GenerateSeriesExecutor : public ExecutorBase {
 public:
  GenerateSeriesExecutor(int64_t start, int64_t stop, int64_t step = 1);
  GenerateSeriesExecutor(const GenerateSeriesExecutor&) = delete;
  GenerateSeriesExecutor(GenerateSeriesExecutor&&) = delete;
  GenerateSeriesExecutor& operator=(const GenerateSeriesExecutor&) = delete;
  GenerateSeriesExecutor& operator=(GenerateSeriesExecutor&&) = delete;
  ~GenerateSeriesExecutor() override = default;

  bool Next(Row* dst, RowPosition* rp) override;
  void Dump(std::ostream& o, int indent) const override;

  [[nodiscard]] int64_t Start() const { return start_; }
  [[nodiscard]] int64_t Stop() const { return stop_; }
  [[nodiscard]] int64_t Step() const { return step_; }

 private:
  int64_t start_;
  int64_t stop_;
  int64_t step_;
  int64_t current_;
  bool started_{false};
  bool finished_{false};
};

}  // namespace tinylamb

#endif  // TINYLAMB_GENERATE_SERIES_HPP
