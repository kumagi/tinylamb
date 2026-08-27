/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/generate_series.hpp"

#include <cstdint>
#include <ostream>
#include <stdexcept>
#include <string>

#include "common/constants.hpp"
#include "type/row.hpp"
#include "type/value.hpp"

namespace tinylamb {

GenerateSeriesExecutor::GenerateSeriesExecutor(int64_t start, int64_t stop,
                                               int64_t step)
    : start_(start), stop_(stop), step_(step), current_(start) {
  if (step_ == 0) {
    throw std::runtime_error("step size cannot be zero in GENERATE_SERIES");
  }
}

bool GenerateSeriesExecutor::Next(Row* dst, RowPosition* /*rp*/) {
  if (finished_) {
    return false;
  }
  if (!started_) {
    started_ = true;
    current_ = start_;
  } else {
    int64_t next_val = 0;
    if (__builtin_add_overflow(current_, step_, &next_val)) {
      finished_ = true;
      return false;
    }
    current_ = next_val;
  }

  if (step_ > 0) {
    if (current_ > stop_) {
      finished_ = true;
      return false;
    }
  } else {
    if (current_ < stop_) {
      finished_ = true;
      return false;
    }
  }

  *dst = Row({Value(current_)});
  return true;
}

void GenerateSeriesExecutor::Dump(std::ostream& o, int /*indent*/) const {
  o << "GenerateSeries (" << start_ << ", " << stop_ << ", " << step_ << ")\n";
}

}  // namespace tinylamb
