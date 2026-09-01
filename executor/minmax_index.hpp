/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_MINMAX_INDEX_EXECUTOR_HPP
#define TINYLAMB_MINMAX_INDEX_EXECUTOR_HPP

#include <cstddef>

#include "executor/executor_base.hpp"

namespace tinylamb {

class MinMaxIndexExecutor final : public ExecutorBase {
 public:
  MinMaxIndexExecutor(Executor source, size_t value_slot)
      : source_(std::move(source)), value_slot_(value_slot) {}

  bool Next(Row* destination, RowPosition* position) override;
  void Dump(std::ostream& output, int indent) const override;

 private:
  Executor source_;
  size_t value_slot_;
  bool emitted_{false};
};

}  // namespace tinylamb

#endif  // TINYLAMB_MINMAX_INDEX_EXECUTOR_HPP
