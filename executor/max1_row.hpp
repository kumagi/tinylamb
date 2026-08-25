/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_MAX1_ROW_EXECUTOR_HPP
#define TINYLAMB_MAX1_ROW_EXECUTOR_HPP

#include "executor/executor_base.hpp"

namespace tinylamb {

class Max1RowExecutor final : public ExecutorBase {
 public:
  explicit Max1RowExecutor(Executor source) : source_(std::move(source)) {}

  bool Next(Row* destination, RowPosition* position) override;
  void Dump(std::ostream& output, int indent) const override;

 private:
  Executor source_;
  bool emitted_{false};
};

}  // namespace tinylamb

#endif  // TINYLAMB_MAX1_ROW_EXECUTOR_HPP
