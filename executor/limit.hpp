/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_LIMIT_EXECUTOR_HPP
#define TINYLAMB_LIMIT_EXECUTOR_HPP
#include "executor/executor_base.hpp"
namespace tinylamb {
class LimitExecutor : public ExecutorBase {
 public:
  LimitExecutor(Executor source, size_t limit, size_t offset)
      : source_(std::move(source)), limit_(limit), offset_(offset) {}
  bool Next(Row* dst, RowPosition* rp) override;
  void Dump(std::ostream& output, int indent) const override;

 private:
  Executor source_;
  size_t limit_;
  size_t offset_;
  size_t skipped_{0};
  size_t emitted_{0};
  size_t consumed_rows_{0};
  bool early_stop_{false};
};
}  // namespace tinylamb
#endif
