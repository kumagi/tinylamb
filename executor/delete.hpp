/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_DELETE_EXECUTOR_HPP
#define TINYLAMB_DELETE_EXECUTOR_HPP

#include "executor/executor_base.hpp"

namespace tinylamb {
class Table;
class Transaction;

class DeleteExecutor : public ExecutorBase {
 public:
  DeleteExecutor(Transaction& txn, Table& target, Executor source)
      : txn_(&txn), target_(&target), source_(std::move(source)) {}
  DeleteExecutor(Transaction& txn, Table& target, Executor source,
                 int64_t assert_rows_modified)
      : txn_(&txn),
        target_(&target),
        source_(std::move(source)),
        assert_rows_modified_(assert_rows_modified) {}
  bool Next(Row* dst, RowPosition* rp) override;
  void Dump(std::ostream& output, int indent) const override;

 private:
  Transaction* txn_;
  Table* target_;
  Executor source_;
  int64_t assert_rows_modified_{-1};
  bool finished_{false};
};
}  // namespace tinylamb
#endif
