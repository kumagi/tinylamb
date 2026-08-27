/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_TRUNCATE_HPP
#define TINYLAMB_TRUNCATE_HPP

#include <cstddef>
#include <ostream>

#include "executor/executor_base.hpp"
#include "page/row_position.hpp"
#include "table/table.hpp"
#include "transaction/transaction.hpp"
#include "type/row.hpp"
#include "type/value.hpp"

namespace tinylamb {

class TruncateExecutor : public ExecutorBase {
 public:
  TruncateExecutor(Transaction& txn, Table& table);
  TruncateExecutor(const TruncateExecutor&) = delete;
  TruncateExecutor(TruncateExecutor&&) = delete;
  TruncateExecutor& operator=(const TruncateExecutor&) = delete;
  TruncateExecutor& operator=(TruncateExecutor&&) = delete;
  ~TruncateExecutor() override = default;

  bool Next(Row* dst, RowPosition* rp) override;
  void Dump(std::ostream& o, int indent) const override;

  [[nodiscard]] size_t DeletedCount() const { return deleted_count_; }

 private:
  Transaction* txn_;
  Table* table_;
  bool executed_{false};
  size_t deleted_count_{0};
};

}  // namespace tinylamb

#endif  // TINYLAMB_TRUNCATE_HPP
