//
// Created by kumagi on 22/05/11.
//

#ifndef TINYLAMB_INSERT_HPP
#define TINYLAMB_INSERT_HPP

#include <string>
#include <string_view>
#include <utility>

#include "executor/executor_base.hpp"

namespace tinylamb {
class Table;
class Transaction;

class Insert : public ExecutorBase {
 public:
  explicit Insert(Transaction& txn, Table* target, Executor src)
      : txn_(&txn), target_(target), src_(std::move(src)) {}
  ~Insert() override = default;
  bool Next(Row* dst, RowPosition* rp) override;
  void Dump(std::ostream& o, int indent) const override;

 private:
  Transaction* txn_;
  Table* target_;
  Executor src_;
  bool finished_{false};
};

}  // namespace tinylamb

#endif  // TINYLAMB_INSERT_HPP
