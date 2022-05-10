//
// Created by kumagi on 22/05/11.
//

#ifndef TINYLAMB_UPDATE_HPP
#define TINYLAMB_UPDATE_HPP

#include "executor_base.hpp"

namespace tinylamb {
class Transaction;
class Table;

class Update : public ExecutorBase {
 public:
  explicit Update(Transaction& txn, Table* target, Executor src)
      : txn_(&txn), target_(target), src_(std::move(src)) {}

  bool Next(Row* dst, RowPosition* rp) override;
  void Dump(std::ostream& o, int indent) const override;

 private:
  Transaction* txn_;
  Table* target_;
  Executor src_;
  bool finished_{false};
};
}  // namespace tinylamb

#endif  // TINYLAMB_UPDATE_HPP
