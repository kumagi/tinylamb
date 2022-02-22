//
// Created by kumagi on 2022/02/21.
//

#ifndef TINYLAMB_FULL_SCAN_HPP
#define TINYLAMB_FULL_SCAN_HPP

#include <memory>

#include "executor/executor_base.hpp"
#include "table/iterator.hpp"

namespace tinylamb {
class TableInterface;
class Transaction;

class FullScan : public ExecutorBase {
 public:
  FullScan(Transaction& txn, TableInterface* table);
  bool Next(Row* dst) override;
  ~FullScan() override = default;

 private:
  TableInterface* table_;
  Iterator iter_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_FULL_SCAN_HPP
