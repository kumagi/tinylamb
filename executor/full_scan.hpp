//
// Created by kumagi on 2022/02/21.
//

#ifndef TINYLAMB_FULL_SCAN_HPP
#define TINYLAMB_FULL_SCAN_HPP

#include <memory>

#include "executor/executor.hpp"
#include "page/row_position.hpp"
#include "table/iterator.hpp"
#include "table/table.hpp"

namespace tinylamb {
class TableInterface;
class Transaction;

class FullScan : public ExecutorBase {
 public:
  FullScan(Transaction& txn, Table* table);
  ~FullScan() override = default;
  bool Next(Row* dst, RowPosition* rp) override;
  void Dump(std::ostream& o, int indent) const override;

 private:
  Table* table_;
  Iterator iter_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_FULL_SCAN_HPP
