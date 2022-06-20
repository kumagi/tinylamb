//
// Created by kumagi on 2022/02/21.
//

#ifndef TINYLAMB_FULL_SCAN_HPP
#define TINYLAMB_FULL_SCAN_HPP

#include <memory>

#include "executor/executor_base.hpp"
#include "table/iterator.hpp"

namespace tinylamb {
class Table;
class Transaction;
struct Row;
struct RowPosition;

class FullScan : public ExecutorBase {
 public:
  FullScan(Transaction& txn, const Table& table);
  ~FullScan() override = default;
  bool Next(Row* dst, RowPosition* rp) override;
  void Dump(std::ostream& o, int indent) const override;

 private:
  const Table* table_;
  Iterator iter_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_FULL_SCAN_HPP
