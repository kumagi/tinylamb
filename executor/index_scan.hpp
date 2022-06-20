//
// Created by kumagi on 22/06/19.
//

#ifndef TINYLAMB_INDEX_SCAN_HPP
#define TINYLAMB_INDEX_SCAN_HPP

#include "executor/executor_base.hpp"
#include "table/iterator.hpp"

namespace tinylamb {
class Index;
class Table;
class Transaction;

class IndexScan : public ExecutorBase {
 public:
  IndexScan(Transaction& txn, Table* table, Index* index, const Row& begin,
            const Row& end, bool ascending);
  ~IndexScan() override = default;
  bool Next(Row* dst, RowPosition* rp) override;
  void Dump(std::ostream& o, int indent) const override;

 private:
  Iterator iter_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_INDEX_SCAN_HPP
