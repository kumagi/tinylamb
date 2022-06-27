//
// Created by kumagi on 22/06/19.
//

#ifndef TINYLAMB_INDEX_SCAN_HPP
#define TINYLAMB_INDEX_SCAN_HPP

#include "executor/executor_base.hpp"
#include "expression/expression.hpp"
#include "table/iterator.hpp"
#include "type/schema.hpp"

namespace tinylamb {
class Index;
class Table;
class Transaction;

class IndexScan : public ExecutorBase {
 public:
  IndexScan(Transaction& txn, const Table& table, const Index& index,
            const Value& begin, const Value& end, bool ascending,
            Expression where, const Schema& sc);
  ~IndexScan() override = default;
  bool Next(Row* dst, RowPosition* rp) override;
  void Dump(std::ostream& o, int indent) const override;

 private:
  Iterator iter_;
  Expression cond_;
  const Schema& schema_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_INDEX_SCAN_HPP
