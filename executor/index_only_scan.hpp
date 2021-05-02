//
// Created by kumagi on 22/06/30.
//

#ifndef TINYLAMB_INDEX_ONLY_SCAN_HPP
#define TINYLAMB_INDEX_ONLY_SCAN_HPP

#include "executor_base.hpp"
#include "expression/expression.hpp"
#include "index/index_scan_iterator.hpp"
#include "type/schema.hpp"

namespace tinylamb {
class Index;
class Table;
class Schema;
class Transaction;
class Value;

class IndexOnlyScan : public ExecutorBase {
 public:
  IndexOnlyScan(Transaction& txn, const Table& table, const Index& index,
                const Value& begin, const Value& end, bool ascending,
                Expression where, const Schema& sc);
  IndexOnlyScan(const IndexOnlyScan&) = delete;
  IndexOnlyScan(IndexOnlyScan&&) = delete;
  IndexOnlyScan& operator=(const IndexOnlyScan&) = delete;
  IndexOnlyScan& operator=(IndexOnlyScan&&) = delete;
  ~IndexOnlyScan() override = default;

  bool Next(Row* dst, RowPosition* rp) override;
  void Dump(std::ostream& o, int indent) const override;

 private:
  static Schema KeySchema(const Index& idx, const Schema& input_schema);
  static Schema ValueSchema(const Index& idx, const Schema& input_schema);
  static Schema OutputSchema(const Index& idx, const Schema& input_schema);

  IndexScanIterator iter_;
  Expression cond_;
  Schema key_schema_;
  Schema value_schema_;
  Schema output_schema_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_INDEX_ONLY_SCAN_HPP
