//
// Created by kumagi on 22/06/30.
//

#include "index_only_scan.hpp"

#include "common/debug.hpp"
#include "index/index.hpp"
#include "index/index_scan_iterator.hpp"
#include "index/index_schema.hpp"

namespace tinylamb {

IndexOnlyScan::IndexOnlyScan(Transaction& txn, const Table& table,
                             const Index& index, const Value& begin,
                             const Value& end, bool ascending, Expression where,
                             const Schema& sc)
    : iter_(table, index, txn, begin, end, ascending),
      cond_(std::move(where)),
      key_schema_(KeySchema(index, sc)),
      value_schema_(ValueSchema(index, sc)),
      output_schema_(OutputSchema(index, sc)) {}

Schema IndexOnlyScan::KeySchema(const Index& idx, const Schema& input_schema) {
  const IndexSchema& is = idx.sc_;
  std::vector<Column> cols;
  cols.reserve(is.key_.size());
  for (const auto& key : is.key_) {
    cols.push_back(input_schema.GetColumn(key));
  }
  return {"", cols};
}

Schema IndexOnlyScan::ValueSchema(const Index& idx,
                                  const Schema& input_schema) {
  const IndexSchema& is = idx.sc_;
  std::vector<Column> cols;
  cols.reserve(is.include_.size());
  for (const auto& key : is.key_) {
    cols.push_back(input_schema.GetColumn(key));
  }
  return {"", cols};
}

Schema IndexOnlyScan::OutputSchema(const Index& idx,
                                   const Schema& input_schema) {
  const IndexSchema& is = idx.sc_;
  std::vector<Column> cols;
  cols.reserve(is.key_.size() + is.include_.size());
  for (const auto& key : is.key_) {
    cols.push_back(input_schema.GetColumn(key));
  }
  for (const auto& key : is.key_) {
    cols.push_back(input_schema.GetColumn(key));
  }
  return {"", cols};
}

bool IndexOnlyScan::Next(Row* dst, RowPosition* /*rp*/) {
  do {
    if (!iter_.IsValid()) {
      return false;
    }
    *dst = iter_.GetKey() + iter_.Include();
    ++iter_;
  } while (!cond_->Evaluate(*dst, output_schema_).Truthy());
  return true;
}

void IndexOnlyScan::Dump(std::ostream& o, int /*indent*/) const {
  o << "IndexOnlyScan: " << iter_ << " where " << *cond_;
}

}  // namespace tinylamb