/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_BITMAP_SCAN_HPP
#define TINYLAMB_BITMAP_SCAN_HPP

#include <algorithm>
#include <cstddef>
#include <memory>
#include <ostream>
#include <string>
#include <utility>
#include <vector>

#include "executor/executor_base.hpp"
#include "expression/expression.hpp"
#include "index/index.hpp"
#include "page/row_position.hpp"
#include "table/table.hpp"
#include "transaction/transaction.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"

namespace tinylamb {

struct RowPositionComparator {
  bool operator()(const RowPosition& a, const RowPosition& b) const {
    if (a.page_id != b.page_id) {
      return a.page_id < b.page_id;
    }
    return a.slot < b.slot;
  }
};

inline std::vector<RowPosition> BitmapAnd(const std::vector<RowPosition>& a,
                                          const std::vector<RowPosition>& b) {
  std::vector<RowPosition> result;
  std::set_intersection(a.begin(), a.end(), b.begin(), b.end(),
                        std::back_inserter(result), RowPositionComparator{});
  return result;
}

inline std::vector<RowPosition> BitmapOr(const std::vector<RowPosition>& a,
                                         const std::vector<RowPosition>& b) {
  std::vector<RowPosition> result;
  std::set_union(a.begin(), a.end(), b.begin(), b.end(),
                 std::back_inserter(result), RowPositionComparator{});
  return result;
}

class BitmapIndexScan {
 public:
  BitmapIndexScan(Transaction& txn, const Table& table, const Index& index,
                  const Value& begin, const Value& end);
  BitmapIndexScan(Transaction& txn, const Table& table, const Index& index,
                  const std::vector<Value>& begin_key,
                  const std::vector<Value>& end_key);

  [[nodiscard]] std::vector<RowPosition> ScanPositions() const;

 private:
  Transaction* txn_;
  const Table* table_;
  const Index* index_;
  std::vector<Value> begin_key_;
  std::vector<Value> end_key_;
};

class BitmapHeapScan : public ExecutorBase {
 public:
  BitmapHeapScan(Transaction& txn, const Table& table,
                 std::vector<RowPosition> positions, Expression where,
                 Schema schema, std::string bitmap_operation);
  BitmapHeapScan(const BitmapHeapScan&) = delete;
  BitmapHeapScan(BitmapHeapScan&&) = delete;
  BitmapHeapScan& operator=(const BitmapHeapScan&) = delete;
  BitmapHeapScan& operator=(BitmapHeapScan&&) = delete;
  ~BitmapHeapScan() override = default;

  bool Next(Row* dst, RowPosition* rp) override;
  void Dump(std::ostream& o, int indent) const override;

 private:
  Transaction* txn_;
  const Table* table_;
  std::vector<RowPosition> positions_;
  size_t offset_{0};
  Expression where_;
  Schema schema_;
  std::string bitmap_operation_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_BITMAP_SCAN_HPP
