/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/bitmap_scan.hpp"

#include <algorithm>
#include <ostream>
#include <string>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/status_or.hpp"
#include "executor/detail/scan_filter.hpp"
#include "expression/expression.hpp"
#include "expression/rewrite.hpp"
#include "index/index_scan_iterator.hpp"
#include "page/row_position.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"

namespace tinylamb {

BitmapIndexScan::BitmapIndexScan(
    Transaction& txn, const Table& table, const Index& index,
    const Value& begin, const Value& end,
    TransactionManager::IndexReadGuard* index_read_guard)
    : txn_(&txn),
      table_(&table),
      index_(&index),
      begin_key_({begin}),
      end_key_({end}),
      index_read_guard_(index_read_guard) {}

BitmapIndexScan::BitmapIndexScan(
    Transaction& txn, const Table& table, const Index& index,
    const std::vector<Value>& begin_key, const std::vector<Value>& end_key,
    TransactionManager::IndexReadGuard* index_read_guard)
    : txn_(&txn),
      table_(&table),
      index_(&index),
      begin_key_(begin_key),
      end_key_(end_key),
      index_read_guard_(index_read_guard) {}

std::vector<RowPosition> BitmapIndexScan::ScanPositions() const {
  std::vector<RowPosition> positions;
  for (IndexScanIterator it(*table_, *index_, *txn_, begin_key_, end_key_, true,
                            false, index_read_guard_);
       it.IsValid(); ++it) {
    positions.push_back(it.Position());
  }
  std::sort(positions.begin(), positions.end(), RowPositionComparator{});
  positions.erase(std::unique(positions.begin(), positions.end()),
                  positions.end());
  return positions;
}

BitmapHeapScan::BitmapHeapScan(Transaction& txn, const Table& table,
                               std::vector<RowPosition> positions,
                               Expression where, Schema schema,
                               std::string bitmap_operation)
    : txn_(&txn),
      table_(&table),
      positions_(std::move(positions)),
      where_(std::move(where)),
      where_conjuncts_(SplitConjuncts(where_)),
      schema_(std::move(schema)),
      bitmap_operation_(std::move(bitmap_operation)) {
  std::sort(positions_.begin(), positions_.end(), RowPositionComparator{});
  positions_.erase(std::unique(positions_.begin(), positions_.end()),
                   positions_.end());
}

size_t BitmapHeapScan::NextBatch(DataChunk* destination, size_t max_rows) {
  destination->Reset(schema_, max_rows);
  while (destination->Size() < max_rows && offset_ < positions_.size()) {
    const RowPosition pos = positions_[offset_++];
    StatusOr<Row> row_or = table_->Read(*txn_, pos);
    if (row_or.GetStatus() != Status::kSuccess) {
      FailWith(row_or.GetStatus());
      return destination->Size();
    }
    if (where_) {
      StatusOr<bool> pass = relational_detail::EvaluateConjunctsTolerant(
          where_conjuncts_, [&](const Expression& conjunct) {
            return conjunct->TryEvaluate(row_or.Value(), schema_);
          });
      if (!pass.HasValue()) {
        FailWith(pass.GetStatus());
        return destination->Size();
      }
      if (!pass.Value()) {
        continue;
      }
    }
    destination->Append(std::move(row_or.Value()), pos);
  }
  return destination->Size();
}

void BitmapHeapScan::Dump(std::ostream& o, int /*indent*/) const {
  o << bitmap_operation_ << "\n  BitmapHeapScan: (" << positions_.size()
    << " positions)";
  if (where_) {
    o << "\n  Recheck: " << *where_;
  }
}

}  // namespace tinylamb
