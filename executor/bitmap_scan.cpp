/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/bitmap_scan.hpp"

#include <algorithm>
#include <ostream>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/status_or.hpp"
#include "index/index_scan_iterator.hpp"

namespace tinylamb {

BitmapIndexScan::BitmapIndexScan(Transaction& txn, const Table& table,
                                 const Index& index, const Value& begin,
                                 const Value& end)
    : txn_(&txn),
      table_(&table),
      index_(&index),
      begin_key_({begin}),
      end_key_({end}) {}

BitmapIndexScan::BitmapIndexScan(Transaction& txn, const Table& table,
                                 const Index& index,
                                 const std::vector<Value>& begin_key,
                                 const std::vector<Value>& end_key)
    : txn_(&txn),
      table_(&table),
      index_(&index),
      begin_key_(begin_key),
      end_key_(end_key) {}

std::vector<RowPosition> BitmapIndexScan::ScanPositions() const {
  std::vector<RowPosition> positions;
  for (IndexScanIterator it(*table_, *index_, *txn_, begin_key_, end_key_,
                            true);
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
      schema_(std::move(schema)),
      bitmap_operation_(std::move(bitmap_operation)) {
  std::sort(positions_.begin(), positions_.end(), RowPositionComparator{});
  positions_.erase(std::unique(positions_.begin(), positions_.end()),
                   positions_.end());
}

bool BitmapHeapScan::Next(Row* dst, RowPosition* rp) {
  while (offset_ < positions_.size()) {
    const RowPosition pos = positions_[offset_++];
    StatusOr<Row> row_or = table_->Read(*txn_, pos);
    if (row_or.GetStatus() != Status::kSuccess) {
      continue;
    }
    if (where_) {
      Value res = where_->Evaluate(row_or.Value(), schema_);
      if (res.IsNull() || !res.Truthy()) {
        continue;
      }
    }
    *dst = std::move(row_or.Value());
    if (rp != nullptr) {
      *rp = pos;
    }
    return true;
  }
  return false;
}

void BitmapHeapScan::Dump(std::ostream& o, int /*indent*/) const {
  o << bitmap_operation_ << "\n  BitmapHeapScan: (" << positions_.size()
    << " positions)";
  if (where_) {
    o << "\n  Recheck: " << *where_;
  }
}

}  // namespace tinylamb
