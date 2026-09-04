/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/skip_scan_distinct.hpp"

#include <cassert>
#include <cstddef>
#include <iostream>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "common/decoder.hpp"
#include "common/status_or.hpp"
#include "index/b_plus_tree.hpp"
#include "index/b_plus_tree_iterator.hpp"
#include "index/index.hpp"
#include "page/page_manager.hpp"
#include "page/page_ref.hpp"
#include "table/table.hpp"
#include "transaction/transaction.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"

namespace tinylamb {

namespace {

std::string EncodeParts(const std::vector<Value>& parts) {
  std::string encoded;
  for (const Value& value : parts) {
    if (value.IsNull()) {
      break;
    }
    encoded += value.EncodeMemcomparableFormat();
  }
  return encoded;
}

std::string EncodeEndParts(const Index& index,
                           const std::vector<Value>& parts) {
  std::string encoded = EncodeParts(parts);
  if (!encoded.empty() && parts.size() < index.sc_.key_.size()) {
    encoded.push_back(static_cast<char>(0xff));
  }
  return encoded;
}

}  // namespace

SkipScanDistinct::SkipScanDistinct(Transaction& txn, const Table& table,
                                   const Index& index, const Value& begin,
                                   const Value& end, bool ascending,
                                   Expression where, Schema sc,
                                   size_t prefix_cols)
    : SkipScanDistinct(
          txn, table, index,
          begin.IsNull() ? std::vector<Value>{} : std::vector<Value>{begin},
          end.IsNull() ? std::vector<Value>{} : std::vector<Value>{end},
          ascending, std::move(where), std::move(sc), prefix_cols) {}

SkipScanDistinct::SkipScanDistinct(Transaction& txn, const Table& table,
                                   const Index& index,
                                   const std::vector<Value>& begin_key,
                                   const std::vector<Value>& end_key,
                                   bool ascending, Expression where, Schema sc,
                                   size_t prefix_cols)
    : txn_(txn),
      table_(table),
      index_(index),
      begin_key_(begin_key),
      end_key_(end_key),
      ascending_(ascending),
      where_(std::move(where)),
      schema_(std::move(sc)),
      prefix_cols_(prefix_cols),
      is_unique_(index.StoresSingleValue()),
      bpt_(index.Root()),
      iter_(&bpt_, &txn_, EncodeParts(begin_key_),
            EncodeEndParts(index_, end_key_), ascending_) {}

void SkipScanDistinct::ResolveCurrentRow() const {
  if (current_row_resolved_) {
    return;
  }
  PageRef ref = txn_.GetPageManager()->GetPage(current_pos_.page_id, true);
  if (!ref.IsValid()) {
    current_row_.Clear();
    current_row_resolved_ = true;
    return;
  }
  StatusOr<std::string_view> row = ref->Read(txn_, current_pos_.slot);
  if (!row.HasValue()) {
    current_row_.Clear();
    current_row_resolved_ = true;
    return;
  }
  txn_.AddReadSet(current_pos_);
  const std::string encoded_row(row.Value());
  current_row_.Deserialize(encoded_row.data(), table_.GetSchema());
  current_row_resolved_ = true;
}

void SkipScanDistinct::SeekNextDistinct(const std::string& /*prev_encoded_key*/,
                                        const Row& prev_key) {
  if (prefix_cols_ == 0 || prefix_cols_ >= index_.sc_.key_.size()) {
    // Distinct on the entire index key:
    // Advancing the BPlusTreeIterator jumps directly past all duplicate rows
    // stored under this key entry!
    if (ascending_) {
      ++iter_;
    } else {
      --iter_;
    }
    return;
  }

  // Prefix distinct: jump past all entries sharing the same leading prefix.
  if (ascending_) {
    ++iter_;
    while (iter_.IsValid()) {
      Row next_key;
      next_key.DecodeMemcomparableFormat(iter_.Key());
      bool prefix_equal = true;
      for (size_t col = 0;
           col < prefix_cols_ && col < prev_key.Size() && col < next_key.Size();
           ++col) {
        if (prev_key[col] != next_key[col]) {
          prefix_equal = false;
          break;
        }
      }
      if (!prefix_equal) {
        break;
      }
      // Same prefix, seek past this prefix using BPlusTree seek with prefix
      // upper bound!
      std::vector<Value> prefix_values;
      for (size_t col = 0; col < prefix_cols_ && col < prev_key.Size(); ++col) {
        prefix_values.push_back(prev_key[col]);
      }
      std::string seek_after = EncodeParts(prefix_values);
      seek_after.push_back(static_cast<char>(0xff));
      iter_ = BPlusTreeIterator(&bpt_, &txn_, seek_after,
                                EncodeEndParts(index_, end_key_), ascending_);
    }
  } else {
    --iter_;
    while (iter_.IsValid()) {
      Row next_key;
      next_key.DecodeMemcomparableFormat(iter_.Key());
      bool prefix_equal = true;
      for (size_t col = 0;
           col < prefix_cols_ && col < prev_key.Size() && col < next_key.Size();
           ++col) {
        if (prev_key[col] != next_key[col]) {
          prefix_equal = false;
          break;
        }
      }
      if (!prefix_equal) {
        break;
      }
      --iter_;
    }
  }
}

bool SkipScanDistinct::Next(Row* dst, RowPosition* rp) {
  assert(dst != nullptr);
  while (!finished_ && iter_.IsValid()) {
    current_index_key_.Clear();
    current_index_key_.DecodeMemcomparableFormat(iter_.Key());

    if (is_unique_) {
      auto val = Decode<Table::IndexValueType>(iter_.Value());
      current_pos_ = val.pos;
    } else {
      auto val = Decode<std::vector<Table::IndexValueType>>(iter_.Value());
      if (val.empty()) {
        if (ascending_) {
          ++iter_;
        } else {
          --iter_;
        }
        continue;
      }
      current_pos_ = val[0].pos;
    }

    current_row_resolved_ = false;
    ResolveCurrentRow();

    bool matched = true;
    if (where_) {
      matched = where_->Evaluate(current_row_, schema_).Truthy();
    }

    std::string prev_encoded_key = iter_.Key();
    Row prev_key = current_index_key_;

    SeekNextDistinct(prev_encoded_key, prev_key);

    if (matched && current_row_.IsValid()) {
      *dst = current_row_;
      if (rp != nullptr) {
        *rp = current_pos_;
      }
      return true;
    }
  }
  finished_ = true;
  return false;
}

size_t SkipScanDistinct::NextBatch(DataChunk* destination, size_t max_rows) {
  if (destination == nullptr || max_rows == 0) {
    return 0;
  }
  size_t count = 0;
  Row row;
  RowPosition rp;
  while (count < max_rows && Next(&row, &rp)) {
    destination->Append(std::move(row), rp);
    ++count;
  }
  return count;
}

void SkipScanDistinct::Dump(std::ostream& o, int /*indent*/) const {
  o << "SkipScanDistinct on " << table_.GetSchema().Name() << " using "
    << index_.sc_.name_ << " (prefix_cols=" << prefix_cols_ << ")";
}

void SkipScanDistinct::Explain(std::ostream& o, int indent) const {
  Dump(o, indent);
}

}  // namespace tinylamb
