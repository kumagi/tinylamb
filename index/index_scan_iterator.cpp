/**
 * Copyright 2023 KUMAZAKI Hiroki
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "index_scan_iterator.hpp"

#include <cstddef>
#include <cstdint>
#include <ostream>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "common/decoder.hpp"
#include "common/status_or.hpp"
#include "index/b_plus_tree.hpp"
#include "index/index.hpp"
#include "index_schema.hpp"
#include "page/page_manager.hpp"
#include "table/iterator_base.hpp"
#include "table/table.hpp"
#include "transaction/transaction.hpp"
#include "type/value.hpp"

namespace tinylamb {
namespace {

std::string EncodeParts(const std::vector<Value>& parts) {
  std::string encoded;
  for (const Value& value : parts) {
    if (value.IsNull()) { break;
}
    encoded += value.EncodeMemcomparableFormat();
  }
  return encoded;
}

std::string EncodeEndParts(const Index& index, const std::vector<Value>& parts) {
  std::string encoded = EncodeParts(parts);
  if (!encoded.empty() && parts.size() < index.sc_.key_.size()) {
    encoded.push_back(static_cast<char>(0xff));
  }
  return encoded;
}

Value FirstOrNull(const std::vector<Value>& parts) {
  return parts.empty() ? Value() : parts.front();
}

}  // namespace

IndexScanIterator::IndexScanIterator(const Table& table, const Index& index,
                                     Transaction& txn, const Value& begin,
                                     const Value& end, bool ascending)
    : IndexScanIterator(table, index, txn,
                        begin.IsNull() ? std::vector<Value>{}
                                       : std::vector<Value>{begin},
                        end.IsNull() ? std::vector<Value>{}
                                     : std::vector<Value>{end},
                        ascending) {}

IndexScanIterator::IndexScanIterator(const Table& table, const Index& index,
                                     Transaction& txn,
                                     const std::vector<Value>& begin_key,
                                     const std::vector<Value>& end_key,
                                     bool ascending)
    : table_(table),
      index_(index),
      txn_(txn),
      begin_(FirstOrNull(begin_key)),
      end_(FirstOrNull(end_key)),
      ascending_(ascending),
      is_unique_(index.StoresSingleValue()),
      bpt_(index.Root()),
      iter_(&bpt_, &txn, EncodeParts(begin_key), EncodeEndParts(index, end_key),
            ascending) {
  if (!iter_.IsValid()) {
    return;
  }
  keys_.DecodeMemcomparableFormat(iter_.Key());
  if (is_unique_) {
    auto val = Decode<Table::IndexValueType>(iter_.Value());
    pos_ = val.pos;
    include_ = val.include;
  } else {
    auto val = Decode<std::vector<Table::IndexValueType> >(iter_.Value());
    if (val.empty()) {
      // A corrupted (or future empty) value list must not underflow the
      // offset below; leave the row state cleared.
      Clear();
      return;
    }
    value_offset_ =
        ascending_ ? 0 : static_cast<int64_t>(val.size()) - 1;
    pos_ = val[static_cast<size_t>(value_offset_)].pos;
    include_ = val[static_cast<size_t>(value_offset_)].include;
  }
}

bool IndexScanIterator::IsValid() const {
  return !invalidated_ && iter_.IsValid();
}

std::string IndexScanIterator::GetValue() const { return iter_.Value(); }

RowPosition IndexScanIterator::Position() const {
  if (!IsValid()) {
    return {};
  }
  return pos_;
}

void IndexScanIterator::Clear() {
  // A cleared iterator must behave as exhausted: Position()/operator++ on it
  // used to leak a valid-looking {0,0} row position to IndexScan::Next.
  invalidated_ = true;
  pos_ = RowPosition();
  keys_.Clear();
  include_.Clear();
  current_row_.Clear();
}

void IndexScanIterator::UpdateIteratorState() {
  if (!iter_.IsValid()) {
    Clear();
    return;
  }
  keys_.DecodeMemcomparableFormat(iter_.Key());
  if (is_unique_) {
    auto rp = Decode<Table::IndexValueType>(GetValue());
    pos_ = rp.pos;
    include_ = rp.include;
  } else {
    auto val = Decode<std::vector<Table::IndexValueType> >(iter_.Value());
    if (val.empty() || value_offset_ < 0 ||
        static_cast<size_t>(value_offset_) >= val.size()) {
      Clear();
      return;
    }
    const Table::IndexValueType& row_value =
        val[static_cast<size_t>(value_offset_)];
    pos_ = row_value.pos;
    include_ = row_value.include;
  }
}

void IndexScanIterator::ResolveRow() const {
  PageRef ref = txn_.GetPageManager()->GetPage(pos_.page_id, true);
  if (!ref.IsValid()) {
    return;
  }
  StatusOr<std::string_view> row = ref->Read(txn_, pos_.slot);
  if (!row.HasValue()) {
    current_row_.Clear();
    current_row_resolved_ = true;
    return;
  }
  // Only register rows that actually exist so the read set stays clean.
  txn_.AddReadSet(pos_);
  // Length-prefixed page images: decoded by offsets, never as C strings.
  current_row_.Deserialize(
      row.Value().data(),  // NOLINT(bugprone-suspicious-stringview-data-usage)
      table_.schema_);
  if (index_.RetainsDeletedEntries() &&
      index_.GenerateKey(current_row_) != iter_.Key()) {
    // A retained entry may point at a row whose indexed key was changed by a
    // newer visible version.  It is valid for an old snapshot, but not this
    // one.  Rechecking here also keeps direct BeginIndexScan callers correct
    // when no SQL predicate is available above the iterator.
    current_row_.Clear();
  }
  current_row_resolved_ = true;
}

IteratorBase& IndexScanIterator::operator++() {
  current_row_resolved_ = false;
  if (!IsValid()) {
    // Guard against advancing past the end (IndexScanIterator::operator++ is
    // called unconditionally by some loops).
    return *this;
  }
  if (!ascending_) {
    if (is_unique_) {
      --iter_;
    } else if (0 < value_offset_) {
      --value_offset_;
    } else {
      --iter_;
      if (iter_.IsValid()) {
        auto val = Decode<std::vector<Table::IndexValueType> >(iter_.Value());
        value_offset_ = static_cast<int64_t>(val.size()) - 1;
      }
    }
    UpdateIteratorState();
    return *this;
  }
  if (is_unique_) {
    ++iter_;
  } else {
    auto val = Decode<std::vector<Table::IndexValueType> >(iter_.Value());
    ++value_offset_;
    if (std::cmp_less_equal(val.size(), value_offset_)) {
      ++iter_;
      value_offset_ = 0;
    }
  }
  UpdateIteratorState();
  return *this;
}

IteratorBase& IndexScanIterator::operator--() {
  current_row_resolved_ = false;
  if (!IsValid()) {
    return *this;
  }
  if (is_unique_) {
    --iter_;
  } else {
    if (0 < value_offset_) {
      --value_offset_;
    } else {
      --iter_;
      if (iter_.IsValid()) {
        auto val = Decode<std::vector<Table::IndexValueType> >(iter_.Value());
        value_offset_ = static_cast<int64_t>(val.size()) - 1;
      }
    }
  }
  UpdateIteratorState();
  return *this;
}

const Row& IndexScanIterator::operator*() const {
  if (!current_row_resolved_) {
    ResolveRow();
  }
  return current_row_;
}

Row& IndexScanIterator::operator*() {
  if (!current_row_resolved_) {
    ResolveRow();
  }
  return current_row_;
}

void IndexScanIterator::Dump(std::ostream& o, int /*indent*/) const {
  o << index_.sc_.name_ << " on " << table_.GetSchema().Name() << ": {";
  for (size_t i = 0; i < index_.sc_.key_.size(); ++i) {
    if (0 < i) {
      o << ", ";
    }
    o << index_.sc_.key_[i];
  }
  o << "},";
  if (!index_.sc_.include_.empty()) {
    o << " Include: {";
    for (size_t i = 0; i < index_.sc_.include_.size(); ++i) {
      if (0 < i) {
        o << ", ";
      }
      o << index_.sc_.include_[i];
    }
    o << "},";
  }
  o << " [";
  if (begin_ == end_) {
    o << begin_;
  } else if (ascending_) {
    o << begin_ << " -> " << end_;
  } else {
    o << end_ << " -> " << begin_;
  }
  o << "]";
}
}  // namespace tinylamb
