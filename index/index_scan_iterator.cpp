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

#include "index/b_plus_tree.hpp"
#include "index/index.hpp"
#include "page/page_manager.hpp"
#include "table/table.hpp"
#include "transaction/transaction.hpp"

namespace tinylamb {

IndexScanIterator::IndexScanIterator(const Table& table, const Index& index,
                                     Transaction& txn, const Value& begin,
                                     const Value& end, bool ascending)
    : table_(table),
      index_(index),
      txn_(txn),
      begin_(begin),
      end_(end),
      ascending_(ascending),
      is_unique_(index.sc_.mode_ == IndexMode::kUnique),
      value_offset_(ascending_ ? 0 : -1),
      bpt_(index.Root()),
      iter_(&bpt_, &txn,
            begin_.IsNull() ? "" : begin_.EncodeMemcomparableFormat(),
            end_.IsNull() ? "" : end_.EncodeMemcomparableFormat(), ascending) {
  if (!iter_.IsValid()) {
    return;
  }
  keys_.DecodeMemcomparableFormat(iter_.Key());
  if (is_unique_) {
    auto val = Decode<Table::IndexValueType>(iter_.Value());
    pos_ = val.pos;
    include_ = val.include;
  } else {
    auto val = Decode<std::vector<Table::IndexValueType>>(iter_.Value());
    value_offset_ = ascending ? 0 : val.size() - 1;
    pos_ = val[value_offset_].pos;
    include_ = val[value_offset_].include;
  }
}

bool IndexScanIterator::IsValid() const { return iter_.IsValid(); }

std::string IndexScanIterator::GetValue() const { return iter_.Value(); }

RowPosition IndexScanIterator::Position() const {
  if (!iter_.IsValid()) {
    return {};
  }
  return pos_;
}

void IndexScanIterator::Clear() {
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
    auto val = Decode<std::vector<Table::IndexValueType>>(iter_.Value());
    const Table::IndexValueType& row_value = val[value_offset_];
    pos_ = row_value.pos;
    include_ = row_value.include;
  }
}

void IndexScanIterator::ResolveRow() const {
  PageRef ref = txn_.PageManager()->GetPage(pos_.page_id);
  if (!ref.IsValid()) {
    return;
  }
  txn_.AddReadSet(pos_);
  ASSIGN_OR_CRASH(std::string_view, row, ref->Read(txn_, pos_.slot));
  current_row_.Deserialize(row.data(), table_.schema_);
  current_row_resolved_ = true;
}

IteratorBase& IndexScanIterator::operator++() {
  current_row_resolved_ = false;
  if (is_unique_) {
    ++iter_;
  } else {
    auto val = Decode<std::vector<Table::IndexValueType>>(iter_.Value());
    ++value_offset_;
    if (val.size() <= value_offset_) {
      ++iter_;
      value_offset_ = 0;
    }
  }
  UpdateIteratorState();
  return *this;
}

IteratorBase& IndexScanIterator::operator--() {
  current_row_resolved_ = false;
  if (is_unique_) {
    --iter_;
  } else {
    if (0 < value_offset_) {
      --value_offset_;
    } else {
      --iter_;
      if (iter_.IsValid()) {
        auto val = Decode<std::vector<Table::IndexValueType>>(iter_.Value());
        value_offset_ = val.size() - 1;
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
  LOG(INFO) << current_row_;
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