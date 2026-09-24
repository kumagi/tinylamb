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

//
// Created by kumagi on 22/07/14.
//

#include "executor/index_join.hpp"

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <memory>
#include <ostream>
#include <sstream>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "executor/executor_base.hpp"
#include "expression/binary_expression.hpp"
#include "index/index.hpp"
#include "index/index_scan_iterator.hpp"
#include "table/table.hpp"
#include "transaction/transaction.hpp"
#include "type/value.hpp"

namespace tinylamb {
namespace {

bool JoinValueEqual(const Value& left, const Value& right) {
  if (left.IsNull() || right.IsNull()) {
    return false;
  }
  const auto result = TryEvaluateBinary(BinaryOperation::kEquals, left, right);
  return result.HasValue() && !result.Value().IsNull() &&
         result.Value().Truthy();
}

bool JoinKeyUnmatchable(const Row& key) {
  return std::ranges::any_of(key.values_, [](const Value& value) {
    return value.type == ValueType::kDouble &&
           std::isnan(value.value.double_value);
  });
}

bool JoinRowsEqual(const Row& left, const Row& right) {
  if (left.values_.size() != right.values_.size()) {
    return false;
  }
  for (size_t i = 0; i < left.values_.size(); ++i) {
    if (!JoinValueEqual(left[i], right[i])) {
      return false;
    }
  }
  return true;
}

TransactionManager::IndexReadGuard AcquireIndexJoinReadGuard(
    Transaction& txn, const Index& index,
    TransactionManager::IndexReadGuard index_read_guard) {
  if (index_read_guard.HoldsLock()) {
    return index_read_guard;
  }
  return txn.AcquireIndexReadGuard(index.Root());
}

}  // namespace

IndexJoin::IndexJoin(Transaction& txn, Executor left,
                     std::vector<slot_t> left_cols, const Table& tbl,
                     const Index& idx, std::vector<slot_t> right_cols,
                     TransactionManager::IndexReadGuard index_read_guard)
    : txn_(txn),
      left_(std::move(left)),
      left_cols_(std::move(left_cols)),
      right_(tbl),
      right_idx_(idx),
      index_read_guard_(
          AcquireIndexJoinReadGuard(txn, idx, std::move(index_read_guard))),
      right_it_(nullptr),
      right_cols_(std::move(right_cols)) {
  if (txn_.IndexKeysMayBeStale(right_idx_.Root())) {
    FailWith(Status::kConflicts);
  }
}

bool IndexJoin::Load() {
  if (GetStatus() != Status::kSuccess) {
    return false;
  }
  for (;;) {
    if (!left_->Next(&hold_left_, nullptr)) {
      hold_left_.Clear();
      if (left_->GetStatus() != Status::kSuccess) {
        FailWith(left_->GetStatus());
      }
      return false;
    }
    const Row left_key = hold_left_.Extract(left_cols_);
    if (JoinKeyUnmatchable(left_key) ||
        std::ranges::any_of(left_key.values_, [](const Value& value) {
          return value.IsNull();
        })) {
      continue;
    }
    std::vector<Value> seek_key;
    const size_t seek_columns =
        std::min(left_key.values_.size(), right_idx_.sc_.key_.size());
    seek_key.assign(
        left_key.values_.begin(),
        left_key.values_.begin() + static_cast<std::ptrdiff_t>(seek_columns));
    right_it_ = std::make_unique<IndexScanIterator>(right_, right_idx_, txn_,
                                                    seek_key, seek_key, true,
                                                    false, &index_read_guard_);
    if (right_it_->IsValid()) {
      return true;
    }
    if (right_it_->GetStatus() != Status::kSuccess) {
      FailWith(right_it_->GetStatus());
      return false;
    }
  }
}

bool IndexJoin::Next(Row* dst, RowPosition* /* rp */) {
  if (!hold_left_.IsValid() && !Load()) {
    return false;
  }
  for (;;) {
    const Row left_key = hold_left_.Extract(left_cols_);
    Row right_row = **right_it_;
    const Row right_key = right_row.Extract(right_cols_);
    ++*right_it_;
    *dst = hold_left_ + right_row;
    if (!right_it_->IsValid()) {
      if (right_it_->GetStatus() != Status::kSuccess) {
        return FailWith(right_it_->GetStatus());
      }
      const bool matched = JoinRowsEqual(left_key, right_key);
      hold_left_.Clear();
      if (matched) {
        return true;
      }
      if (!Load()) {
        return false;
      }
      continue;
    }
    if (JoinRowsEqual(left_key, right_key)) {
      return true;
    }
  }
}

void IndexJoin::Dump(std::ostream& o, int indent) const {
  std::stringstream ss;
  ss << "left: {";
  for (size_t i = 0; i < left_cols_.size(); ++i) {
    if (0 < i) {
      ss << ", ";
    }
    ss << left_cols_[i];
  }
  ss << "} right: with " << right_idx_ << " {";
  for (size_t i = 0; i < right_cols_.size(); ++i) {
    if (0 < i) {
      ss << ", ";
    }
    ss << right_cols_[i];
  }
  ss << "}";
  o << "IndexJoin: " << ss.str() << "\n"
    << Indent(static_cast<size_t>(indent) + 2);
  left_->Dump(o, indent + 2);
  o << "\n" << Indent(static_cast<size_t>(indent) + 2) << right_.GetSchema();
}
}  // namespace tinylamb
