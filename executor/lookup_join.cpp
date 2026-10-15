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

#include "executor/lookup_join.hpp"

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <memory>
#include <optional>
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

bool JoinValueOrdered(const Value& left, const Value& right,
                      BinaryOperation operation) {
  // Three-valued logic: a comparison against NULL is UNKNOWN, never TRUE, so
  // it cannot produce a match even though the index may still hand us the
  // NULL-keyed entry the seek range covered.
  if (left.IsNull() || right.IsNull()) {
    return false;
  }
  const auto result = TryEvaluateBinary(operation, left, right);
  return result.HasValue() && !result.Value().IsNull() &&
         result.Value().Truthy();
}

bool JoinKeyUnmatchable(const Value& value) {
  return value.type == ValueType::kDouble &&
         std::isnan(value.value.double_value);
}

bool IsMirroredRange(BinaryOperation operation) {
  return operation == BinaryOperation::kGreaterThan ||
         operation == BinaryOperation::kGreaterThanEquals;
}

// Whether `value` may be handed to the index as a range bound.
//
// The seek range is only an enclosure -- the re-check below is authoritative
// -- so the requirement is that the bound's memcomparable encoding orders the
// way the re-check's comparison does, and that the encoded bytes stay inside
// the seek range:
//
//  * INT64 and DATE encode as order-preserving signed integers, matching
//    Value::operator< on the same types. They are the only shapes the probe
//    accepts; any other type (FLOAT64 NaN/-0.0, VARCHAR collation, arrays)
//    could seek outside the rows the comparison accepts, so the planner leaves
//    such a range to the residual predicate.
//  * A UINT64-tagged INT64 shares the INT64 storage but encodes under a
//    different type tag whose entries sort outside the signed range, so a seek
//    built from it can miss the rows the comparison matches. Per row it falls
//    back to the wider prefix scan.
//  * The memcomparable encoding is type-tagged, so a bound whose type differs
//    from the index key column's lands in a different key space entirely --
//    `u < 2.0` on a DATE column would seek past every DATE entry. The planner
//    rejects cross-typed ranges, and the executor rejects them again per row.
//  * NULL and NaN bounds are rejected here; their comparison is never TRUE,
//    so the re-check drops them and only the wider scan would be wasted.
bool UsableRangeBound(const Value& value, ValueType key_type) {
  if (value.IsNull() || JoinKeyUnmatchable(value) || value.type != key_type) {
    return false;
  }
  if (value.type == ValueType::kInt64) {
    return !value.IsUnsigned();
  }
  return value.type == ValueType::kDate;
}

TransactionManager::IndexReadGuard AcquireLookupJoinReadGuard(
    Transaction& txn, const Index& index,
    TransactionManager::IndexReadGuard index_read_guard) {
  if (index_read_guard.HoldsLock()) {
    return index_read_guard;
  }
  return txn.AcquireIndexReadGuard(index.Root());
}

}  // namespace

LookupJoin::LookupJoin(Transaction& txn, Executor left,
                       std::vector<slot_t> left_cols, const Table& tbl,
                       const Index& idx, std::vector<slot_t> right_cols,
                       std::optional<size_t> range_position,
                       BinaryOperation range_operation,
                       TransactionManager::IndexReadGuard index_read_guard)
    : txn_(txn),
      left_(std::move(left)),
      left_cols_(std::move(left_cols)),
      right_(tbl),
      right_idx_(idx),
      index_read_guard_(
          AcquireLookupJoinReadGuard(txn, idx, std::move(index_read_guard))),
      right_it_(nullptr),
      right_cols_(std::move(right_cols)),
      range_position_(range_position),
      range_operation_(range_operation),
      range_key_type_(ValueType::kNull),
      at_most_one_(false) {
  // The probe columns must be aligned with the index's key order: the seek key
  // is built from left_cols_ and has to be a prefix of the stored key image.
  // A misaligned vector seeks between unrelated keys and silently drops
  // matches, so the contract is checked instead of assumed. The relational
  // factory resolves the columns from ProductPlan's LookupSeek, which is
  // derived in the index's key order for exactly this reason.
  CHECK_MSG(left_cols_.size() == right_cols_.size(),
            "LookupJoin: probe column vectors must be the same length");
  for (size_t i = 0; i < right_cols_.size(); ++i) {
    const bool key_in_prefix =
        i < right_idx_.sc_.key_.size() &&
        right_cols_[i] < right_.GetSchema().ColumnCount();
    CHECK_MSG(key_in_prefix,
              "LookupJoin: probe columns exceed the index key prefix");
    CHECK_MSG(right_.GetSchema().GetColumn(right_cols_[i]).Name() ==
                  right_.GetSchema().GetColumn(right_idx_.sc_.key_[i]).Name(),
              "LookupJoin: probe columns are not in index key order");
  }
  if (txn_.IndexKeysMayBeStale(right_idx_.Root())) {
    FailWith(Status::kConflicts);
  }
  if (range_position_ && *range_position_ >= left_cols_.size()) {
    // Defensive: a range position outside the aligned column vectors would
    // read past the extracted key.  Drop the range rather than read garbage;
    // the caller's residual predicate still decides the rows.
    range_position_.reset();
  }
  if (range_position_) {
    // Remember the index key column's declared type: a bound of any other type
    // encodes into a different key space and must not reach the seek. The
    // alignment loop above already proved the offset is in range.
    range_key_type_ = right_.GetSchema()
                          .GetColumn(right_idx_.sc_.key_[*range_position_])
                          .Type();
  }
  // A single-value unique index stores exactly one position per key, so a
  // point probe on the complete key has at most one candidate to examine.
  at_most_one_ = !range_position_.has_value() &&
                 right_idx_.StoresSingleValue() &&
                 left_cols_.size() == right_idx_.sc_.key_.size();
}

void LookupJoin::BuildSeekRange(const Row& left_key) {
  const size_t prefix =
      std::min(range_position_.value_or(left_key.values_.size()),
               left_key.values_.size());
  begin_key_.assign(
      left_key.values_.begin(),
      left_key.values_.begin() + static_cast<std::ptrdiff_t>(prefix));
  end_key_ = begin_key_;
  if (!range_position_ ||
      !UsableRangeBound(left_key[*range_position_], range_key_type_)) {
    // Equality prefix only. IndexScanIterator turns a short end key into the
    // prefix ceiling, so this is "every entry under the prefix"; when the
    // prefix is the complete key it degenerates to a point probe.
    return;
  }
  const Value& bound = left_key[*range_position_];
  if (IsMirroredRange(range_operation_)) {
    // `right > bound` / `right >= bound`: seek starts at the bound and runs to
    // the end of the prefix.
    begin_key_.push_back(bound);
  } else {
    // `right < bound` / `right <= bound`: seek starts at the prefix and
    // stops at the bound. The seek's end is inclusive, so the boundary row
    // always reaches the re-check: `<` drops it there, `<=` keeps it.
    end_key_.push_back(bound);
  }
}

bool LookupJoin::Matches(const Row& left_key, const Row& right_key) const {
  if (left_key.values_.size() != right_key.values_.size()) {
    return false;
  }
  for (size_t i = 0; i < left_key.values_.size(); ++i) {
    if (range_position_ && i == *range_position_) {
      // The seek range already excludes most of the key space, but boundary
      // inclusivity and the per-row bound fallback are only decidable here.
      if (!JoinValueOrdered(right_key[i], left_key[i], range_operation_)) {
        return false;
      }
      continue;
    }
    if (!JoinValueEqual(left_key[i], right_key[i])) {
      return false;
    }
  }
  return true;
}

bool LookupJoin::Load() {
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
    // SQL's `NULL = NULL` is UNKNOWN, so a NULL equality key can never match;
    // a NaN key has no stable position in the memcomparable order. Both are
    // skipped before any seek, exactly as the residual predicate would.
    if (std::ranges::any_of(left_key.values_, [](const Value& value) {
          return value.IsNull() || JoinKeyUnmatchable(value);
        })) {
      continue;
    }
    BuildSeekRange(left_key);
    right_it_ = std::make_unique<IndexScanIterator>(right_, right_idx_, txn_,
                                                    begin_key_, end_key_, true,
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

bool LookupJoin::Next(Row* dst, RowPosition* /* rp */) {
  if (!hold_left_.IsValid() && !Load()) {
    return false;
  }
  for (;;) {
    const Row left_key = hold_left_.Extract(left_cols_);
    Row right_row = **right_it_;
    const Row right_key = right_row.Extract(right_cols_);
    const bool matched = Matches(left_key, right_key);
    if (at_most_one_) {
      // The probe was a point lookup on a single-value unique key: this is
      // the only position that key can name, so step the outer cursor instead
      // of walking the index.
      const Row left_row = hold_left_;
      hold_left_.Clear();
      if (matched) {
        *dst = left_row + right_row;
        return true;
      }
      if (!Load()) {
        return false;
      }
      continue;
    }
    ++*right_it_;
    *dst = hold_left_ + right_row;
    if (!right_it_->IsValid()) {
      if (right_it_->GetStatus() != Status::kSuccess) {
        return FailWith(right_it_->GetStatus());
      }
      const bool last_matched = matched;
      hold_left_.Clear();
      if (last_matched) {
        return true;
      }
      if (!Load()) {
        return false;
      }
      continue;
    }
    if (matched) {
      return true;
    }
  }
}

void LookupJoin::Dump(std::ostream& o, int indent) const {
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
    if (range_position_ && i == *range_position_) {
      ss << ' ' << ToString(range_operation_);
    }
  }
  ss << "}";
  o << "LookupJoin: " << ss.str() << "\n"
    << Indent(static_cast<size_t>(indent) + 2);
  left_->Dump(o, indent + 2);
  o << "\n" << Indent(static_cast<size_t>(indent) + 2) << right_.GetSchema();
}
}  // namespace tinylamb
