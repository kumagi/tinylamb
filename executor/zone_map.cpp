/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/zone_map.hpp"

#include <cmath>
#include <cstdint>
#include <string>
#include <string_view>

#include "common/constants.hpp"
#include "type/collation.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb {

void ZoneMap::Add(const Value& value) {
  initialized_ = true;
  if (value.IsNull()) {
    ++null_count_;
    return;
  }
  ++value_count_;
  has_collated_values_ =
      has_collated_values_ || CollationOf(value) == Collation::kCaseInsensitive;
  // Arrays carry no useful scalar ordering for pruning (element-wise
  // lexicographic comparison throws across NULL/non-NULL elements), so keep
  // them out of the min/max envelope.
  if (value.IsArray()) {
    return;
  }  // PRODUCTION FIX: NaN fails every IEEE comparison, so a NaN that arrived
  // first poisoned min/max into (NaN, NaN) and MayMatch then pruned batches
  // that actually contained matching rows. Exclude NaN from the envelope and
  // keep a flag: its presence must conservatively keep the zone eligible.
  if (value.type == ValueType::kDouble &&
      std::isnan(value.value.double_value)) {
    has_nan_ = true;
    return;
  }
  if (minimum_ && minimum_->type != value.type) {
    has_nan_ = true;
    minimum_.reset();
    maximum_.reset();
    return;
  }
  if (!minimum_ || value < *minimum_) {
    minimum_ = value;
  }
  if (!maximum_ || *maximum_ < value) {
    maximum_ = value;
  }
}

void ZoneMap::AddInt(int64_t value) {
  initialized_ = true;
  ++value_count_;
  if (!minimum_ || value < minimum_->value.int_value) {
    minimum_ = Value(value);
  }
  if (!maximum_ || maximum_->value.int_value < value) {
    maximum_ = Value(value);
  }
}

void ZoneMap::AddDate(int64_t days) {
  initialized_ = true;
  ++value_count_;
  if (!minimum_ || days < minimum_->value.int_value) {
    minimum_ = Value::DateFromDays(days);
  }
  if (!maximum_ || maximum_->value.int_value < days) {
    maximum_ = Value::DateFromDays(days);
  }
}

void ZoneMap::AddDouble(double value) {
  initialized_ = true;
  ++value_count_;
  // Same NaN guard as ZoneMap::Add: a leading NaN poisoned the envelope.
  if (std::isnan(value)) {
    has_nan_ = true;
    return;
  }
  if (!minimum_ || value < minimum_->value.double_value) {
    minimum_ = Value(value);
  }
  if (!maximum_ || maximum_->value.double_value < value) {
    maximum_ = Value(value);
  }
}

void ZoneMap::AddString(std::string_view value) {
  initialized_ = true;
  ++value_count_;
  // T4171: an improving extremum reuses the stored Value's string buffer
  // (CopyFromStringView assigns in place) instead of allocating a fresh owned
  // string and dropping the old one on every new min/max.
  if (!minimum_ || value < minimum_->value.varchar_value) {
    if (minimum_) {
      minimum_->CopyFromStringView(value);
    } else {
      minimum_ = Value(std::string(value));
    }
  }
  if (!maximum_ || maximum_->value.varchar_value < value) {
    if (maximum_) {
      maximum_->CopyFromStringView(value);
    } else {
      maximum_ = Value(std::string(value));
    }
  }
}

void ZoneMap::AddNull() {
  initialized_ = true;
  ++null_count_;
}

void ZoneMap::AddNulls(size_t count) {
  if (count == 0) {
    return;
  }
  initialized_ = true;
  null_count_ += count;
}

void ZoneMap::AddIntRange(int64_t mn, int64_t mx, size_t count) {
  if (count == 0) {
    return;
  }
  initialized_ = true;
  value_count_ += count;
  if (minimum_ && minimum_->type != ValueType::kInt64) {
    has_nan_ = true;
    minimum_.reset();
    maximum_.reset();
    return;
  }
  // T1935: a reversed interval would make MayMatch reject every row (same
  // contract AddDoubleRange already enforces).
  if (mn > mx) {
    has_nan_ = true;
    minimum_.reset();
    maximum_.reset();
    return;
  }
  if (!minimum_ || mn < minimum_->value.int_value) {
    minimum_ = Value(mn);
  }
  if (!maximum_ || maximum_->value.int_value < mx) {
    maximum_ = Value(mx);
  }
}

void ZoneMap::AddDateRange(int64_t mn, int64_t mx, size_t count) {
  if (count == 0) {
    return;
  }
  initialized_ = true;
  value_count_ += count;
  if (minimum_ && minimum_->type != ValueType::kDate) {
    has_nan_ = true;
    minimum_.reset();
    maximum_.reset();
    return;
  }
  // T1935: reversed interval -> unbounded envelope (see AddIntRange).
  if (mn > mx) {
    has_nan_ = true;
    minimum_.reset();
    maximum_.reset();
    return;
  }
  if (!minimum_ || mn < minimum_->value.int_value) {
    minimum_ = Value::DateFromDays(mn);
  }
  if (!maximum_ || maximum_->value.int_value < mx) {
    maximum_ = Value::DateFromDays(mx);
  }
}

void ZoneMap::AddDoubleRange(double mn, double mx, bool has_bounds,
                             bool saw_nan, size_t count) {
  if (count == 0) {
    return;
  }
  initialized_ = true;
  value_count_ += count;
  if (saw_nan) {
    has_nan_ = true;
  }
  if (!has_bounds) {
    return;
  }
  if (minimum_ && minimum_->type != ValueType::kDouble) {
    has_nan_ = true;
    minimum_.reset();
    maximum_.reset();
    return;
  }
  // A reversed interval would make MayMatch reject every row.
  if (mn > mx) {
    has_nan_ = true;
    minimum_.reset();
    maximum_.reset();
    return;
  }
  if (!minimum_ || mn < minimum_->value.double_value) {
    minimum_ = Value(mn);
  }
  if (!maximum_ || maximum_->value.double_value < mx) {
    maximum_ = Value(mx);
  }
}

void ZoneMap::AddStringRange(std::string_view mn, std::string_view mx,
                             size_t count) {
  if (count == 0) {
    return;
  }
  initialized_ = true;
  value_count_ += count;
  if (minimum_ && minimum_->type != ValueType::kVarChar) {
    has_nan_ = true;
    minimum_.reset();
    maximum_.reset();
    return;
  }
  // T1935: reversed interval -> unbounded envelope, never a false prune.
  if (mx < mn) {
    has_nan_ = true;
    minimum_.reset();
    maximum_.reset();
    return;
  }
  if (!minimum_ || mn < minimum_->value.varchar_value) {
    minimum_ = Value(std::string(mn));
  }
  if (!maximum_ || maximum_->value.varchar_value < mx) {
    maximum_ = Value(std::string(mx));
  }
}

void ZoneMap::AddOpaque() {
  initialized_ = true;
  ++value_count_;
}

void ZoneMap::Merge(const ZoneMap& other) {
  if (!other.initialized_) {
    return;
  }
  initialized_ = true;
  null_count_ += other.null_count_;
  value_count_ += other.value_count_;
  has_nan_ = has_nan_ || other.has_nan_;
  has_collated_values_ = has_collated_values_ || other.has_collated_values_;
  if (minimum_ && other.minimum_ && minimum_->type != other.minimum_->type) {
    has_nan_ = true;
    minimum_.reset();
    maximum_.reset();
    return;
  }
  if (other.minimum_ &&
      (!minimum_ || CompareForOrderBy(*other.minimum_, *minimum_) < 0)) {
    minimum_ = other.minimum_;
  }
  if (other.maximum_ &&
      (!maximum_ || CompareForOrderBy(*maximum_, *other.maximum_) < 0)) {
    maximum_ = other.maximum_;
  }
}

void ZoneMap::Reset() {
  minimum_.reset();
  maximum_.reset();
  null_count_ = 0;
  value_count_ = 0;
  initialized_ = false;
  has_nan_ = false;
  has_collated_values_ = false;
}

bool ZoneMap::MayMatch(BinaryOperation operation, const Value& constant) const {
  // An uninitialized zone map proves nothing: the producing operator may not
  // have maintained it, so pruning would silently drop matching rows.
  if (!initialized_) {
    return true;
  }
  // T1621: a NaN-bearing zone used to keep every row ("NaN compares false
  // against everything, so envelope pruning is unsound").  But the engine's
  // AST ground truth (binary_expression.cpp) is IEEE-unordered: EVERY
  // comparison against a NaN -- ordered, `=`, and even `!=` -- evaluates to
  // FALSE, so a NaN row can never satisfy a BinaryOperation predicate.
  // AddDouble/AddDoubleRange already keep NaN out of the envelope, so the
  // finite min/max remains a sound pruning envelope for the whole zone and
  // the unconditional bail-out only cost pruning for the batch's finite
  // rows.  Zones whose envelope was dropped as unreliable (mixed types,
  // reversed ranges) or that saw ONLY NaN keep the conservative answer via
  // the empty-envelope branch below.
  if (constant.IsNull()) {
    return false;
  }
  if (!minimum_ || !maximum_) {
    // Non-NULL values were observed but excluded from the envelope (arrays,
    // NaN-only zones) or the envelope was dropped as unreliable (mixed
    // types, reversed ranges): nothing can be proven, so keep every row.
    // value_count_ is the single authority for "a non-NULL cell was seen" --
    // every path that sets has_nan_ has already incremented it, so the two
    // checks were redundant. has_nan_ only describes why the envelope is
    // empty and must not independently disable pruning.
    return value_count_ > 0;
  }
  // Cross-type comparisons (e.g. int constant vs double zone) may match after
  // implicit conversion; pruning them would silently drop rows.
  if (minimum_->type != constant.type) {
    return true;
  }
  // An interval-tagged constant compared against an envelope built from
  // untagged VARCHAR values uses non-commutative TryLess semantics
  // (untagged < tagged is always true, tagged < untagged is always false),
  // so envelope pruning is unsound.  Keep the batch.
  if (constant.IsInterval()) {
    return true;
  }
  // A collation can change the order of values inside a byte-order range:
  // e.g. ["Z", "a"] contains a match for COLLATE("z", "und:ci"). Until
  // the zone carries an envelope for each collation, retain such batches.
  if (has_collated_values_ ||
      CollationOf(constant) == Collation::kCaseInsensitive) {
    return true;
  }
  switch (operation) {
    case BinaryOperation::kEquals:
      return *minimum_ <= constant && constant <= *maximum_;
    case BinaryOperation::kNotEquals:
      return !(*minimum_ == constant && *maximum_ == constant);
    case BinaryOperation::kLessThan:
      return *minimum_ < constant;
    case BinaryOperation::kLessThanEquals:
      return *minimum_ <= constant;
    case BinaryOperation::kGreaterThan:
      return constant < *maximum_;
    case BinaryOperation::kGreaterThanEquals:
      return constant <= *maximum_;
    default:
      return true;
  }
}

}  // namespace tinylamb
