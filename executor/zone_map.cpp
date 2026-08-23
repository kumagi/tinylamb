/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/zone_map.hpp"

#include <cstdint>
#include <string_view>
#include <string>
#include "type/value.hpp"
#include "common/constants.hpp"

namespace tinylamb {

void ZoneMap::Add(const Value& value) {
  initialized_ = true;
  if (value.IsNull()) {
    ++null_count_;
    return;
  }
  ++value_count_;
  if (!minimum_ || value < *minimum_) { minimum_ = value;
}
  if (!maximum_ || *maximum_ < value) { maximum_ = value;
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
  if (!minimum_ || value < minimum_->value.varchar_value) {
    minimum_ = Value(std::string(value));
  }
  if (!maximum_ || maximum_->value.varchar_value < value) {
    maximum_ = Value(std::string(value));
  }
}

void ZoneMap::AddNull() {
  initialized_ = true;
  ++null_count_;
}

void ZoneMap::Reset() {
  minimum_.reset();
  maximum_.reset();
  null_count_ = 0;
  value_count_ = 0;
  initialized_ = false;
}

bool ZoneMap::MayMatch(BinaryOperation operation, const Value& constant) const {
  // An uninitialized zone map proves nothing: the producing operator may not
  // have maintained it, so pruning would silently drop matching rows.
  if (!initialized_) {
    return true;
  }
  // An initialized zone holding only NULLs cannot compare, and NULL
  // constants never compare either.  minimum_/maximum_ are always populated
  // together, so checking both here keeps the invariant explicit.
  if (!minimum_ || !maximum_ || constant.IsNull()) {
    return false;
  }
  // Cross-type comparisons (e.g. int constant vs double zone) may match after
  // implicit conversion; pruning them would silently drop rows.
  if (minimum_->type != constant.type) {
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
