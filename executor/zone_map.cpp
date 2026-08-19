/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/zone_map.hpp"

namespace tinylamb {

void ZoneMap::Add(const Value& value) {
  if (value.IsNull()) {
    ++null_count_;
    return;
  }
  ++value_count_;
  if (!minimum_ || value < *minimum_) minimum_ = value;
  if (!maximum_ || *maximum_ < value) maximum_ = value;
}

void ZoneMap::Reset() {
  minimum_.reset();
  maximum_.reset();
  null_count_ = 0;
  value_count_ = 0;
}

bool ZoneMap::MayMatch(BinaryOperation operation, const Value& constant) const {
  if (!minimum_ || constant.IsNull() || minimum_->type != constant.type) {
    return false;
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
