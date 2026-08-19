/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_EXECUTOR_ZONE_MAP_HPP
#define TINYLAMB_EXECUTOR_ZONE_MAP_HPP

#include <cstddef>
#include <optional>

#include "common/constants.hpp"
#include "type/value.hpp"

namespace tinylamb {

class ZoneMap {
 public:
  void Add(const Value& value);
  void Reset();

  [[nodiscard]] bool MayMatch(BinaryOperation operation,
                              const Value& constant) const;
  [[nodiscard]] const std::optional<Value>& Minimum() const { return minimum_; }
  [[nodiscard]] const std::optional<Value>& Maximum() const { return maximum_; }
  [[nodiscard]] size_t NullCount() const { return null_count_; }
  [[nodiscard]] size_t ValueCount() const { return value_count_; }

 private:
  std::optional<Value> minimum_;
  std::optional<Value> maximum_;
  size_t null_count_{0};
  size_t value_count_{0};
};

}  // namespace tinylamb

#endif  // TINYLAMB_EXECUTOR_ZONE_MAP_HPP
