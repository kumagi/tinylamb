/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_EXECUTOR_ZONE_MAP_HPP
#define TINYLAMB_EXECUTOR_ZONE_MAP_HPP

#include <cstddef>
#include <optional>
#include <string_view>

#include "common/constants.hpp"
#include "type/value.hpp"

namespace tinylamb {

class ZoneMap {
 public:
  void Add(const Value& value);
  // Typed variants for callers that hold unboxed storage.  They keep the
  // minimum/maximum Values correctly typed (kInt64/kDate/kDouble/kVarChar)
  // so MayMatch still compares against same-type constants.
  void AddInt(int64_t value);
  void AddDate(int64_t days);
  void AddDouble(double value);
  void AddString(std::string_view value);
  void AddNull();
  // Records a non-NULL value that carries no scalar ordering (arrays): it
  // bumps value_count_ so MayMatch keeps the zone instead of pruning a batch
  // that holds only array values.
  void AddOpaque();
  void Reset();

  [[nodiscard]] bool MayMatch(BinaryOperation operation,
                              const Value& constant) const;
  // False until any Add* observed a row.  Callers must treat an
  // uninitialized zone conservatively (it may match anything); only an
  // initialized, value-free zone proves "no non-NULL values here".
  [[nodiscard]] bool Initialized() const { return initialized_; }
  [[nodiscard]] const std::optional<Value>& Minimum() const { return minimum_; }
  [[nodiscard]] const std::optional<Value>& Maximum() const { return maximum_; }
  [[nodiscard]] size_t NullCount() const { return null_count_; }
  [[nodiscard]] size_t ValueCount() const { return value_count_; }

 private:
  std::optional<Value> minimum_;
  std::optional<Value> maximum_;
  size_t null_count_{0};
  size_t value_count_{0};
  bool initialized_{false};
  // Set when a DOUBLE NaN was added: NaN is excluded from the envelope but
  // must conservatively keep the zone eligible for every predicate.
  bool has_nan_{false};
};

}  // namespace tinylamb

#endif  // TINYLAMB_EXECUTOR_ZONE_MAP_HPP
