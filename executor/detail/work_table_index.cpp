/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/detail/work_table_index.hpp"

#include <functional>
#include <utility>

#include "executor/detail/expression_eval.hpp"
#include "type/collation.hpp"
#include "type/value.hpp"

namespace tinylamb::relational_detail {

bool WorkTableRowEqual::operator()(const Row& left, const Row& right) const {
  if (left.values_.size() != right.values_.size()) {
    return false;
  }
  for (size_t i = 0; i < left.values_.size(); ++i) {
    // NULL equals NULL, NaN collapses, -0.0 equals +0.0 and a collated
    // VARCHAR pair folds -- the one peer rule (type/collation.hpp).
    if (!EqualUnderCollation(left.values_[i], right.values_[i])) {
      return false;
    }
  }
  return true;
}

size_t WorkTableRowHash::operator()(const Row& row) const {
  uint64_t hash = 14695981039346656037ULL;  // FNV-1a offset basis
  for (const Value& value : row.values_) {
    uint64_t h = 0;
    if (value.type == ValueType::kVarChar && !value.IsNull() &&
        !value.IsInterval()) {
      // EqualUnderCollation folds a VARCHAR pair when EITHER side carries a
      // case-insensitive tag, so hash every text cell folded (tagged or not)
      // so CI-tagged and untagged peers land in the same bucket (mirrors
      // executor/distinct.cpp DistinctRowHash).
      h = std::hash<std::string>{}(
          FoldCaseForCollation(value.value.varchar_value));
    } else {
      h = std::hash<Value>()(CanonicalDistinctValue(value));
    }
    hash ^= h;
    hash *= 1099511628211ULL;  // FNV-1a prime
  }
  return static_cast<size_t>(hash);
}

WorkTableIndexCounters& WorkTableIndexStats() {
  static WorkTableIndexCounters counters;
  return counters;
}

bool WorkTableIndex::Insert(const Row& row) {
  WorkTableIndexCounters* counters =
      count_stats_ ? &WorkTableIndexStats() : nullptr;
  if (counters != nullptr) {
    counters->inserts.fetch_add(1, std::memory_order_relaxed);
  }
  const auto [it, inserted] = rows_.insert(row);
  (void)it;
  if (!inserted && counters != nullptr) {
    counters->duplicates.fetch_add(1, std::memory_order_relaxed);
  }
  return inserted;
}

bool WorkTableIndex::Contains(const Row& row) const {
  return rows_.contains(row);
}

}  // namespace tinylamb::relational_detail
