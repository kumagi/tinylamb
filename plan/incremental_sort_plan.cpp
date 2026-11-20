/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "plan/incremental_sort_plan.hpp"

#include <cstddef>
#include <optional>
#include <ostream>
#include <string>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "expression/expression.hpp"
#include "plan/plan.hpp"
#include "plan/sort_plan.hpp"

namespace tinylamb {

Collation IncrementalSortPlan::OrderCollation() const {
  Collation resolved = Collation::kNone;
  for (const auto* keys : {&prefix_keys_, &suffix_keys_}) {
    for (const SortKey& key : *keys) {
      resolved = ResolveCollation(resolved, key.collation);
    }
  }
  return resolved;
}

IncrementalSortPlan::IncrementalSortPlan(Plan child,
                                         std::vector<SortKey> prefix_keys,
                                         std::vector<SortKey> suffix_keys)
    : child_(std::move(child)),
      prefix_keys_(std::move(prefix_keys)),
      suffix_keys_(std::move(suffix_keys)) {}

bool IncrementalSortPlan::IsOrderedBy(
    const std::vector<Expression>& expressions,
    const std::vector<bool>& ascending) const {
  if (expressions.size() != ascending.size() ||
      expressions.size() != prefix_keys_.size() + suffix_keys_.size()) {
    return false;
  }
  size_t offset = 0;
  for (const std::vector<SortKey>* key : {&prefix_keys_, &suffix_keys_}) {
    for (const SortKey& sort_key : *key) {
      if (!expressions[offset] || !sort_key.expression ||
          expressions[offset]->ToString() != sort_key.expression->ToString() ||
          ascending[offset] != sort_key.ascending) {
        return false;
      }
      ++offset;
    }
  }
  return true;
}

bool IncrementalSortPlan::IsOrderedBy(
    const std::vector<Expression>& expressions,
    const std::vector<bool>& ascending,
    const std::vector<std::optional<bool>>& nulls_first) const {
  if (expressions.size() != ascending.size() ||
      expressions.size() != prefix_keys_.size() + suffix_keys_.size()) {
    return false;
  }
  size_t offset = 0;
  for (const std::vector<SortKey>* key : {&prefix_keys_, &suffix_keys_}) {
    for (const SortKey& sort_key : *key) {
      if (!expressions[offset] || !sort_key.expression ||
          expressions[offset]->ToString() != sort_key.expression->ToString() ||
          ascending[offset] != sort_key.ascending) {
        return false;
      }
      const bool provided = sort_key.nulls_first.value_or(sort_key.ascending);
      const bool requested =
          offset < nulls_first.size()
              ? nulls_first[offset].value_or(ascending[offset])
              : ascending[offset];
      if (provided != requested) {
        return false;
      }
      ++offset;
    }
  }
  return true;
}

void IncrementalSortPlan::Dump(std::ostream& output, int indent) const {
  output << Indent(static_cast<size_t>(indent)) << ToString() << "\n";
  DumpChild(output, *child_, indent + 2);
}

std::string IncrementalSortPlan::ToString() const {
  std::string result = "IncrementalSort";
  auto append_keys = [&](std::string_view label, const auto& keys) {
    result += " " + std::string(label) + "=";
    for (size_t i = 0; i < keys.size(); ++i) {
      if (i != 0) {
        result += ",";
      }
      result += keys[i].expression ? keys[i].expression->ToString() : "<null>";
      result += keys[i].ascending ? " ASC" : " DESC";
      if (keys[i].nulls_first.has_value()) {
        result += *keys[i].nulls_first ? " NULLS FIRST" : " NULLS LAST";
      }
      if (keys[i].collation == Collation::kCaseInsensitive) {
        result += " COLLATE und:ci";
      }
    }
  };
  if (!prefix_keys_.empty()) {
    append_keys("presorted", prefix_keys_);
  }
  if (!suffix_keys_.empty()) {
    append_keys("sorting", suffix_keys_);
  }
  return result;
}

}  // namespace tinylamb
