/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "plan/incremental_sort_plan.hpp"

#include <ostream>
#include <utility>

#include "common/constants.hpp"

namespace tinylamb {

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
  output << Indent(indent) << ToString() << "\n";
  child_->Dump(output, indent + 2);
}

std::string IncrementalSortPlan::ToString() const {
  std::string result = "IncrementalSort";
  if (!prefix_keys_.empty() && prefix_keys_.front().expression) {
    result += " presorted=" + prefix_keys_.front().expression->ToString();
  }
  return result;
}

}  // namespace tinylamb
