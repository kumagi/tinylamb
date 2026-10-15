/** Copyright 2026 KUMAZAKI Hiroki. Licensed under the Apache-2.0 license. */
#include "sort_plan.hpp"

#include <cstddef>
#include <optional>
#include <ostream>
#include <sstream>
#include <string>
#include <vector>

#include "common/constants.hpp"
#include "expression/expression.hpp"
#include "expression/parameter_slot.hpp"

namespace tinylamb {

bool SortPlan::IsOrderedBy(const std::vector<Expression>& expressions,
                           const std::vector<bool>& ascending) const {
  if (expressions.size() > keys_.size() ||
      ascending.size() != expressions.size()) {
    return false;
  }
  for (size_t i = 0; i < expressions.size(); ++i) {
    if (keys_[i].ascending != ascending[i] ||
        keys_[i].expression->ToString() != expressions[i]->ToString()) {
      return false;
    }
  }
  return true;
}

bool SortPlan::IsOrderedBy(
    const std::vector<Expression>& expressions,
    const std::vector<bool>& ascending,
    const std::vector<std::optional<bool>>& nulls_first) const {
  if (expressions.size() > keys_.size() ||
      ascending.size() != expressions.size()) {
    return false;
  }
  for (size_t i = 0; i < expressions.size(); ++i) {
    if (keys_[i].ascending != ascending[i] ||
        keys_[i].expression->ToString() != expressions[i]->ToString()) {
      return false;
    }
    // Unspecified sides resolve to the engine default (NULLS FIRST for ASC,
    // NULLS LAST for DESC); anything else must match exactly.
    const bool provided = keys_[i].nulls_first.value_or(keys_[i].ascending);
    const bool requested = i < nulls_first.size()
                               ? nulls_first[i].value_or(ascending[i])
                               : ascending[i];
    if (provided != requested) {
      return false;
    }
  }
  return true;
}

Collation SortPlan::OrderCollation() const {
  // A sort compares its keys left to right, so the delivered ordering is
  // case-insensitive as soon as one key asks for it (CollationOfOrdering's
  // rule). Reporting the resolved collation — not the first key's — keeps
  // the answer correct for the composite keys the memo pushes down.
  Collation resolved = Collation::kNone;
  for (const SortKey& key : keys_) {
    resolved = ResolveCollation(resolved, key.collation);
  }
  return resolved;
}

std::string SortPlan::KeysToString() const {
  std::ostringstream output;
  for (size_t i = 0; i < keys_.size(); ++i) {
    if (i > 0) {
      output << ", ";
    }
    output << keys_[i].expression->ToString()
           << (keys_[i].ascending ? " ASC" : " DESC");
  }
  return output.str();
}

std::string SortPlan::MemorySuffix() const { return {}; }

void SortPlan::Dump(std::ostream& output, int indent) const {
  output << Indent(static_cast<size_t>(indent)) << StrategyName() << ": ["
         << KeysToString() << "]" << MemorySuffix() << "\n"
         << Indent(static_cast<size_t>(indent) + 2);
  child_->Dump(output, indent + 2);
}

std::string SortPlan::ToString() const {
  return StrategyName() + ": [" + KeysToString() + "]" + MemorySuffix();
}

std::string BufferedSortPlan::MemorySuffix() const {
  std::ostringstream output;
  output << " (buffered, budget=" << Memory().memory_budget_bytes
         << "B, rows=" << RowCapacity() << ")";
  return output.str();
}

std::string ExternalMergeSortPlan::MemorySuffix() const {
  std::ostringstream output;
  output << " (spill runs, budget=" << Memory().memory_budget_bytes
         << "B, rows=" << SpillThresholdRows()
         << ", window=" << MergeWindowRows() << ")";
  return output.str();
}

Plan MakeSortPlan(Plan child, std::vector<SortKey> keys, double estimated_rows,
                  const SortMemoryPolicy& memory) {
  if (SortStrategyForRows(estimated_rows, memory) == SortStrategy::kBuffered) {
    return std::make_shared<BufferedSortPlan>(std::move(child), std::move(keys),
                                              memory);
  }
  return std::make_shared<ExternalMergeSortPlan>(std::move(child),
                                                 std::move(keys), memory);
}

bool SortPlan::ContainsParameterSlots() const {
  if (child_->ContainsParameterSlots()) {
    return true;
  }
  return std::ranges::any_of(keys_, [](const SortKey& key) {
    return ContainsParameterSlot(key.expression);
  });
}

bool SortPlan::InstallParameterSlots(const std::vector<Value>& parameters,
                                     size_t* cursor,
                                     std::vector<Expression>* slots) {
  if (!child_->InstallParameterSlots(parameters, cursor, slots)) {
    return false;
  }
  for (SortKey& key : keys_) {
    if (!SlotizeInPlace(&key.expression, parameters, cursor, slots)) {
      return false;
    }
  }
  return true;
}

}  // namespace tinylamb
