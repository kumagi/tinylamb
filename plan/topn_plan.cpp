/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "topn_plan.hpp"

#include <algorithm>
#include <cstddef>
#include <limits>
#include <optional>
#include <ostream>
#include <sstream>
#include <string>
#include <vector>

#include "common/constants.hpp"
#include "expression/expression.hpp"
#include "expression/parameter_slot.hpp"

namespace tinylamb {

size_t TopNPlan::EmitRowCount() const {
  // LIMIT 0 yields zero rows: TopNExecutor never emits for an empty bound,
  // so the estimate must agree (the engine reads limit==0 as a real bound
  // here, unlike the unordered-limit paths that treat 0 as "unbounded").
  // WITH TIES cannot rescue an empty bound either: ties extend only the
  // rows inside the bound, and with 0 rows there is no boundary row to tie
  // against (TopNExecutor::Materialize returns before any tie logic).
  if (limit_ == 0) {
    return 0;
  }
  // The executor skips prior_offset_ rows in an outer LIMIT node plus this
  // node's own offset_. EnforcesLimit() already accounts for both; the row
  // estimate must too, or cost/spill decisions see too many output rows.
  if (offset_ > std::numeric_limits<size_t>::max() - prior_offset_) {
    return 0;
  }
  const size_t skip = prior_offset_ + offset_;
  const size_t input = child_->EmitRowCount();
  if (input <= skip) {
    return 0;
  }
  const size_t remaining = input - skip;
  if (with_ties_) {
    return remaining;
  }
  return std::min(remaining, limit_);
}

bool TopNPlan::IsOrderedBy(const std::vector<Expression>& expressions,
                           const std::vector<bool>& ascending) const {
  if (expressions.size() > keys_.size() ||
      ascending.size() != expressions.size()) {
    return false;
  }
  for (size_t i = 0; i < expressions.size(); ++i) {
    if (!keys_[i].expression || !expressions[i] ||
        keys_[i].ascending != ascending[i] ||
        keys_[i].expression->ToString() != expressions[i]->ToString()) {
      return false;
    }
  }
  return true;
}

bool TopNPlan::IsOrderedBy(
    const std::vector<Expression>& expressions,
    const std::vector<bool>& ascending,
    const std::vector<std::optional<bool>>& nulls_first) const {
  if (expressions.size() > keys_.size() ||
      ascending.size() != expressions.size()) {
    return false;
  }
  for (size_t i = 0; i < expressions.size(); ++i) {
    if (keys_[i].ascending != ascending[i] || !keys_[i].expression ||
        !expressions[i] ||
        keys_[i].expression->ToString() != expressions[i]->ToString()) {
      return false;
    }
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

Collation TopNPlan::OrderCollation() const {
  Collation resolved = Collation::kNone;
  for (const TopNKey& key : keys_) {
    resolved = ResolveCollation(resolved, key.collation);
  }
  return resolved;
}

void TopNPlan::Dump(std::ostream& output, int indent) const {
  output << Indent(static_cast<size_t>(indent)) << ToString() << "\n"
         << Indent(static_cast<size_t>(indent) + 2);
  DumpChild(output, *child_, indent + 2);
}

std::string TopNPlan::ToString() const {
  std::ostringstream output;
  output << "TopN (limit=" << limit_ << ", offset=" << offset_
         << (with_ties_ ? ", with ties" : "") << ") [";
  for (size_t i = 0; i < keys_.size(); ++i) {
    if (i > 0) {
      output << ", ";
    }
    output << (keys_[i].expression ? keys_[i].expression->ToString()
                                   : "<missing expression>")
           << (keys_[i].ascending ? " ASC" : " DESC");
    // T2668: mirror SortPlan::KeysToString -- explicit NULLS placement is
    // part of the order contract IsOrderedBy checks, so it belongs in the
    // render too.
    const auto nulls_first = keys_[i].nulls_first;
    if (nulls_first.has_value()) {
      output << (*nulls_first ? " NULLS FIRST" : " NULLS LAST");
    }
  }
  output << "]";
  return output.str();
}

bool TopNPlan::ContainsParameterSlots() const {
  if (child_->ContainsParameterSlots()) {
    return true;
  }
  return std::ranges::any_of(keys_, [](const TopNKey& key) {
    return ContainsParameterSlot(key.expression);
  });
}

bool TopNPlan::InstallParameterSlots(const std::vector<Value>& parameters,
                                     size_t* cursor,
                                     std::vector<Expression>* slots) {
  if (!child_->InstallParameterSlots(parameters, cursor, slots)) {
    return false;
  }
  for (TopNKey& key : keys_) {
    if (!CollectParameterSlots(&key.expression, slots)) {
      return false;
    }
  }
  return true;
}

}  // namespace tinylamb
