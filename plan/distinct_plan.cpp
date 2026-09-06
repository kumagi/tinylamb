/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "distinct_plan.hpp"

#include <cstddef>
#include <ostream>
#include <string>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "expression/expression.hpp"
#include "expression/named_expression.hpp"
#include "plan/plan.hpp"
#include "plan/skip_scan_distinct_plan.hpp"
#include "table/table_statistics.hpp"
#include "type/schema.hpp"

namespace tinylamb {

void DistinctPlan::Dump(std::ostream& output, int indent) const {
  output << Indent(static_cast<size_t>(indent)) << "HashDistinct";
  if (!distinct_on_.empty()) {
    output << " (on=[";
    for (size_t i = 0; i < distinct_on_.size(); ++i) {
      if (i > 0) {
        output << ", ";
      }
      output << *distinct_on_[i];
    }
    output << "])";
  }
  output << "\n";
  child_->Dump(output, indent + 2);
}

std::string DistinctPlan::ToString() const { return "HashDistinct"; }

// ===== SkipScanDistinctPlan =====

SkipScanDistinctPlan::SkipScanDistinctPlan(
    const Table& table, const Index& index, TableStatistics stats,
    bool ascending, std::vector<NamedExpression> select_items,
    Schema output_schema)
    : table_(table),
      index_(index),
      stats_(std::move(stats)),
      ascending_(ascending),
      select_items_(std::move(select_items)),
      output_schema_(std::move(output_schema)) {}

size_t SkipScanDistinctPlan::AccessRowCount() const { return stats_.Rows(); }

size_t SkipScanDistinctPlan::EmitRowCount() const {
  // One output row per distinct index-key value.
  const size_t distinct =
      stats_.Columns() == 0 ? 0 : stats_.Column(0).Distinct();
  return distinct == 0 ? 1 : distinct;
}

bool SkipScanDistinctPlan::IsOrderedBy(
    const std::vector<Expression>& expressions,
    const std::vector<bool>& ascending) const {
  // The skip scan walks the index in key order, so the output is ordered by
  // the (single) distinct column when the scan is ascending.
  if (!ascending_ || expressions.size() != select_items_.size()) {
    return false;
  }
  for (size_t i = 0; i < expressions.size(); ++i) {
    if (!ascending[i]) {
      return false;
    }
    if (expressions[i]->ToString() != select_items_[i].expression->ToString()) {
      return false;
    }
  }
  return true;
}

void SkipScanDistinctPlan::Dump(std::ostream& o, int indent) const {
  o << Indent(static_cast<size_t>(indent)) << ToString() << "\n";
}

std::string SkipScanDistinctPlan::ToString() const {
  return "SkipScanDistinct";
}

}  // namespace tinylamb
