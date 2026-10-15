/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "distinct_plan.hpp"

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <optional>
#include <ostream>
#include <string>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "expression/expression.hpp"
#include "expression/named_expression.hpp"
#include "plan/plan.hpp"
#include "plan/skip_scan_distinct_plan.hpp"
#include "table/table.hpp"
#include "table/table_statistics.hpp"
#include "type/schema.hpp"

namespace tinylamb {

TableStatistics EstimateDistinctPlanStats(const Plan& child) {
  TableStatistics stats = child->GetStats();
  if (stats.Rows() == 0 || child->GetSchema().ColumnCount() == 0) {
    return stats;
  }
  std::vector<size_t> columns;
  columns.reserve(child->GetSchema().ColumnCount());
  for (size_t i = 0; i < child->GetSchema().ColumnCount(); ++i) {
    columns.push_back(i);
  }
  const auto output_rows =
      static_cast<size_t>(std::ceil(stats.EstimateDistinctRows(columns)));
  return stats.ScaleToRows(std::min(output_rows, stats.Rows()));
}

DistinctPlan::DistinctPlan(Plan child, std::vector<Expression> distinct_on)
    : child_(std::move(child)),
      distinct_on_(std::move(distinct_on)),
      stats_(distinct_on_.empty() ? EstimateDistinctPlanStats(child_)
                                  : child_->GetStats()) {}

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
  // The covering scan supplies statistics in output-column order.
  return static_cast<size_t>(std::ceil(stats_.EstimateDistinctRows({0})));
}

bool SkipScanDistinctPlan::IsOrderedBy(
    const std::vector<Expression>& expressions,
    const std::vector<bool>& ascending) const {
  return IsOrderedBy(expressions, ascending, {});
}

bool SkipScanDistinctPlan::IsOrderedBy(
    const std::vector<Expression>& expressions,
    const std::vector<bool>& ascending,
    const std::vector<std::optional<bool>>& nulls_first) const {
  if (!ascending_ || expressions.size() != select_items_.size() ||
      ascending.size() != expressions.size()) {
    return false;
  }
  for (size_t i = 0; i < expressions.size(); ++i) {
    if (!ascending[i] || !expressions[i] || !select_items_[i].expression) {
      return false;
    }
    const bool requested =
        i < nulls_first.size() ? nulls_first[i].value_or(true) : true;
    if (!requested) {
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
