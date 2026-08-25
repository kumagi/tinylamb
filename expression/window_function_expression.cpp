/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "expression/window_function_expression.hpp"

#include <sstream>
#include <stdexcept>
#include <utility>

namespace tinylamb {

namespace {
std::string OrderTermsToString(const std::vector<WindowOrderTerm>& terms) {
  std::string out;
  for (const auto& term : terms) {
    if (!out.empty()) { out += ", "; }
    out += term.expression->ToString();
    out += term.ascending ? " ASC" : " DESC";
    if (term.nulls_first.has_value()) {
      out += *term.nulls_first ? " NULLS FIRST" : " NULLS LAST";
    }
  }
  return out;
}

std::string FrameBoundToString(const WindowFrameBound& bound) {
  switch (bound.type) {
    case WindowFrameBoundType::kUnboundedPreceding:
      return "UNBOUNDED PRECEDING";
    case WindowFrameBoundType::kOffsetPreceding:
      return bound.offset ? bound.offset->ToString() + " PRECEDING"
                          : "OFFSET PRECEDING";
    case WindowFrameBoundType::kCurrentRow:
      return "CURRENT ROW";
    case WindowFrameBoundType::kOffsetFollowing:
      return bound.offset ? bound.offset->ToString() + " FOLLOWING"
                          : "OFFSET FOLLOWING";
    case WindowFrameBoundType::kUnboundedFollowing:
      return "UNBOUNDED FOLLOWING";
  }
  return "?";
}
}  // namespace

Value WindowFunctionCallExpression::Evaluate(const Row&,
                                              const Schema&) const {
  throw std::runtime_error(
      "window function " + function +
      " evaluated without pre-computation (internal error)");
}

Type WindowFunctionCallExpression::ResultType(const Schema&) const {
  // The executor fixes the hidden column type from the computed values.
  return {TypeTag::kVarChar};
}

std::unordered_set<ColumnName> WindowFunctionCallExpression::TouchedColumns()
    const {
  std::unordered_set<ColumnName> columns;
  auto collect = [&columns](const Expression& expression) {
    if (expression) {
      for (const auto& column : expression->TouchedColumns()) {
        columns.insert(column);
      }
    }
  };
  for (const Expression& argument : args) { collect(argument); }
  if (where_filter) { collect(where_filter); }
  for (const auto& term : inner_order_by) { collect(term.expression); }
  for (const Expression& expression : partition_by) { collect(expression); }
  for (const auto& term : order_by) { collect(term.expression); }
  if (frame_start.offset) { collect(frame_start.offset); }
  if (frame_end.offset) { collect(frame_end.offset); }
  return columns;
}

std::string WindowFunctionCallExpression::ToString() const {
  std::ostringstream out;
  out << function << "(";
  if (distinct) { out << "DISTINCT "; }
  for (size_t i = 0; i < args.size(); ++i) {
    if (i) { out << ", "; }
    out << args[i]->ToString();
  }
  if (!inner_order_by.empty()) {
    out << " ORDER BY " << OrderTermsToString(inner_order_by);
  }
  if (inner_limit.has_value()) {
    out << " LIMIT " << *inner_limit;
  }
  out << ") OVER (";
  if (!partition_by.empty()) {
    out << "PARTITION BY ";
    for (size_t i = 0; i < partition_by.size(); ++i) {
      if (i) { out << ", "; }
      out << partition_by[i]->ToString();
    }
    out << " ";
  }
  if (!order_by.empty()) {
    out << "ORDER BY " << OrderTermsToString(order_by) << " ";
  }
  if (has_frame) {
    const char* unit = frame_unit == WindowFrameUnit::kRange
                           ? "RANGE"
                           : frame_unit == WindowFrameUnit::kGroups ? "GROUPS"
                                                                    : "ROWS";
    out << unit << " BETWEEN " << FrameBoundToString(frame_start) << " AND "
        << FrameBoundToString(frame_end);
    switch (exclusion) {
      case WindowFrameExclusion::kCurrentRow:
        out << " EXCLUDE CURRENT ROW";
        break;
      case WindowFrameExclusion::kGroup:
        out << " EXCLUDE GROUP";
        break;
      case WindowFrameExclusion::kTies:
        out << " EXCLUDE TIES";
        break;
      case WindowFrameExclusion::kNone:
        break;
    }
  }
  out << ")";
  return out.str();
}

Expression WindowFunctionCallExp(
    std::string function, std::vector<Expression> args,
    std::vector<Expression> partition_by,
    std::vector<WindowOrderTerm> order_by) {
  auto node = std::make_shared<WindowFunctionCallExpression>();
  node->function = std::move(function);
  node->args = std::move(args);
  node->partition_by = std::move(partition_by);
  node->order_by = std::move(order_by);
  return node;
}

}  // namespace tinylamb
