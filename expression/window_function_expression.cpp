/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "expression/window_function_expression.hpp"

#include <cstddef>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/string_util.hpp"
#include "expression/expression.hpp"
#include "type/column_name.hpp"
#include "type/type.hpp"
#include "type/value.hpp"

namespace tinylamb {

namespace {
std::string OrderTermsToString(const std::vector<WindowOrderTerm>& terms) {
  std::string out;
  for (const auto& term : terms) {
    if (!out.empty()) {
      out += ", ";
    }
    out += term.expression ? term.expression->ToString() : std::string("?");
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
                          : "? PRECEDING";
    case WindowFrameBoundType::kCurrentRow:
      return "CURRENT ROW";
    case WindowFrameBoundType::kOffsetFollowing:
      return bound.offset ? bound.offset->ToString() + " FOLLOWING"
                          : "? FOLLOWING";
    case WindowFrameBoundType::kUnboundedFollowing:
      return "UNBOUNDED FOLLOWING";
  }
  return "?";
}
}  // namespace

StatusOr<Value> WindowFunctionCallExpression::TryEvaluate(
    const Row& /*row*/, const Schema& /*schema*/) const {
  return StatusError(StatusCode::kRuntimeError,
                     "window function " + function +
                         " evaluated without pre-computation (internal error)");
}

Value WindowFunctionCallExpression::Evaluate(const Row& row,
                                             const Schema& schema) const {
  return ExcShimUnwrap(TryEvaluate(row, schema),
                       "WindowFunctionCallExpression::Evaluate");
}

Type WindowFunctionCallExpression::ResultType(const Schema& schema) const {
  // The executor still fixes the hidden column type from the computed values,
  // but consumers that read the expression's static type ahead of execution
  // (UNION coercion, ORDER BY encoding, server type reporting) must not see a
  // blanket VARCHAR: ROW_NUMBER yields INT64, PERCENT_RANK a DOUBLE, and
  // LAG/FIRST_VALUE carry their argument's type.
  const std::string fn = AsciiToUpper(function);
  if (fn == "ROW_NUMBER" || fn == "RANK" || fn == "DENSE_RANK" ||
      fn == "NTILE" || fn == "COUNT" || fn == "COUNTIF") {
    return {TypeTag::kBigInt};
  }
  if (fn == "PERCENT_RANK" || fn == "CUME_DIST" || fn == "PERCENTILE_CONT" ||
      fn == "AVG" || fn == "STDDEV_SAMP" || fn == "STDDEV_POP" ||
      fn == "VAR_SAMP" || fn == "VAR_POP" || fn == "COVAR_SAMP" ||
      fn == "COVAR_POP" || fn == "CORR") {
    return {TypeTag::kDouble};
  }
  // Navigation functions and value aggregates carry the first argument's
  // type. STRING_AGG (and unrecognized functions) keep the VARCHAR fallback.
  if (fn == "LAG" || fn == "LEAD" || fn == "FIRST_VALUE" ||
      fn == "LAST_VALUE" || fn == "NTH_VALUE" || fn == "MIN" || fn == "MAX" ||
      fn == "SUM" || fn == "ANY_VALUE" || fn == "PERCENTILE_DISC" ||
      fn == "ARRAY_AGG") {
    if (!args.empty() && args.front()) {
      return args.front()->ResultType(schema);
    }
  }
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
  for (const Expression& argument : args) {
    collect(argument);
  }
  if (where_filter) {
    collect(where_filter);
  }
  for (const auto& term : inner_order_by) {
    collect(term.expression);
  }
  for (const Expression& expression : partition_by) {
    collect(expression);
  }
  for (const auto& term : order_by) {
    collect(term.expression);
  }
  if (frame_start.offset) {
    collect(frame_start.offset);
  }
  if (frame_end.offset) {
    collect(frame_end.offset);
  }
  return columns;
}

std::string WindowFunctionCallExpression::ToString() const {
  std::ostringstream out;
  out << function << "(";
  if (distinct) {
    out << "DISTINCT ";
  }
  for (size_t i = 0; i < args.size(); ++i) {
    if (i != 0U) {
      out << ", ";
    }
    // Hand-built / degenerate nodes may carry a null argument; TouchedColumns
    // already skips those. Render a placeholder instead of dereferencing it
    // so EXPLAIN / dump / rewrite identity keys cannot segfault.
    out << (args[i] ? args[i]->ToString() : std::string("?"));
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
      if (i != 0U) {
        out << ", ";
      }
      out << (partition_by[i] ? partition_by[i]->ToString() : std::string("?"));
    }
    out << " ";
  }
  if (!order_by.empty()) {
    out << "ORDER BY " << OrderTermsToString(order_by) << " ";
  }
  if (has_frame) {
    const char* unit = frame_unit == WindowFrameUnit::kRange    ? "RANGE"
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

Expression WindowFunctionCallExp(std::string function,
                                 std::vector<Expression> args,
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
