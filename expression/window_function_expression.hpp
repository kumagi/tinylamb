/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_WINDOW_FUNCTION_EXPRESSION_HPP
#define TINYLAMB_WINDOW_FUNCTION_EXPRESSION_HPP

#include <memory>
#include <optional>
#include <string>
#include <unordered_set>
#include <vector>

#include "expression/expression.hpp"

namespace tinylamb {

enum class WindowFrameUnit { kDefault, kRows, kRange, kGroups };

enum class WindowFrameExclusion { kNone, kCurrentRow, kGroup, kTies };

enum class WindowFrameBoundType {
  kUnboundedPreceding,
  kOffsetPreceding,
  kCurrentRow,
  kOffsetFollowing,
  kUnboundedFollowing,
};

struct WindowFrameBound {
  WindowFrameBoundType type{WindowFrameBoundType::kUnboundedPreceding};
  Expression offset;
};

// An ORDER BY term inside a window specification or aggregate call.
// `nulls_first` refines the engine default (NULLS FIRST on ASC, NULLS LAST on
// DESC) when the SQL spelled out NULLS FIRST / NULLS LAST.
struct WindowOrderTerm {
  Expression expression;
  bool ascending{true};
  std::optional<bool> nulls_first;
};

// An analytic (window) function call: `SUM(x) OVER (PARTITION BY p ORDER BY o
// ROWS BETWEEN ... AND ...)`.  Window functions need whole-partition context,
// so they are never evaluated row-at-a-time; the relational executor
// pre-computes one hidden column per call (executor/detail/window_eval) and
// rewrites these nodes into references to those columns.
class WindowFunctionCallExpression : public ExpressionBase {
 public:
  std::string function;
  std::vector<Expression> args;
  Expression where_filter;  // AGG(x WHERE cond) OVER (...)
  bool distinct{false};
  // ORDER BY written inside the aggregate call itself, e.g.
  // ARRAY_AGG(x ORDER BY y): ordering applied while the window aggregates.
  std::vector<WindowOrderTerm> inner_order_by;
  std::optional<size_t> inner_limit;
  std::vector<Expression> partition_by;
  std::vector<WindowOrderTerm> order_by;
  WindowFrameUnit frame_unit{WindowFrameUnit::kDefault};
  WindowFrameBound frame_start;
  WindowFrameBound frame_end;
  bool has_frame{false};
  WindowFrameExclusion exclusion{WindowFrameExclusion::kNone};

  WindowFunctionCallExpression() = default;
  [[nodiscard]] TypeTag Type() const override {
    return TypeTag::kWindowFunctionExp;
  }
  [[nodiscard]] Value Evaluate(const Row& row,
                               const Schema& schema) const override;
  [[nodiscard]] tinylamb::Type ResultType(
      const Schema& schema) const override;
  [[nodiscard]] std::unordered_set<ColumnName> TouchedColumns() const override;
  [[nodiscard]] std::string ToString() const override;
  void Dump(std::ostream& o) const override { o << ToString(); }
};

Expression WindowFunctionCallExp(
    std::string function, std::vector<Expression> args,
    std::vector<Expression> partition_by,
    std::vector<WindowOrderTerm> order_by);

}  // namespace tinylamb

#endif  // TINYLAMB_WINDOW_FUNCTION_EXPRESSION_HPP
