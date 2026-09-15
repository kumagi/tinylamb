/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_WINDOW_EVAL_HPP
#define TINYLAMB_WINDOW_EVAL_HPP

#include <cstddef>
#include <vector>

#include "executor/detail/relation.hpp"
#include "executor/detail/subquery_runtime.hpp"
#include "expression/window_function_expression.hpp"
#include "query/statement.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"

namespace tinylamb::relational_detail {

// True when any select-list / order-by / qualify expression of the statement
// contains a window function call.
bool HasWindowFunctions(const SelectStatement& statement);

struct WindowedInput {
  Relation input;  // rows extended with $winK columns
  std::shared_ptr<SelectStatement> statement;  // rewritten copy
  size_t hidden_columns{0};                    // trailing $win column count
};

// Pre-computes every window-function call in the statement into hidden
// `$winN` columns appended to the input rows, and rewrites the statement so
// those calls become plain column references.  Must run after WHERE filtering
// and before projection (SQL: WHERE -> window -> QUALIFY -> ORDER BY).
StatusOr<WindowedInput> ApplyWindows(TransactionContext& context,
                                     const SelectStatement& statement,
                                     Relation&& input, const Scope* outer,
                                     const CteMap& ctes);

// Single-window entry point for the Cascades WindowExecutor (same evaluator,
// one call at a time; window clauses without subquery scope pass null/empty
// outer and CTE maps).
Status ComputeOneWindow(TransactionContext& context,
                        const WindowFunctionCallExpression& window,
                        std::vector<Row>& rows, const Schema& schema,
                        const Scope* outer, const CteMap& ctes,
                        std::vector<Value>* out);

}  // namespace tinylamb::relational_detail

#endif  // TINYLAMB_WINDOW_EVAL_HPP
