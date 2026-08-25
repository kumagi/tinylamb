/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_WINDOW_EVAL_HPP
#define TINYLAMB_WINDOW_EVAL_HPP

#include <cstddef>

#include "executor/detail/relation.hpp"
#include "executor/detail/subquery_runtime.hpp"
#include "query/statement.hpp"

namespace tinylamb {
namespace relational_detail {

// True when any select-list / order-by / qualify expression of the statement
// contains a window function call.
bool HasWindowFunctions(const SelectStatement& statement);

struct WindowedInput {
  Relation input;                            // rows extended with $winK columns
  std::shared_ptr<SelectStatement> statement;  // rewritten copy
  size_t hidden_columns{0};                  // trailing $win column count
};

// Pre-computes every window-function call in the statement into hidden
// `$winN` columns appended to the input rows, and rewrites the statement so
// those calls become plain column references.  Must run after WHERE filtering
// and before projection (SQL: WHERE -> window -> QUALIFY -> ORDER BY).
WindowedInput ApplyWindows(TransactionContext& context,
                           const SelectStatement& statement, Relation&& input,
                           const Scope* outer, const CteMap& ctes);

// Drops the trailing $winN columns produced by ApplyWindows.
Relation TrimHiddenColumns(Relation&& input, size_t hidden_columns);

}  // namespace relational_detail
}  // namespace tinylamb

#endif  // TINYLAMB_WINDOW_EVAL_HPP
