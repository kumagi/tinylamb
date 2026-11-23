/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_WINDOW_EVAL_HPP
#define TINYLAMB_WINDOW_EVAL_HPP

#include <cstddef>
#include <optional>
#include <span>
#include <string>
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

// Hash of a PARTITION BY key tuple, consistent with the partition equality in
// the safe direction: equal tuples hash equally. VARCHAR components are folded
// under kCaseInsensitive when any component carries a CI tag before hashing
// (std::hash<Value> alone cannot fold a one-sided CI peer into the same
// bucket), then `std::hash<Value>` canonicalizes NULL, NaN and interval-shaped
// text like `ValuesEqual`, so a hash collision can never merge two partitions
// -- the equality decides -- while an equal pair always lands in one bucket.
// Shared by
// the in-memory grouping table and the spill-file router so the two can never
// disagree about where a partition lives.
size_t WindowPartitionKeyHash(const std::vector<Value>& key);

struct WindowedInput {
  Relation input;  // rows extended with $winK columns
  std::shared_ptr<SelectStatement> statement;  // rewritten copy
  size_t hidden_columns{0};                    // trailing $win column count
};

// Pre-computes every window-function call in the statement into hidden
// `$winN` columns appended to the input rows, and rewrites the statement so
// those calls become plain column references.  Must run after WHERE filtering
// and before projection (SQL: WHERE -> window -> QUALIFY -> ORDER BY).
// `allow_window_halo` enables bounded input trimming for compatible TopN and
// ROWS-window shapes; pass true only when WHERE has already been applied to
// the input. The default false keeps the complete input.
StatusOr<WindowedInput> ApplyWindows(TransactionContext& context,
                                     const SelectStatement& statement,
                                     Relation&& input, const Scope* outer,
                                     const CteMap& ctes,
                                     bool allow_window_halo = false);

// Single-window entry point for the Cascades WindowExecutor (same evaluator,
// one call at a time; window clauses without subquery scope pass null/empty
// outer and CTE maps).
Status ComputeOneWindow(TransactionContext& context,
                        const WindowFunctionCallExpression& window,
                        std::vector<Row>& rows, const Schema& schema,
                        const Scope* outer, const CteMap& ctes,
                        std::vector<Value>* out);

// Batch entry point: evaluates every call over the same `rows` in one pass.
// Calls whose PARTITION BY / ORDER BY shape AND layout-detail requirements
// (order_values / peer_end) agree share one built WindowOrderLayout, so N
// windows over the same partitioning no longer pay N regroup+stable-sort
// passes. outputs[i] receives one value per input row for windows[i].
Status ComputeWindowColumns(
    TransactionContext& context,
    const std::vector<const WindowFunctionCallExpression*>& windows,
    std::vector<Row>& rows, const Schema& schema, const Scope* outer,
    const CteMap& ctes, std::vector<std::vector<Value>>* outputs);

// Evaluates `win_nodes` directly over a `HasColumnarOnly()` relation without
// full row materialization, emitting a `HasColumnarOnly()` relation (or empty
// Relation when input has 0 rows) whose columns are `keep_base_slots` (or all
// base columns when `keep_base_slots` is `std::nullopt`) followed by `$win`
// columns named `win_names`.
StatusOr<Relation> ExtendColumnarRelationWithWindows(
    TransactionContext& context, Relation&& input,
    std::span<const WindowFunctionCallExpression* const> win_nodes,
    std::span<const std::string> win_names, const Scope* outer,
    const CteMap& ctes,
    std::optional<std::span<const size_t>> keep_base_slots = std::nullopt);

}  // namespace tinylamb::relational_detail

#endif  // TINYLAMB_WINDOW_EVAL_HPP
