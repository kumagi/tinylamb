/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_QUERY_GROUPED_PIPELINE_LOWERING_HPP
#define TINYLAMB_QUERY_GROUPED_PIPELINE_LOWERING_HPP

#include <functional>
#include <optional>
#include <string>

#include "common/status_or.hpp"
#include "database/transaction_context.hpp"
#include "executor/detail/pipeline/pipeline_ir.hpp"
#include "expression/expression.hpp"
#include "plan/plan.hpp"
#include "query/statement.hpp"

namespace tinylamb {

// Attempts to lower an aggregated / grouped SELECT statement executed on top of
// a Cascades FROM+WHERE `core_plan` into a purely out-of-core `PipelineGraph`
// (`OocGroupAggSink` -> optional `FilterOp(HAVING)` -> `ProjectOp` -> optional
// `OocDistinctSink` -> optional `OocSortSink`/`LimitOp` -> `ProjectOp`).
//
// Returns `std::nullopt` when the statement relies on complex relational
// features not yet in the P3a pipeline (representative-row ungrouped columns,
// window functions, QUALIFY, WITH TIES, DISTINCT ON, correlated subqueries in
// SELECT/HAVING/GROUP BY, or non-standard aggregate modifiers) so callers can
// fall back cleanly to `GroupByPlan` / `RelationalPlan`.
[[nodiscard]] StatusOr<std::optional<PipelineGraph>>
TryLowerGroupedStatementToPipeline(
    const SelectStatement& select, const Plan& core_plan,
    TransactionContext& ctx,
    std::function<std::string()> explain_text_producer = {},
    const std::function<Expression(const Expression&)>& resolve_alias = {});

}  // namespace tinylamb

#endif  // TINYLAMB_QUERY_GROUPED_PIPELINE_LOWERING_HPP
