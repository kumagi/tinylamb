/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_EXECUTOR_DETAIL_RELATIONAL_PROJECTION_HPP
#define TINYLAMB_EXECUTOR_DETAIL_RELATIONAL_PROJECTION_HPP

#include "executor/detail/subquery_runtime.hpp"

namespace tinylamb::relational_detail {

void RefineProjectionTypes(const Relation& output,
                           std::vector<Column>& columns);

bool IsKeyOnlyHavingConjunct(const Expression& conjunct,
                             const SelectStatement& resolved);

Expression InlineHavingAliases(const SelectStatement& statement,
                               const Schema& schema);

Expression ReplaceSubExpressions(
    const Expression& expression,
    const std::function<std::optional<Expression>(const Expression&)>& replace);

bool StatementIsAggregated(const SelectStatement& statement);

StatusOr<Relation> ExecuteWindowOverAggregate(
    TransactionContext& context, const SelectStatement& statement,
    Relation input, const Scope* outer, const CteMap& ctes, bool apply_where,
    bool build_grouped_input = false, bool columnar_output = false);

}  // namespace tinylamb::relational_detail

#endif  // TINYLAMB_EXECUTOR_DETAIL_RELATIONAL_PROJECTION_HPP
