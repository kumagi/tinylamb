/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <vector>

#include "common/status_or.hpp"
#include "database/transaction_context.hpp"
#include "executor/detail/planning_heuristics.hpp"
#include "executor/detail/relation.hpp"
#include "executor/detail/subquery_runtime.hpp"
#include "query/statement.hpp"
#include "type/schema.hpp"

namespace tinylamb::relational_detail {

StatusOr<Relation> LateralExpandRelation(TransactionContext& context,
                                         const SelectSource& source,
                                         Relation& prefix, const Scope* outer,
                                         const CteMap& ctes);

std::shared_ptr<SelectStatement> RestrictUnionInput(
    TransactionContext& context, const SelectStatement& parent, size_t target,
    const CteMap& ctes);

StatusOr<std::optional<Relation>> TryUnmatchedLeftJoin(
    Relation& left, const Relation& right, const SelectSource& source,
    slot_t null_column, bool emit_right_columns);

StatusOr<std::optional<Relation>> TryMaterializedStarJoin(
    std::vector<Relation>& relations, const std::vector<uint8_t>& absorbed,
    const std::vector<PredicateInfo>& predicates,
    const SelectStatement& statement, bool columnar_output = false);

void ReduceUnmatchedOuterProbes(TransactionContext& context,
                                const SelectStatement& statement,
                                std::vector<Relation>& relations,
                                const std::vector<PredicateInfo>& predicates,
                                const CteMap& ctes);

}  // namespace tinylamb::relational_detail
