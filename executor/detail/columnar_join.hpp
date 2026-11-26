/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_EXECUTOR_DETAIL_COLUMNAR_JOIN_HPP
#define TINYLAMB_EXECUTOR_DETAIL_COLUMNAR_JOIN_HPP

#include "executor/detail/planning_heuristics.hpp"

namespace tinylamb::relational_detail {

inline constexpr size_t kParallelProbeMaxWorkers = 16;

bool ColumnWithConstantBias(const Expression& expression, ColumnName* column,
                            int64_t* bias);

StatusOr<std::optional<Relation>> TryColumnarCrossOrNullJoin(
    TransactionContext& context, Relation& left, Relation& right,
    bool want_left_nulls, bool want_right_nulls, const Schema& result_schema,
    const std::vector<Expression>& residual,
    std::shared_ptr<const std::vector<std::string>> using_columns,
    const Relation& stats_seed);

StatusOr<std::optional<Relation>> TryColumnarHashJoin(
    TransactionContext& context, Relation& left, Relation& right, bool flip,
    bool want_left_nulls, bool want_right_nulls, bool has_null_safe_key,
    bool has_using_residuals, const std::vector<slot_t>& left_columns,
    const std::vector<slot_t>& right_columns,
    const std::vector<int64_t>& left_key_offsets,
    const std::vector<int64_t>& right_key_offsets,
    const std::vector<Expression>& residual,
    const std::optional<std::vector<BytecodeProgram>>& compiled_residuals,
    std::shared_ptr<const std::vector<std::string>> using_columns,
    const Relation& stats_seed);

}  // namespace tinylamb::relational_detail

#endif  // TINYLAMB_EXECUTOR_DETAIL_COLUMNAR_JOIN_HPP
