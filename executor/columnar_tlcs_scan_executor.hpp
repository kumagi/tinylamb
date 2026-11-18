/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_EXECUTOR_COLUMNAR_TLCS_SCAN_EXECUTOR_HPP
#define TINYLAMB_EXECUTOR_COLUMNAR_TLCS_SCAN_EXECUTOR_HPP

#include <cstddef>
#include <optional>
#include <vector>

#include "common/status_or.hpp"
#include "executor/detail/pipeline/ooc_sinks.hpp"
#include "executor/executor_base.hpp"
#include "expression/expression.hpp"
#include "plan/plan.hpp"

namespace tinylamb {
class TransactionContext;

// Attempts to lower a single-table plain scan / filter / ORDER BY / LIMIT
// plan directly to a lazy parallel columnar TLCS executor (the columnar TLCS
// scan executor). Returns nullptr when the plan shape or transaction
// snapshot state is outside the columnar fast path.
StatusOr<Executor> TryCreateDirectColumnarScanOrTopNExecutor(
    const Plan& plan, TransactionContext& ctx, bool distinct,
    const std::vector<Expression>& order_expressions,
    const std::vector<bool>& order_ascending,
    const std::vector<std::optional<bool>>& order_nulls_first,
    const std::vector<OocSortSink::KeySpec>& sort_keys, bool has_limit,
    size_t limit, size_t offset, bool with_ties, size_t visible_columns,
    size_t final_select_size, bool keep_plan_explain_text);

}  // namespace tinylamb

#endif  // TINYLAMB_EXECUTOR_COLUMNAR_TLCS_SCAN_EXECUTOR_HPP
