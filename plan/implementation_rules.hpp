/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_PLAN_IMPLEMENTATION_RULES_HPP
#define TINYLAMB_PLAN_IMPLEMENTATION_RULES_HPP

#include <vector>

#include "common/status_or.hpp"
#include "expression/named_expression.hpp"
#include "plan/cascades.hpp"
#include "query/query_data.hpp"

namespace tinylamb {

// Default physical implementation rules (Phase 4): scans, joins and the
// root-layer operators read their filter / condition payloads from the memo
// and their catalog data from the explicit RuleContext (D4) instead of a
// thread-local closure.
const cascades::ImplementationRuleSet& DefaultImplementationRules();

// Exact one-entry MIN/MAX optimization. All other single-table statements
// participate in the same logical and physical rule search as joins.
StatusOr<Plan> OptimizeMinMaxIndex(
    const QueryData& query, const Expression& predicate,
    const std::vector<NamedExpression>& projection_items,
    const cascades::RuleContext& context);

}  // namespace tinylamb

#endif  // TINYLAMB_PLAN_IMPLEMENTATION_RULES_HPP
