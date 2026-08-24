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

// Plans the common single-relation case without constructing and exploring a
// multi-group Cascades search.  It uses the same scan alternatives and cost
// model as the default implementation rules, then adds the root projection,
// aggregation and LIMIT layers directly.  The caller must already have
// rewritten and resolved the QueryData and populated RuleContext.
StatusOr<Plan> OptimizeSingleRelation(
    const QueryData& query, const Expression& predicate,
    const std::vector<NamedExpression>& projection_items, bool has_aggregate,
    const cascades::PhysicalProperties& required,
    const cascades::RuleContext& context);

}  // namespace tinylamb

#endif  // TINYLAMB_PLAN_IMPLEMENTATION_RULES_HPP
