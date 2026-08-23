/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_PLAN_IMPLEMENTATION_RULES_HPP
#define TINYLAMB_PLAN_IMPLEMENTATION_RULES_HPP

#include "plan/cascades.hpp"

namespace tinylamb {

// Default physical implementation rules (Phase 4): scans, joins and the
// root-layer operators read their filter / condition payloads from the memo
// and their catalog data from the explicit RuleContext (D4) instead of a
// thread-local closure.
const cascades::ImplementationRuleSet& DefaultImplementationRules();

}  // namespace tinylamb

#endif  // TINYLAMB_PLAN_IMPLEMENTATION_RULES_HPP
