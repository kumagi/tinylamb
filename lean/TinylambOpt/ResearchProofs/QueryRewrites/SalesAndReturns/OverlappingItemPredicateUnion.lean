import TinylambOpt.ResearchGoals.QueryRewrites.SalesAndReturns

/-!
# OverlappingItemPredicateUnion

Testing membership in the union of eligible item sets agrees with the disjunctive
predicate, including items that satisfy more than one branch.

The executor can form a single eligibility set and accumulate each qualifying fact row
once.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.QueryRewrites.SalesAndReturns.OverlappingItemPredicateUnion
open TinylambOpt.ResearchGoals.QueryRewrites

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.SalesAndReturns.OverlappingItemPredicateUnion.Goal := by
  intro facts a b price
  unfold total
  congr 1
  apply congrArg (List.map price)
  apply List.filter_congr
  intro k hk
  simp [List.contains_eq_mem]
end TinylambOpt.ResearchProofs.QueryRewrites.SalesAndReturns.OverlappingItemPredicateUnion
