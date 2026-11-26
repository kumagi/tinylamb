import TinylambOpt.ResearchGoals.QueryRewrites.SalesAndReturns

/-!
# NullRejectingRefundOuterJoinReduction

The refund threshold rejects the NULL-extended rows produced by the modeled outer join.

The join can use the inner path when the refund predicate is a required condition and
rejects NULL under SQL semantics.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.QueryRewrites.SalesAndReturns.NullRejectingRefundOuterJoinReduction
open TinylambOpt.ResearchGoals.QueryRewrites
open TinylambOpt.ResearchGoals.QueryRewrites.SalesAndReturns

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.SalesAndReturns.NullRejectingRefundOuterJoinReduction.Goal := by
  intro sold refunds
  dsimp
  induction sold with
  | nil => rfl
  | cons k ks ih =>
    simp only [List.flatMap_cons, List.filter_append]
    rw [ih]
    by_cases h : (refunds k).isEmpty
    · have hkempty : refunds k = [] := List.isEmpty_iff.mp h
      simp [h, hkempty]
    · simp [h]
end TinylambOpt.ResearchProofs.QueryRewrites.SalesAndReturns.NullRejectingRefundOuterJoinReduction
