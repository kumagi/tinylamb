import TinylambOpt.ResearchGoals.QueryRewrites.SalesAndReturns

/-!
# RunLengthShippingDelayHistogram

Replacing runs of equal date pairs with their lengths preserves the counts in the
shipping-delay buckets.

The executor can compute each run's delay once and add the run length to its bucket.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false


namespace TinylambOpt.ResearchProofs.QueryRewrites.SalesAndReturns.RunLengthShippingDelayHistogram
open TinylambOpt.ResearchGoals.QueryRewrites
open TinylambOpt.ResearchGoals.QueryRewrites
open TinylambOpt.ResearchGoals.QueryRewrites.SalesAndReturns

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.SalesAndReturns.RunLengthShippingDelayHistogram.Goal := by
  intro runs bin
  dsimp only
  simp only [countWhere]
  rw [← List.countP_eq_length_filter]
  rw [List.countP_flatMap]
  induction runs with
  | nil => simp
  | cons r rs ih =>
    simp only [Function.comp_apply, List.map_cons, List.sum_cons]
    rw [List.countP_replicate]
    by_cases h : bucket [30, 60, 90, 120] (r.1.2 - r.1.1) == bin
    · simp [h, ih]
    · simp [h, ih]
end TinylambOpt.ResearchProofs.QueryRewrites.SalesAndReturns.RunLengthShippingDelayHistogram
