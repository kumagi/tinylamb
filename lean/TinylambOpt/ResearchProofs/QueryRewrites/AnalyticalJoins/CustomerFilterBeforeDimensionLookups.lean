import TinylambOpt.ResearchGoals.QueryRewrites.AnalyticalJoins

/-!
# CustomerFilterBeforeDimensionLookups

Applying the customer filter first preserves the qualifying sales and their store and
product matches. The executor can skip those dimension probes for rejected sales.
-/


set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.QueryRewrites.AnalyticalJoins.CustomerFilterBeforeDimensionLookups

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.AnalyticalJoins.CustomerFilterBeforeDimensionLookups.Goal := by
  intro sales dimRows eligible
  dsimp only
  induction sales with
  | nil => simp
  | cons k ks ih =>
      by_cases h : eligible k > 0
      · simp [h, ih]
      · have hz : eligible k = 0 := by omega
        simp [h, hz, ih]

end TinylambOpt.ResearchProofs.QueryRewrites.AnalyticalJoins.CustomerFilterBeforeDimensionLookups

#print axioms TinylambOpt.ResearchProofs.QueryRewrites.AnalyticalJoins.CustomerFilterBeforeDimensionLookups.sound
