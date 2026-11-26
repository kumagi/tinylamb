import TinylambOpt.ResearchGoals.QueryRewrites.SalesAndReturns

/-!
# CustomerItemFilterBeforeQuantityRead

Only catalog quantity rows for item and customer keys demanded by ticket matches can
contribute. The executor can restrict those payload reads to the demanded keys.
-/


set_option maxHeartbeats 0
namespace TinylambOpt.ResearchProofs.QueryRewrites.SalesAndReturns.CustomerItemFilterBeforeQuantityRead
open TinylambOpt.ResearchGoals.QueryRewrites

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.SalesAndReturns.CustomerItemFilterBeforeQuantityRead.Goal := by
  intro catalog tickets
  induction catalog with
  | nil => simp [total]
  | cons r rest ih =>
    by_cases h : (tickets r.1 != 0) = true
    · simpa [total, h] using ih
    · have hz : tickets r.1 = 0 := by
        simp at h
        exact h
      simpa [total, h, hz] using ih

end TinylambOpt.ResearchProofs.QueryRewrites.SalesAndReturns.CustomerItemFilterBeforeQuantityRead

#print axioms TinylambOpt.ResearchProofs.QueryRewrites.SalesAndReturns.CustomerItemFilterBeforeQuantityRead.sound
