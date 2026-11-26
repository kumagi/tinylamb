import TinylambOpt.ResearchGoals.QueryRewrites.SalesAndReturns

/-!
# OverlappingWeekFilterUnion

The union of weeks containing the selected dates preserves the return-date filter
without counting a shared week twice. The executor can reuse one eligibility set.
-/


set_option maxHeartbeats 0
namespace TinylambOpt.ResearchProofs.QueryRewrites.SalesAndReturns.OverlappingWeekFilterUnion
open TinylambOpt.ResearchGoals.QueryRewrites
open TinylambOpt.ResearchGoals.QueryRewrites.SalesAndReturns

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.SalesAndReturns.OverlappingWeekFilterUnion.Goal := by
  intro weeks returns
  simp [List.mem_eraseDups]

end TinylambOpt.ResearchProofs.QueryRewrites.SalesAndReturns.OverlappingWeekFilterUnion

#print axioms TinylambOpt.ResearchProofs.QueryRewrites.SalesAndReturns.OverlappingWeekFilterUnion.sound
