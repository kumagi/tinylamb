import TinylambOpt.ResearchGoals.QueryRewrites.SalesAndReturns

/-!
# ExclusionByUnionOfChannels

The union of the two excluded name and date sets preserves the modeled set difference.
The executor can build one exclusion lookup for the store keys.
-/


set_option maxHeartbeats 0
namespace TinylambOpt.ResearchProofs.QueryRewrites.SalesAndReturns.ExclusionByUnionOfChannels
open TinylambOpt.ResearchGoals.QueryRewrites
open TinylambOpt.ResearchGoals.QueryRewrites.SalesAndReturns

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.SalesAndReturns.ExclusionByUnionOfChannels.Goal := by
  intro s c w
  simp [List.contains_append, Bool.and_comm]

end TinylambOpt.ResearchProofs.QueryRewrites.SalesAndReturns.ExclusionByUnionOfChannels

#print axioms TinylambOpt.ResearchProofs.QueryRewrites.SalesAndReturns.ExclusionByUnionOfChannels.sound
