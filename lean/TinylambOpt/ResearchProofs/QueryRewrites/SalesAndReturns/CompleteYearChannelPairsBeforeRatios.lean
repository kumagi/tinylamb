import TinylambOpt.ResearchGoals.QueryRewrites.SalesAndReturns

/-!
# CompleteYearChannelPairsBeforeRatios

Only customers with both channels and both years can enter the modeled ratio comparison.
The executor can check state presence before dividing.
-/


set_option maxHeartbeats 0
namespace TinylambOpt.ResearchProofs.QueryRewrites.SalesAndReturns.CompleteYearChannelPairsBeforeRatios
open TinylambOpt.ResearchGoals.QueryRewrites
open TinylambOpt.ResearchGoals.QueryRewrites.SalesAndReturns

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.SalesAndReturns.CompleteYearChannelPairsBeforeRatios.Goal := by
  intro s1 s2 w1 w2 k
  dsimp only [members]
  simp [List.mem_eraseDups, Bool.and_assoc, Bool.and_left_comm, Bool.and_comm]

end TinylambOpt.ResearchProofs.QueryRewrites.SalesAndReturns.CompleteYearChannelPairsBeforeRatios

#print axioms TinylambOpt.ResearchProofs.QueryRewrites.SalesAndReturns.CompleteYearChannelPairsBeforeRatios.sound
