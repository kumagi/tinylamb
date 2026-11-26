import TinylambOpt.ResearchGoals.QueryRewrites.SalesAndReturns

/-!
# SharedWeekDateFilterAcrossChannels

A shared set of qualifying dates represents the modeled week filter across channels. The
executor can resolve that set once and apply it to each fact stream.
-/


set_option maxHeartbeats 0
namespace TinylambOpt.ResearchProofs.QueryRewrites.SalesAndReturns.SharedWeekDateFilterAcrossChannels

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.SalesAndReturns.SharedWeekDateFilterAcrossChannels.Goal := by
  intro days week date
  apply Bool.eq_iff_iff.mpr
  simp [List.any_eq_true]

end TinylambOpt.ResearchProofs.QueryRewrites.SalesAndReturns.SharedWeekDateFilterAcrossChannels

#print axioms TinylambOpt.ResearchProofs.QueryRewrites.SalesAndReturns.SharedWeekDateFilterAcrossChannels.sound
