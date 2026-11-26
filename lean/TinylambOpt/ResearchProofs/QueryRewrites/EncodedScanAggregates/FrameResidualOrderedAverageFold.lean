import TinylambOpt.ResearchGoals.QueryRewrites.EncodedScanAggregates

/-!
# FrameResidualOrderedAverageFold

Fusing frame reconstruction with the same cast and accumulator step preserves
even an order-sensitive average state.
-/

namespace TinylambOpt.ResearchProofs.QueryRewrites.EncodedScanAggregates.FrameResidualOrderedAverageFold

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.EncodedScanAggregates.FrameResidualOrderedAverageFold.Goal := by
  intro deltas base step initial
  induction deltas generalizing initial with
  | nil => simp
  | cons d ds ih =>
      simp [ih]

#print axioms sound

end TinylambOpt.ResearchProofs.QueryRewrites.EncodedScanAggregates.FrameResidualOrderedAverageFold
