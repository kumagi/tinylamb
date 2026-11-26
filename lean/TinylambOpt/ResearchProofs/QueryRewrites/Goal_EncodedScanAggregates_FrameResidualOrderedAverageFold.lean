import TinylambOpt.ResearchGoals.QueryRewrites.EncodedScanAggregates

set_option linter.unusedSimpArgs false
set_option linter.deprecated false


/-!
# FrameResidualOrderedAverageFold

Fusing frame reconstruction with the same cast and accumulator step preserves
even an order-sensitive average state.
-/

namespace TinylambOpt.ResearchProofs.QueryRewrites.EncodedScanAggregates.Goal_FrameResidualOrderedAverageFold

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.EncodedScanAggregates.FrameResidualOrderedAverageFold.Goal := by
  intro deltas base step initial
  induction deltas generalizing initial with
  | nil => simp
  | cons d ds ih =>
      simp [ih]


end TinylambOpt.ResearchProofs.QueryRewrites.EncodedScanAggregates.Goal_FrameResidualOrderedAverageFold
