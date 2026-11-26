import TinylambOpt.ResearchGoals.QueryRewrites.EncodedScanAggregates

namespace TinylambOpt.ResearchProofs.QueryRewrites.EncodedScanAggregates.Goal_FrameResidualOrderedAverageFold

open TinylambOpt.ResearchGoals.QueryRewrites.EncodedScanAggregates


/-!
Fusing frame reconstruction into an accumulator step does not change the
order-sensitive state: folding residuals with `step acc (base + d)` is the same
as folding the decoded values. SQL may therefore decode inside the aggregate
loop without materializing an intermediate column.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

theorem sound :
    TinylambOpt.ResearchGoals.QueryRewrites.EncodedScanAggregates.FrameResidualOrderedAverageFold.Goal := by
  intro deltas base step initial
  induction deltas generalizing initial with
  | nil => rfl
  | cons d ds ih =>
      simpa using ih (step initial (base + d))
end TinylambOpt.ResearchProofs.QueryRewrites.EncodedScanAggregates.Goal_FrameResidualOrderedAverageFold
