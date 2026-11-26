import TinylambOpt.ResearchGoals.ResourcePolicies.MergeScheduling

namespace TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.MergeScheduling.Goal_RowShrinkPredictsByteShrink

open TinylambOpt.ResearchGoals.ResourcePolicies.MergeScheduling


/-! Refutation of `RowShrinkPredictsByteShrink`: a row-count reduction does not
    imply a byte-count reduction when discarded rows are narrower than kept rows. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

open TinylambOpt.ResearchGoals.ResourcePolicies.MergeScheduling

theorem refute : ¬ TinylambOpt.ResearchGoals.ResourcePolicies.MergeScheduling.RowShrinkPredictsByteShrink.Goal := by
  intro h
  -- kept = [100], rest = [0]: row counts 1 vs 2 pass 5*1 <= 4*2,
  -- but byte sums 100 vs 100 fail 5*100 <= 4*100.
  have H := h [100] [0] (by native_decide)
  have hnot : ¬ 5 * [100].sum ≤ 4 * ([100] ++ [0]).sum := by native_decide
  exact hnot H
end TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.MergeScheduling.Goal_RowShrinkPredictsByteShrink
