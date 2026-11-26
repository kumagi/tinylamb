import TinylambOpt.ResearchGoals.ResourcePolicies.MergeScheduling

namespace TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.MergeScheduling.Goal_RankTileByteBudget

open TinylambOpt.ResearchGoals.ResourcePolicies.MergeScheduling


/-! Refutation of `RankTileByteBudget`: an average-per-size bound does not
    control individual rank tiles, which may be much larger than the average. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

open TinylambOpt.ResearchGoals.ResourcePolicies.MergeScheduling

theorem refute : ¬ TinylambOpt.ResearchGoals.ResourcePolicies.MergeScheduling.RankTileByteBudget.Goal := by
  intro h
  -- sizes = [0,100], unit = 50: total 100 <= 2*50, but tile 1 at index 1 needs 100 > 50.
  have H := h [0, 100] 1 50 (by native_decide) 1
  have hnot : ¬ (((List.drop 1 [0, 100]).take 1).sum ≤ 1 * 50) := by native_decide
  exact hnot H
end TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.MergeScheduling.Goal_RankTileByteBudget
