import TinylambOpt.ResearchGoals.ResourcePolicies.MergeScheduling

namespace TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.MergeScheduling.Goal_WeightGuidedRotation

open TinylambOpt.ResearchGoals.ResourcePolicies.MergeScheduling


/-! Soundness of `WeightGuidedRotation`: choosing the smaller outer run for the
    final merge is exactly the write-minimising choice for a three-run tree. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

open TinylambOpt.ResearchGoals.ResourcePolicies.MergeScheduling

theorem sound : TinylambOpt.ResearchGoals.ResourcePolicies.MergeScheduling.WeightGuidedRotation.Goal := by
  intro a b c
  simp only [TinylambOpt.ResearchGoals.ResourcePolicies.MergeScheduling.WeightGuidedRotation.Goal,
    writes, size]
  omega
end TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.MergeScheduling.Goal_WeightGuidedRotation
