import TinylambOpt.ResearchGoals.ResourcePolicies.MergeScheduling

namespace TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.MergeScheduling.Goal_SiblingExcessPeakOrder

open TinylambOpt.ResearchGoals.ResourcePolicies.MergeScheduling


/-! Soundness of `SiblingExcessPeakOrder`: putting the sibling with the larger
    excess peak first cannot increase the temporary-file high-water mark. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

open TinylambOpt.ResearchGoals.ResourcePolicies.MergeScheduling

theorem sound : TinylambOpt.ResearchGoals.ResourcePolicies.MergeScheduling.SiblingExcessPeakOrder.Goal := by
  intro a b h
  simp only [TinylambOpt.ResearchGoals.ResourcePolicies.MergeScheduling.SiblingExcessPeakOrder.Goal,
    peak, temp] at *
  omega
end TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.MergeScheduling.Goal_SiblingExcessPeakOrder
