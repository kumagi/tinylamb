import TinylambOpt.ResearchGoals.ResourcePolicies.MergeScheduling

namespace TinylambOpt.ResearchProofs.ResourcePolicies.MergeScheduling.WeightGuidedRotation

open TinylambOpt.ResearchGoals.ResourcePolicies.MergeScheduling

theorem goal : WeightGuidedRotation.Goal := by
  intro a b c
  simp only [WeightGuidedRotation.Goal, writes, size]
  omega

end TinylambOpt.ResearchProofs.ResourcePolicies.MergeScheduling.WeightGuidedRotation
