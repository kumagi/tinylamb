import TinylambOpt.ResearchGoals.ResourcePolicies.MergeScheduling

namespace TinylambOpt.ResearchProofs.ResourcePolicies.MergeScheduling.SiblingExcessPeakOrder

open TinylambOpt.ResearchGoals.ResourcePolicies.MergeScheduling

theorem goal : SiblingExcessPeakOrder.Goal := by
  intro a b h
  simp only [SiblingExcessPeakOrder.Goal, peak, temp, size] at *
  omega

end TinylambOpt.ResearchProofs.ResourcePolicies.MergeScheduling.SiblingExcessPeakOrder
