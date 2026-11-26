import TinylambOpt.ResearchGoals.ResourcePolicies.SpillExtents

namespace TinylambOpt.ResearchProofs.ResourcePolicies.SpillExtents.LocalDirectoryTruncateFence

open TinylambOpt.ResearchGoals.ResourcePolicies.SpillExtents

theorem sound : Not LocalDirectoryTruncateFence.Goal := by
  intro h
  have := h [[5], [1]] [1] (by simp)
  simp [roof] at this

end TinylambOpt.ResearchProofs.ResourcePolicies.SpillExtents.LocalDirectoryTruncateFence
