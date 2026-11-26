import TinylambOpt.ResearchGoals.ResourcePolicies.SpillExtents

namespace TinylambOpt.ResearchProofs.ResourcePolicies.SpillExtents.LiveTotalPredictsRelocationPeak

open TinylambOpt.ResearchGoals.ResourcePolicies.SpillExtents

theorem sound : Not LiveTotalPredictsRelocationPeak.Goal := by
  intro h
  have := h [3, 0] [1, 2] (by simp)
  simp at this

end TinylambOpt.ResearchProofs.ResourcePolicies.SpillExtents.LiveTotalPredictsRelocationPeak
