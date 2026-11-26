import TinylambOpt.ResearchGoals.ResourcePolicies.WaitQueue

namespace TinylambOpt.ResearchProofs.ResourcePolicies.WaitQueue.BypassElapsedTimeBound

open TinylambOpt.ResearchGoals.ResourcePolicies.WaitQueue

theorem sound : Not BypassElapsedTimeBound.Goal := by
  intro h
  have := h [1] 5 (by simp)
  simp at this

end TinylambOpt.ResearchProofs.ResourcePolicies.WaitQueue.BypassElapsedTimeBound
