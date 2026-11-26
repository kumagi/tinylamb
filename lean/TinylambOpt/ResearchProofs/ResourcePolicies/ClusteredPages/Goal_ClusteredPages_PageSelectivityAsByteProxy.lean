import TinylambOpt.ResearchGoals.ResourcePolicies.ClusteredPages
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_ClusteredPages_PageSelectivityAsByteProxy
open TinylambOpt.ResearchGoals.ResourcePolicies.ClusteredPages PageSelectivityAsByteProxy

theorem refute : ¬ Goal := by
  intro h
  have := h [⟨[1], 10⟩] [⟨[1], 1⟩, ⟨[1], 1⟩] 1 1 (by native_decide)
  simp [readBytes, candidate] at this
end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_ClusteredPages_PageSelectivityAsByteProxy
