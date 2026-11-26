import TinylambOpt.ResearchGoals.ResourcePolicies.GrantAdmission
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace Nov26.Proof_GrantAdmission_WorkerPeaksAreSimultaneous
open TinylambOpt.ResearchGoals.ResourcePolicies.GrantAdmission WorkerPeaksAreSimultaneous
theorem refute : ¬ Goal := by
  intro h
  have := h [(1, 0), (0, 1)]
  simp at this
end Nov26.Proof_GrantAdmission_WorkerPeaksAreSimultaneous
