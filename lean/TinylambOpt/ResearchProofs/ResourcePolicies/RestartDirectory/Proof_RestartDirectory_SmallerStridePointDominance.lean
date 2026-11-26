import TinylambOpt.ResearchGoals.ResourcePolicies.RestartDirectory
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace Nov26.Proof_RestartDirectory_SmallerStridePointDominance
open TinylambOpt.ResearchGoals.ResourcePolicies.RestartDirectory
theorem refute : ¬ SmallerStridePointDominance.Goal := by
  intro h
  have h' := h 5 6 12 (by decide) (by decide)
  simp [anchor] at h'
end Nov26.Proof_RestartDirectory_SmallerStridePointDominance
