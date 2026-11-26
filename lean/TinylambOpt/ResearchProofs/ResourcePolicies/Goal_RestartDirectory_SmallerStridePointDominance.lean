import TinylambOpt.ResearchGoals.ResourcePolicies.RestartDirectory
set_option linter.unnecessarySimpa false
set_option linter.deprecated false
set_option linter.unusedVariables false
set_option linter.unusedSectionVars false

/-! Proof/counterexample for `TinylambOpt.ResearchGoals.ResourcePolicies.RestartDirectory.SmallerStridePointDominance.Goal`. The theorem below certifies the resource-policy claim (or refutes the strong hypothesis) in the model defined by that module, preserving the modeled result. -/
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_RestartDirectory_SmallerStridePointDominance
open TinylambOpt.ResearchGoals.ResourcePolicies.RestartDirectory
theorem refute : ¬ SmallerStridePointDominance.Goal := by
  intro h
  have h' := h 5 6 12 (by decide) (by decide)
  simp [anchor] at h'
end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_RestartDirectory_SmallerStridePointDominance
