import TinylambOpt.ResearchGoals.ResourcePolicies.GrantAdmission
set_option linter.unnecessarySimpa false
set_option linter.deprecated false
set_option linter.unusedVariables false
set_option linter.unusedSectionVars false

/-! Proof/counterexample for `TinylambOpt.ResearchGoals.ResourcePolicies.GrantAdmission.WorkerPeaksAreSimultaneous.Goal`. The theorem below certifies the resource-policy claim (or refutes the strong hypothesis) in the model defined by that module, preserving the modeled result. -/
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_GrantAdmission_WorkerPeaksAreSimultaneous
open TinylambOpt.ResearchGoals.ResourcePolicies.GrantAdmission WorkerPeaksAreSimultaneous
theorem refute : ¬ Goal := by
  intro h
  have := h [(1, 0), (0, 1)]
  simp at this
end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_GrantAdmission_WorkerPeaksAreSimultaneous
