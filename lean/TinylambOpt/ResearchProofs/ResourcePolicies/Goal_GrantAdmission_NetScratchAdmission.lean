import TinylambOpt.ResearchGoals.ResourcePolicies.GrantAdmission
set_option linter.unnecessarySimpa false
set_option linter.deprecated false
set_option linter.unusedVariables false
set_option linter.unusedSectionVars false

/-! Proof/counterexample for `TinylambOpt.ResearchGoals.ResourcePolicies.GrantAdmission.NetScratchAdmission.Goal`. The theorem below certifies the resource-policy claim (or refutes the strong hypothesis) in the model defined by that module, preserving the modeled result. -/
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_GrantAdmission_NetScratchAdmission
open TinylambOpt.ResearchGoals.ResourcePolicies.GrantAdmission NetScratchAdmission
theorem refute : ¬ Goal := by
  intro h
  have := h [⟨0, 2⟩, ⟨10, 0⟩] 0 (by decide)
  simp [stalled] at this
end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_GrantAdmission_NetScratchAdmission
