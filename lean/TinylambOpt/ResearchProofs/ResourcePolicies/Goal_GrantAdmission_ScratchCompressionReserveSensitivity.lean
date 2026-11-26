import TinylambOpt.ResearchGoals.ResourcePolicies.GrantAdmission
set_option linter.unnecessarySimpa false
set_option linter.deprecated false
set_option linter.unusedVariables false
set_option linter.unusedSectionVars false

/-! Proof/counterexample for `TinylambOpt.ResearchGoals.ResourcePolicies.GrantAdmission.ScratchCompressionReserveSensitivity.Goal`. The theorem below certifies the resource-policy claim (or refutes the strong hypothesis) in the model defined by that module, preserving the modeled result. -/
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_GrantAdmission_ScratchCompressionReserveSensitivity
open TinylambOpt.ResearchGoals.ResourcePolicies.GrantAdmission ScratchCompressionReserveSensitivity
theorem sound : Goal := by
  intro jobs cut
  induction jobs with
  | nil => simp [headroom]
  | cons j js ih =>
    simp only [headroom, List.map_cons]
    have := ih
    omega
end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_GrantAdmission_ScratchCompressionReserveSensitivity
