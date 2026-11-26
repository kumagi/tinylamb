import TinylambOpt.ResearchGoals.ResourcePolicies.GrantAdmission
set_option linter.unusedSimpArgs false
set_option linter.unnecessarySimpa false
set_option linter.deprecated false
set_option linter.unusedVariables false
set_option linter.unusedSectionVars false

/-! Proof/counterexample for `TinylambOpt.ResearchGoals.ResourcePolicies.GrantAdmission.LargestHolderAdmission.Goal`. The theorem below certifies the resource-policy claim (or refutes the strong hypothesis) in the model defined by that module, preserving the modeled result. -/
set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_GrantAdmission_LargestHolderAdmission

open TinylambOpt.ResearchGoals.ResourcePolicies.GrantAdmission

theorem largestHolderAdmission_refute : ¬ LargestHolderAdmission.Goal := by
  intro h
  have := h [⟨7,1⟩, ⟨10,7⟩] [⟨7,1⟩, ⟨10,7⟩] (by simp [List.Perm])
  simp [headroom, List.mergeSort, List.merge] at this
