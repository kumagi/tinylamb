import TinylambOpt.ResearchGoals.ResourcePolicies.RestartDirectory
set_option linter.unnecessarySimpa false
set_option linter.deprecated false
set_option linter.unusedVariables false
set_option linter.unusedSectionVars false

/-! Proof/counterexample for `TinylambOpt.ResearchGoals.ResourcePolicies.RestartDirectory.PatchedWidthLocalMinimum.Goal`. The theorem below certifies the resource-policy claim (or refutes the strong hypothesis) in the model defined by that module, preserving the modeled result. -/
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_RestartDirectory_PatchedWidthLocalMinimum
open TinylambOpt.ResearchGoals.ResourcePolicies.RestartDirectory
theorem refute : ¬ PatchedWidthLocalMinimum.Goal := by
  intro h
  let xs : List Nat := [0, 0, 129]
  have h' := h xs 8 (by decide) (by decide) (by decide) 0
  revert h'
  decide
end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_RestartDirectory_PatchedWidthLocalMinimum
