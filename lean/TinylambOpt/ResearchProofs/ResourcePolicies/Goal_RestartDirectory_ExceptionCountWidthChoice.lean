import TinylambOpt.ResearchGoals.ResourcePolicies.RestartDirectory
set_option linter.unnecessarySimpa false
set_option linter.deprecated false
set_option linter.unusedVariables false
set_option linter.unusedSectionVars false

/-! Proof/counterexample for `TinylambOpt.ResearchGoals.ResourcePolicies.RestartDirectory.ExceptionCountWidthChoice.Goal`. The theorem below certifies the resource-policy claim (or refutes the strong hypothesis) in the model defined by that module, preserving the modeled result. -/
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_RestartDirectory_ExceptionCountWidthChoice
open TinylambOpt.ResearchGoals.ResourcePolicies.RestartDirectory
theorem refute : ¬ ExceptionCountWidthChoice.Goal := by
  intro h
  let a : List Nat := [8, 0, 0, 0, 0, 0, 0, 0]
  let b : List Nat := [127, 0, 0, 0, 0, 0, 0, 0]
  have h' := h 3 a b (by decide) (by decide)
  revert h'
  decide
end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_RestartDirectory_ExceptionCountWidthChoice
