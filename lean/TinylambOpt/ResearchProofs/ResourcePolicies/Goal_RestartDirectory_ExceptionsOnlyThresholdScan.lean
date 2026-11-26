import TinylambOpt.ResearchGoals.ResourcePolicies.RestartDirectory
set_option linter.unnecessarySimpa false
set_option linter.deprecated false
set_option linter.unusedVariables false
set_option linter.unusedSectionVars false

/-! Proof/counterexample for `TinylambOpt.ResearchGoals.ResourcePolicies.RestartDirectory.ExceptionsOnlyThresholdScan.Goal`. The theorem below certifies the resource-policy claim (or refutes the strong hypothesis) in the model defined by that module, preserving the modeled result. -/
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_RestartDirectory_ExceptionsOnlyThresholdScan
open TinylambOpt.ResearchGoals.ResourcePolicies.RestartDirectory
theorem sound : ExceptionsOnlyThresholdScan.Goal := by
  intro w xs threshold hth
  unfold flagged
  rw [List.filter_filter]
  congr 1
  funext x
  by_cases hx : threshold ≤ x
  · simp [hx, Nat.le_trans hth hx]
  · simp [hx]
end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_RestartDirectory_ExceptionsOnlyThresholdScan
