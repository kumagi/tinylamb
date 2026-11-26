import TinylambOpt.ResearchGoals.ResourcePolicies.MaintenanceDebt
set_option linter.unnecessarySimpa false
set_option linter.deprecated false
set_option linter.unusedVariables false
set_option linter.unusedSectionVars false

/-! Proof/counterexample for `TinylambOpt.ResearchGoals.ResourcePolicies.MaintenanceDebt.InitialBacklogSensitivity.Goal`. The theorem below certifies the resource-policy claim (or refutes the strong hypothesis) in the model defined by that module, preserving the modeled result. -/
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_MaintenanceDebt_InitialBacklogSensitivity
open TinylambOpt.ResearchGoals.ResourcePolicies.MaintenanceDebt InitialBacklogSensitivity
theorem sound : Goal := by
  intro ts
  induction ts with
  | nil =>
    intro a b hab
    simp [endDebt]
  | cons t ts ih =>
    intro a b hab
    simp only [endDebt, List.foldl_cons]
    have hstep : step a t ≤ step b t := by simp [step]; omega
    have hdiff : step b t - step a t ≤ b - a := by simp [step]; omega
    exact ih (step a t) (step b t) hstep |> fun h => Nat.le_trans h hdiff
end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_MaintenanceDebt_InitialBacklogSensitivity
