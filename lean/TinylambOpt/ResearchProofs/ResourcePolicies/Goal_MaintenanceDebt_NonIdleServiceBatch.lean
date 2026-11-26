import TinylambOpt.ResearchGoals.ResourcePolicies.MaintenanceDebt
set_option linter.unnecessarySimpa false
set_option linter.deprecated false
set_option linter.unusedVariables false
set_option linter.unusedSectionVars false

/-! Proof/counterexample for `TinylambOpt.ResearchGoals.ResourcePolicies.MaintenanceDebt.NonIdleServiceBatch.Goal`. The theorem below certifies the resource-policy claim (or refutes the strong hypothesis) in the model defined by that module, preserving the modeled result. -/
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_MaintenanceDebt_NonIdleServiceBatch
open TinylambOpt.ResearchGoals.ResourcePolicies.MaintenanceDebt NonIdleServiceBatch
theorem sound : Goal := by
  intro a b start h
  simp [endDebt, step] at *
  omega
end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_MaintenanceDebt_NonIdleServiceBatch
