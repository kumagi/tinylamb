import TinylambOpt.ResearchGoals.ResourcePolicies.MaintenanceDebt
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace Nov26.Proof_MaintenanceDebt_NonIdleServiceBatch
open TinylambOpt.ResearchGoals.ResourcePolicies.MaintenanceDebt NonIdleServiceBatch
theorem sound : Goal := by
  intro a b start h
  simp [endDebt, step] at *
  omega
end Nov26.Proof_MaintenanceDebt_NonIdleServiceBatch
