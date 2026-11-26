import TinylambOpt.ResearchGoals.ResourcePolicies.MaintenanceDebt
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace Nov26.Proof_MaintenanceDebt_RemoveAfterCoalescing
open TinylambOpt.ResearchGoals.ResourcePolicies.MaintenanceDebt RemoveAfterCoalescing
theorem refute : ¬ Goal := by
  intro h
  have := h [1] [1] 1 (by simp)
  revert this
  native_decide
end Nov26.Proof_MaintenanceDebt_RemoveAfterCoalescing
