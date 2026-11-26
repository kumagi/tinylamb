/-
Scratch proof for `NonIdleServiceBatch.Goal`. When a service tick has no idle
time, its unused-credit subtraction does not truncate, so two consecutive
ticks can be merged into one tick with summed arrivals and capacities without
changing the resulting debt. This justifies batching maintenance work as long
as the first tick was busy.
-/
import TinylambOpt.ResearchGoals.ResourcePolicies.MaintenanceDebt

/-!
Proof contribution for ResourcePolicies_MaintenanceDebt_NonIdleServiceBatch. The theorem below
records a planning or execution decision that preserves the modeled
SQL result, or an explicit counterexample when the proposed rewrite is
unsound.
-/


set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.ResourcePolicies.MaintenanceDebt.Goal_NonIdleServiceBatch

open TinylambOpt.ResearchGoals.ResourcePolicies.MaintenanceDebt


/-- Truncated subtraction is associative here because the first service does not idle. -/
theorem sound : NonIdleServiceBatch.Goal := by
  intro a b start hnonidle
  simp [endDebt, step]
  omega

end TinylambOpt.ResearchProofs.ResourcePolicies.MaintenanceDebt.Goal_NonIdleServiceBatch
