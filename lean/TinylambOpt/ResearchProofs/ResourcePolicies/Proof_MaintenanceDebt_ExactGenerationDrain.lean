/-
Scratch proof for `ExactGenerationDrain.Goal`. A bounded drain must remove an
entry only at its exact (slot, generation) identity. Filtering out an older
generation therefore leaves every entry of a different generation in place,
which is what allows a newer dirty generation to survive the drain.
-/
import TinylambOpt.ResearchGoals.ResourcePolicies.MaintenanceDebt

/-!
Proof contribution for ResourcePolicies_MaintenanceDebt_ExactGenerationDrain. The theorem below
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

namespace TinylambOpt.ResearchProofs.ResourcePolicies.MaintenanceDebt.Goal_ExactGenerationDrain

open TinylambOpt.ResearchGoals.ResourcePolicies.MaintenanceDebt


/-- Different generations cannot be conflated by the filter. -/
theorem sound : ExactGenerationDrain.Goal := by
  intro pending slot oldGen newGen hne
  apply List.count_filter
  have hne' : newGen ≠ oldGen := fun h => hne h.symm
  simp [hne']

end TinylambOpt.ResearchProofs.ResourcePolicies.MaintenanceDebt.Goal_ExactGenerationDrain
