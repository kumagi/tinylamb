import TinylambOpt.ResearchGoals.ResourcePolicies.MaintenanceDebt
set_option linter.unusedSectionVars false

/-! Proof/counterexample for `TinylambOpt.ResearchGoals.ResourcePolicies.MaintenanceDebt.SharedMaintenancePeak.Goal`. The theorem below certifies the resource-policy claim (or refutes the strong hypothesis) in the model defined by that module, preserving the modeled result. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_MaintenanceDebt_SharedMaintenancePeak
open TinylambOpt.ResearchGoals.ResourcePolicies.MaintenanceDebt

/--
Refutation: provisioning combined flush+compaction activity at the larger of the
independent peaks is unsound. For a = b = [⟨8,4⟩,⟨0,4⟩] each trace peaks at 4,
but the tickwise-combined trace [⟨16,8⟩,⟨0,8⟩] peaks at 8.
-/
theorem refute : ¬ SharedMaintenancePeak.Goal := by
  intro h
  have key : ¬ (high 0 (List.zipWith (fun (x y : Tick) => Tick.mk (x.added + y.added) (x.capacity + y.capacity))
      ([⟨8,4⟩,⟨0,4⟩] : List Tick) [⟨8,4⟩,⟨0,4⟩]) ≤
      max (high 0 ([⟨8,4⟩,⟨0,4⟩] : List Tick)) (high 0 [⟨8,4⟩,⟨0,4⟩])) := by
    native_decide
  exact key (h [⟨8,4⟩,⟨0,4⟩] [⟨8,4⟩,⟨0,4⟩] rfl)

end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_MaintenanceDebt_SharedMaintenancePeak
