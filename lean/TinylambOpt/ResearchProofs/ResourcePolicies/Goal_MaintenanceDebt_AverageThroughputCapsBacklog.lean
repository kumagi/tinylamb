import TinylambOpt.ResearchGoals.ResourcePolicies.MaintenanceDebt
set_option linter.unusedSectionVars false

/-! Proof/counterexample for `TinylambOpt.ResearchGoals.ResourcePolicies.MaintenanceDebt.AverageThroughputCapsBacklog.Goal`. The theorem below certifies the resource-policy claim (or refutes the strong hypothesis) in the model defined by that module, preserving the modeled result. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_MaintenanceDebt_AverageThroughputCapsBacklog
open TinylambOpt.ResearchGoals.ResourcePolicies.MaintenanceDebt

/--
Refutation: a sustainable average write rate does not bound the peak backlog
by a single service quantum. Trace ⟨2,1⟩,⟨2,1⟩,⟨0,1⟩,⟨0,1⟩ with quantum 1
satisfies every capacity and average-rate premise, yet the backlog peaks at 2.
-/
theorem refute : ¬ AverageThroughputCapsBacklog.Goal := by
  intro h
  have key : ¬ (high 0 ([⟨2,1⟩,⟨2,1⟩,⟨0,1⟩,⟨0,1⟩] : List Tick) ≤ (1 : Nat)) := by native_decide
  have hcap : ∀ t ∈ ([⟨2,1⟩,⟨2,1⟩,⟨0,1⟩,⟨0,1⟩] : List Tick), t.capacity = 1 := by native_decide
  have hsum : (([⟨2,1⟩,⟨2,1⟩,⟨0,1⟩,⟨0,1⟩] : List Tick).map Tick.added).sum ≤
      List.length ([⟨2,1⟩,⟨2,1⟩,⟨0,1⟩,⟨0,1⟩] : List Tick) * 1 := by native_decide
  exact key (h _ 1 hcap hsum)

end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_MaintenanceDebt_AverageThroughputCapsBacklog
