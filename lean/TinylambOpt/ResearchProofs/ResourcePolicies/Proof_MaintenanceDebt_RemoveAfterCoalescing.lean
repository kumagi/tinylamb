/-
Scratch proof for `RemoveAfterCoalescing.Goal`. The claim says deleting a
finished id from a coalesced queue equals coalescing after deleting it from
the old queue. A fresh invalidation of the very id being deleted refutes
this: the finished generation may be removed, but the new invalidation must
stay pending.
-/
import TinylambOpt.ResearchGoals.ResourcePolicies.MaintenanceDebt

/-!
Proof contribution for ResourcePolicies_MaintenanceDebt_RemoveAfterCoalescing. The theorem below
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

namespace TinylambOpt.ResearchProofs.ResourcePolicies.MaintenanceDebt.Goal_RemoveAfterCoalescing

open TinylambOpt.ResearchGoals.ResourcePolicies.MaintenanceDebt


/-- Counterexample: a fresh invalidation survives removal of the old entry. -/
theorem refute : ¬ RemoveAfterCoalescing.Goal := by
  intro h
  have hb := h [0] [0] 0 (by simp)
  have hbad : ¬ (queue [0] [0]).erase 0 = queue ([0].erase 0) [0] := by
    decide
  exact hbad hb

end TinylambOpt.ResearchProofs.ResourcePolicies.MaintenanceDebt.Goal_RemoveAfterCoalescing
