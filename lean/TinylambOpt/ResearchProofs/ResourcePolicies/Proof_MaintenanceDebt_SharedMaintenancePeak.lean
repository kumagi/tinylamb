/-
Scratch proof for `SharedMaintenancePeak.Goal`. The claim proposes sizing a
combined maintenance stream by the larger of two independently observed
peaks. Two streams that each add one unit in a tick with no service show the
claim is false: their peaks add instead of nesting.
-/
import TinylambOpt.ResearchGoals.ResourcePolicies.MaintenanceDebt

/-!
Proof contribution for ResourcePolicies_MaintenanceDebt_SharedMaintenancePeak. The theorem below
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

namespace TinylambOpt.ResearchProofs.ResourcePolicies.MaintenanceDebt.Goal_SharedMaintenancePeak

open TinylambOpt.ResearchGoals.ResourcePolicies.MaintenanceDebt


/-- Counterexample: two peak arrivals in the same tick cannot both fit one peak. -/
theorem refute : ¬ SharedMaintenancePeak.Goal := by
  intro h
  let a : List Tick := [⟨1, 0⟩]
  let b : List Tick := [⟨1, 0⟩]
  have hlen : a.length = b.length := by simp [a, b]
  have hb := h a b hlen
  have hbad : ¬ high 0 (a.zipWith (fun x y => Tick.mk (x.added + y.added) (x.capacity + y.capacity)) b)
        ≤ max (high 0 a) (high 0 b) := by
    decide
  exact hbad hb

end TinylambOpt.ResearchProofs.ResourcePolicies.MaintenanceDebt.Goal_SharedMaintenancePeak
