import TinylambOpt.ResearchGoals.ResourcePolicies.SpillExtents
/-!
Registers the checked result for `SpillExtents.BunchedAppendBlockDebt.Goal`.

The theorem below either proves the stated claim under its assumptions or
refutes it with an explicit counterexample; no `sorry` or new axioms are used.
-/
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_SpillExtents_BunchedAppendBlockDebt
open TinylambOpt.ResearchGoals.ResourcePolicies.SpillExtents BunchedAppendBlockDebt

theorem sound : Goal := by
  intro u n a b hu
  have h1 : blocks u n ≤ blocks u (n + a) := by
    unfold blocks
    exact Nat.div_le_div_right (by omega)
  have h2 : blocks u (n + a) ≤ blocks u (n + a + b) := by
    unfold blocks
    exact Nat.div_le_div_right (by omega)
  omega
end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_SpillExtents_BunchedAppendBlockDebt
