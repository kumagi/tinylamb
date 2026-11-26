import TinylambOpt.ResearchGoals.ResourcePolicies.ScanCache
/-!
Registers the checked result for `ScanCache.CountReuseForByteCache.Goal`.

The theorem below either proves the stated claim under its assumptions or
refutes it with an explicit counterexample; no `sorry` or new axioms are used.
-/
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_ScanCache_CountReuseForByteCache
open TinylambOpt.ResearchGoals.ResourcePolicies.ScanCache CountReuseForByteCache

theorem refute : ¬ Goal := by
  intro h
  let bytes : Nat → Nat := fun i => if i == 0 then 1 else 10
  have := h bytes 2 0 [1] (by decide) (by decide) (by decide)
  revert this
  native_decide
end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_ScanCache_CountReuseForByteCache
