import TinylambOpt.ResearchGoals.ResourcePolicies.ScanCache
/-!
Registers the checked result for `ScanCache.FixedShardCacheMissParity.Goal`.

The theorem below either proves the stated claim under its assumptions or
refutes it with an explicit counterexample; no `sorry` or new axioms are used.
-/
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_ScanCache_FixedShardCacheMissParity
open TinylambOpt.ResearchGoals.ResourcePolicies.ScanCache FixedShardCacheMissParity

theorem refute : ¬ Goal := by
  intro h
  have := h [0, 2, 0, 2] 1
  simp [demandMisses, touch, fit, run] at this
end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_ScanCache_FixedShardCacheMissParity
