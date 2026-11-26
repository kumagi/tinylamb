import TinylambOpt.ResearchGoals.ResourcePolicies.ScanCache
/-!
Registers the checked result for `ScanCache.GenerationBlindPrefetchFill.Goal`.

The theorem below either proves the stated claim under its assumptions or
refutes it with an explicit counterexample; no `sorry` or new axioms are used.
-/
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_ScanCache_GenerationBlindPrefetchFill
open TinylambOpt.ResearchGoals.ResourcePolicies.ScanCache GenerationBlindPrefetchFill

theorem refute : ¬ Goal := by
  intro h
  let readBlock : Request → List Nat := fun r => [r.generation]
  have := h readBlock 0 0 0 1 (by decide)
  simp [readBlock] at this
end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_ScanCache_GenerationBlindPrefetchFill
