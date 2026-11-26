import TinylambOpt.ResearchGoals.ResourcePolicies.SlabReuse
/-!
Registers the checked result for `SlabReuse.BestFitTraceDominance.Goal`.

The theorem below either proves the stated claim under its assumptions or
refutes it with an explicit counterexample; no `sorry` or new axioms are used.
-/
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_SlabReuse_BestFitTraceDominance
open TinylambOpt.ResearchGoals.ResourcePolicies.SlabReuse BestFitTraceDominance

theorem refute : ¬ Goal := by
  intro h
  have := h [3, 2] [1, 2, 2] (by native_decide)
  revert this
  native_decide
end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_SlabReuse_BestFitTraceDominance
