import TinylambOpt.ResearchGoals.ResourcePolicies.SpillExtents
/-!
Registers the checked result for `SpillExtents.LogicalHighWaterPaysFragmentation.Goal`.

The theorem below either proves the stated claim under its assumptions or
refutes it with an explicit counterexample; no `sorry` or new axioms are used.
-/
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_SpillExtents_LogicalHighWaterPaysFragmentation
open TinylambOpt.ResearchGoals.ResourcePolicies.SpillExtents LogicalHighWaterPaysFragmentation

theorem refute : ¬ Goal := by
  intro h
  have := h 2 [1, 1, 1, 1] 4 (by decide) (by native_decide)
  simp [tailCharge, blocks] at this
end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_SpillExtents_LogicalHighWaterPaysFragmentation
