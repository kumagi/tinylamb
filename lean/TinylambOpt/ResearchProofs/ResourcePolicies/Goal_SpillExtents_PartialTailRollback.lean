import TinylambOpt.ResearchGoals.ResourcePolicies.SpillExtents
/-!
Registers the checked result for `SpillExtents.PartialTailRollback.Goal`.

The theorem below either proves the stated claim under its assumptions or
refutes it with an explicit counterexample; no `sorry` or new axioms are used.
-/
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_SpillExtents_PartialTailRollback
open TinylambOpt.ResearchGoals.ResourcePolicies.SpillExtents PartialTailRollback

theorem sound : Goal := by
  intro old payload written
  simp [List.take_append]
end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_SpillExtents_PartialTailRollback
