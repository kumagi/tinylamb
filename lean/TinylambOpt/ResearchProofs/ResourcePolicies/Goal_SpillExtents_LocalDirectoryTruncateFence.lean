import TinylambOpt.ResearchGoals.ResourcePolicies.SpillExtents
/-!
Registers the checked result for `SpillExtents.LocalDirectoryTruncateFence.Goal`.

The theorem below either proves the stated claim under its assumptions or
refutes it with an explicit counterexample; no `sorry` or new axioms are used.
-/
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_SpillExtents_LocalDirectoryTruncateFence
open TinylambOpt.ResearchGoals.ResourcePolicies.SpillExtents LocalDirectoryTruncateFence

theorem refute : ¬ Goal := by
  intro h
  have := h [[5], [1]] [1] (by simp)
  simp [roof] at this
end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_SpillExtents_LocalDirectoryTruncateFence
