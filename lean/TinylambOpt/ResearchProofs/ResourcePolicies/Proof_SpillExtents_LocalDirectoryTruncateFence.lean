import TinylambOpt.ResearchGoals.ResourcePolicies.SpillExtents

namespace TinylambOpt.ResearchProofs.ResourcePolicies.SpillExtents.Goal_LocalDirectoryTruncateFence

open TinylambOpt.ResearchGoals.ResourcePolicies.SpillExtents


/-! Scratch refutation: truncating from one recently touched segment ignores
    higher blocks retained by another segment. A global live-block scan (or an
    equivalent fence) is required before shrinking the backing file. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

open TinylambOpt.ResearchGoals.ResourcePolicies.SpillExtents

theorem refute : ¬ TinylambOpt.ResearchGoals.ResourcePolicies.SpillExtents.LocalDirectoryTruncateFence.Goal := by
  intro h
  have hbad := h [[0], [5]] [0] (by decide)
  exact (by decide : ¬ roof ([[0], [5]] : List (List Nat)).flatten ≤ roof [0]) hbad
end TinylambOpt.ResearchProofs.ResourcePolicies.SpillExtents.Goal_LocalDirectoryTruncateFence
