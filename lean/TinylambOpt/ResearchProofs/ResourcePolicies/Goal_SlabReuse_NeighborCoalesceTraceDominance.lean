import TinylambOpt.ResearchGoals.ResourcePolicies.SlabReuse

/-!
Refutes `NeighborCoalesceTraceDominance`: merging adjacent free extents can
change which hole first-fit selects and lose a previously feasible trace. With
holes `[2, 2, 3]` the requests `[3, 2, 2]` fit, but after coalescing the first
two holes into `[4, 3]` the last request no longer fits.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_SlabReuse_NeighborCoalesceTraceDominance

open TinylambOpt.ResearchGoals.ResourcePolicies.SlabReuse

theorem refute : ¬ NeighborCoalesceTraceDominance.Goal := by
  intro h
  have hfin := h [] 2 2 [3] [3, 2, 2] (by native_decide)
  have key : (place ([4, 3] : List Nat) [3, 2, 2]).isSome = false := by native_decide
  change (place ([4, 3] : List Nat) [3, 2, 2]).isSome = true at hfin
  rw [key] at hfin
  cases hfin

end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_SlabReuse_NeighborCoalesceTraceDominance
