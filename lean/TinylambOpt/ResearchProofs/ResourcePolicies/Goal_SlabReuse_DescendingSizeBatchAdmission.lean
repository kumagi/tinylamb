import TinylambOpt.ResearchGoals.ResourcePolicies.SlabReuse

/-!
Refutes `DescendingSizeBatchAdmission`: sorting a feasible batch by descending
request size can destroy its first-fit admission. Holes `[4, 3]` admit the batch
`[2, 2, 3]` in its original order, while the descending order `[3, 2, 2]` fails:
the 3 takes the first hole, leaving two holes of size 1 for the last request.
Thus descending-size placement must not replace a known feasible grant order.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_SlabReuse_DescendingSizeBatchAdmission

open TinylambOpt.ResearchGoals.ResourcePolicies.SlabReuse

theorem refute : ¬ DescendingSizeBatchAdmission.Goal := by
  intro h
  have hfin := h [4, 3] [2, 2, 3] [2, 2, 3] (by decide) (by native_decide)
  have key : (place [4, 3] ([2, 2, 3].mergeSort (fun a b => decide (b ≤ a)))).isSome = false := by
    native_decide
  rw [key] at hfin
  cases hfin

end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_SlabReuse_DescendingSizeBatchAdmission
