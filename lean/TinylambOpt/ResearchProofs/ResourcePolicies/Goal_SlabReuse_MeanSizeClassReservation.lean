import TinylambOpt.ResearchGoals.ResourcePolicies.SlabReuse

/-!
Refutes `MeanSizeClassReservation`: rounding the mean request size does not
bound the reserved bytes of a skewed trace. For classes `[1, 2]` and requests
`[0, 2]` the charge is 3 bytes, while the rounded ceiling mean is the class 1,
so the claimed bound of `2 * 1` bytes fails.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_SlabReuse_MeanSizeClassReservation

open TinylambOpt.ResearchGoals.ResourcePolicies.SlabReuse

theorem refute : ¬ MeanSizeClassReservation.Goal := by
  intro h
  have hfin := h [1, 2] [0, 2] 3 1 (by decide) (by native_decide) (by native_decide)
  have key : ¬ (3 ≤ ([0, 2] : List Nat).length * 1) := by native_decide
  exact key hfin

end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_SlabReuse_MeanSizeClassReservation
