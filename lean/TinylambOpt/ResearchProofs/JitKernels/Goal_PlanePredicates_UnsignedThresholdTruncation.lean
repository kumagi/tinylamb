import TinylambOpt.ResearchGoals.JitKernels.PlanePredicates

/-!
Ballot generation from bit planes must truncate the threshold to the frame
width. This file refutes the claim that the comparison is correct when only the
residual is known to fit the frame: a threshold above `2^w` has the same low
`w` bits as a smaller threshold, so the ballot cannot distinguish them.

Refuted by counterexample: `x = 0`, `c = 2`, `w = 1`; the sole plane agrees but
`0 < 2`.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_PlanePredicates_UnsignedThresholdTruncation

open TinylambOpt.ResearchGoals.JitKernels.PlanePredicates

theorem refute : ¬ TinylambOpt.ResearchGoals.JitKernels.PlanePredicates.UnsignedThresholdTruncation.Goal := by
  intro h
  have heq := h 0 2 1 (by native_decide)
  exact (by native_decide : foldBits 0 2 1 ≠ decide (0 < 2)) heq

end TinylambOpt.ResearchProofs.JitKernels.Goal_PlanePredicates_UnsignedThresholdTruncation