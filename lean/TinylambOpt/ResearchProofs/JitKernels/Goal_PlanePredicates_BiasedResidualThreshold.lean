import TinylambOpt.ResearchGoals.JitKernels.PlanePredicates

/-!
Biasing an inclusive threshold by subtracting the page base is not equivalent
to the absolute comparison when the threshold lies below the base: truncated
subtraction maps the threshold to zero, so a zero residual passes the biased
test while no absolute address can satisfy the original one.

Refuted by counterexample: `base = 1`, `x = 0`, `c = 0`; `0 ≤ 1 - 0 = 0` holds
but `1 + 0 ≤ 0` does not.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_PlanePredicates_BiasedResidualThreshold

open TinylambOpt.ResearchGoals.JitKernels.PlanePredicates

theorem refute : ¬ BiasedResidualThreshold.Goal := by
  intro h
  exact (by native_decide : ¬ (0 ≤ 0 - 1 ↔ 1 + 0 ≤ 0)) (h 1 0 0)

end TinylambOpt.ResearchProofs.JitKernels.Goal_PlanePredicates_BiasedResidualThreshold
