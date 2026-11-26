import TinylambOpt.ResearchGoals.JitKernels.ExcursionGuards

set_option maxHeartbeats 0
set_option maxRecDepth 10000

/-! Refutation: a nonnegative net change and in-range endpoints do not certify
intermediate prefix sums. A negative term followed by a larger positive term
stays within the range at both ends but dips below the lower bound in between. -/

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_ExcursionGuards_NetChangeVectorGuard

open TinylambOpt.ResearchGoals.JitKernels.ExcursionGuards

theorem refute : ¬ TinylambOpt.ResearchGoals.JitKernels.ExcursionGuards.NetChangeVectorGuard.Goal := by
  intro h
  have hbad := h (-5) 10 0 [-10, 10] (by native_decide) (by omega) (by native_decide)
  have hc : safe (-5) 10 0 [-10, 10] ≠ true := by native_decide
  exact hc hbad

end TinylambOpt.ResearchProofs.JitKernels.Goal_ExcursionGuards_NetChangeVectorGuard