import TinylambOpt.ResearchGoals.JitKernels.ExcursionGuards

set_option maxHeartbeats 0
set_option maxRecDepth 10000

/-! Refutation: endpoint representability is monotone only for nonnegative
terms. A negative term after an overflowing prefix brings the later endpoint
back in range, so bisection on endpoints misses the first overflow. -/

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_ExcursionGuards_EndpointBisectionOfOverflow

open TinylambOpt.ResearchGoals.JitKernels.ExcursionGuards

theorem refute : ¬ EndpointBisectionOfOverflow.Goal := by
  intro h
  have hbad := h 0 0 [10, -10] 1 2 (by omega) (by decide) (by native_decide)
  have hc : ¬ 0 < 0 + ([10, -10].take 2).sum := by native_decide
  exact hc hbad

end TinylambOpt.ResearchProofs.JitKernels.Goal_ExcursionGuards_EndpointBisectionOfOverflow
