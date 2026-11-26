import TinylambOpt.ResearchGoals.JitKernels.FloatingReduction

/-! Floating-point addition is not associative even at finite values:
`2^53 + 1` rounds back to `2^53`, while `1 + (-1)` is exact. -/
set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_FloatingReduction_FinitePairwiseAccumulators

open TinylambOpt.ResearchGoals.JitKernels.FloatingReduction

theorem refute : ¬ FinitePairwiseAccumulators.Goal := by
  intro h
  have h' := h (9007199254740992 : Float) 1 (-1)
    (by native_decide) (by native_decide) (by native_decide)
  have hbad : ¬ same (((9007199254740992 : Float) + 1) + (-1))
      ((9007199254740992 : Float) + (1 + (-1))) := by
    unfold same bits
    native_decide
  exact hbad h'

end TinylambOpt.ResearchProofs.JitKernels.Goal_FloatingReduction_FinitePairwiseAccumulators
