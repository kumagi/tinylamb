import TinylambOpt.ResearchGoals.JitKernels.FloatingReduction
/-! Certificate for `TinylambOpt.ResearchGoals.JitKernels.FloatingReduction.FinitePairwiseAccumulators.Goal`. -/

/-! Floating-point addition is not associative even at finite values:
`2^53 + 1` rounds back to `2^53`, while `1 + (-1)` is exact. -/
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace Nov26.Proof_FloatingReduction_FinitePairwiseAccumulators

open TinylambOpt.ResearchGoals.JitKernels.FloatingReduction
open TinylambOpt.ResearchGoals.JitKernels.FloatingReduction.FinitePairwiseAccumulators

theorem refute : ¬ Goal := by
  intro h
  have h' := h (9007199254740992 : Float) 1 (-1)
    (by native_decide) (by native_decide) (by native_decide)
  have hbad : ¬ same (((9007199254740992 : Float) + 1) + (-1))
      ((9007199254740992 : Float) + (1 + (-1))) := by
    unfold same bits
    native_decide
  exact hbad h'

end Nov26.Proof_FloatingReduction_FinitePairwiseAccumulators
