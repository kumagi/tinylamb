import TinylambOpt.ResearchGoals.JitKernels.FloatingReduction
set_option linter.unusedSimpArgs false

/-! Multiplication by a rounded reciprocal is not bitwise equal to division:
`3 / 5` and `3 * (1 / 5)` differ in the last bit. -/
set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_FloatingReduction_GroupedMeanReciprocal

open TinylambOpt.ResearchGoals.JitKernels.FloatingReduction

theorem refute : ¬ GroupedMeanReciprocal.Goal := by
  intro h
  have h' := h (3 : Float) (5 : UInt64) (by native_decide) (by decide)
  have hbad : ¬ same ((3 : Float) / (5 : UInt64).toFloat)
      ((3 : Float) * (1 / (5 : UInt64).toFloat)) := by
    unfold same bits
    native_decide
  exact hbad h'

end TinylambOpt.ResearchProofs.JitKernels.Goal_FloatingReduction_GroupedMeanReciprocal
