import TinylambOpt.ResearchGoals.JitKernels.InductionLowering

/-! A signed derivative can need one more bit than either endpoint: the
    output-only width check is refuted by the jump from the minimum to a
    large positive value. -/

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_InductionLowering_NarrowDerivativeFromOutputs

open TinylambOpt.ResearchGoals.JitKernels.InductionLowering

theorem refute : ¬ TinylambOpt.ResearchGoals.JitKernels.InductionLowering.NarrowDerivativeFromOutputs.Goal := by
  intro h
  have hlo : signedFits 2 (-4 : Int) := by
    unfold signedFits
    native_decide
  have hhi : signedFits 2 (3 : Int) := by
    unfold signedFits
    native_decide
  have hh := h 2 (-4 : Int) (3 : Int) hlo hhi
  have hbad : ¬ (-(2^2:Int) ≤ (7:Int) ∧ (7:Int) < (2^2:Int)) := by native_decide
  exact hbad (by simpa [signedFits] using hh)

end TinylambOpt.ResearchProofs.JitKernels.Goal_InductionLowering_NarrowDerivativeFromOutputs
