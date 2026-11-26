import TinylambOpt.ResearchGoals.JitKernels.FloatingReduction

/-! IEEE multiplication by `+0.0` preserves the sign of a negative operand,
so masking by multiplication produces `-0.0` rather than canonical `+0.0`. -/
set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_FloatingReduction_MultiplyMaskInsteadOfSelect

open TinylambOpt.ResearchGoals.JitKernels.FloatingReduction

theorem refute : ¬ MultiplyMaskInsteadOfSelect.Goal := by
  intro h
  have h' := h (-1 : Float) (by native_decide)
  have hbad : ¬ same ((-1 : Float) * 0) 0 := by
    unfold same bits
    native_decide
  exact hbad h'

end TinylambOpt.ResearchProofs.JitKernels.Goal_FloatingReduction_MultiplyMaskInsteadOfSelect
