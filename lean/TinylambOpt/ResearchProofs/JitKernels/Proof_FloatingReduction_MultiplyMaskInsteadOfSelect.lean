import TinylambOpt.ResearchGoals.JitKernels.FloatingReduction
/-! Certificate for `TinylambOpt.ResearchGoals.JitKernels.FloatingReduction.MultiplyMaskInsteadOfSelect.Goal`. -/

/-! IEEE multiplication by `+0.0` preserves the sign of a negative operand,
so masking by multiplication produces `-0.0` rather than canonical `+0.0`. -/
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace Nov26.Proof_FloatingReduction_MultiplyMaskInsteadOfSelect

open TinylambOpt.ResearchGoals.JitKernels.FloatingReduction
open TinylambOpt.ResearchGoals.JitKernels.FloatingReduction.MultiplyMaskInsteadOfSelect

theorem refute : ¬ Goal := by
  intro h
  have h' := h (-1 : Float) (by native_decide)
  have hbad : ¬ same ((-1 : Float) * 0) 0 := by
    unfold same bits
    native_decide
  exact hbad h'

end Nov26.Proof_FloatingReduction_MultiplyMaskInsteadOfSelect
