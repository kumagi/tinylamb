import TinylambOpt.ResearchGoals.JitKernels.FloatingReduction

/-!
Multiplying by zero preserves the sign of a negative zero operand, while a bit
select of the canonical inactive-lane value produces positive zero. The SQL
observable bits therefore differ.
-/
namespace TinylambOpt.ResearchProofs.JitKernels.Goal_FloatingReduction_MultiplyMaskInsteadOfSelect

open TinylambOpt.ResearchGoals.JitKernels.FloatingReduction

theorem refute : ¬ TinylambOpt.ResearchGoals.JitKernels.FloatingReduction.MultiplyMaskInsteadOfSelect.Goal := by
  intro h
  have h' := h (-0.0 : Float) (by native_decide)
  have hne : ¬ same ((-0.0 : Float) * 0) 0 := by
    unfold same bits
    native_decide
  exact hne h'

end TinylambOpt.ResearchProofs.JitKernels.Goal_FloatingReduction_MultiplyMaskInsteadOfSelect
