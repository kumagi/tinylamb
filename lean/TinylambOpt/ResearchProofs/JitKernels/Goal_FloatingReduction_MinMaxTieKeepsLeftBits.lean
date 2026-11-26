import TinylambOpt.ResearchGoals.JitKernels.FloatingReduction

/-! The two tie-breaking expressions differ on signed zeros: they compare
    equal numerically but select operands with different bit patterns. -/

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_FloatingReduction_MinMaxTieKeepsLeftBits

open TinylambOpt.ResearchGoals.JitKernels.FloatingReduction

theorem refute : ¬ TinylambOpt.ResearchGoals.JitKernels.FloatingReduction.MinMaxTieKeepsLeftBits.Goal := by
  intro h
  let a : Float := -0.0
  let b : Float := 0.0
  have hh := h a b (by native_decide) (by native_decide)
  unfold same bits at hh
  have hne : ¬ ((if a ≤ b then a else b).toBits = (if a < b then a else b).toBits) := by native_decide
  exact hne hh

end TinylambOpt.ResearchProofs.JitKernels.Goal_FloatingReduction_MinMaxTieKeepsLeftBits
