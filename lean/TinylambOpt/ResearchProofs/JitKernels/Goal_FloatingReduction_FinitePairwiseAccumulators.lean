import TinylambOpt.ResearchGoals.JitKernels.FloatingReduction

/-! Finite inputs do not make binary64 addition associative: cancellation in
    one grouping can be lost in the other. -/

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_FloatingReduction_FinitePairwiseAccumulators

open TinylambOpt.ResearchGoals.JitKernels.FloatingReduction

theorem refute : ¬ TinylambOpt.ResearchGoals.JitKernels.FloatingReduction.FinitePairwiseAccumulators.Goal := by
  intro h
  let a : Float := 1e308
  let b : Float := -1e308
  let c : Float := 1
  have hh := h a b c (by native_decide) (by native_decide) (by native_decide)
  unfold same bits at hh
  have hne : ¬ (((a+b)+c).toBits = (a+(b+c)).toBits) := by native_decide
  exact hne hh

end TinylambOpt.ResearchProofs.JitKernels.Goal_FloatingReduction_FinitePairwiseAccumulators
