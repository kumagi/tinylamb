import TinylambOpt.ResearchGoals.JitKernels.TierInvestment

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_TierInvestment_FailuresOnlyBoundRecompileCost

open TinylambOpt.ResearchGoals.JitKernels.TierInvestment

/-! A passing batch pays a guard fee on every execution, which is unrelated to
the number of deoptimizations. -/
theorem refute : ¬ TinylambOpt.ResearchGoals.JitKernels.TierInvestment.FailuresOnlyBoundRecompileCost.Goal := by
  rw [FailuresOnlyBoundRecompileCost.Goal]
  intro h
  let t : Tariff := ⟨0,1,1,1,0⟩
  let xs : List Batch := [⟨1,true⟩]
  have hh := h t xs (by decide)
  have hbad : ¬ (bill t xs 0 ≤ bill t xs xs.length + t.compile +
      (xs.countP (fun b => !b.passes))*(t.replay+t.guardFee)) := by
    native_decide
  exact hbad hh

end TinylambOpt.ResearchProofs.JitKernels.Goal_TierInvestment_FailuresOnlyBoundRecompileCost
