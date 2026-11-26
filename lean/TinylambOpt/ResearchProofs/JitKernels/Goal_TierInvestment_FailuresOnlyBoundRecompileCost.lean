import TinylambOpt.ResearchGoals.JitKernels.TierInvestment

/-! Guard fees on passing batches with little work are not covered by a bound counting only failures. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.TierInvestment.Goal_TierInvestment_FailuresOnlyBoundRecompileCost

open TinylambOpt.ResearchGoals.JitKernels.TierInvestment
open TinylambOpt.ResearchGoals.JitKernels.TierInvestment.FailuresOnlyBoundRecompileCost

theorem refute : ¬ FailuresOnlyBoundRecompileCost.Goal := by
  intro h
  let t : Tariff := { compile := 0, guardFee := 1, slow := 0, fast := 0, replay := 0 }
  let xs : List Batch := [⟨0, true⟩]
  have h' := h t xs (by simp [t])
  simp [bill, scalar, compiled, t, xs] at h'

end TinylambOpt.ResearchProofs.JitKernels.TierInvestment.Goal_TierInvestment_FailuresOnlyBoundRecompileCost
