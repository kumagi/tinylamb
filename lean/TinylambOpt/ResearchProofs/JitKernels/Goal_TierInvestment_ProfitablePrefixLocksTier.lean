import TinylambOpt.ResearchGoals.JitKernels.TierInvestment

/-! A profitable prefix does not lock the tier: later guard misses can make the full trace cheaper interpreted. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.TierInvestment.Goal_TierInvestment_ProfitablePrefixLocksTier

open TinylambOpt.ResearchGoals.JitKernels.TierInvestment
open TinylambOpt.ResearchGoals.JitKernels.TierInvestment.ProfitablePrefixLocksTier

theorem refute : ¬ ProfitablePrefixLocksTier.Goal := by
  intro h
  let t : Tariff := { compile := 0, guardFee := 0, slow := 1, fast := 0, replay := 1 }
  let a : List Batch := [⟨1, true⟩]
  let b : List Batch := [⟨0, false⟩, ⟨0, false⟩]
  have h' := h t a b
  simp [bill, scalar, compiled, t, a, b] at h'

end TinylambOpt.ResearchProofs.JitKernels.TierInvestment.Goal_TierInvestment_ProfitablePrefixLocksTier
