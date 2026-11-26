import TinylambOpt.ResearchGoals.JitKernels.TierInvestment

/-! A majority of passing rows does not pay for compilation when guard fees are large. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.TierInvestment.Goal_TierInvestment_CompileIfHotRowsDominate

open TinylambOpt.ResearchGoals.JitKernels.TierInvestment
open TinylambOpt.ResearchGoals.JitKernels.TierInvestment.CompileIfHotRowsDominate

theorem refute : ¬ CompileIfHotRowsDominate.Goal := by
  intro h
  let t : Tariff := { compile := 0, guardFee := 0, slow := 1, fast := 0, replay := 1 }
  let xs : List Batch := [⟨0, false⟩, ⟨0, false⟩, ⟨1, true⟩]
  have h' := h t xs (by simp [t]) (by simp [t, xs]) (by simp [xs])
  simp [bill, scalar, compiled, t, xs] at h'

end TinylambOpt.ResearchProofs.JitKernels.TierInvestment.Goal_TierInvestment_CompileIfHotRowsDominate
