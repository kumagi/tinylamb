import TinylambOpt.ResearchGoals.JitKernels.TierInvestment

/-! The same row total and pass count can charge different costs when rows sit in different guard outcomes. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.TierInvestment.Goal_TierInvestment_GuardRateOnlyAdmission

open TinylambOpt.ResearchGoals.JitKernels.TierInvestment
open TinylambOpt.ResearchGoals.JitKernels.TierInvestment.GuardRateOnlyAdmission

theorem refute : ¬ GuardRateOnlyAdmission.Goal := by
  intro h
  let t : Tariff := { compile := 0, guardFee := 0, slow := 0, fast := 1, replay := 0 }
  let a : List Batch := [⟨1, true⟩, ⟨0, false⟩]
  let b : List Batch := [⟨0, true⟩, ⟨1, false⟩]
  have h' := h t a b (by simp [a, b]) (by simp [a, b]) (by simp [a, b])
  simp [bill, compiled, t, a, b] at h'

end TinylambOpt.ResearchProofs.JitKernels.TierInvestment.Goal_TierInvestment_GuardRateOnlyAdmission
