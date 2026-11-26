import TinylambOpt.ResearchGoals.JitKernels.TierInvestment

/-! A passing batch fused with a failing one can lose the passing batch's specialization benefit. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.TierInvestment.Goal_TierInvestment_MixedShapeBatchFusion

open TinylambOpt.ResearchGoals.JitKernels.TierInvestment
open TinylambOpt.ResearchGoals.JitKernels.TierInvestment.MixedShapeBatchFusion

theorem refute : ¬ MixedShapeBatchFusion.Goal := by
  intro h
  let t : Tariff := { compile := 0, guardFee := 0, slow := 1, fast := 0, replay := 0 }
  let a : Batch := ⟨0, false⟩
  let b : Batch := ⟨1, true⟩
  have h' := h t a b (by simp [t])
  simp [compiled, t, a, b] at h'

end TinylambOpt.ResearchProofs.JitKernels.TierInvestment.Goal_TierInvestment_MixedShapeBatchFusion
