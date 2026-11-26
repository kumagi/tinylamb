import TinylambOpt.ResearchGoals.JitKernels.TierInvestment

-- Refutation: losing specialization on a mixed pair can cost more than the saved dispatch fee.

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_TierInvestment_MixedShapeBatchFusion

theorem refute : ¬ TinylambOpt.ResearchGoals.JitKernels.TierInvestment.MixedShapeBatchFusion.Goal := by
  intro h
  have c := h ⟨0, 0, 2, 1, 0⟩ ⟨10, true⟩ ⟨0, false⟩ (by native_decide)
  exact (by native_decide : ¬ (
    TinylambOpt.ResearchGoals.JitKernels.TierInvestment.compiled ⟨0, 0, 2, 1, 0⟩
        ⟨10 + 0, true && false⟩ ≤
      TinylambOpt.ResearchGoals.JitKernels.TierInvestment.compiled ⟨0, 0, 2, 1, 0⟩ ⟨10, true⟩ +
      TinylambOpt.ResearchGoals.JitKernels.TierInvestment.compiled ⟨0, 0, 2, 1, 0⟩ ⟨0, false⟩)) c

end TinylambOpt.ResearchProofs.JitKernels.Goal_TierInvestment_MixedShapeBatchFusion
