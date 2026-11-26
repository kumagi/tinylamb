import TinylambOpt.ResearchGoals.JitKernels.TierInvestment

/-! A fused batch with identical guard outcomes costs no more than two separate dispatches, because the shared guard fee is paid once. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.TierInvestment.Goal_TierInvestment_SameShapeBatchFusion

open TinylambOpt.ResearchGoals.JitKernels.TierInvestment
open TinylambOpt.ResearchGoals.JitKernels.TierInvestment.SameShapeBatchFusion

theorem sound : SameShapeBatchFusion.Goal := by
  intro t a b hp
  cases ha : a.passes <;> cases hb : b.passes <;> simp [compiled, ha, hb] at hp ⊢ <;> rw [Nat.mul_add] <;> omega

end TinylambOpt.ResearchProofs.JitKernels.TierInvestment.Goal_TierInvestment_SameShapeBatchFusion
