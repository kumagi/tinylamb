import TinylambOpt.ResearchGoals.StorageExecution.GatherPlans

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_GatherPlans_TwoTier_HotTail

open TinylambOpt.ResearchGoals.StorageExecution.GatherPlans
open TinylambOpt.ResearchGoals.StorageExecution.GatherPlans.TwoTier

theorem sound_HotTail : TwoTier.HotTail.Goal := by
  intro p c a b hpa hab
  unfold TwoTier.price span
  have h1 : min (b + 1) p = p := Nat.min_eq_right (by omega)
  have h2 : min a p = p := Nat.min_eq_right hpa
  rw [h1, h2]
  simp

end TinylambOpt.ResearchProofs.StorageExecution.Goal_GatherPlans_TwoTier_HotTail
