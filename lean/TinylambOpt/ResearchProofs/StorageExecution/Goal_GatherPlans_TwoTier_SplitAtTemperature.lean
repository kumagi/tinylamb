import TinylambOpt.ResearchGoals.StorageExecution.GatherPlans

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_GatherPlans_TwoTier_SplitAtTemperature

open TinylambOpt.ResearchGoals.StorageExecution.GatherPlans
open TinylambOpt.ResearchGoals.StorageExecution.GatherPlans.TwoTier

/-- Splitting an extent at the temperature pivot charges one extra startup but
splits the cold-prefix surcharge exactly at the pivot. -/
theorem sound_SplitAtTemperature : TwoTier.SplitAtTemperature.Goal := by
  intro pivot cold a b hab hpb
  have hpos : 0 < pivot := by omega
  have hpm1 : pivot - 1 + 1 = pivot := by omega
  have hmin_a : min a pivot = a := Nat.min_eq_left (by omega)
  have hmin_b : min (b + 1) pivot = pivot := Nat.min_eq_right (by omega)
  simp [TwoTier.price, span, hpm1, hmin_a, hmin_b]
  omega

end TinylambOpt.ResearchProofs.StorageExecution.Goal_GatherPlans_TwoTier_SplitAtTemperature
