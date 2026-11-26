import TinylambOpt.ResearchGoals.StorageExecution.GatherPlans

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_GatherPlans_StripedWorkers_ActivationCeiling

open TinylambOpt.ResearchGoals.StorageExecution.GatherPlans
open TinylambOpt.ResearchGoals.StorageExecution.GatherPlans.StripedWorkers

theorem sound_ActivationCeiling : StripedWorkers.ActivationCeiling.Goal := by
  intro stripe workers a b _hab
  unfold StripedWorkers.price
  have hmin : min (workers + 1) (pages (stripe + 1) a b) ≤ workers + 1 :=
    Nat.min_le_left _ _
  omega

end TinylambOpt.ResearchProofs.StorageExecution.Goal_GatherPlans_StripedWorkers_ActivationCeiling
