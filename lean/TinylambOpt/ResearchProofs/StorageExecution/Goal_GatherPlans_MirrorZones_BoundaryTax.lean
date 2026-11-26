import TinylambOpt.ResearchGoals.StorageExecution.GatherPlans

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_GatherPlans_MirrorZones_BoundaryTax

open TinylambOpt.ResearchGoals.StorageExecution.GatherPlans
open TinylambOpt.ResearchGoals.StorageExecution.GatherPlans.MirrorZones

theorem sound_BoundaryTax : MirrorZones.BoundaryTax.Goal := by
  intro z h a b hab
  unfold price span
  simp
  omega

end TinylambOpt.ResearchProofs.StorageExecution.Goal_GatherPlans_MirrorZones_BoundaryTax
