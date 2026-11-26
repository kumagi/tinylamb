import TinylambOpt.ResearchGoals.StorageExecution.DerivedZones

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_BucketSum_SingleGroup

open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones
open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.BucketSum

theorem sound_SingleGroup : BucketSum.SingleGroup.Goal := by
  intro t a b h
  simpa [kernel] using h.symm

end TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_BucketSum_SingleGroup
