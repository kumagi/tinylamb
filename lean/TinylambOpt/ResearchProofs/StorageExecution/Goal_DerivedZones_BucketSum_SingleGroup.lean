import TinylambOpt.ResearchGoals.StorageExecution.DerivedZones

/-!
Scan pruning can exploit derived bounds only when every matching row remains covered by
the block summary.

Proved

- `BucketSum.SingleGroup` — `∀ t a b, (a.hi+b.hi)/(t+1)=(a.lo+b.lo)/(t+1) →
  (kernel.bounds t a b).lo=(kernel.bounds t a b).hi`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_BucketSum_SingleGroup

open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones
open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.BucketSum

theorem sound_SingleGroup : BucketSum.SingleGroup.Goal := by
  intro t a b h
  simpa [kernel] using h.symm

end TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_BucketSum_SingleGroup
